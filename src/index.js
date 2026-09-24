export class QuotaExceededError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'QuotaExceededError';
    this.code = code;
  }
}

// Internal state of every gate, shared with child gates through a WeakMap so
// the public gate object keeps its exact shape.
const gateState = new WeakMap();

// Monotonic counter that never repeats or moves backwards, independent of the
// (mocked, possibly frozen) wall clock; gives queued entries a global arrival
// order so reparented queues can merge fairly.
let enqueueSeq = 0;

function assertInteger(value, name) {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`);
  }
}

function assertSignal(signal) {
  if (
    signal !== undefined &&
    signal !== null &&
    (typeof signal !== 'object' ||
      typeof signal.addEventListener !== 'function' ||
      typeof signal.removeEventListener !== 'function')
  ) {
    throw new TypeError('signal must be an AbortSignal');
  }
}

export function createGate({ limit, windowMs, maxWaitMs = 0, parent } = {}) {
  assertInteger(limit, 'limit');
  assertInteger(windowMs, 'windowMs');
  assertInteger(maxWaitMs, 'maxWaitMs');
  if (limit <= 0) throw new RangeError('limit must be a positive integer');
  if (windowMs <= 0) throw new RangeError('windowMs must be a positive integer');
  if (maxWaitMs < 0) throw new RangeError('maxWaitMs must not be negative');

  let parentState = null;
  if (parent !== undefined && parent !== null) {
    parentState = gateState.get(parent);
    if (parentState === undefined) {
      throw new TypeError('parent must be a gate created by createGate');
    }
    if (limit > parentState.limit) {
      throw new RangeError('limit must not exceed parent limit');
    }
  }

  const state = {
    limit,
    windowMs,
    maxWaitMs,
    parent: parentState,
    children: new Set(),
    isRoot: parentState === null,
    // Own-window accounting.
    ownGen: 0, // bumped on every own-window flip, invalidates old handles
    usedOwn: 0, // own units granted in this gate's current own window
    inFlight: 0, // units occupied through this gate and not yet cleared
    borrowedInFlight: 0, // borrowed units still occupied; reclaimed at lender flip
    poolDraw: 0, // units charged into the current window of the root pool
    // Reservations held on this gate's window by uncommitted cross-gate
    // requests: ownReserved backs the gate's own members, lentReserved backs
    // descendants borrowing through the direct-parent chain.
    ownReserved: 0,
    lentReserved: 0,
    // Leg bookkeeping: every unit of a combo is one leg, either an "own" leg
    // of its member or a "borrow" leg lent by an ancestor. Sets make leg
    // insertion, commit, rollback and reclaim constant time per leg.
    pendingLegs: new Set(), // uncommitted legs backed by this gate as lender
    activeLegs: new Set(), // committed legs backed by this gate as lender
    pendingBorrowerLegs: new Set(), // uncommitted legs this gate borrows
    activeBorrowerLegs: new Set(), // committed legs this gate borrows
    // Observability.
    waiting: 0, // units currently queued through this gate
    granted: 0,
    refused: 0,
    expired: 0,
    cancelled: 0,
    rolledBack: 0, // failed gather attempts for cross-gate combos
    openHandles: new Set(), // { gen, drop } records of live occupations
    root: null,
    timer: null,
  };

  let root;
  if (parentState === null) {
    root = state;
    // The quota pool shared by the whole hierarchy lives on the root gate.
    Object.assign(state, {
      poolLimit: limit,
      poolGen: 0, // bumped on every root-window flip
      poolUsed: 0, // units granted through every gate in the current pool window
      poolReserved: 0, // units reserved by uncommitted combos in this window
      queue: [], // queued requests from every gate, in arrival order
      head: 0, // index of the first live entry; entries are never shifted
      liveQueued: 0, // entries not yet settled
      minDeadline: Infinity, // earliest live deadline, skips idle sweeps
      lastNow: 0, // monotonic clock: never moves backwards
      members: new Set(),
    });
    state.members.add(state);
  } else {
    root = parentState.root;
    root.members.add(state);
    parentState.children.add(state);
  }
  state.root = root;

  // Operational helpers read state.root dynamically rather than capturing the
  // local `root`: reparent() can move this gate (and its descendants) into
  // another family's pool.
  function now() {
    const r = state.root;
    const t = Date.now();
    if (t > r.lastNow) r.lastNow = t;
    return r.lastNow;
  }

  // --- Occupation handles ---------------------------------------------------

  // Handles returned to callers expose a single detached `release`; the
  // internal record is what window flips use to prune stale occupations.
  function addHandleRecord(gate, gen, drop) {
    const record = { gate, gen, drop, released: false };
    gate.openHandles.add(record);
    return record;
  }

  // One release closure drops the occupations it was built with. A single
  // acquire has one occ; a combo handle has one occ per member.
  function makeOccupationHandle(occs) {
    const records = occs.map((occ) =>
      addHandleRecord(occ.member, occ.gen, (record) => {
        occ.member.openHandles.delete(record);
        if (occ.gen !== occ.member.ownGen) return; // member window closed; cleared
        // Only legs whose lender window is still open remain in this member's
        // inFlight; lender-side reclaims already subtracted the other ones.
        let dropUnits = 0;
        let dropBorrow = 0;
        for (const leg of occ.legs) {
          if (leg.kind === 'own') {
            dropUnits += leg.amount;
          } else if (leg.lLive) {
            dropUnits += leg.amount;
            dropBorrow += leg.amount;
          }
          leg.released = true;
        }
        occ.member.inFlight -= dropUnits;
        occ.member.borrowedInFlight -= dropBorrow;
      }),
    );
    let handleActive = true;
    return {
      release() {
        if (!handleActive) return;
        handleActive = false;
        for (const record of records) {
          if (!record.released) record.drop(record);
          record.released = true;
        }
      },
    };
  }

  // A single request fits when the shared pool has room. A child covers
  // whatever its own remaining quota cannot by borrowing from the pool, so its
  // own limit never blocks a grant; only the pool does.
  function fits(units) {
    const r = state.root;
    return units <= r.poolLimit - r.poolUsed;
  }

  // Grant a single request the baseline way: the whole draw is charged to the
  // pool first, then split into an own leg and a root-lent borrow leg for
  // window accounting. The caller already checked `fits`.
  function applyGrant(target, units) {
    const targetRoot = target.root;
    targetRoot.poolUsed += units;
    target.poolDraw += units;
    const occ = { member: target, units, legs: [] };
    let ownPart;
    if (target === targetRoot) {
      ownPart = units;
    } else {
      ownPart = Math.min(units, Math.max(0, target.limit - target.usedOwn));
    }
    const borrowPart = units - ownPart;
    if (ownPart > 0) {
      const leg = makeLeg(target, target, ownPart, 'own', occ);
      leg.active = true;
      target.activeLegs.add(leg);
      target.activeBorrowerLegs.add(leg);
      occ.legs.push(leg);
    }
    if (borrowPart > 0) {
      const leg = makeLeg(target, targetRoot, borrowPart, 'borrow', occ);
      leg.active = true;
      targetRoot.activeLegs.add(leg);
      target.activeBorrowerLegs.add(leg);
      occ.legs.push(leg);
    }
    target.usedOwn += ownPart;
    target.inFlight += units;
    target.borrowedInFlight += borrowPart;
    occ.ownPart = ownPart;
    occ.borrowPart = borrowPart;
    occ.gen = target.ownGen;
    return { ownPart, borrowPart, occ };
  }

  function makeSingleHandle(occ) {
    return makeOccupationHandle([occ]);
  }

  // --- Cross-gate combo legs ------------------------------------------------

  // One leg covers `amount` units of an occupation/reservation through
  // `borrower`, backed by `lender` (the member itself for an own leg, or an
  // ancestor for a borrow leg). Each side of a leg can be reclaimed
  // independently when its window flips; the leg object keeps which sides are
  // still live so neither side subtracts the same units twice.
  function makeLeg(borrower, lender, amount, kind, occ) {
    const r = borrower.root;
    return {
      borrower,
      lender,
      amount,
      kind, // 'own' when borrower === lender, otherwise 'borrow'
      bGen: borrower.ownGen,
      lGen: lender.ownGen,
      pGen: r.poolGen,
      active: false, // committed once the whole combo commits
      released: false, // borrower-side occupation released by its holder
      bLive: true,
      lLive: true,
      occ,
    };
  }

  // Add a freshly reserved leg to every counter/set it participates in.
  function reserveLeg(leg) {
    const r = leg.borrower.root;
    if (leg.kind === 'own') leg.lender.ownReserved += leg.amount;
    else leg.lender.lentReserved += leg.amount;
    r.poolReserved += leg.amount;
    leg.lender.pendingLegs.add(leg);
    if (leg.kind === 'borrow') leg.borrower.pendingBorrowerLegs.add(leg);
    leg.occ.legs.push(leg);
  }

  // Tear one uncommitted leg out again. Used by failed gathers, by non-grant
  // settlements and by window flips reclaiming uncommitted reservations.
  function releasePendingLeg(leg) {
    if (leg.active || !leg.lLive) return; // idempotent across the two sides
    const r = leg.borrower.root;
    if (leg.kind === 'own') leg.lender.ownReserved -= leg.amount;
    else leg.lender.lentReserved -= leg.amount;
    if (leg.pGen === r.poolGen) r.poolReserved -= leg.amount;
    leg.lender.pendingLegs.delete(leg);
    leg.borrower.pendingBorrowerLegs.delete(leg);
    const legs = leg.occ.legs;
    legs.splice(legs.indexOf(leg), 1);
    leg.bLive = false;
    leg.lLive = false;
  }

  function releaseEntryReservation(entry) {
    for (const occ of entry.occs) {
      while (occ.legs.length > 0) releasePendingLeg(occ.legs[0]);
    }
  }

  // Reserve the full combo on member-own quotas first, then up the direct
  // parent chain of each member, one ancestor at a time. Every leg draws the
  // current root-pool window as well, so the pool window caps the total.
  // Returns false (without leaving any reservation behind) as soon as one more
  // unit is needed than every window in the chain can jointly cover.
  function gatherCombo(entry) {
    const r = entry.owner.root;
    // Every leg placed in the current pool window draws pool capacity, so
    // each take is bounded by the backing window and by the pool room left.
    const poolFree = () => r.poolLimit - r.poolUsed - r.poolReserved;
    for (const occ of entry.occs) {
      const member = occ.member;
      let need = occ.units;
      // Own quota.
      const windowCap =
        member === r
          ? poolFree()
          : member.limit - member.usedOwn - member.ownReserved - member.lentReserved;
      const ownTake = Math.min(need, Math.max(0, windowCap), poolFree());
      if (ownTake > 0) {
        reserveLeg(makeLeg(member, member, ownTake, 'own', occ));
        need -= ownTake;
      }
      // Direct ancestors, nearest first; the root lends out of the pool.
      let lender = member.parent;
      while (need > 0 && lender !== null) {
        const windowCap2 =
          lender === r
            ? poolFree()
            : lender.limit - lender.usedOwn - lender.ownReserved - lender.lentReserved;
        const take = Math.min(need, Math.max(0, windowCap2), poolFree());
        if (take > 0) {
          reserveLeg(makeLeg(member, lender, take, 'borrow', occ));
          need -= take;
        }
        lender = lender.parent;
      }
      if (need > 0) {
        // All-or-nothing: undo every leg this attempt placed, including the
        // complete reservations gathered for earlier members.
        releaseEntryReservation(entry);
        return false;
      }
    }
    return true;
  }

  // Commit a complete reservation: pending counters become admitted ones,
  // every leg turns active and the occs become occupation records.
  function commitCombo(entry) {
    const r = entry.owner.root;
    entry.owner.granted += 1;
    for (const occ of entry.occs) {
      const member = occ.member;
      member.inFlight += occ.units;
      let ownPart = 0;
      let borrowPart = 0;
      for (const leg of occ.legs) {
        if (leg.kind === 'own') leg.lender.ownReserved -= leg.amount;
        else leg.lender.lentReserved -= leg.amount;
        if (leg.pGen === r.poolGen) {
          r.poolReserved -= leg.amount;
          r.poolUsed += leg.amount;
          member.poolDraw += leg.amount;
        }
        leg.lender.pendingLegs.delete(leg);
        member.pendingBorrowerLegs.delete(leg);
        leg.active = true;
        leg.lender.activeLegs.add(leg);
        member.activeBorrowerLegs.add(leg);
        if (leg.kind === 'own') {
          member.usedOwn += leg.amount;
          ownPart += leg.amount;
        } else {
          // A root-lent leg is pool capacity, not the root's own quota.
          if (leg.lender !== r) leg.lender.usedOwn += leg.amount;
          member.borrowedInFlight += leg.amount;
          borrowPart += leg.amount;
        }
      }
      occ.ownPart = ownPart;
      occ.borrowPart = borrowPart;
      occ.gen = member.ownGen;
    }
    return entry.occs;
  }

  // (combos use makeOccupationHandle directly with their committed occs)
  function detachSignal(entry) {
    if (entry.signal !== null) {
      entry.signal.removeEventListener('abort', entry.onAbort);
      entry.signal = null;
    }
  }

  function waitingDelta(entry, sign) {
    entry.owner.waiting += sign * entry.units;
    if (entry.isCombo) {
      // The owner already carries the total; a member that is the owner
      // itself must not be counted a second time.
      for (const occ of entry.occs) {
        if (occ.member !== entry.owner) occ.member.waiting += sign * occ.units;
      }
    }
  }

  // One queue entry leaves the queue exactly once, and exactly one of
  // granted / refused / expired / cancelled moves as a result. Every non-grant
  // outcome also drops whatever reservation the entry was holding.
  function settleEntry(entry, outcome) {
    if (entry.removed) return;
    const ownerRoot = entry.owner.root;
    entry.removed = true;
    ownerRoot.liveQueued -= 1;
    clearTimeout(entry.timer);
    detachSignal(entry);
    waitingDelta(entry, -1);
    if (outcome === 'granted') {
      if (entry.isCombo) {
        const occs = commitCombo(entry);
        entry.resolve(makeOccupationHandle(occs));
      } else {
        const { occ } = applyGrant(entry.owner, entry.units);
        entry.owner.granted += 1;
        entry.resolve(makeSingleHandle(occ));
      }
      return;
    }
    if (entry.isCombo) releaseEntryReservation(entry);
    if (outcome === 'refused') {
      entry.owner.refused += 1;
      entry.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    } else if (outcome === 'cancelled') {
      entry.owner.cancelled += 1;
      entry.reject(
        new QuotaExceededError('CANCELLED', 'queued request was cancelled before quota was available'),
      );
    } else {
      entry.owner.expired += 1;
      entry.reject(
        new QuotaExceededError('WAIT_EXPIRED', 'queued request expired before quota was available'),
      );
    }
  }

  // The wait timer measures real elapsed time, like the baseline. The
  // deadline field additionally gates window-flip sweeps when the monotonic
  // clock and the wall clock disagree (mocked/frozen clocks).
  function armEntry(entry, delay) {
    entry.timer = setTimeout(() => {
      if (entry.removed) return;
      settleEntry(entry, 'expired');
    }, Math.max(1, delay));
    if (typeof entry.timer.unref === 'function') entry.timer.unref();
  }

  // At the same moment expiry beats cancellation: an abort arriving once the
  // deadline has already passed settles as WAIT_EXPIRED.
  function onEntryAbort(entry) {
    if (entry.removed) return;
    if (entry.deadline <= now()) {
      settleEntry(entry, 'expired');
    } else {
      settleEntry(entry, 'cancelled');
    }
  }

  function recomputeMinDeadline() {
    const r = state.root;
    let min = Infinity;
    const q = r.queue;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (!entry.removed && entry.deadline < min) min = entry.deadline;
    }
    r.minDeadline = min;
  }

  // Expiry is judged before quota allocation: anything due at or before the
  // flip moment expires rather than being granted.
  function sweepExpired(t) {
    const r = state.root;
    if (r.minDeadline > t) return;
    let min = Infinity;
    for (let i = r.head; i < r.queue.length; i += 1) {
      const entry = r.queue[i];
      if (entry.removed) continue;
      if (entry.deadline <= t) settleEntry(entry, 'expired');
      else if (entry.deadline < min) min = entry.deadline;
    }
    r.minDeadline = min;
  }

  // Grant queued requests in arrival order, all-or-nothing. A single request
  // that does not fit blocks every later one. A combo that cannot be gathered
  // right now steps aside twice, keeping its place at the head, so later
  // arrivals that fit are not starved; after the two yields it blocks the
  // queue like any other head.
  function allocate() {
    const r = state.root;
    const q = r.queue;
    let i = r.head;
    while (i < q.length) {
      const entry = q[i];
      if (entry.removed) {
        i += 1;
        continue;
      }
      if (entry.isCombo) {
        if (gatherCombo(entry)) {
          settleEntry(entry, 'granted');
          i += 1;
          continue;
        }
        // The failed attempt has rolled itself back; count the rollback and
        // decide whether the combo yields once more or now blocks.
        entry.owner.rolledBack += 1;
        if (entry.yields >= 2) break;
        entry.yields += 1;
        i += 1; // step aside for this pass, but keep the place in the queue
        continue;
      }
      if (!fits(entry.units)) break;
      settleEntry(entry, 'granted');
      i += 1;
    }
    // The live head is whichever entry stopped the pass, so yielded combos
    // get retried first at the next window flip.
    let h = r.head;
    while (h < q.length && q[h].removed) h += 1;
    r.head = h;
    if (r.head > 32 && r.head * 2 >= q.length) {
      r.queue = q.slice(r.head);
      r.head = 0;
    }
  }
  // Reclaim the lender side of committed legs this window had lent out:
  // admitted occupations are never torn apart, but the borrower-side counters
  // drop and the (still valid) handles simply have less to release later.
  function reclaimActiveAsLender(lender) {
    for (const leg of lender.activeLegs) {
      leg.lLive = false;
      if (leg.kind === 'own') continue; // borrower === lender, counters reset wholesale
      const b = leg.borrower;
      if (!leg.released && b.ownGen === leg.bGen) {
        b.inFlight -= leg.amount;
        b.borrowedInFlight -= leg.amount;
      }
    }
    lender.activeLegs.clear();
  }

  // A member's own window flips: its borrower side vanishes wholesale; its
  // lender side is reclaimed after its uncommitted reservations, in that
  // order. Legs backed by ancestor windows survive on the lender side until
  // those windows themselves flip.
  function flipOwn(target) {
    now();
    target.ownGen += 1;

    // Uncommitted reservations first: the ones this window borrows from
    // ancestors (the ancestors release their held quota) and the ones this
    // window itself backs (own legs and lends to descendants).
    for (const leg of target.pendingBorrowerLegs) releasePendingLeg(leg);
    for (const leg of target.pendingLegs) releasePendingLeg(leg);
    target.pendingBorrowerLegs.clear();
    target.pendingLegs.clear();

    target.usedOwn = 0;
    target.inFlight = 0; // occupations from the closed window clear automatically
    target.borrowedInFlight = 0;
    // Committed legs this gate borrows survive only on their lenders' sides.
    for (const leg of target.activeBorrowerLegs) leg.bLive = false;
    target.activeBorrowerLegs.clear();

    // Admitted lends second: handles stay valid, only accounting settles.
    reclaimActiveAsLender(target);

    // Occupation records born in the closed window are inert now; drop them
    // so handles their holders never release cannot accumulate.
    for (const record of target.openHandles) {
      if (record.gen !== target.ownGen) target.openHandles.delete(record);
    }
  }

  function flipRoot() {
    const t = now();
    sweepExpired(t);
    state.ownGen += 1;
    state.poolGen += 1;

    // Reclaim uncommitted reservations before admitted borrows. Every pending
    // leg is dropped here (including ones backed by still-open member
    // windows): the allocation pass immediately following re-gathers the
    // queue in arrival order from a clean slate, so no holder can observe the
    // gap and no queued combo inherits an out-of-order hold.
    const q = state.queue;
    for (let i = state.head; i < q.length; i += 1) {
      const entry = q[i];
      if (!entry.removed && entry.isCombo) releaseEntryReservation(entry);
    }

    // The root's own occupations and its admitted lends share the pool;
    // reclaim the lends member side first (handles stay valid), then reset.
    reclaimActiveAsLender(state);
    state.usedOwn = 0;
    state.inFlight = 0;
    state.borrowedInFlight = 0;

    // The pool window closes: every draw expires. Legs backed by still-open
    // member windows survive detached from the pool; their member-side
    // inFlight is untouched.
    state.poolUsed = 0;
    state.poolReserved = 0;
    for (const member of state.members) {
      member.poolDraw = 0;
    }

    for (const record of state.openHandles) {
      if (record.gen !== state.ownGen) state.openHandles.delete(record);
    }

    allocate();
  }

  function onWindowFire() {
    if (state.isRoot) flipRoot();
    else flipOwn(state);
  }
  state.timer = setInterval(onWindowFire, windowMs);
  if (typeof state.timer.unref === 'function') state.timer.unref();

  // --- Runtime adjustments -------------------------------------------------

  function validateHierarchyLimit(target, newLimit) {
    // "Must not pass over the direct parent" and "must not sit below a direct
    // child": only the direct neighbours constrain the value. Transitive
    // bounds already follow from theirs.
    if (target.parent !== null && newLimit > target.parent.limit) {
      throw new RangeError('limit must not exceed the direct parent limit');
    }
    for (const child of target.children) {
      if (child.limit > newLimit) {
        throw new RangeError('limit must not be below a direct child limit');
      }
    }
  }

  // Shrink fallout: queued requests that can never fit are refused
  // immediately and leave the queue, so they never block later arrivals.
  // Occupations are never torn apart. A queued combo is unfit when its total
  // draw can never fit the shrunk pool, or as soon as one of its members can
  // never cover its share of its own limit.
  function refuseUnfittable(ownerFilter, newLimit) {
    const r = state.root;
    const q = r.queue;
    let touched = false;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (entry.removed) continue;
      let unfit;
      if (entry.isCombo) {
        unfit =
          (state.isRoot && entry.units > newLimit) ||
          entry.occs.some((occ) => occ.units > occ.member.limit);
        if (unfit) unfit = entry.occs.some((occ) => ownerFilter(occ.member));
      } else {
        unfit = entry.units > newLimit && ownerFilter(entry.owner);
      }
      if (unfit) {
        settleEntry(entry, 'refused');
        touched = true;
      }
    }
    if (touched) recomputeMinDeadline();
  }

  function updateLimit(newLimit) {
    assertInteger(newLimit, 'limit');
    if (newLimit <= 0) throw new RangeError('limit must be a positive integer');
    validateHierarchyLimit(state, newLimit);
    // Validation above throws before anything mutates, so an invalid call
    // produces no observable cut. The change and its shrink fallout are one
    // synchronous cut; waiters are never granted early.
    state.limit = newLimit;
    if (state.isRoot) {
      state.root.poolLimit = newLimit;
      refuseUnfittable(() => true, newLimit);
    } else {
      refuseUnfittable((member) => member === state, newLimit);
    }
  }

  function updateWindowMs(newWindowMs) {
    assertInteger(newWindowMs, 'windowMs');
    if (newWindowMs <= 0) throw new RangeError('windowMs must be a positive integer');
    // The current window keeps running with a fresh, full length measured
    // from this adjustment. Used quota is not cleared, queued deadlines do
    // not move, and no request is granted early.
    state.windowMs = newWindowMs;
    clearInterval(state.timer);
    state.timer = setInterval(onWindowFire, newWindowMs);
    if (typeof state.timer.unref === 'function') state.timer.unref();
  }

  function updateMaxWaitMs(newMaxWaitMs) {
    assertInteger(newMaxWaitMs, 'maxWaitMs');
    if (newMaxWaitMs < 0) throw new RangeError('maxWaitMs must not be negative');
    // Existing waiters keep their original enqueue time; only the cap moves.
    // Waiters already past the new deadline settle as expired on the spot;
    // nothing is granted early and no deadline moves backwards into life.
    state.maxWaitMs = newMaxWaitMs;
    const t = now();
    let changed = false;
    const r = state.root;
    const q = r.queue;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (entry.removed || entry.owner !== state) continue;
      const newDeadline = entry.enqueuedAt + newMaxWaitMs;
      if (newDeadline <= t) {
        settleEntry(entry, 'expired');
        changed = true;
        continue;
      }
      if (newDeadline !== entry.deadline) {
        entry.deadline = newDeadline;
        clearTimeout(entry.timer);
        armEntry(entry, newDeadline - t);
        changed = true;
      }
    }
    if (changed) recomputeMinDeadline();
  }

  function collectSubtree(target) {
    const out = new Set();
    const stack = [target];
    while (stack.length > 0) {
      const node = stack.pop();
      out.add(node);
      for (const child of node.children) stack.push(child);
    }
    return out;
  }

  function isDescendantOf(maybeDescendant, ancestor) {
    let node = maybeDescendant;
    while (node !== null) {
      if (node === ancestor) return true;
      node = node.parent;
    }
    return false;
  }
  // Migrate the active legs of one moved gate. Legs whose lender stays
  // outside the moved subtree were lent by an ancestor the gate no longer
  // has: the borrow is settled on that lender and re-opened against the new
  // family's pool, exactly as the baseline re-stamps moved handles. Every
  // moved leg keeps drawing the (new) pool window.
  function migrateLegsOf(node, oldRootState, newRootState, moved) {
    for (const leg of node.activeBorrowerLegs) {
      // Mirror the baseline, which re-stamps every moved handle against the
      // new pool: detached legs re-attach there even though the pool charge
      // itself only moves by subtreeDraw.
      leg.pGen = newRootState.poolGen;
      // A leg crosses the boundary when its lender stays outside the moved
      // subtree, or when it was lent by the old pool root itself (the old
      // pool ceases to exist as a pool even when that gate moves along).
      // Own legs are never boundary legs: lender and borrower are one gate.
      const boundaryLender = leg.kind === 'borrow' &&
        (!moved.has(leg.lender) || leg.lender === oldRootState);
      // Only legs the outside lender window is still backing cross the
      // boundary; a leg the old lender already reclaimed has no side to move.
      if (boundaryLender && leg.lLive) {
        leg.lender.activeLegs.delete(leg);
        // The old lender's own window loses the lend it can no longer make
        // (a root-lent leg never counted against the root's own quota).
        if (leg.lender !== oldRootState) leg.lender.usedOwn -= leg.amount;
        leg.lender = newRootState;
        leg.lGen = newRootState.ownGen;
        newRootState.activeLegs.add(leg);
      }
    }
  }

  // Partition the old root queue at the moved subtree boundary, then merge
  // the moved entries into the new root queue preserving global arrival
  // order (entry.seq is assigned monotonically at enqueue time). Queued
  // combos lose their uncommitted reservations either way. This runs before
  // any gate's `root` pointer moves, so a combo whose members do not all move
  // with the subtree settles (refused, since it can never gather in the new
  // family) against the old family's counters.
  function migrateQueue(oldRootState, newRootState, moved) {
    const staying = [];
    const movedEntries = [];
    let movedLive = 0;
    const oldQ = oldRootState.queue;
    for (let i = oldRootState.head; i < oldQ.length; i += 1) {
      const entry = oldQ[i];
      if (entry.removed) continue;
      if (moved.has(entry.owner)) {
        if (entry.isCombo) {
          if (entry.occs.every((occ) => moved.has(occ.member))) {
            releaseEntryReservation(entry);
          } else {
            settleEntry(entry, 'refused');
            continue;
          }
        }
        movedEntries.push(entry);
        movedLive += 1;
      } else if (
        entry.isCombo &&
        entry.occs.some((occ) => moved.has(occ.member))
      ) {
        // The owner stays in the old family while one of its members moves
        // to the new one: the combination can never gather across families.
        settleEntry(entry, 'refused');
      } else {
        staying.push(entry);
      }
    }
    oldRootState.queue = staying;
    oldRootState.head = 0;
    oldRootState.liveQueued -= movedLive;
    oldRootState.minDeadline = Infinity;
    for (const entry of staying) {
      if (entry.deadline < oldRootState.minDeadline) oldRootState.minDeadline = entry.deadline;
    }

    const existing = [];
    const newQ = newRootState.queue;
    for (let i = newQ.head; i < newQ.length; i += 1) {
      const entry = newQ[i];
      if (!entry.removed) existing.push(entry);
    }
    const merged = [];
    let a = 0;
    let b = 0;
    while (a < existing.length || b < movedEntries.length) {
      if (b === movedEntries.length || (a < existing.length && existing[a].seq <= movedEntries[b].seq)) {
        merged.push(existing[a]);
        a += 1;
      } else {
        merged.push(movedEntries[b]);
        b += 1;
      }
    }
    newRootState.queue = merged;
    newRootState.head = 0;
    newRootState.liveQueued += movedLive;
    newRootState.minDeadline = Infinity;
    for (const entry of merged) {
      if (entry.deadline < newRootState.minDeadline) newRootState.minDeadline = entry.deadline;
    }
  }

  function reparent(newParent) {
    if (
      newParent === null ||
      newParent === undefined ||
      typeof newParent !== 'object' ||
      gateState.get(newParent) === undefined
    ) {
      throw new TypeError('reparent target must be a gate created by createGate');
    }
    const newParentState = gateState.get(newParent);
    if (newParentState === state) {
      throw new RangeError('cannot reparent a gate under itself');
    }
    if (isDescendantOf(newParentState, state)) {
      throw new RangeError('cannot reparent a gate under one of its own descendants');
    }
    if (state.limit > newParentState.limit) {
      throw new RangeError('limit must not exceed the new parent limit');
    }
    // Every check above happens before any mutation: invalid reparents roll
    // back trivially and produce no cut.

    const oldRootState = state.root;
    const newRootState = newParentState.root;
    const moved = collectSubtree(state);
    let subtreeDraw = 0;
    for (const node of moved) subtreeDraw += node.poolDraw;

    if (oldRootState !== newRootState) {
      // Settle the borrow in the old family's window, then re-borrow from the
      // new family. Both sides of the trade are visible in the same
      // synchronous snapshot. If the new pool cannot cover the draw, the
      // whole reparent is rejected with nothing changed.
      if (newRootState.poolUsed + subtreeDraw > newRootState.poolLimit) {
        throw new QuotaExceededError(
          'QUOTA_EXCEEDED',
          'cannot reparent: the new family pool cannot cover the borrowed quota',
        );
      }

      const oldDirectParent = state.parent;
      oldRootState.poolUsed -= subtreeDraw;
      newRootState.poolUsed += subtreeDraw;
      // Queue bookkeeping first, while every moved gate still reports the
      // old family as its root: pending reservations are released into the
      // old pool and split combos settle against the old family counters.
      migrateQueue(oldRootState, newRootState, moved);
      for (const node of moved) {
        oldRootState.members.delete(node);
        newRootState.members.add(node);
        node.root = newRootState;
        migrateLegsOf(node, oldRootState, newRootState, moved);
      }
      // The moved waiters carry deadlines stamped by the old family clock;
      // make the joined clock at least as large so the monotonic guarantee
      // survives the move and no deadline is judged already past by accident.
      if (oldRootState.lastNow > newRootState.lastNow) {
        newRootState.lastNow = oldRootState.lastNow;
      }
      if (oldDirectParent !== null) oldDirectParent.children.delete(state);
      newParentState.children.add(state);
      state.parent = newParentState;
      if (state === oldRootState) {
        // The old root gate becomes an ordinary child: its next window fire
        // is a plain own-window flip; the new root owns the shared pool.
        state.isRoot = false;
      }
    } else {
      // Same family: only the direct link moves; the shared pool is
      // untouched, so the old direct parent sees no cut at all. Pending
      // reservations touching the moved subtree are dropped first: their
      // borrow chain was computed against an ancestry that is about to
      // change, and they are re-gathered with the new chain at the next
      // pool flip. Admitted occupations are never torn apart.
      const sameQ = oldRootState.queue;
      for (let i = oldRootState.head; i < sameQ.length; i += 1) {
        const entry = sameQ[i];
        if (entry.removed || !entry.isCombo) continue;
        if (entry.occs.some((occ) => moved.has(occ.member))) releaseEntryReservation(entry);
      }
      state.parent.children.delete(state);
      newParentState.children.add(state);
      state.parent = newParentState;
    }
  }
  function queueEntry(entry, maxWait) {
    const r = state.root;
    r.liveQueued += 1;
    return new Promise((resolve, reject) => {
      const t = now();
      Object.assign(entry, {
        resolve,
        reject,
        seq: (enqueueSeq += 1),
        enqueuedAt: t,
        deadline: t + maxWait,
        timer: null,
        removed: false,
        signal: entry.signal !== undefined && entry.signal !== null ? entry.signal : null,
        onAbort: null,
      });
      entry.onAbort = () => onEntryAbort(entry);
      if (entry.signal !== null) {
        entry.signal.addEventListener('abort', entry.onAbort, { once: true });
      }
      armEntry(entry, maxWait);
      if (entry.deadline < r.minDeadline) r.minDeadline = entry.deadline;
      r.queue.push(entry);
    });
  }

  function acquire(units = 1, signal) {
    assertInteger(units, 'units');
    assertSignal(signal);
    if (units < 1 || units > state.limit) {
      throw new RangeError(`units must be between 1 and ${state.limit}`);
    }
    // A signal that fired before the call settles as CANCELLED without ever
    // entering the queue or touching quota.
    if (signal !== undefined && signal !== null && signal.aborted) {
      state.cancelled += 1;
      return Promise.reject(
        new QuotaExceededError('CANCELLED', 'acquire was cancelled before it could queue'),
      );
    }
    // A non-empty queue means earlier arrivals are still owed quota; new
    // requests must not overtake them even if they would fit.
    const r = state.root;
    if (r.liveQueued === 0 && fits(units)) {
      const { occ } = applyGrant(state, units);
      state.granted += 1;
      return Promise.resolve(makeSingleHandle(occ));
    }
    if (state.maxWaitMs === 0) {
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    const entry = {
      owner: state,
      units,
      isCombo: false,
      occs: null,
      yields: 0,
      signal: signal !== undefined && signal !== null ? signal : null,
    };
    waitingDelta(entry, +1);
    return queueEntry(entry, state.maxWaitMs);
  }

  // Validate an acquireAll member list. Every check runs before any counter
  // moves, so illegal calls produce no observable cut.
  function normalizeMembers(members) {
    if (!Array.isArray(members)) {
      throw new TypeError('members must be an array of { gate, units }');
    }
    if (members.length === 0) {
      throw new RangeError('members must contain at least one member');
    }
    const occs = [];
    const seen = new Set();
    let total = 0;
    const r = state.root;
    for (const member of members) {
      if (member === null || typeof member !== 'object') {
        throw new TypeError('each member must be an object { gate, units }');
      }
      if (!Number.isInteger(member.units)) {
        throw new TypeError('units must be an integer');
      }
      const memberState = gateState.get(member.gate);
      if (memberState === undefined) {
        throw new TypeError('member gate must be a gate created by createGate');
      }
      const units = member.units;
      if (units < 1 || units > memberState.limit) {
        throw new RangeError(`units must be between 1 and ${memberState.limit}`);
      }
      if (seen.has(memberState)) {
        throw new RangeError('the same gate must not appear twice in one acquireAll');
      }
      if (memberState.root !== r) {
        throw new RangeError('all members of an acquireAll must belong to the same gate family');
      }
      seen.add(memberState);
      occs.push({ member: memberState, units, legs: [] });
      total += units;
    }
    // No member may be an ancestor of another: their own windows would
    // double-count the same quota inside one all-or-nothing gather.
    for (let i = 0; i < occs.length; i += 1) {
      for (let j = i + 1; j < occs.length; j += 1) {
        if (
          isDescendantOf(occs[i].member, occs[j].member) ||
          isDescendantOf(occs[j].member, occs[i].member)
        ) {
          throw new RangeError('members of an acquireAll must not form an ancestor chain');
        }
      }
    }
    return { occs, total };
  }

  function acquireAll(members, signal) {
    assertSignal(signal);
    const { occs, total } = normalizeMembers(members);
    if (signal !== undefined && signal !== null && signal.aborted) {
      state.cancelled += 1;
      return Promise.reject(
        new QuotaExceededError('CANCELLED', 'acquireAll was cancelled before it could queue'),
      );
    }
    const entry = {
      owner: state,
      units: total,
      isCombo: true,
      occs,
      yields: 0,
      signal: signal !== undefined && signal !== null ? signal : null,
    };
    const r = state.root;
    // Empty queue: try to reserve and commit the whole combo on the spot; a
    // synchronous grant never queues, so waiting stays at zero.
    if (r.liveQueued === 0) {
      if (gatherCombo(entry)) {
        const handle = makeOccupationHandle(commitCombo(entry));
        return Promise.resolve(handle);
      }
      entry.owner.rolledBack += 1;
      if (state.maxWaitMs === 0) {
        entry.owner.refused += 1;
        return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
      }
    } else if (state.maxWaitMs === 0) {
      // Earlier arrivals are owed quota and zero-wait requests never queue.
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    } else {
      // Earlier arrivals block the commit, but anything gatherable right now
      // is reserved and held until the next pool window flips; an
      // ungatherable combo rolls back and retries from scratch at the flip.
      if (!gatherCombo(entry)) entry.owner.rolledBack += 1;
    }
    waitingDelta(entry, +1);
    return queueEntry(entry, state.maxWaitMs);
  }

  function stats() {
    return {
      limit: state.limit,
      windowMs: state.windowMs,
      inFlight: state.inFlight,
      waiting: state.waiting,
      granted: state.granted,
      refused: state.refused,
      expired: state.expired,
      cancelled: state.cancelled,
      reserved: state.ownReserved + state.lentReserved,
      rolledBack: state.rolledBack,
    };
  }

  const gate = { acquire, acquireAll, stats, updateLimit, updateWindowMs, updateMaxWaitMs, reparent };
  gateState.set(gate, state);
  return gate;
}
