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
    usedOwn: 0, // own units occupied or lent out in the current own window
    inFlight: 0, // units occupied through this gate and not yet cleared
    borrowedInFlight: 0, // units this gate's occupations borrowed, still live
    lentInFlight: 0, // own units lent to descendants' admitted occupations
    poolDraw: 0, // committed pool charge, valid only when drawGen === poolGen
    drawGen: -1,
    // Cross-gate reservation accounting. A pending acquireAll first reserves
    // the member's own current-window slice, then own slices up the direct
    // parent chain (each step registering the lend); the root pool covers the
    // remainder. Every gathered unit draws pool room until commit or rollback.
    //
    // A non-root gate's reserved units are pendingOwn + pendingLends: own
    // window units promised to pending combos, as member or as ancestor
    // lender. The root's reserved units are poolResvUnits: uncommitted units
    // drawn from the pool. Across the family every reserved unit counts once.
    rolledBack: 0, // failed whole-combo reservation attempts owned here
    pendingOwn: 0,
    pendingLends: 0,
    resvAsMember: new Set(), // entries holding pendingOwn here
    resvAsLender: new Set(), // entries holding pendingLends here
    lentLinks: new Set(), // admitted occupation links lent from this gate
    // Observability.
    waiting: 0, // units currently queued through this gate
    granted: 0,
    refused: 0,
    expired: 0,
    cancelled: 0,
    openHandles: new Set(),
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
      poolUsed: 0, // units committed or reserved in the current pool window
      poolLinks: new Set(), // admitted pool borrows, reclaimed at the pool flip
      poolResv: new Set(), // uncommitted pool reservation links
      poolResvUnits: 0, // sum of units behind poolResv
      queue: [], // queued requests from every gate, in arrival order
      head: 0, // index of the first live entry; entries are never shifted
      liveQueued: 0, // entries not yet settled
      serveWave: 0, // monotonic count of serves that overtook a blocked combo
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

  function effectiveDraw(node) {
    return node.drawGen === node.root.poolGen ? node.poolDraw : 0;
  }

  function chargeDraw(node, units) {
    if (node.drawGen !== node.root.poolGen) {
      node.drawGen = node.root.poolGen;
      node.poolDraw = 0;
    }
    node.poolDraw += units;
  }

  function reservedOn(g) {
    return g === g.root ? g.root.poolResvUnits : g.pendingOwn + g.pendingLends;
  }

  // --- Combo reservations ---------------------------------------------------
  //
  // seg:  { gate, units, own, ownReleased, links, gen }
  // link: { kind:'own'|'pool', lender, units, released, gen }
  //
  // Gather plans first and applies second, so a plan the pool cannot cover is
  // rejected without any mutation and the whole-combo rollback is trivial.
  // Sibling members share ancestor chains; the tentative `claimed` map debits
  // ancestor headroom cumulatively inside one plan. Every registration is
  // intrusive: commit, rollback and window-flip reclaims touch only the gates
  // involved, never the queue or the whole family.

  function buildSegs(comboGates, unitsOf) {
    return comboGates.map((g) => ({
      gate: g,
      units: unitsOf(g),
      own: 0,
      ownReleased: false, // member own slice already reclaimed by an own flip
      links: [],
      gen: -1,
    }));
  }

  function ownHeadroom(g, claimed) {
    return Math.max(0, g.limit - g.usedOwn - g.pendingOwn - g.pendingLends - (claimed.get(g) ?? 0));
  }

  // Pure plan: fills seg.own / seg.links and returns total pool need. A
  // member slice larger than that gate's own limit simply borrows the excess
  // up the chain — member limits never cap a combo, only the root pool does.
  function planGather(segs) {
    const r = state.root;
    const claimed = new Map();
    let poolNeed = 0;
    for (const seg of segs) {
      const m = seg.gate;
      if (m === r) {
        seg.links.push({ kind: 'pool', lender: r, units: seg.units, released: false, gen: -1 });
        poolNeed += seg.units;
        continue;
      }
      let need = seg.units;
      const own = Math.min(need, ownHeadroom(m, claimed));
      seg.own = own;
      claimed.set(m, (claimed.get(m) ?? 0) + own);
      need -= own;
      let cur = m.parent;
      while (need > 0 && cur !== null && cur !== r) {
        const lend = Math.min(need, ownHeadroom(cur, claimed));
        if (lend > 0) {
          seg.links.push({ kind: 'own', lender: cur, units: lend, released: false, gen: -1 });
          claimed.set(cur, (claimed.get(cur) ?? 0) + lend);
          need -= lend;
        }
        cur = cur.parent;
      }
      if (need > 0) {
        seg.links.push({ kind: 'pool', lender: r, units: need, released: false, gen: -1 });
        poolNeed += need;
      }
    }
    return poolNeed;
  }

  // Apply a planned reservation; the caller pre-checked poolNeed against room.
  function applyGather(entry, segs) {
    const r = entry.owner.root;
    const gen = r.poolGen;
    for (const seg of segs) {
      const m = seg.gate;
      seg.gen = gen;
      seg.ownReleased = false;
      if (m !== r && seg.own > 0) {
        m.pendingOwn += seg.own;
        m.resvAsMember.add(entry);
        r.poolUsed += seg.own;
      }
      for (const link of seg.links) {
        link.gen = gen;
        r.poolUsed += link.units;
        if (link.kind === 'own') {
          link.lender.pendingLends += link.units;
          link.lender.resvAsLender.add(entry);
        } else {
          r.poolResv.add(link);
          r.poolResvUnits += link.units;
          if (m === r) m.resvAsMember.add(entry);
        }
      }
    }
    entry.segs = segs;
    entry.holdsHeld = true;
  }

  // Plan, check pool room, apply — or reject without touching anything. The
  // whole combo draws pool room: a member's own slice and every ancestor lend
  // are partitions OF the pool, not capacity beyond it. Feasibility is
  // therefore simply poolUsed + total combo units <= poolLimit; the plan's
  // own/lender/pool partition only decides which own window each unit lands
  // on (and thus where its flip reclaims it).
  function tryGather(entry, segs) {
    const r = entry.owner.root;
    let total = 0;
    for (const seg of segs) total += seg.units;
    if (r.poolUsed + total > r.poolLimit) {
      for (const seg of segs) {
        seg.own = 0;
        seg.links = [];
      }
      return false;
    }
    planGather(segs);
    applyGather(entry, segs);
    return true;
  }

  // Release a held reservation wholesale. Pool room is refunded only while
  // the charged pool window is still the current one; a pool flip already
  // reclaimed it (the link is marked released and its gen is stale).
  function releaseHolds(entry) {
    if (!entry.holdsHeld || entry.segs === null) return;
    const r = entry.owner.root;
    for (const seg of entry.segs) {
      const m = seg.gate;
      if (!seg.ownReleased && seg.own > 0) {
        seg.ownReleased = true;
        m.pendingOwn -= seg.own;
        m.resvAsMember.delete(entry);
        if (seg.gen === r.poolGen) r.poolUsed -= seg.own;
      }
      for (const link of seg.links) {
        if (link.released) continue;
        link.released = true;
        if (link.kind === 'pool') {
          // delete() reports whether the link still charged the pool: a pool
          // flip already cleared the set and zeroed poolResvUnits, so a stale
          // link must not subtract twice.
          if (r.poolResv.delete(link)) r.poolResvUnits -= link.units;
          m.resvAsMember.delete(entry);
        } else {
          link.lender.pendingLends -= link.units;
          link.lender.resvAsLender.delete(entry);
        }
        if (link.gen === r.poolGen) r.poolUsed -= link.units;
      }
    }
    entry.holdsHeld = false;
    entry.segs = null;
  }

  // --- Handles --------------------------------------------------------------
  //
  // The handle exposes exactly one own property, release, as a closure: the
  // destructured method called without a receiver works the same. Every link
  // remembers the generations of both sides, so a release after either window
  // flipped never subtracts quota that flip already settled.

  function makeHandle(records) {
    let active = true;
    return {
      release() {
        if (!active) return;
        active = false;
        for (const rec of records) {
          const seg = rec.seg;
          const m = seg.member;
          const r = m.root;
          m.openHandles.delete(rec);
          if (seg.mgen === m.ownGen && seg.own > 0) {
            m.inFlight -= seg.own;
          }
          for (const link of seg.links) {
            const memberLive = seg.mgen === m.ownGen;
            const lenderLive =
              link.kind === 'pool' ? link.lgen === r.poolGen : link.lgen === link.lender.ownGen;
            if (!memberLive || !lenderLive) continue; // a window flip settled it
            m.inFlight -= link.units;
            m.borrowedInFlight -= link.units;
            if (link.kind === 'pool') {
              r.poolLinks.delete(link);
            } else {
              link.lender.lentLinks.delete(link);
              link.lender.inFlight -= link.units;
              link.lender.lentInFlight -= link.units;
            }
          }
        }
      },
    };
  }

  // Every admitted occupation, single or combined, commits through the same
  // gather machinery: provisional member-own promises and ancestor lends
  // become admitted occupation parts; the uncommitted pool remainder is
  // converted into an admitted pool borrow. Pool room was charged at gather
  // time, so poolUsed does not move here.
  function commitEntry(entry) {
    const r = entry.owner.root;
    const records = [];
    const lenderCleanup = new Set();
    for (const seg0 of entry.segs) {
      const m = seg0.gate;
      const units = seg0.units;
      chargeDraw(m, units);
      let seg;
      if (m === r) {
        // The root member's whole slice is its own window and drew the pool
        // directly; consume its pool reservation link so it does not survive
        // the commit as uncommitted quota.
        for (const l0 of seg0.links) {
          if (l0.kind === 'pool' && r.poolResv.delete(l0)) r.poolResvUnits -= l0.units;
        }
        m.usedOwn += units;
        m.inFlight += units;
        m.resvAsMember.delete(entry);
        seg = { member: m, units, own: units, mgen: m.ownGen, links: [] };
      } else {
        const ownPart = seg0.own;
        m.usedOwn += ownPart;
        m.pendingOwn -= ownPart;
        m.resvAsMember.delete(entry);
        m.inFlight += units;
        const links = [];
        for (const l0 of seg0.links) {
          const link = {
            kind: l0.kind,
            lender: l0.lender,
            member: m,
            units: l0.units,
            lgen: l0.kind === 'pool' ? r.poolGen : l0.lender.ownGen,
            mgen: m.ownGen,
          };
          if (l0.kind === 'pool') {
            if (r.poolResv.delete(l0)) r.poolResvUnits -= l0.units;
            r.poolLinks.add(link);
          } else {
            const g = l0.lender;
            g.usedOwn += l0.units;
            g.pendingLends -= l0.units;
            lenderCleanup.add(g);
            g.inFlight += l0.units;
            g.lentInFlight += l0.units;
            link.pgen = r.poolGen; // pool window this lend charged
            g.lentLinks.add(link);
          }
          links.push(link);
        }
        m.borrowedInFlight += units - ownPart;
        seg = { member: m, units, own: ownPart, mgen: m.ownGen, links };
      }
      const rec = { seg };
      m.openHandles.add(rec);
      records.push(rec);
    }
    // One entry may borrow from the same ancestor for several sibling
    // members; drop its lender registration once, after every link settled.
    for (const g of lenderCleanup) g.resvAsLender.delete(entry);
    entry.holdsHeld = false;
    entry.segs = null;
    return makeHandle(records);
  }

  // --- Queue entry lifecycle ------------------------------------------------

  function detachSignal(entry) {
    if (entry.signal !== null) {
      entry.signal.removeEventListener('abort', entry.onAbort);
      entry.signal = null;
    }
  }

  // One queue entry leaves the queue exactly once, and exactly one of
  // granted / refused / expired / cancelled moves as a result.
  function settleEntry(entry, outcome) {
    if (entry.removed) return;
    const ownerRoot = entry.owner.root;
    entry.removed = true;
    ownerRoot.liveQueued -= 1;
    clearTimeout(entry.timer);
    detachSignal(entry);
    for (const charge of entry.waitCharges) charge.gate.waiting -= charge.units;
    if (outcome === 'granted') {
      const handle = commitEntry(entry);
      entry.owner.granted += 1;
      entry.resolve(handle);
      return;
    }
    // Expiry, cancellation and refusal give back any hold; none of those
    // outcomes itself counts as a reservation rollback. Singles and combos
    // alike hold their gather until they settle.
    releaseHolds(entry);
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

  // Allocation walks arrivals in order against the freshly reset pool. Both
  // singles and combos turn against it: a head entry that cannot be gathered
  // whole does not starve later arrivals — a later request that fits outright
  // passes it and the blocker yields once; after yielding twice (cumulatively,
  // across flips) it becomes a hard barrier. The pass is O(1) per live queue
  // entry: a single monotonic counter (serveWave, bumped once per overtaking
  // serve) lets each blocked entry derive its yield count without being touched
  // by the requests that pass it.
  function allocate() {
    const r = state.root;
    const q = r.queue;
    let wave = r.serveWave; // overtakes counted so far, monotonic across passes
    let stopWave = Infinity; // earliest wave at which a blocker hardens
    let blockerCount = 0; // blocked entries registered earlier in this/prev passes
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (entry.removed) continue;
      if (wave >= stopWave) break;
      let served = false;
      // The entry's turn against the fresh pool: the pool flip reclaimed its
      // pool side and ancestor-window flips reclaimed own-side parts; release
      // whatever still holds and gather anew along the current chain.
      if (entry.holdsHeld) releaseHolds(entry);
      if (tryGather(entry, buildSegs(entry.comboGates, (g) => entry.comboUnits.get(g)))) {
        settleEntry(entry, 'granted');
        served = true;
      } else if (!entry.combo) {
        // A single that does not fit is an ordinary hard barrier: nothing
        // later may overtake it.
        break;
      } else {
        // Any step short: the plan touched nothing; count one whole
        // rollback (no other counter moves) and register a soft barrier.
        entry.owner.rolledBack += 1;
        if (entry.blockG !== -1) entry.yields += wave - entry.blockG;
        entry.blockG = wave;
        const hardensAt = wave + (2 - entry.yields);
        blockerCount += 1;
        if (hardensAt < stopWave) stopWave = hardensAt;
      }
      if (served && blockerCount > 0) wave += 1; // one yield per overtaken blocker
    }
    r.serveWave = wave;
    while (r.head < q.length && q[r.head].removed) r.head += 1;
    if (r.head > 32 && r.head * 2 >= q.length) {
      r.queue = q.slice(r.head);
      r.head = 0;
    }
  }

  // --- Window flips ----------------------------------------------------------

  // A non-root own window reclaims quota borrowed FROM this gate, uncommitted
  // reservations first (member own slice, then ancestor lends), then admitted
  // lends. Occupations admitted through this gate end with its window.
  // Handles never break.
  function flipOwn(target) {
    now();
    const r = target.root;

    // 1a) Uncommitted member-own promises on the flipping gate.
    for (const entry of target.resvAsMember) {
      if (entry.removed || !entry.holdsHeld) continue;
      for (const seg of entry.segs) {
        if (seg.gate !== target || seg.ownReleased || seg.own === 0) continue;
        seg.ownReleased = true;
        target.pendingOwn -= seg.own;
        if (seg.gen === r.poolGen) r.poolUsed -= seg.own;
      }
    }
    target.resvAsMember.clear();

    // 1b) Uncommitted lends promised FROM the flipping gate.
    for (const entry of target.resvAsLender) {
      if (entry.removed || !entry.holdsHeld) continue;
      for (const seg of entry.segs) {
        for (const link of seg.links) {
          if (link.released || link.kind !== 'own' || link.lender !== target) continue;
          link.released = true;
          target.pendingLends -= link.units;
          if (link.gen === r.poolGen) r.poolUsed -= link.units;
        }
      }
    }
    target.resvAsLender.clear();

    // 2) Occupations admitted THROUGH the flipping gate end with its window.
    //    The handles go inert; links survive for their own side's reclaim, and
    //    the mgen guard below keeps that reclaim idempotent.
    target.openHandles.clear();

    // 3) Reclaim admitted lends FROM the flipping gate: borrowing members keep
    //    valid handles; only their inFlight settles, in this same snapshot.
    //    The pool room charged for those lent units at gather time frees here,
    //    exactly like the baseline's parent-window borrow reclaim.
    for (const link of target.lentLinks) {
      if (link.mgen === link.member.ownGen) {
        link.member.inFlight -= link.units;
        link.member.borrowedInFlight -= link.units;
      }
      if (link.lgen === target.ownGen && target !== r && link.pgen === r.poolGen) {
        r.poolUsed -= link.units;
      }
    }
    target.lentLinks.clear();

    target.ownGen += 1;
    target.usedOwn = 0;
    target.inFlight = 0;
    target.borrowedInFlight = 0;
    target.lentInFlight = 0;
    target.pendingOwn = 0;
    target.pendingLends = 0;
  }

  function flipRoot() {
    const t = now();
    const r = state;

    // Expiry is judged before anything is reclaimed or granted.
    sweepExpired(t);

    // 1) Reclaim uncommitted reservation borrows from the pool FIRST, before
    //    admitted occupations' pool borrows. The combos stay queued and gather
    //    again at their turn below; non-root own slices survive their windows.
    for (const link of r.poolResv) link.released = true;
    r.poolResv.clear();
    r.poolResvUnits = 0;
    r.resvAsMember.clear();

    // 2) Reclaim admitted pool borrows. Handles stay valid; the inFlight drop
    //    is visible in this same snapshot.
    for (const link of r.poolLinks) {
      if (link.mgen === link.member.ownGen) {
        link.member.inFlight -= link.units;
        link.member.borrowedInFlight -= link.units;
      }
    }
    r.poolLinks.clear();

    // 3) The root's own occupations end with its own window.
    r.openHandles.clear();
    r.ownGen += 1;
    r.usedOwn = 0;
    r.inFlight = 0;
    r.borrowedInFlight = 0;
    r.lentInFlight = 0;

    // The pool window resets. Committed poolDraw is generation-tagged, so no
    // family-wide sweep is needed.
    r.poolGen += 1;
    r.poolUsed = 0;

    // 4) Serve the queue in arrival order, re-gathering combos as they come.
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
  // Occupations are never torn apart. A unit count above a member's own limit
  // is covered by parent-chain lends and the pool, so only a root (pool) cap
  // shrink can make a queued single or combo impossible to satisfy; such an
  // entry is refused as a whole, releasing its reservation in the same cut.
  function refuseUnfittable(newLimit) {
    const r = state.root;
    const q = r.queue;
    let touched = false;
    const poolShrink = state === r;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (entry.removed) continue;
      if (poolShrink && entry.totalUnits > newLimit) {
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
    }
    refuseUnfittable(newLimit);
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

  // Rebuild the borrow side of one admitted occupation segment after its
  // member moved into a new family. The member's own slice and handle survive
  // (a former root turned child keeps only its limit-sized own slice); every
  // old link is detached, settling live sides, and the borrowed remainder is
  // drawn from the new family's pool directly — the same flat partition a
  // single-gate occupation always used. The bulk pool charge moved once with
  // the subtree (subtreeDraw), so this function moves no poolUsed.
  function reborrowSeg(seg, oldRootState, newRootState) {
    const m = seg.member;
    for (const link of seg.links) {
      if (link.kind === 'pool') {
        oldRootState.poolLinks.delete(link);
      } else if (link.lgen === link.lender.ownGen) {
        // The lend leaves the old family with the moving occupation: settle
        // its lender side as well as the occupation side.
        const g = link.lender;
        g.lentLinks.delete(link);
        g.usedOwn -= link.units;
        g.inFlight -= link.units;
        g.lentInFlight -= link.units;
      }
      if (link.mgen === m.ownGen) {
        m.inFlight -= link.units;
        m.borrowedInFlight -= link.units;
      }
    }
    seg.links = [];

    let ownPart = seg.own;
    if (m === oldRootState) {
      // A former root counted the whole occupation as its own; as a child only
      // its limit-sized slice stays own, and inFlight must not change.
      ownPart = Math.min(seg.units, m.limit);
      m.usedOwn -= seg.units - ownPart;
      seg.own = ownPart;
    }
    const need = seg.units - ownPart;
    if (need > 0) {
      const link = {
        kind: 'pool',
        lender: newRootState,
        member: m,
        units: need,
        lgen: newRootState.poolGen,
        mgen: m.ownGen,
      };
      newRootState.poolLinks.add(link);
      seg.links.push(link);
      m.borrowedInFlight += need;
      if (m !== oldRootState) m.inFlight += need;
    }
  }

  // Release provisional links of a staying queued combo whose lender (or whose
  // member own quota) belongs to a subtree that is leaving the family.
  function releaseMovedHolds(entry, moved, oldRootState) {
    if (!entry.holdsHeld) return;
    for (const seg of entry.segs) {
      if (!seg.ownReleased && seg.own > 0 && moved.has(seg.gate)) {
        seg.ownReleased = true;
        seg.gate.pendingOwn -= seg.own;
        seg.gate.resvAsMember.delete(entry);
        if (seg.gen === oldRootState.poolGen) oldRootState.poolUsed -= seg.own;
      }
      for (const link of seg.links) {
        if (link.released || !moved.has(link.lender)) continue;
        link.released = true;
        if (link.kind === 'pool') {
          if (oldRootState.poolResv.delete(link)) oldRootState.poolResvUnits -= link.units;
        } else {
          link.lender.pendingLends -= link.units;
          link.lender.resvAsLender.delete(entry);
        }
        if (link.gen === oldRootState.poolGen) oldRootState.poolUsed -= link.units;
      }
    }
  }

  // Partition the old/new queues at the moved-subtree boundary, preserving
  // global arrival order. Entries were already settled or had their
  // reservations released by the caller; removed entries are skipped.
  function migrateQueue(oldRootState, newRootState, moved) {
    const staying = [];
    const movedEntries = [];
    let movedLive = 0;
    const oldQ = oldRootState.queue;
    for (let i = oldRootState.head; i < oldQ.length; i += 1) {
      const entry = oldQ[i];
      if (entry.removed) continue;
      let isMoved;
      if (entry.combo) {
        isMoved = entry.comboGates.every((g) => moved.has(g));
      } else {
        isMoved = moved.has(entry.owner);
      }
      if (isMoved) {
        if (entry.combo) {
          // The overtake count belongs to the old family's fairness clock;
          // the combo starts yielding fresh in the joined family.
          entry.yields = 0;
          entry.blockG = -1;
        }
        movedEntries.push(entry);
        movedLive += 1;
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
    for (let i = newRootState.head; i < newQ.length; i += 1) {
      const entry = newQ[i];
      if (!entry.removed) existing.push(entry);
    }
    const merged = [];
    let a = 0;
    let b = 0;
    while (a < existing.length || b < movedEntries.length) {
      if (
        b === movedEntries.length ||
        (a < existing.length && existing[a].seq <= movedEntries[b].seq)
      ) {
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

    if (oldRootState !== newRootState) {
      // Read-only classification first: split entries cannot reserve across
      // families; wholly moved ones (singles included — a single gathers like
      // a one-member combo) gather anew in the joined family.
      const whollyMoved = [];
      const stayingCombos = [];
      const straddlers = [];
      const oldQ = oldRootState.queue;
      for (let i = oldRootState.head; i < oldQ.length; i += 1) {
        const entry = oldQ[i];
        if (entry.removed) continue;
        let inMoved = 0;
        for (const g of entry.comboGates) if (moved.has(g)) inMoved += 1;
        if (inMoved > 0 && inMoved < entry.comboGates.length) straddlers.push(entry);
        else if (inMoved === entry.comboGates.length) whollyMoved.push(entry);
        else stayingCombos.push(entry);
      }

      // Capacity is decided before anything settles, so a rejected move rolls
      // back trivially with no cut at all.
      let subtreeDraw = 0;
      for (const node of moved) subtreeDraw += effectiveDraw(node);
      if (newRootState.poolUsed + subtreeDraw > newRootState.poolLimit) {
        throw new QuotaExceededError(
          'QUOTA_EXCEEDED',
          'cannot reparent: the new family pool cannot cover the borrowed quota',
        );
      }

      // Uncommitted reservations cannot cross families. Every release below
      // happens while the entries and gates still reference the old root.
      for (const entry of straddlers) settleEntry(entry, 'refused');
      for (const entry of whollyMoved) releaseHolds(entry);
      for (const entry of stayingCombos) releaseMovedHolds(entry, moved, oldRootState);

      // Settle the admitted borrow in the old pool, re-borrow from the new one.
      const oldDirectParent = state.parent;
      oldRootState.poolUsed -= subtreeDraw;
      newRootState.poolUsed += subtreeDraw;
      for (const node of moved) {
        oldRootState.members.delete(node);
        newRootState.members.add(node);
        node.root = newRootState;
        node.drawGen = newRootState.poolGen; // the draw now counts in the new pool
      }
      // Re-link the tree FIRST, so borrow walks below traverse the NEW chain.
      if (oldDirectParent !== null) oldDirectParent.children.delete(state);
      newParentState.children.add(state);
      state.parent = newParentState;
      if (state === oldRootState) {
        // The old root gate becomes an ordinary child: its next window fire
        // is a plain own-window flip; the new root owns the shared pool.
        state.isRoot = false;
      }
      // Outstanding occupations keep running with the very same handles;
      // only their borrow links are rebuilt against the new ancestor chain.
      const processedSegs = new Set();
      for (const node of moved) {
        for (const rec of node.openHandles) {
          if (moved.has(rec.seg.member) && !processedSegs.has(rec.seg)) {
            processedSegs.add(rec.seg);
            reborrowSeg(rec.seg, oldRootState, newRootState);
          }
        }
      }
      migrateQueue(oldRootState, newRootState, moved);
      // The moved waiters carry deadlines stamped by the old family clock;
      // make the joined clock at least as large so the monotonic guarantee
      // survives the move and no deadline is judged already past by accident.
      if (oldRootState.lastNow > newRootState.lastNow) {
        newRootState.lastNow = oldRootState.lastNow;
      }
    } else {
      // Same family: only the direct link moves; the shared pool is
      // untouched, so the old direct parent sees no cut at all. Outstanding
      // reservations and lend links keep pointing at their original gates.
      state.parent.children.delete(state);
      newParentState.children.add(state);
      state.parent = newParentState;
    }
  }

  // --- Enqueueing ------------------------------------------------------------

  function makeEntry({
    owner,
    combo,
    units,
    resolve,
    reject,
    signal,
    waitCharges,
    comboGates,
    comboUnits,
    totalUnits,
  }) {
    const t = now();
    const entry = {
      owner,
      combo,
      units,
      resolve,
      reject,
      seq: (enqueueSeq += 1),
      enqueuedAt: t,
      deadline: t + owner.maxWaitMs,
      timer: null,
      removed: false,
      signal: signal !== undefined && signal !== null ? signal : null,
      onAbort: null,
      waitCharges,
      yields: 0, // total overtakes conceded while blocked
      blockG: -1, // serveWave at which this combo last registered as blocked
      holdsHeld: false,
      segs: null,
      comboGates: comboGates ?? null,
      comboUnits: comboUnits ?? null,
      totalUnits: totalUnits ?? units,
    };
    entry.onAbort = () => onEntryAbort(entry);
    return entry;
  }

  function enqueueEntry(r, entry) {
    if (entry.signal !== null) {
      entry.signal.addEventListener('abort', entry.onAbort, { once: true });
    }
    armEntry(entry, entry.owner.maxWaitMs);
    if (entry.deadline < r.minDeadline) r.minDeadline = entry.deadline;
    r.queue.push(entry);
  }

  // Shared admission path for one single-gate request and one combination.
  // The whole reservation is gathered up front: the member's own remainder,
  // lends registered step by step up the direct-parent chain, and the root
  // pool for whatever remains. With no earlier arrival a fully gathered entry
  // commits to an occupation in the same cut; otherwise the hold waits for the
  // entry's turn and the pool flip reclaims it before allocation. A shortfall
  // with wait cap zero refuses QUOTA_EXCEEDED on the spot; otherwise the entry
  // queues in arrival order and settles independently of every other entry.
  function admit({ combo, comboGates, comboUnits, totalUnits, signal }) {
    const r = state.root;
    // waiting mirrors the occupation the entry would commit: every member
    // carries its own slice. A single charges its own gate only.
    const waitCharges = combo
      ? comboGates.map((g) => ({ gate: g, units: comboUnits.get(g) }))
      : [{ gate: state, units: totalUnits }];
    const holder = makeEntry({
      owner: state,
      combo,
      units: totalUnits,
      resolve: () => {},
      reject: () => {},
      signal: null,
      waitCharges: [],
      comboGates,
      comboUnits,
      totalUnits,
    });
    const gathered = tryGather(holder, buildSegs(comboGates, (g) => comboUnits.get(g)));

    if (gathered && r.liveQueued === 0) {
      state.granted += 1;
      return Promise.resolve(commitEntry(holder));
    }
    if (state.maxWaitMs === 0) {
      // Zero wait refuses on the spot. A shortfall already rolled back; a
      // gathered entry still cannot overtake earlier arrivals. rolledBack
      // tracks whole-combination rollbacks only; a refused single moves just
      // the refused counter.
      if (gathered) releaseHolds(holder);
      if (!gathered && combo) state.rolledBack += 1;
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    if (!gathered && combo) {
      // Any step short: the plan touched nothing; count one whole rollback and
      // queue; the combo gathers again at every pool window flip.
      state.rolledBack += 1;
    }

    for (const charge of waitCharges) charge.gate.waiting += charge.units;
    r.liveQueued += 1;
    return new Promise((resolve, reject) => {
      holder.resolve = resolve;
      holder.reject = reject;
      holder.signal = signal !== undefined && signal !== null ? signal : null;
      holder.onAbort = () => onEntryAbort(holder);
      holder.waitCharges = waitCharges;
      enqueueEntry(r, holder);
    });
  }

  function acquire(units = 1, signal) {
    if (typeof units !== 'number') {
      throw new TypeError('units must be a number');
    }
    if (!Number.isInteger(units)) {
      throw new RangeError('units must be an integer');
    }
    assertSignal(signal);
    // A unit count above this gate's own limit is NOT a parameter error: the
    // remainder first uses the gate's own headroom, then borrows up the
    // direct-parent chain (registering every lend) and finally from the family
    // pool. Only non-positive counts are RangeErrors here.
    if (units < 1) {
      throw new RangeError('units must be a positive integer');
    }
    // A signal that fired before the call settles as CANCELLED without ever
    // entering the queue or touching quota.
    if (signal !== undefined && signal !== null && signal.aborted) {
      state.cancelled += 1;
      return Promise.reject(
        new QuotaExceededError('CANCELLED', 'acquire was cancelled before it could queue'),
      );
    }
    const comboUnits = new Map([[state, units]]);
    return admit({
      combo: false,
      comboGates: [state],
      comboUnits,
      totalUnits: units,
      signal,
    });
  }

  // Validate an acquireAll member list and translate public gates into
  // internal member states. Nothing here mutates runtime state.
  function validateMembers(members) {
    if (!Array.isArray(members)) {
      throw new TypeError('members must be an array of { gate, units }');
    }
    if (members.length === 0) {
      throw new RangeError('members must contain at least one gate');
    }
    const r = state.root;
    const seen = new Set();
    const gates = [];
    const unitMap = new Map();
    let total = 0;
    for (const member of members) {
      if (member === null || typeof member !== 'object') {
        throw new TypeError('each member must be { gate, units }');
      }
      const { gate, units } = member;
      if (gate === null || typeof gate !== 'object' || gateState.get(gate) === undefined) {
        throw new TypeError('member gate must be a gate created by createGate');
      }
      const g = gateState.get(gate);
      if (typeof units !== 'number') {
        throw new TypeError('units must be a number');
      }
      if (!Number.isInteger(units)) {
        throw new RangeError('units must be an integer');
      }
      if (units < 1) {
        throw new RangeError('units must be a positive integer');
      }
      if (seen.has(g)) {
        throw new RangeError('the same gate must not appear twice in one acquireAll');
      }
      if (g.root !== r) {
        throw new RangeError('all member gates must belong to the same gate family');
      }
      seen.add(g);
      gates.push(g);
      unitMap.set(g, units);
      total += units;
    }
    // Grandparent/grandchild loop: one member must not sit on another's
    // ancestor chain.
    for (const g of gates) {
      let node = g.parent;
      while (node !== null) {
        if (seen.has(node)) {
          throw new RangeError('member gates must not be ancestors of one another');
        }
        node = node.parent;
      }
    }
    return { gates, unitMap, total };
  }

  function acquireAll(members, signal) {
    assertSignal(signal);
    const { gates: comboGates, unitMap: comboUnits, total: totalUnits } = validateMembers(members);
    // A signal that fired before the call settles as CANCELLED without ever
    // entering the queue or reserving anything.
    if (signal !== undefined && signal !== null && signal.aborted) {
      state.cancelled += 1;
      return Promise.reject(
        new QuotaExceededError('CANCELLED', 'acquireAll was cancelled before it could queue'),
      );
    }
    return admit({ combo: true, comboGates, comboUnits, totalUnits, signal });
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
      reserved: reservedOn(state),
      rolledBack: state.rolledBack,
    };
  }

  const gate = { acquire, acquireAll, stats, updateLimit, updateWindowMs, updateMaxWaitMs, reparent };
  gateState.set(gate, state);
  return gate;
}
