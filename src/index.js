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

// setTimeout/setInterval clamp delays beyond the signed 32-bit range to 1ms,
// which would expire a patient waiter — or flip a long window — almost
// immediately. Long delays therefore fire in real-time chunks: each fire
// consumes one chunk and re-arms the remainder, so the measured wait is real
// elapsed time exactly like a single timer.
const MAX_TIMER_DELAY = 0x7fffffff;

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

// --- Family-root-wide helpers (module scope) --------------------------------
//
// A queued entry does not always live in the same family as the gate through
// which it entered: a reparent can move every member gate of a combo while the
// entry-point gate stays behind, and the entry then queues in the JOINED
// family. Such helpers therefore take the queue root explicitly (entry.qRoot)
// instead of guessing it from entry.owner.root.

function rootNow(r) {
  const t = Date.now();
  if (t > r.lastNow) r.lastNow = t;
  return r.lastNow;
}

function effectiveDrawFor(node) {
  return node.drawGen === node.root.poolGen ? node.poolDraw : 0;
}

function chargeDrawFor(node, units) {
  if (node.drawGen !== node.root.poolGen) {
    node.drawGen = node.root.poolGen;
    node.poolDraw = 0;
  }
  node.poolDraw += units;
}

// --- Combo reservations -----------------------------------------------------
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

function buildSegsFor(comboGates, unitsOf) {
  return comboGates.map((g) => ({
    gate: g,
    units: unitsOf(g),
    own: 0,
    ownReleased: false, // member own slice already reclaimed by an own flip
    links: [],
    gen: -1,
  }));
}

function ownHeadroomAt(g, claimed) {
  return Math.max(0, g.limit - g.usedOwn - g.pendingOwn - g.pendingLends - (claimed.get(g) ?? 0));
}

// Pure plan: fills seg.own / seg.links and returns total pool need. A
// member slice larger than that gate's own limit simply borrows the excess
// up the chain — member limits never cap a combo, only the root pool does.
function planGatherFor(r, segs) {
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
    const own = Math.min(need, ownHeadroomAt(m, claimed));
    seg.own = own;
    claimed.set(m, (claimed.get(m) ?? 0) + own);
    need -= own;
    let cur = m.parent;
    while (need > 0 && cur !== null && cur !== r) {
      const lend = Math.min(need, ownHeadroomAt(cur, claimed));
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
function applyGatherFor(entry, r, segs) {
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
  entry.holdValid = true;
  entry.holdGen = gen;
}

// Plan, check pool room, apply — or reject without touching anything. The
// whole combo draws pool room: a member's own slice and every ancestor lend
// are partitions OF the pool, not capacity beyond it. Feasibility is
// therefore simply poolUsed + total combo units <= poolLimit; the plan's
// own/lender/pool partition only decides which own window each unit lands
// on (and thus where its flip reclaims it).
function tryGatherFor(entry, r, segs) {
  let total = 0;
  for (const seg of segs) total += seg.units;
  if (r.poolUsed + total > r.poolLimit) {
    for (const seg of segs) {
      seg.own = 0;
      seg.links = [];
    }
    return false;
  }
  planGatherFor(r, segs);
  applyGatherFor(entry, r, segs);
  return true;
}

// Same whole reservation for one gate: the entry's own current-window slice,
// lends up the direct parent chain, then the root pool. A single-gate
// occupation gathers exactly the way a one-member combo would — its own
// limit never caps the request, only the family pool does.
function tryGatherOneFor(entry, r, g, units) {
  return tryGatherFor(entry, r, buildSegsFor([g], () => units));
}

// Release a held reservation wholesale. Pool room is refunded only while
// the charged pool window is still the current one; a pool flip already
// reclaimed it (the link is marked released and its gen is stale). Returns
// whether current-window pool room came back.
function releaseHoldsEntry(entry) {
  if (!entry.holdsHeld || entry.segs === null) return false;
  const r = entry.qRoot;
  let refunded = false;
  for (const seg of entry.segs) {
    const m = seg.gate;
    if (!seg.ownReleased && seg.own > 0) {
      seg.ownReleased = true;
      m.pendingOwn -= seg.own;
      m.resvAsMember.delete(entry);
      if (seg.gen === r.poolGen) {
        r.poolUsed -= seg.own;
        refunded = true;
      }
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
      if (link.gen === r.poolGen) {
        r.poolUsed -= link.units;
        refunded = true;
      }
    }
  }
  entry.holdsHeld = false;
  entry.holdValid = false;
  entry.segs = null;
  if (refunded) r.roomVer += 1; // current-window room came back
  return refunded;
}

function detachSignal(entry) {
  if (entry.signal !== null) {
    entry.signal.removeEventListener('abort', entry.onAbort);
    entry.signal = null;
  }
}

// --- Handles ----------------------------------------------------------------
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

// Commit a fully gathered combo: provisional promises become admitted
// occupations. Pool room was charged at gather time, so poolUsed does not
// move here.
function commitComboFor(entry, r) {
  const records = [];
  const lenderCleanup = new Set();
  for (const seg0 of entry.segs) {
    const m = seg0.gate;
    const units = seg0.units;
    chargeDrawFor(m, units);
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

// One queue entry leaves its queue exactly once, and exactly one of
// granted / refused / expired / cancelled moves as a result. Waiting and
// reservation charges belong to the member gates wherever they now live;
// the terminal counter belongs to the entry-point gate, so an entry queued
// in a joined family still settles against the gate that started it and
// zeroes every charge in one snapshot.
function settleEntryEntry(entry, outcome) {
  if (entry.removed) return;
  const r = entry.qRoot;
  entry.removed = true;
  r.liveQueued -= 1;
  clearTimeout(entry.timer);
  detachSignal(entry);
  entry.owner.ownedEntries.delete(entry);
  for (const charge of entry.waitCharges) charge.gate.waiting -= charge.units;
  if (outcome === 'granted') {
    // Both singles and combos arrive here holding a gathered reservation;
    // the same commit turns provisional promises into the occupation.
    const handle = commitComboFor(entry, r);
    entry.owner.granted += 1;
    entry.resolve(handle);
    return;
  }
  // Expiry, cancellation and refusal give back any hold; none of those
  // outcomes itself counts as a reservation rollback. Pool room that comes
  // back is a real chance for the waiters behind, so the caller re-serves.
  releaseHoldsEntry(entry);
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
// clock and the wall clock disagree (mocked/frozen clocks). Delays beyond
// the 32-bit timer range fire in chunks so the cap is honoured in full
// instead of overflowing to 1ms.
function armEntry(entry, delay) {
  const chunk = Math.min(Math.max(1, delay), MAX_TIMER_DELAY);
  entry.timer = setTimeout(() => {
    if (entry.removed) return;
    const remaining = delay - chunk;
    if (remaining > 0) {
      armEntry(entry, remaining);
      return;
    }
    // Expiry settles before any grant: the fairness pass below only runs
    // once this entry has left as WAIT_EXPIRED.
    settleEntryEntry(entry, 'expired');
    serveQueueRoot(entry.qRoot);
  }, chunk);
  if (typeof entry.timer.unref === 'function') entry.timer.unref();
}

// At the same moment expiry beats cancellation: an abort arriving once the
// deadline has already passed settles as WAIT_EXPIRED.
function onEntryAbort(entry) {
  if (entry.removed) return;
  if (entry.deadline <= rootNow(entry.qRoot)) {
    settleEntryEntry(entry, 'expired');
  } else {
    settleEntryEntry(entry, 'cancelled');
  }
  // A withdrawn head must not freeze the waiters behind it: retry the queue
  // in the same cut, still strictly in arrival order.
  serveQueueRoot(entry.qRoot);
}

function enqueueEntry(r, entry) {
  entry.owner.ownedEntries.add(entry);
  if (entry.signal !== null) {
    entry.signal.addEventListener('abort', entry.onAbort, { once: true });
  }
  for (const charge of entry.waitCharges) charge.gate.waiting += charge.units;
  armEntry(entry, entry.owner.maxWaitMs);
  r.queue.push(entry);
  r.liveQueued += 1;
  // Stamp the room version this arrival gathered against. A new arrival does
  // not itself open a pass: the queue is released at pool-window flips and,
  // when a blocked head leaves early (withdrawal, expiry, shrink refusal),
  // by the retry that event triggers — a later request never jumps ahead at
  // arrival time, but waiters behind a departed head do not stall.
  entry.roomVer = r.roomVer;
}

// Try one entry against the current room. Returns true when it committed.
// A hold gathered earlier commits as long as no own/lender window flip has
// partly reclaimed it (holdValid) and its pool window is still current
// (holdGen): its units charged poolUsed at gather time, so nothing ahead
// committing can take that room away. An entry without a live hold is
// replanned only when the room version moved since its last failed plan,
// which keeps a permanently blocked head costing O(1) per unrelated arrival.
function attemptEntry(entry, r, countRollback, force) {
  const holdLive = entry.holdsHeld && entry.holdValid && entry.holdGen === r.poolGen;
  if (entry.holdsHeld && !holdLive) releaseHoldsEntry(entry);
  if (holdLive) {
    // The hold charged its units when it gathered. It may commit in an
    // event-driven retry only while the window it charged is still within
    // the current pool cap: a pool shrink can leave the running window
    // oversubscribed by occupations it cannot tear apart, in which case the
    // hold waits for the fresh window at the next flip. At a flip holds are
    // always stale, so this guard only ever applies to mid-window retries.
    if (!countRollback && r.poolUsed > r.poolLimit) return false;
    settleEntryEntry(entry, 'granted');
    return true;
  }
  // Event retries force one fresh plan of the head: the head leaving is
  // itself a reason to retry even when it held no pool room (so the room
  // version did not move). Otherwise the room-version cache keeps a known
  // blocked head at O(1) per unrelated event.
  if (!countRollback && !force && entry.roomVer === r.roomVer) return false;
  entry.roomVer = r.roomVer;
  let ok;
  if (entry.combo) {
    const segs = buildSegsFor(entry.comboGates, (g) => entry.comboUnits.get(g));
    ok = tryGatherFor(entry, r, segs);
  } else {
    ok = tryGatherOneFor(entry, r, entry.owner, entry.units);
  }
  if (ok) {
    settleEntryEntry(entry, 'granted');
    return true;
  }
  // Only a genuine pool-window flip counts a failed combo gather as one
  // whole rollback; event-driven re-attempts move no counter.
  if (countRollback && entry.combo) entry.owner.rolledBack += 1;
  return false;
}

// Allocation walks arrivals in order against the current pool.
//
// At a pool-window flip (countRollback) a head combo that cannot be gathered
// whole does not starve later arrivals: a later request that fits outright
// passes it and the combo yields once; after yielding twice (cumulatively,
// across flips) it hardens into an ordinary barrier like a non-fitting
// single. A failed combo gather at a flip moves the whole-rollback counter.
//
// An event-driven retry (withdrawal, expiry, shrink refusal) only reopens
// the head: it serves arrivals while they keep fitting and stops at the
// first blocked entry — it never lets a later request overtake a live head
// (those passes belong to flips), but waiters behind a departed head no
// longer stall. The room-version cache makes such a retry O(served + 1),
// and every served entry leaves the queue, so the total over a queue's life
// stays linear.
function allocateRoot(r, countRollback) {
  const q = r.queue;
  let wave = r.serveWave; // overtakes counted so far, monotonic across flips
  let stopWave = Infinity; // earliest wave at which a blocker hardens
  let blockerCount = 0; // blocked combos registered earlier in this flip
  let atHead = true; // event mode forces one fresh plan of the current head
  for (let i = r.head; i < q.length; i += 1) {
    const entry = q[i];
    if (entry.removed) continue;
    if (countRollback && wave >= stopWave) break;
    if (attemptEntry(entry, r, countRollback, atHead)) {
      if (countRollback && blockerCount > 0) wave += 1; // one yield per blocker
      continue;
    }
    atHead = false;
    if (!countRollback) break; // event retries do not overtake a live head
    if (!entry.combo) break; // single-gate requests are ordinary barriers
    // Blocked combo at a flip: register a soft barrier and count one whole
    // rollback (already moved in attemptEntry); remember where it hardens.
    if (entry.blockG !== -1) entry.yields += wave - entry.blockG;
    entry.blockG = wave;
    const hardensAt = wave + (2 - entry.yields);
    blockerCount += 1;
    if (hardensAt < stopWave) stopWave = hardensAt;
  }
  if (countRollback) r.serveWave = wave;
  while (r.head < q.length && q[r.head].removed) r.head += 1;
  if (r.head > 32 && r.head * 2 >= q.length) {
    r.queue = q.slice(r.head);
    r.head = 0;
  }
}

// Event-driven fairness pass. Re-entrant calls (e.g. an acquire started from
// inside a grant resolver) skip: the in-progress pass already walks the queue
// in arrival order and the next event retries.
function serveQueueRoot(r) {
  if (r.serving) return;
  r.serving = true;
  try {
    allocateRoot(r, false);
  } finally {
    r.serving = false;
  }
}

// --- Pure tree helpers --------------------------------------------------------

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
// member own quota) belongs to a subtree that is leaving the family. Returns
// whether current-window pool room came back.
function releaseMovedHolds(entry, moved, oldRootState) {
  if (!entry.holdsHeld) return false;
  let refunded = false;
  let touched = false;
  for (const seg of entry.segs) {
    if (!seg.ownReleased && seg.own > 0 && moved.has(seg.gate)) {
      seg.ownReleased = true;
      seg.gate.pendingOwn -= seg.own;
      seg.gate.resvAsMember.delete(entry);
      if (seg.gen === oldRootState.poolGen) {
        oldRootState.poolUsed -= seg.own;
        refunded = true;
      }
      touched = true;
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
      if (link.gen === oldRootState.poolGen) {
        oldRootState.poolUsed -= link.units;
        refunded = true;
      }
      touched = true;
    }
  }
  // Part of the hold left with the subtree: the remainder can never cover
  // the whole request, so it must be re-gathered before it can commit.
  if (touched) entry.holdValid = false;
  if (refunded) oldRootState.roomVer += 1;
  return refunded;
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
    // Entries that entered through this gate and are still queued, even when
    // their queue lives in another family after a reparent (a combo whose
    // member gates all moved while this entry-point gate stayed behind).
    ownedEntries: new Set(),
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
      serving: false, // re-entrancy guard for event-driven fairness passes
      roomVer: 0, // bumped whenever current-window pool room might increase
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

  function reservedOn(g) {
    return g === g.root ? g.root.poolResvUnits : g.pendingOwn + g.pendingLends;
  }

  // --- Window flips ----------------------------------------------------------

  // A non-root own window reclaims quota borrowed FROM this gate, uncommitted
  // reservations first (member own slice, then ancestor lends), then admitted
  // lends. Occupations admitted through this gate end with its window.
  // Handles never break.
  function flipOwn(target) {
    rootNow(target.root);
    const r = target.root;

    // 1a) Uncommitted member-own promises on the flipping gate.
    let roomFreed = false;
    for (const entry of target.resvAsMember) {
      if (entry.removed || !entry.holdsHeld) continue;
      for (const seg of entry.segs) {
        if (seg.gate !== target || seg.ownReleased || seg.own === 0) continue;
        seg.ownReleased = true;
        target.pendingOwn -= seg.own;
        if (seg.gen === r.poolGen) {
          r.poolUsed -= seg.own;
          roomFreed = true;
        }
        // The hold no longer covers the whole request in the current pool
        // window; it must be released and re-gathered before it can commit.
        entry.holdValid = false;
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
          if (link.gen === r.poolGen) {
            r.poolUsed -= link.units;
            roomFreed = true;
          }
          entry.holdValid = false;
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
        roomFreed = true;
      }
    }
    target.lentLinks.clear();
    // The pool window kept running but current-window room came back; bump
    // the version so the next event-driven pass replans cached blockers.
    // Allocation itself stays owned by pool-window flips, like the baseline.
    if (roomFreed) r.roomVer += 1;

    target.ownGen += 1;
    target.usedOwn = 0;
    target.inFlight = 0;
    target.borrowedInFlight = 0;
    target.lentInFlight = 0;
    target.pendingOwn = 0;
    target.pendingLends = 0;
  }

  // Expiry is judged before quota allocation: anything due at or before the
  // flip moment expires rather than being granted.
  function sweepExpired(r, t) {
    const q = r.queue;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (!entry.removed && entry.deadline <= t) settleEntryEntry(entry, 'expired');
    }
  }

  function flipRoot() {
    const r = state;
    const t = rootNow(r);

    // Expiry is judged before anything is reclaimed or granted.
    sweepExpired(r, t);

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
    r.roomVer += 1; // a brand-new room: cached blockers replan below

    // 4) Serve the queue in arrival order, re-gathering combos as they come.
    //    Window retries are the attempts that move whole-rollback counters.
    r.serving = true;
    try {
      allocateRoot(r, true);
    } finally {
      r.serving = false;
    }
  }

  function onWindowFire() {
    if (state.isRoot) flipRoot();
    else flipOwn(state);
  }

  // The window flip really fires on the event loop, even when the window is
  // longer than the 32-bit timer range: fire in real-time chunks and flip
  // only once a full window has elapsed. The next window is armed before the
  // flip runs, so the cadence survives anything the flip itself does.
  function armWindow(delay) {
    const chunk = Math.min(Math.max(1, delay), MAX_TIMER_DELAY);
    state.timer = setTimeout(() => {
      const remaining = delay - chunk;
      if (remaining > 0) {
        armWindow(remaining);
        return;
      }
      armWindow(state.windowMs);
      onWindowFire();
    }, chunk);
    if (typeof state.timer.unref === 'function') state.timer.unref();
  }

  armWindow(windowMs);

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
  // Occupations are never torn apart; a combo that can no longer be satisfied
  // is refused as a whole, releasing its reservation in the same cut. Only
  // the family pool cap can refuse, and this family's queue already holds
  // exactly the entries it governs — including waiters that migrated in from
  // another family — while waiters this gate owns but another family now
  // queues are governed by that family's pool.
  function refuseUnfittable(newLimit) {
    const poolShrink = state.isRoot;
    let touched = false;
    const r = state.root;
    const q = r.queue;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (entry.removed) continue;
      // The whole request must fit the pool; member slices above a member's
      // own limit are simply borrowed up the chain, so only shrinking the
      // family pool itself can make it impossible to ever satisfy.
      const need = entry.combo ? entry.totalUnits : entry.units;
      if (poolShrink && need > newLimit) {
        settleEntryEntry(entry, 'refused');
        touched = true;
      }
    }
    // A refused head must not stop the waiters behind it: re-serve the queue
    // in the same cut, still in arrival order.
    if (touched) serveQueueRoot(r);
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
    clearTimeout(state.timer);
    armWindow(newWindowMs);
  }

  function updateMaxWaitMs(newMaxWaitMs) {
    assertInteger(newMaxWaitMs, 'maxWaitMs');
    if (newMaxWaitMs < 0) throw new RangeError('maxWaitMs must not be negative');
    // Existing waiters keep their original enqueue time; only the cap moves.
    // Waiters already past the new deadline settle as expired on the spot;
    // nothing is granted early and no deadline moves backwards into life.
    // Owned entries are followed even into a family joined by a reparent.
    state.maxWaitMs = newMaxWaitMs;
    const touchedRoots = new Set();
    for (const entry of [...state.ownedEntries]) {
      if (entry.removed) continue;
      const r = entry.qRoot;
      touchedRoots.add(r);
      const t = rootNow(r);
      const newDeadline = entry.enqueuedAt + newMaxWaitMs;
      if (newDeadline <= t) {
        settleEntryEntry(entry, 'expired');
        continue;
      }
      if (newDeadline !== entry.deadline) {
        entry.deadline = newDeadline;
        clearTimeout(entry.timer);
        armEntry(entry, newDeadline - t);
      }
    }
    // Expiring an entry (possibly a head) reopens the fairness pass in every
    // family this gate owned waiters in.
    for (const r of touchedRoots) serveQueueRoot(r);
  }

  // Partition the old/new queues at the moved-entry boundary, preserving
  // global arrival order. Entries were already settled or had their
  // reservations released by the caller; removed entries are skipped.
  function migrateQueue(oldRootState, newRootState, movedEntries) {
    const staying = [];
    const movedList = [];
    const oldQ = oldRootState.queue;
    for (let i = oldRootState.head; i < oldQ.length; i += 1) {
      const entry = oldQ[i];
      if (entry.removed) continue;
      if (movedEntries.has(entry)) {
        // The overtake count belongs to the old family's fairness clock;
        // the request starts yielding fresh in the joined family.
        entry.qRoot = newRootState;
        if (entry.combo) {
          entry.yields = 0;
          entry.blockG = -1;
        }
        movedList.push(entry);
      } else {
        staying.push(entry);
      }
    }
    oldRootState.queue = staying;
    oldRootState.head = 0;
    oldRootState.liveQueued -= movedList.length;

    const merged = [];
    let a = 0;
    let b = 0;
    const newQ = newRootState.queue;
    const existingStart = newRootState.head;
    while (a < newQ.length - existingStart || b < movedList.length) {
      const ea = a < newQ.length - existingStart ? newQ[existingStart + a] : null;
      if (ea !== null && (b === movedList.length || ea.seq <= movedList[b].seq)) {
        merged.push(ea);
        a += 1;
      } else {
        merged.push(movedList[b]);
        b += 1;
      }
    }
    newRootState.queue = merged;
    newRootState.head = 0;
    newRootState.liveQueued += movedList.length;
  }

  // Wholly moved waiters gather anew in the joined family, in merged arrival
  // order. This only takes provisional holds: an adjustment cut never grants
  // early, so a request that gathers here still commits at the next pool
  // flip; one that cannot gather (combos included) simply waits for that
  // flip and no counter moves for this administrative re-gather.
  function regatherMoved(newRootState, movedEntries) {
    for (const entry of newRootState.queue) {
      if (entry.removed || !movedEntries.has(entry)) continue;
      if (entry.holdsHeld) releaseHoldsEntry(entry);
      if (entry.combo) {
        const segs = buildSegsFor(entry.comboGates, (g) => entry.comboUnits.get(g));
        tryGatherFor(entry, newRootState, segs);
      } else {
        tryGatherOneFor(entry, newRootState, entry.owner, entry.units);
      }
      // The version belongs to the joined family now; stamp it so a later
      // event does not mistake an old-family stamp for a fresh plan.
      entry.roomVer = newRootState.roomVer;
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
      // Read-only classification first: split combos cannot reserve across
      // families; wholly moved combos and moved single requests gather anew
      // in the joined family. Queue membership follows the request itself,
      // keyed by entry.qRoot, not by where the entry-point gate ends up.
      const whollyMoved = [];
      const movedSingles = [];
      const stayingCombos = [];
      const straddlers = [];
      const movedEntries = new Set();
      const oldQ = oldRootState.queue;
      for (let i = oldRootState.head; i < oldQ.length; i += 1) {
        const entry = oldQ[i];
        if (entry.removed) continue;
        if (!entry.combo) {
          // A moved single's lend chain pointed into the old family; its hold
          // is released below and re-gathered after the join.
          if (moved.has(entry.owner)) {
            movedSingles.push(entry);
            movedEntries.add(entry);
          }
          continue;
        }
        let inMoved = 0;
        for (const g of entry.comboGates) if (moved.has(g)) inMoved += 1;
        if (inMoved > 0 && inMoved < entry.comboGates.length) {
          straddlers.push(entry);
        } else if (inMoved === entry.comboGates.length) {
          whollyMoved.push(entry);
          movedEntries.add(entry);
        } else {
          stayingCombos.push(entry);
        }
      }

      // Capacity is decided before anything settles, so a rejected move rolls
      // back trivially with no cut at all.
      let subtreeDraw = 0;
      for (const node of moved) subtreeDraw += effectiveDrawFor(node);
      if (newRootState.poolUsed + subtreeDraw > newRootState.poolLimit) {
        throw new QuotaExceededError(
          'QUOTA_EXCEEDED',
          'cannot reparent: the new family pool cannot cover the borrowed quota',
        );
      }

      // Uncommitted reservations cannot cross families. Every release below
      // happens while the entries and gates still reference the old root.
      for (const entry of straddlers) settleEntryEntry(entry, 'refused');
      for (const entry of whollyMoved) releaseHoldsEntry(entry);
      for (const entry of movedSingles) releaseHoldsEntry(entry);
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
      migrateQueue(oldRootState, newRootState, movedEntries);
      regatherMoved(newRootState, movedEntries);
      // The moved waiters carry deadlines stamped by the old family clock;
      // make the joined clock at least as large so the monotonic guarantee
      // survives the move and no deadline is judged already past by accident.
      if (oldRootState.lastNow > newRootState.lastNow) {
        newRootState.lastNow = oldRootState.lastNow;
      }
      // No serve on either side: reparent is one adjustment cut and never
      // grants queued requests early. Moved holds gathered anew above commit
      // at the joined family's next pool flip; old-family waiters (whose room
      // grew when the subtree left) likewise wait for their next flip, or for
      // a head withdrawal/expiry that explicitly retries them.
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
    const t = rootNow(owner.root);
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
      holdValid: true, // false once an own/lender flip partly reclaimed the hold
      holdGen: -1, // pool window the hold charged
      segs: null,
      roomVer: 0, // room version this entry last planned against
      // The family whose pool and queue this entry currently settles against.
      // Usually owner.root, but a wholly migrated combo keeps queuing in the
      // joined family even when the entry-point gate stayed behind.
      qRoot: owner.root,
      comboGates: comboGates ?? null,
      comboUnits: comboUnits ?? null,
      totalUnits: totalUnits ?? units,
    };
    entry.onAbort = () => onEntryAbort(entry);
    return entry;
  }

  function acquire(units = 1, signal) {
    if (typeof units !== 'number') throw new TypeError('units must be a number');
    assertSignal(signal);
    // Over-limit is NOT a parameter error: the occupation borrows whatever
    // the gate's own quota cannot cover. Only non-positive or non-integer
    // counts are rejected here.
    if (!Number.isInteger(units) || units < 1) {
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
    const r = state.root;

    // Reserve up front, exactly like a one-member combination: the gate's own
    // remaining quota, then lends up the direct parent chain, then the pool.
    // With no earlier arrival the whole reservation commits in the same cut;
    // otherwise the hold waits for this request's turn and the pool flip
    // reclaims it before allocation.
    const holder = makeEntry({
      owner: state,
      combo: false,
      units,
      resolve: () => {},
      reject: () => {},
      signal: null,
      waitCharges: [],
    });
    const gathered = tryGatherOneFor(holder, r, state, units);

    if (gathered && r.liveQueued === 0) {
      state.granted += 1;
      return Promise.resolve(commitComboFor(holder, r));
    }
    if (state.maxWaitMs === 0) {
      // Zero wait refuses on the spot. A gathered request still cannot
      // overtake earlier arrivals; give its hold back first.
      if (gathered) releaseHoldsEntry(holder);
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }

    return new Promise((resolve, reject) => {
      holder.resolve = resolve;
      holder.reject = reject;
      holder.signal = signal !== undefined && signal !== null ? signal : null;
      holder.onAbort = () => onEntryAbort(holder);
      holder.waitCharges = [{ gate: state, units }];
      enqueueEntry(r, holder);
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
    const r = state.root;

    // waiting mirrors the occupation the combo would commit: every member
    // carries its own slice. The entry-point gate is charged only when it is
    // itself one of the members.
    const waitCharges = comboGates.map((g) => ({ gate: g, units: comboUnits.get(g) }));

    // Reserve up front. With no earlier arrival the whole reservation commits
    // in the same cut; otherwise the hold waits for the combo's turn and the
    // pool flip reclaims it before allocation.
    const holder = makeEntry({
      owner: state,
      combo: true,
      units: totalUnits,
      resolve: () => {},
      reject: () => {},
      signal: null,
      waitCharges: [],
      comboGates,
      comboUnits,
      totalUnits,
    });
    const gathered = tryGatherFor(holder, r, buildSegsFor(comboGates, (g) => comboUnits.get(g)));

    if (gathered && r.liveQueued === 0) {
      state.granted += 1;
      return Promise.resolve(commitComboFor(holder, r));
    }
    if (state.maxWaitMs === 0) {
      // Zero wait refuses on the spot. A shortfall already rolled back; a
      // gathered combo still cannot overtake earlier arrivals.
      if (gathered) releaseHoldsEntry(holder);
      if (!gathered) state.rolledBack += 1;
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    if (!gathered) {
      // Any step short: the plan touched nothing; count one whole rollback
      // here. The immediate fairness pass below is the same attempt and does
      // not count again; further counted retries happen at pool flips.
      state.rolledBack += 1;
    }

    return new Promise((resolve, reject) => {
      holder.resolve = resolve;
      holder.reject = reject;
      holder.signal = signal !== undefined && signal !== null ? signal : null;
      holder.onAbort = () => onEntryAbort(holder);
      holder.waitCharges = waitCharges;
      enqueueEntry(r, holder);
    });
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
