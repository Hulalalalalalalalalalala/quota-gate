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
    borrowedInFlight: 0, // borrowed units still occupied; reclaimed at root flip
    poolDraw: 0, // units charged into the current window of the root pool
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
      poolUsed: 0, // units granted through every gate in the current pool window
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

  // A request fits when the shared pool has room. A child covers whatever its
  // own remaining quota cannot by borrowing from the pool, so its own limit
  // never blocks a grant; only the pool does.
  function fits(units) {
    const r = state.root;
    return units <= r.poolLimit - r.poolUsed;
  }

  function applyGrant(target, units) {
    const targetRoot = target.root;
    targetRoot.poolUsed += units;
    target.poolDraw += units;
    if (target === targetRoot) {
      target.usedOwn += units;
      target.inFlight += units;
      return { ownPart: units, borrowPart: 0 };
    }
    const ownPart = Math.min(units, Math.max(0, target.limit - target.usedOwn));
    const borrowPart = units - ownPart;
    target.usedOwn += ownPart;
    target.inFlight += units;
    target.borrowedInFlight += borrowPart;
    return { ownPart, borrowPart };
  }

  function makeHandle(target, ownPart, borrowPart, ownGen, poolRoot, poolGen) {
    const handle = {
      _active: true,
      _target: target,
      _ownPart: ownPart,
      _borrowPart: borrowPart,
      _ownGen: ownGen,
      _poolRoot: poolRoot,
      _poolGen: poolGen,
      release() {
        if (!this._active) return;
        this._active = false;
        this._target.openHandles.delete(this);
        if (this._ownGen !== this._target.ownGen) return; // window closed; cleared
        this._target.inFlight -= this._ownPart;
        if (this._borrowPart > 0 && this._poolGen === this._poolRoot.poolGen) {
          this._target.inFlight -= this._borrowPart;
          this._target.borrowedInFlight -= this._borrowPart;
        }
        // A stale poolGen means the root window already reclaimed the
        // borrowed part; the handle stays valid but must not subtract twice.
      },
    };
    target.openHandles.add(handle);
    return handle;
  }

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
    entry.owner.waiting -= entry.units;
    if (outcome === 'granted') {
      const { ownPart, borrowPart } = applyGrant(entry.owner, entry.units);
      entry.owner.granted += 1;
      entry.resolve(
        makeHandle(
          entry.owner,
          ownPart,
          borrowPart,
          entry.owner.ownGen,
          entry.owner.root,
          entry.owner.root.poolGen,
        ),
      );
      return;
    }
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

  // Grant queued requests in arrival order, all-or-nothing; a request that
  // does not fit blocks every later one. New arrivals are queued behind
  // waiters instead of overtaking them, so the head is always served at the
  // next root window flip and nothing is postponed indefinitely.
  function allocate() {
    const r = state.root;
    const q = r.queue;
    while (r.head < q.length) {
      const entry = q[r.head];
      if (entry.removed) {
        r.head += 1;
        continue;
      }
      if (!fits(entry.units)) break;
      r.head += 1;
      settleEntry(entry, 'granted');
    }
    if (r.head > 32 && r.head * 2 >= q.length) {
      r.queue = q.slice(r.head);
      r.head = 0;
    }
  }

  function flipOwn(target) {
    now();
    target.ownGen += 1;
    target.usedOwn = 0;
    target.inFlight = 0; // occupations from the closed window clear automatically
    target.borrowedInFlight = 0;
    // Handles born in the closed window are inert now; drop them so handles
    // their holders never release cannot accumulate.
    for (const handle of target.openHandles) {
      if (handle._ownGen !== target.ownGen) target.openHandles.delete(handle);
    }
  }

  function flipRoot() {
    const t = now();
    sweepExpired(t);
    state.ownGen += 1;
    state.usedOwn = 0;
    state.inFlight = 0;
    state.borrowedInFlight = 0;
    for (const handle of state.openHandles) {
      if (handle._ownGen !== state.ownGen) state.openHandles.delete(handle);
    }
    // Reclaim borrowed quota from every family member. The reclaim does not
    // break admitted occupations: handles stay valid, only the borrowed
    // accounting settles, and the inFlight drop is visible in the same stats
    // snapshot as the pool window flip.
    state.poolGen += 1;
    state.poolUsed = 0;
    for (const member of state.members) {
      member.poolDraw = 0;
      if (member !== state) {
        member.inFlight -= member.borrowedInFlight;
        member.borrowedInFlight = 0;
      }
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
  // Occupations are never torn apart.
  function refuseUnfittable(ownerFilter, newLimit) {
    const r = state.root;
    const q = r.queue;
    let touched = false;
    for (let i = r.head; i < q.length; i += 1) {
      const entry = q[i];
      if (!entry.removed && entry.units > newLimit && ownerFilter(entry.owner)) {
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
      refuseUnfittable((owner) => owner === state, newLimit);
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

  // Partition the old root queue at the moved subtree boundary, then merge
  // the moved entries into the new root queue preserving global arrival
  // order (entry.seq is assigned monotonically at enqueue time).
  function migrateQueue(oldRootState, newRootState, moved) {
    const staying = [];
    const movedEntries = [];
    let movedLive = 0;
    const oldQ = oldRootState.queue;
    for (let i = oldRootState.head; i < oldQ.length; i += 1) {
      const entry = oldQ[i];
      if (entry.removed) continue;
      if (moved.has(entry.owner)) {
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
      for (const node of moved) {
        oldRootState.members.delete(node);
        newRootState.members.add(node);
        node.root = newRootState;
        // Outstanding occupations keep their place in the new pool window:
        // their handles go stale exactly when the NEW pool flips.
        for (const handle of node.openHandles) {
          handle._poolRoot = newRootState;
          handle._poolGen = newRootState.poolGen;
        }
      }
      migrateQueue(oldRootState, newRootState, moved);
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
      // untouched, so the old direct parent sees no cut at all.
      state.parent.children.delete(state);
      newParentState.children.add(state);
      state.parent = newParentState;
    }
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
      const { ownPart, borrowPart } = applyGrant(state, units);
      state.granted += 1;
      return Promise.resolve(
        makeHandle(state, ownPart, borrowPart, state.ownGen, r, r.poolGen),
      );
    }
    if (state.maxWaitMs === 0) {
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    state.waiting += units;
    r.liveQueued += 1;
    return new Promise((resolve, reject) => {
      const t = now();
      const entry = {
        owner: state,
        units,
        resolve,
        reject,
        seq: (enqueueSeq += 1),
        enqueuedAt: t,
        deadline: t + state.maxWaitMs,
        timer: null,
        removed: false,
        signal: signal !== undefined && signal !== null ? signal : null,
        onAbort: null,
      };
      entry.onAbort = () => onEntryAbort(entry);
      if (entry.signal !== null) {
        entry.signal.addEventListener('abort', entry.onAbort, { once: true });
      }
      armEntry(entry, state.maxWaitMs);
      if (entry.deadline < r.minDeadline) r.minDeadline = entry.deadline;
      r.queue.push(entry);
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
    };
  }

  const gate = { acquire, stats, updateLimit, updateWindowMs, updateMaxWaitMs, reparent };
  gateState.set(gate, state);
  return gate;
}
