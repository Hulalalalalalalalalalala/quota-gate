export class QuotaExceededError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'QuotaExceededError';
    this.code = code;
  }
}

// Internal state of every gate, shared with child gates through a WeakMap so
// the public gate object keeps its exact shape
// ({ acquire, stats, updateLimit, updateWindowMs, updateMaxWaitMs, reparent }).
const gateState = new WeakMap();

function assertInteger(value, name) {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`);
  }
}

// Accepts either a bare AbortSignal or an options object carrying one.
function asSignal(options) {
  if (options == null) return null;
  const candidate = typeof options.aborted === 'boolean' ? options : options.signal;
  if (
    candidate != null
    && typeof candidate.aborted === 'boolean'
    && typeof candidate.addEventListener === 'function'
  ) {
    return candidate;
  }
  return null;
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
    used: 0, // own units granted in this gate's current window
    inFlight: 0, // units occupied and not yet released/cleared
    waiting: 0, // units currently queued through this gate
    granted: 0,
    refused: 0,
    expired: 0,
    cancelled: 0,
    generation: 0, // bumped on every window flip, invalidates old handles
    borrowedInFlight: 0, // borrowed units still occupied; reclaimed on the root flip
    borrowGen: 0, // bumped whenever borrowedInFlight is reclaimed or cleared
    poolUnits: 0, // units this gate drew from the shared pool in the current root window
    parent: parentState,
    root: null,
    children: [], // direct child gates, kept in sync by createGate/reparent
  };
  const root = parentState === null ? state : parentState.root;
  state.root = root;
  if (parentState === null) {
    // The quota pool shared by the whole hierarchy lives on the root gate:
    // root.used counts units granted through every gate in the tree.
    root.queue = []; // queued requests from every gate, in arrival order
    root.head = 0; // index of the first live entry; entries are never shifted
    root.liveQueued = 0; // entries not yet granted or expired
    root.minDeadline = Infinity; // earliest live deadline, skips idle sweeps
    root.lastNow = 0; // monotonic clock: never moves backwards
    root.descendants = [];
  } else {
    root.descendants.push(state);
    parentState.children.push(state);
  }

  // The monotonic clock of the family this gate currently belongs to.
  function now() {
    const r = state.root;
    const t = Date.now();
    if (t > r.lastNow) r.lastNow = t;
    return r.lastNow;
  }

  // A request fits when the shared pool has room. A child covers whatever its
  // own remaining quota cannot by borrowing from the pool, so its own limit
  // never blocks a grant; only the pool does.
  function fits(r, units) {
    return units <= r.limit - r.used;
  }

  function applyGrant(target, units) {
    const r = target.root;
    target.poolUnits += units;
    if (target === r) {
      r.used += units;
      r.inFlight += units;
      return { ownPart: units, borrowPart: 0 };
    }
    const ownPart = Math.min(units, Math.max(0, target.limit - target.used));
    const borrowPart = units - ownPart;
    target.used += ownPart;
    target.inFlight += units;
    target.borrowedInFlight += borrowPart;
    r.used += units;
    return { ownPart, borrowPart };
  }

  function makeHandle(target, ownPart, borrowPart, generation, borrowGen) {
    let active = true;
    return {
      release() {
        if (!active) return;
        active = false;
        if (generation !== target.generation) return; // window closed; cleared
        target.inFlight -= ownPart;
        if (borrowPart > 0 && borrowGen === target.borrowGen) {
          target.inFlight -= borrowPart;
          target.borrowedInFlight -= borrowPart;
        }
        // A stale borrowGen means a root window already reclaimed the
        // borrowed part; the handle stays valid but must not subtract twice.
      },
    };
  }

  function detachSignal(entry) {
    if (entry.signal !== null) {
      entry.signal.removeEventListener('abort', entry.onAbort);
      entry.signal = null;
      entry.onAbort = null;
    }
  }

  function expireEntry(entry) {
    if (entry.removed) return;
    entry.removed = true;
    entry.pool.liveQueued -= 1;
    clearTimeout(entry.timer);
    detachSignal(entry);
    entry.owner.waiting -= entry.units;
    entry.owner.expired += 1;
    entry.reject(
      new QuotaExceededError('WAIT_EXPIRED', 'queued request expired before quota was available'),
    );
  }

  // Cancellation settles a still-queued request as CANCELLED and touches only
  // the waiting and cancelled counters. Expiry outranks cancellation at the
  // same moment: a request already due settles as WAIT_EXPIRED instead.
  function cancelEntry(entry) {
    if (entry.removed) return; // already granted or settled: cancelling does not count
    if (entry.deadline <= now()) {
      expireEntry(entry);
      return;
    }
    entry.removed = true;
    entry.pool.liveQueued -= 1;
    clearTimeout(entry.timer);
    detachSignal(entry);
    entry.owner.waiting -= entry.units;
    entry.owner.cancelled += 1;
    entry.reject(new QuotaExceededError('CANCELLED', 'queued request was cancelled'));
  }

  // Expiry is judged before quota allocation: anything due at or before the
  // flip moment expires rather than being granted.
  function sweepExpired(r, t) {
    if (r.minDeadline > t) return;
    let min = Infinity;
    for (let i = r.head; i < r.queue.length; i += 1) {
      const entry = r.queue[i];
      if (entry.removed) continue;
      if (entry.deadline <= t) expireEntry(entry);
      else if (entry.deadline < min) min = entry.deadline;
    }
    r.minDeadline = min;
  }

  function recomputeMinDeadline(r) {
    let min = Infinity;
    for (let i = r.head; i < r.queue.length; i += 1) {
      const entry = r.queue[i];
      if (!entry.removed && entry.deadline < min) min = entry.deadline;
    }
    r.minDeadline = min;
  }

  // Grant queued requests in arrival order, all-or-nothing; a request that
  // does not fit blocks every later one. New arrivals are queued behind
  // waiters instead of overtaking them, so the head is always served at the
  // next root window flip and nothing is postponed indefinitely.
  function allocate(r) {
    const q = r.queue;
    while (r.head < q.length) {
      const entry = q[r.head];
      if (entry.removed) {
        r.head += 1;
        continue;
      }
      if (!fits(r, entry.units)) break;
      r.head += 1;
      entry.removed = true;
      r.liveQueued -= 1;
      clearTimeout(entry.timer);
      detachSignal(entry);
      entry.owner.waiting -= entry.units;
      const { ownPart, borrowPart } = applyGrant(entry.owner, entry.units);
      entry.owner.granted += 1;
      entry.resolve(
        makeHandle(entry.owner, ownPart, borrowPart, entry.owner.generation, entry.owner.borrowGen),
      );
    }
    if (r.head > 32 && r.head * 2 >= q.length) {
      r.queue = q.slice(r.head);
      r.head = 0;
    }
  }

  function flip() {
    const r = state.root;
    const t = now();
    sweepExpired(r, t);
    state.generation += 1;
    state.used = 0;
    state.inFlight = 0; // occupations from the closed window clear automatically
    state.borrowedInFlight = 0;
    state.borrowGen += 1;
    if (state === r) {
      // Reclaim borrowed quota from every child. The reclaim does not break
      // admitted occupations: handles stay valid, only the borrowed
      // accounting settles, and the inFlight drop is visible in the same
      // stats snapshot as the parent window flip.
      for (const child of r.descendants) {
        child.inFlight -= child.borrowedInFlight;
        child.borrowedInFlight = 0;
        child.borrowGen += 1;
        child.poolUnits = 0;
      }
      state.poolUnits = 0;
    }
    allocate(r);
  }

  let windowTimer = setInterval(flip, windowMs);
  if (typeof windowTimer.unref === 'function') windowTimer.unref();

  function acquire(units = 1, options) {
    assertInteger(units, 'units');
    if (units < 1 || units > state.limit) {
      throw new RangeError(`units must be between 1 and ${state.limit}`);
    }
    const signal = asSignal(options);
    if (signal !== null && signal.aborted) {
      // An already-fired signal settles the request as cancelled at once;
      // it never enters the queue and waiting is never touched.
      state.cancelled += 1;
      return Promise.reject(new QuotaExceededError('CANCELLED', 'acquire was cancelled'));
    }
    const r = state.root;
    // A non-empty queue means earlier arrivals are still owed quota; new
    // requests must not overtake them even if they would fit.
    if (r.liveQueued === 0 && fits(r, units)) {
      const { ownPart, borrowPart } = applyGrant(state, units);
      state.granted += 1;
      return Promise.resolve(
        makeHandle(state, ownPart, borrowPart, state.generation, state.borrowGen),
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
        enqueuedAt: t,
        deadline: t + state.maxWaitMs,
        timer: null,
        removed: false,
        pool: r, // the family queue holding this entry; rewired by reparent
        signal: null,
        onAbort: null,
      };
      entry.timer = setTimeout(() => expireEntry(entry), state.maxWaitMs);
      if (signal !== null) {
        entry.signal = signal;
        entry.onAbort = () => cancelEntry(entry);
        signal.addEventListener('abort', entry.onAbort, { once: true });
      }
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

  // Runtime adjustments. Each one is synchronous and atomic: exactly one
  // stats-visible facet, never an early grant for queued requests, never a
  // broken admitted occupation.

  function updateLimit(value) {
    assertInteger(value, 'limit');
    if (value <= 0) throw new RangeError('limit must be a positive integer');
    if (state.parent !== null && value > state.parent.limit) {
      throw new RangeError('limit must not exceed parent limit');
    }
    for (const child of state.children) {
      if (value < child.limit) {
        throw new RangeError('limit must not be below child limit');
      }
    }
    state.limit = value;
    // Queued requests that can never fit again leave the queue at once so
    // they do not block later arrivals; each settles exactly one counter.
    const r = state.root;
    for (let i = r.head; i < r.queue.length; i += 1) {
      const entry = r.queue[i];
      if (entry.removed || entry.owner !== state || entry.units <= value) continue;
      entry.removed = true;
      r.liveQueued -= 1;
      clearTimeout(entry.timer);
      detachSignal(entry);
      state.waiting -= entry.units;
      state.refused += 1;
      entry.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
  }

  function updateWindowMs(value) {
    assertInteger(value, 'windowMs');
    if (value <= 0) throw new RangeError('windowMs must be a positive integer');
    state.windowMs = value;
    // The current window runs the new length from this moment before
    // flipping; used quota and queued deadlines are left untouched.
    clearInterval(windowTimer);
    windowTimer = setInterval(flip, value);
    if (typeof windowTimer.unref === 'function') windowTimer.unref();
  }

  function updateMaxWaitMs(value) {
    assertInteger(value, 'maxWaitMs');
    if (value < 0) throw new RangeError('maxWaitMs must not be negative');
    state.maxWaitMs = value;
    // Re-deadline this gate's queued waiters to enqueue-time plus the new
    // limit; anything already due settles as expired on the spot.
    const r = state.root;
    const t = now();
    for (let i = r.head; i < r.queue.length; i += 1) {
      const entry = r.queue[i];
      if (entry.removed || entry.owner !== state) continue;
      entry.deadline = entry.enqueuedAt + value;
      if (entry.deadline <= t) {
        expireEntry(entry);
      } else {
        clearTimeout(entry.timer);
        entry.timer = setTimeout(() => expireEntry(entry), entry.deadline - t);
      }
    }
    recomputeMinDeadline(r);
  }

  // Move this gate (with its descendants) under another parent gate. The
  // subtree's draw on the shared pool is settled against the old family's
  // window and re-borrowed from the new family's, atomically: the old parent
  // shows no stats facet, admitted occupations are never broken, and queued
  // requests keep waiting in the new family's queue.
  function reparent(newParent) {
    const nextParent = gateState.get(newParent);
    if (nextParent === undefined) {
      throw new TypeError('new parent must be a gate created by createGate');
    }
    for (let p = nextParent; p !== null; p = p.parent) {
      if (p === state) {
        throw new RangeError('cannot reparent a gate to itself or its descendant');
      }
    }
    if (state.limit > nextParent.limit) {
      throw new RangeError('limit must not exceed parent limit');
    }

    const oldRoot = state.root;
    const newRoot = nextParent.root;
    const subtree = [];
    const collect = (s) => {
      subtree.push(s);
      for (const child of s.children) collect(child);
    };
    collect(state);
    const subtreeSet = new Set(subtree);
    let draw = 0;
    for (const s of subtree) draw += s.poolUnits;

    // If the new family's pool cannot cover the moved draw, refuse as
    // quota-exceeded; nothing has been mutated, so the rollback is total.
    const projected = (newRoot === oldRoot ? newRoot.used - draw : newRoot.used) + draw;
    if (projected > newRoot.limit) {
      throw new QuotaExceededError(
        'QUOTA_EXCEEDED',
        'new family pool cannot cover the moved occupations',
      );
    }

    if (state.parent !== null) {
      const siblings = state.parent.children;
      siblings.splice(siblings.indexOf(state), 1);
    }
    state.parent = nextParent;
    nextParent.children.push(state);
    for (const s of subtree) s.root = newRoot;

    if (newRoot !== oldRoot) {
      oldRoot.used -= draw;
      newRoot.used += draw;
      if (state === oldRoot) {
        // The moved gate was the pool root: its `used` was the pool counter
        // and now becomes its own-window units.
        state.used = state.poolUnits;
      }
      if (oldRoot.lastNow > newRoot.lastNow) newRoot.lastNow = oldRoot.lastNow;

      if (state === oldRoot) {
        newRoot.descendants.push(state, ...oldRoot.descendants);
        oldRoot.descendants = [];
      } else {
        oldRoot.descendants = oldRoot.descendants.filter((s) => !subtreeSet.has(s));
        newRoot.descendants.push(...subtree);
      }

      // Migrate the subtree's queued requests to the new family's queue,
      // keeping their relative order, deadlines and timers.
      const moved = [];
      const kept = [];
      for (let i = 0; i < oldRoot.queue.length; i += 1) {
        const entry = oldRoot.queue[i];
        if (entry.removed) continue; // dead entries are compacted away
        if (subtreeSet.has(entry.owner)) moved.push(entry);
        else kept.push(entry);
      }
      oldRoot.queue = kept;
      oldRoot.head = 0;
      oldRoot.liveQueued = kept.length;
      for (const entry of moved) {
        entry.pool = newRoot;
        newRoot.queue.push(entry);
      }
      newRoot.liveQueued += moved.length;
      recomputeMinDeadline(oldRoot);
      recomputeMinDeadline(newRoot);
    }
  }

  const gate = { acquire, stats, updateLimit, updateWindowMs, updateMaxWaitMs, reparent };
  gateState.set(gate, state);
  return gate;
}
