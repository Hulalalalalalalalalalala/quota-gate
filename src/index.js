export class QuotaExceededError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'QuotaExceededError';
    this.code = code;
  }
}

// Internal state of every gate, shared with child gates through a WeakMap so
// the public gate object keeps its exact shape ({ acquire, stats }).
const gateState = new WeakMap();

function assertInteger(value, name) {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`);
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
    used: 0, // own units granted in this gate's current window
    inFlight: 0, // units occupied and not yet released/cleared
    waiting: 0, // units currently queued through this gate
    granted: 0,
    refused: 0,
    expired: 0,
    generation: 0, // bumped on every window flip, invalidates old handles
    borrowedInFlight: 0, // borrowed units still occupied; reclaimed on the root flip
    parent: parentState,
    root: null,
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
  }

  function now() {
    const t = Date.now();
    if (t > root.lastNow) root.lastNow = t;
    return root.lastNow;
  }

  // A request fits when the shared pool has room. A child covers whatever its
  // own remaining quota cannot by borrowing from the pool, so its own limit
  // never blocks a grant; only the pool does.
  function fits(units) {
    return units <= root.limit - root.used;
  }

  function applyGrant(target, units) {
    if (target === root) {
      root.used += units;
      root.inFlight += units;
      return { ownPart: units, borrowPart: 0 };
    }
    const ownPart = Math.min(units, Math.max(0, target.limit - target.used));
    const borrowPart = units - ownPart;
    target.used += ownPart;
    target.inFlight += units;
    target.borrowedInFlight += borrowPart;
    root.used += units;
    return { ownPart, borrowPart };
  }

  function makeHandle(target, ownPart, borrowPart, generation, poolGeneration) {
    let active = true;
    return {
      release() {
        if (!active) return;
        active = false;
        if (generation !== target.generation) return; // window closed; cleared
        target.inFlight -= ownPart;
        if (borrowPart > 0 && poolGeneration === root.generation) {
          target.inFlight -= borrowPart;
          target.borrowedInFlight -= borrowPart;
        }
        // A stale poolGeneration means the root window already reclaimed the
        // borrowed part; the handle stays valid but must not subtract twice.
      },
    };
  }

  function expireEntry(entry) {
    if (entry.removed) return;
    entry.removed = true;
    root.liveQueued -= 1;
    clearTimeout(entry.timer);
    entry.owner.waiting -= entry.units;
    entry.owner.expired += 1;
    entry.reject(
      new QuotaExceededError('WAIT_EXPIRED', 'queued request expired before quota was available'),
    );
  }

  // Expiry is judged before quota allocation: anything due at or before the
  // flip moment expires rather than being granted.
  function sweepExpired(t) {
    if (root.minDeadline > t) return;
    let min = Infinity;
    for (let i = root.head; i < root.queue.length; i += 1) {
      const entry = root.queue[i];
      if (entry.removed) continue;
      if (entry.deadline <= t) expireEntry(entry);
      else if (entry.deadline < min) min = entry.deadline;
    }
    root.minDeadline = min;
  }

  // Grant queued requests in arrival order, all-or-nothing; a request that
  // does not fit blocks every later one. New arrivals are queued behind
  // waiters instead of overtaking them, so the head is always served at the
  // next root window flip and nothing is postponed indefinitely.
  function allocate() {
    const q = root.queue;
    while (root.head < q.length) {
      const entry = q[root.head];
      if (entry.removed) {
        root.head += 1;
        continue;
      }
      if (!fits(entry.units)) break;
      root.head += 1;
      entry.removed = true;
      root.liveQueued -= 1;
      clearTimeout(entry.timer);
      entry.owner.waiting -= entry.units;
      const { ownPart, borrowPart } = applyGrant(entry.owner, entry.units);
      entry.owner.granted += 1;
      entry.resolve(
        makeHandle(entry.owner, ownPart, borrowPart, entry.owner.generation, root.generation),
      );
    }
    if (root.head > 32 && root.head * 2 >= q.length) {
      root.queue = q.slice(root.head);
      root.head = 0;
    }
  }

  function flip() {
    const t = now();
    sweepExpired(t);
    state.generation += 1;
    state.used = 0;
    state.inFlight = 0; // occupations from the closed window clear automatically
    state.borrowedInFlight = 0;
    if (state === root) {
      // Reclaim borrowed quota from every child. The reclaim does not break
      // admitted occupations: handles stay valid, only the borrowed
      // accounting settles, and the inFlight drop is visible in the same
      // stats snapshot as the parent window flip.
      for (const child of root.descendants) {
        child.inFlight -= child.borrowedInFlight;
        child.borrowedInFlight = 0;
      }
    }
    allocate();
  }

  const windowTimer = setInterval(flip, windowMs);
  if (typeof windowTimer.unref === 'function') windowTimer.unref();

  function acquire(units = 1) {
    assertInteger(units, 'units');
    if (units < 1 || units > limit) {
      throw new RangeError(`units must be between 1 and ${limit}`);
    }
    // A non-empty queue means earlier arrivals are still owed quota; new
    // requests must not overtake them even if they would fit.
    if (root.liveQueued === 0 && fits(units)) {
      const { ownPart, borrowPart } = applyGrant(state, units);
      state.granted += 1;
      return Promise.resolve(
        makeHandle(state, ownPart, borrowPart, state.generation, root.generation),
      );
    }
    if (maxWaitMs === 0) {
      state.refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    state.waiting += units;
    root.liveQueued += 1;
    return new Promise((resolve, reject) => {
      const entry = {
        owner: state,
        units,
        resolve,
        reject,
        deadline: now() + maxWaitMs,
        timer: null,
        removed: false,
      };
      entry.timer = setTimeout(() => expireEntry(entry), maxWaitMs);
      if (entry.deadline < root.minDeadline) root.minDeadline = entry.deadline;
      root.queue.push(entry);
    });
  }

  function stats() {
    return {
      limit,
      windowMs,
      inFlight: state.inFlight,
      waiting: state.waiting,
      granted: state.granted,
      refused: state.refused,
      expired: state.expired,
    };
  }

  const gate = { acquire, stats };
  gateState.set(gate, state);
  return gate;
}
