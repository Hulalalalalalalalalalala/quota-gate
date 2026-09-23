export class QuotaExceededError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'QuotaExceededError';
    this.code = code;
  }
}

function assertInteger(value, name) {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new TypeError(`${name} must be an integer`);
  }
}

// Clamped monotonic view of the wall clock. A backwards jump of the system
// clock can never move time backwards here, so an already elapsed window can
// never become live again; a long event-loop stall simply surfaces as one
// large forward jump when the loop gets back to the timer.
function effectiveNow(family) {
  const t = Date.now();
  if (t > family.lastNow) family.lastNow = t;
  return family.lastNow;
}

// Family waiting queue: a doubly-linked list in arrival order plus a scan
// pointer. One sweep examines each entry at most once per window: an entry
// that does not fit is left in place and can never become admissible later
// in the same window (pool and gate usage only grow until the next flip),
// while the sweep moves on so younger, fitting requests pass it. New
// arrivals are appended, expiries unlink in O(1), which keeps the
// acquire/release hot path constant-amortized. At every flip the scan
// pointer returns to the head, so the oldest waiting entry gets the very
// first pick of a fresh window - the guarantee that no request, however
// large, is ever starved.
function drainFamily(family) {
  let entry = family.scan;
  family.scan = null; // a sweep always runs through to the current tail
  while (entry !== null) {
    const next = entry.next;
    const node = entry.node;
    // The grant must fit the shared pool and may not take this gate -
    // borrows included - past its direct parent's limit. A child whose own
    // slice is exhausted borrows the shortfall from the parent side.
    if (
      entry.units <= family.poolLimit - family.poolUsed &&
      node.used + entry.units <= node.parentLimit
    ) {
      unlink(family, entry);
      node.grant(entry);
    }
    entry = next;
  }
}

function appendEntry(family, entry) {
  entry.prev = family.tail;
  entry.next = null;
  if (family.tail !== null) family.tail.next = entry;
  else family.head = entry;
  family.tail = entry;
  // Anything not yet examined this window is ahead of the newcomer; only
  // when the queue was already fully swept does the newcomer get scanned.
  if (family.scan === null) family.scan = entry;
}

function unlink(family, entry) {
  if (entry.prev !== null) entry.prev.next = entry.next;
  else family.head = entry.next;
  if (entry.next !== null) entry.next.prev = entry.prev;
  else family.tail = entry.prev;
  if (family.scan === entry) family.scan = entry.next;
  entry.prev = null;
  entry.next = null;
}

// Expire one queued entry. Returns false if it was already admitted or
// expired. Expiry frees no pool units (the request never held any), so it
// cannot by itself make another waiting entry fit; the next flip re-sweeps.
function removeEntry(family, entry, node) {
  if (entry.removed) return false;
  entry.removed = true;
  unlink(family, entry);
  clearTimeout(entry.timer);
  node.markExpired(entry);
  return true;
}

// Expiry is always computed BEFORE quota is allocated: everything due at or
// before the flip moment expires rather than being granted. Every wait timer
// due in the same tick and the window timer converge on this single step, so
// repeated or re-entrant firings cannot double-expire or double-grant.
function flipFamily(family) {
  if (family.flipping) return;
  family.flipping = true;
  try {
    const now = effectiveNow(family);
    // Walk backwards so unlinking cannot skip the node still to be visited.
    for (let entry = family.tail; entry !== null; ) {
      const prev = entry.prev;
      if (entry.deadline <= now) removeEntry(family, entry, entry.node);
      entry = prev;
    }
    // New window. Dropping the accounting - including outstanding borrows -
    // only detaches occupations from the new window; every handle already
    // admitted keeps running. No admitted occupation is split or reclaimed
    // mid-flight. The inFlight change happens synchronously here, so one
    // stats snapshot sees both the reclaim and anything re-granted below.
    family.poolUsed = 0;
    for (const node of family.nodes) node.beginWindow();
    family.scan = family.head;
    drainFamily(family);
  } finally {
    family.flipping = false;
  }
}

export function createGate({ limit, windowMs, maxWaitMs = 0, parent = null } = {}) {
  assertInteger(limit, 'limit');
  assertInteger(windowMs, 'windowMs');
  assertInteger(maxWaitMs, 'maxWaitMs');
  if (limit <= 0) throw new RangeError('limit must be a positive integer');
  if (windowMs <= 0) throw new RangeError('windowMs must be a positive integer');
  if (maxWaitMs < 0) throw new RangeError('maxWaitMs must not be negative');
  if (parent !== null && (typeof parent !== 'object' || !('_family' in parent))) {
    throw new TypeError('parent must be a gate created by createGate');
  }

  const isRoot = parent === null;
  const family = isRoot
    ? {
        poolLimit: limit,
        windowMs,
        poolUsed: 0, // units granted out of the shared pool this window
        head: null, // family waiting queue head (oldest entry)
        tail: null, // family waiting queue tail (youngest entry)
        scan: null, // next entry the sweep has not yet examined
        nodes: [], // root first, then children in creation order
        lastNow: Date.now(),
        flipping: false,
      }
    : parent._family;
  if (!isRoot && limit > parent._limit) {
    throw new RangeError('child gate limit must not exceed the parent gate limit');
  }

  // Ceiling on this gate's own window usage, borrows included: the root is
  // bounded by its limit; every child is bounded by its direct parent's.
  const parentLimit = isRoot ? limit : parent._limit;

  let used = 0; // units admitted to this gate in the window, borrows included
  let inFlight = 0; // occupied units not released and not cleared by a flip
  let waiting = 0; // units this gate currently has queued
  let granted = 0;
  let refused = 0;
  let expired = 0;
  let generation = 0; // bumped on every window flip, invalidates old handles

  function makeHandle(units, gen) {
    let active = true;
    return {
      release() {
        // Repeated release calls are a no-op and never move any statistic;
        // a handle from a closed window is already detached.
        if (!active || gen !== generation) return;
        active = false;
        inFlight -= units;
      },
    };
  }

  function grant(entry) {
    // All-or-nothing admission against the single shared pool. Whatever the
    // grant needs beyond this gate's own slice is borrowed from the parent
    // side of the same pool; the drain fit checks bound it by the pool and
    // by this gate's parent limit, and acquire() bounds each request by the
    // gate's own limit. All loans last only until the window resets.
    family.poolUsed += entry.units;
    used += entry.units;
    inFlight += entry.units;
    waiting -= entry.units;
    granted += 1;
    clearTimeout(entry.timer);
    entry.admitted = true;
    entry.resolve(makeHandle(entry.units, generation));
  }

  function beginWindow() {
    used = 0;
    inFlight = 0;
    generation += 1;
  }

  // Internal node used by the family-level flip/drain machinery. It closes
  // over this gate's own counters and handle generation.
  const node = {
    get used() {
      return used;
    },
    parentLimit,
    grant,
    beginWindow,
    markExpired(entry) {
      waiting -= entry.units;
      expired += 1;
      entry.reject(
        new QuotaExceededError(
          'WAIT_EXPIRED',
          'queued request expired before quota was available',
        ),
      );
    },
  };
  family.nodes.push(node);

  function acquire(units = 1) {
    assertInteger(units, 'units');
    if (units < 1 || units > limit) {
      throw new RangeError(`units must be between 1 and ${limit}`);
    }
    // Immediate admission needs an empty family queue - earlier arrivals are
    // owed first and must never be overtaken - room in the shared pool, and
    // room under this gate's parent-limit ceiling. A request larger than the
    // gate's remaining own slice is admitted as a borrow. O(1) hot path.
    if (
      family.head === null &&
      units <= family.poolLimit - family.poolUsed &&
      used + units <= parentLimit
    ) {
      family.poolUsed += units;
      used += units;
      inFlight += units;
      granted += 1;
      return Promise.resolve(makeHandle(units, generation));
    }
    if (maxWaitMs === 0) {
      refused += 1;
      return Promise.reject(new QuotaExceededError('QUOTA_EXCEEDED', 'quota exceeded'));
    }
    waiting += units;
    const now = effectiveNow(family);
    const entry = {
      units,
      node,
      deadline: now + maxWaitMs,
      prev: null,
      next: null,
      removed: false,
      admitted: false,
      timer: null,
      resolve: null,
      reject: null,
    };
    const promise = new Promise((resolve, reject) => {
      entry.resolve = resolve;
      entry.reject = reject;
    });
    // Append first, then sweep: the newcomer can never jump an older entry
    // still queued, but if it fits behind whoever is ahead it is admitted in
    // the same tick. The sweep examines each entry at most once per window.
    appendEntry(family, entry);
    drainFamily(family);
    // Only arm the wait timer if the sweep did not admit it immediately;
    // arming an admitted entry would later fire against a stale list node.
    if (!entry.admitted) {
      entry.timer = setTimeout(() => {
        if (entry.removed || entry.admitted) return;
        // Judge expiry against the clamped monotonic clock: a backwards
        // wall-clock jump must not make a not-yet-due deadline fire early.
        // The next window flip settles such an entry authoritatively.
        if (effectiveNow(family) < entry.deadline) return;
        // Expiry frees no pool units (the request never held any) and a
        // request's non-fit can only be cured by the next window, so no
        // re-sweep is needed: the list ordering already protects everyone.
        removeEntry(family, entry, node);
      }, maxWaitMs);
      if (typeof entry.timer.unref === 'function') entry.timer.unref();
    }
    return promise;
  }

  function stats() {
    return {
      limit,
      windowMs: family.windowMs,
      inFlight,
      waiting,
      granted,
      refused,
      expired,
    };
  }

  const api = { acquire, stats };
  // Internal linkage for parent/child wiring; non-enumerable so the exposed
  // surface and key order stay exactly { acquire, stats }.
  Object.defineProperty(api, '_family', { value: family });
  Object.defineProperty(api, '_limit', { value: limit });
  if (isRoot) {
    // Test-only entry to the same single flip step the interval runs;
    // non-enumerable internals never appear on the public surface.
    Object.defineProperty(family, '_flip', { value: () => flipFamily(family) });
    const windowTimer = setInterval(() => flipFamily(family), windowMs);
    if (typeof windowTimer.unref === 'function') windowTimer.unref();
  }
  return api;
}
