// In-process fixed-window quota gate.
// Single process, event-loop timers, no persistence — see README.

export const QUOTA_EXCEEDED = 'QUOTA_EXCEEDED';
export const WAIT_EXPIRED = 'WAIT_EXPIRED';

export class QuotaExceededError extends Error {
  constructor(message = 'Quota exceeded', code = QUOTA_EXCEEDED) {
    super(message);
    this.name = 'QuotaExceededError';
    this.code = code;
  }
}

export function createGate({ limit, windowMs, maxWaitMs = 0 } = {}) {
  if (!Number.isInteger(limit)) {
    throw new TypeError('limit must be an integer number');
  }
  if (!Number.isInteger(windowMs)) {
    throw new TypeError('windowMs must be an integer number');
  }
  if (!Number.isInteger(maxWaitMs)) {
    throw new TypeError('maxWaitMs must be an integer number');
  }
  if (limit <= 0) {
    throw new RangeError('limit must be a positive integer');
  }
  if (windowMs <= 0) {
    throw new RangeError('windowMs must be a positive integer');
  }
  if (maxWaitMs < 0) {
    throw new RangeError('maxWaitMs must not be negative');
  }

  // Units admitted during the current window. Releasing an occupancy ends it
  // early but never refunds this counter; it only resets on a page turn.
  let admittedInWindow = 0;
  // Units of occupancies that have not ended (release or window end).
  let inFlight = 0;
  // Units belonging to queued requests.
  let waiting = 0;
  // Counts per occupancy request; each request lands in exactly one of these.
  let granted = 0;
  let refused = 0;
  let expired = 0;

  const queue = [];
  const active = new Set();

  function admit(units) {
    admittedInWindow += units;
    inFlight += units;
    granted += 1;

    const occupancy = { units, done: false };
    active.add(occupancy);

    return {
      release() {
        if (occupancy.done) return; // idempotent: repeats change nothing
        occupancy.done = true;
        active.delete(occupancy);
        inFlight -= occupancy.units;
        // Quota spent this window is deliberately not refunded.
      },
    };
  }

  function expireEntry(entry) {
    if (entry.done) return;
    entry.done = true;
    clearTimeout(entry.timer);
    const index = queue.indexOf(entry);
    if (index !== -1) queue.splice(index, 1);
    waiting -= entry.units;
    expired += 1;
    entry.reject(new QuotaExceededError('Wait for quota expired', WAIT_EXPIRED));
  }

  function rollover() {
    // New page: window quota resets and every outstanding occupancy ends.
    admittedInWindow = 0;
    inFlight = 0;
    for (const occupancy of active) occupancy.done = true;
    active.clear();

    const now = Date.now();

    // Expiry is judged before allocation: anything due on this same turn
    // counts as expired, never granted.
    for (const entry of [...queue]) {
      if (entry.deadline <= now) expireEntry(entry);
    }

    // Drain strictly in arrival order, all-or-nothing. A request that does
    // not fit blocks every later (even smaller) request until the next page.
    while (queue.length > 0) {
      const entry = queue[0];
      if (entry.units > limit - admittedInWindow) break;
      queue.shift();
      clearTimeout(entry.timer);
      waiting -= entry.units;
      entry.resolve(admit(entry.units));
    }
  }

  const pager = setInterval(rollover, windowMs);
  // The gate must not by itself keep the process alive.
  pager.unref?.();

  function acquire(units = 1) {
    if (!Number.isInteger(units)) {
      throw new TypeError('units must be an integer number');
    }
    if (units < 1 || units > limit) {
      throw new RangeError(`units must be between 1 and ${limit}`);
    }

    // Only serve straight away when nobody is waiting ahead and the current
    // window still covers the whole request.
    if (queue.length === 0 && admittedInWindow + units <= limit) {
      return Promise.resolve(admit(units));
    }

    if (maxWaitMs === 0) {
      refused += 1;
      return Promise.reject(
        new QuotaExceededError('Quota exceeded for current window', QUOTA_EXCEEDED),
      );
    }

    let entry;
    const promise = new Promise((resolve, reject) => {
      entry = { units, resolve, reject, done: false, deadline: 0, timer: null };
    });

    waiting += units;
    entry.deadline = Date.now() + maxWaitMs;
    entry.timer = setTimeout(() => expireEntry(entry), maxWaitMs);
    entry.timer.unref?.();
    queue.push(entry);

    return promise;
  }

  function stats() {
    // Key order follows the stats line in the README; config is echoed back.
    return {
      limit,
      windowMs,
      inFlight,
      waiting,
      granted,
      refused,
      expired,
    };
  }

  return { acquire, stats };
}
