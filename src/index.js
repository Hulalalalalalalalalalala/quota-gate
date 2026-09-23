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

export function createGate({ limit, windowMs, maxWaitMs = 0 } = {}) {
  assertInteger(limit, 'limit');
  assertInteger(windowMs, 'windowMs');
  assertInteger(maxWaitMs, 'maxWaitMs');
  if (limit <= 0) throw new RangeError('limit must be a positive integer');
  if (windowMs <= 0) throw new RangeError('windowMs must be a positive integer');
  if (maxWaitMs < 0) throw new RangeError('maxWaitMs must not be negative');

  let used = 0; // units granted in the current window
  let inFlight = 0; // units occupied and not yet released/cleared
  let waiting = 0; // units currently queued
  let granted = 0;
  let refused = 0;
  let expired = 0;
  let generation = 0; // bumped on every window flip, invalidates old handles
  const queue = []; // queued requests in arrival order

  function makeHandle(units, gen) {
    let active = true;
    return {
      release() {
        if (!active || gen !== generation) return;
        active = false;
        inFlight -= units;
      },
    };
  }

  function expireEntry(entry) {
    const index = queue.indexOf(entry);
    if (index === -1) return;
    queue.splice(index, 1);
    clearTimeout(entry.timer);
    waiting -= entry.units;
    expired += 1;
    entry.reject(
      new QuotaExceededError('WAIT_EXPIRED', 'queued request expired before quota was available'),
    );
  }

  function flip() {
    const now = Date.now();
    // Expiry is judged before quota allocation: anything due at or before
    // the flip moment expires rather than being granted.
    for (let i = queue.length - 1; i >= 0; i -= 1) {
      if (queue[i].deadline <= now) expireEntry(queue[i]);
    }
    generation += 1;
    used = 0;
    inFlight = 0; // occupations from the closed window clear automatically
    // Grant queued requests in arrival order, all-or-nothing; a request that
    // does not fit blocks every later one.
    while (queue.length > 0 && queue[0].units <= limit - used) {
      const entry = queue.shift();
      clearTimeout(entry.timer);
      waiting -= entry.units;
      used += entry.units;
      inFlight += entry.units;
      granted += 1;
      entry.resolve(makeHandle(entry.units, generation));
    }
  }

  const windowTimer = setInterval(flip, windowMs);
  if (typeof windowTimer.unref === 'function') windowTimer.unref();

  function acquire(units = 1) {
    assertInteger(units, 'units');
    if (units < 1 || units > limit) {
      throw new RangeError('units must be between 1 and limit');
    }
    // A non-empty queue means earlier arrivals are still owed quota; new
    // requests must not overtake them even if they would fit.
    if (queue.length === 0 && units <= limit - used) {
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
    return new Promise((resolve, reject) => {
      const entry = {
        units,
        resolve,
        reject,
        deadline: Date.now() + maxWaitMs,
        timer: null,
      };
      entry.timer = setTimeout(() => expireEntry(entry), maxWaitMs);
      queue.push(entry);
    });
  }

  function stats() {
    return { limit, windowMs, inFlight, waiting, granted, refused, expired };
  }

  return { acquire, stats };
}
