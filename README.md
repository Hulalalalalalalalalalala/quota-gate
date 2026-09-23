# quota-gate

In-process quota gate that limits how many units of work may run in a window,
refuses extra work with a distinguishable error, reports wait statistics, and
lets gates be composed into parent/child families sharing one quota pool.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createGate({ limit, windowMs, maxWaitMs = 0, parent = null }) -> Gate`.
- `Gate.acquire(units = 1) -> Promise<{ release }>`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired }`.
- `QuotaExceededError` exported class with a `code` property.

The `parent` option links the new gate under an existing gate. A gate created
without `parent` is the root of a family.

### Shared pool and borrowing

- A parent and all its descendants draw from one quota pool sized by the
  root gate's `limit`; every gate shares the root's window.
- A child gate's `limit` may not exceed its direct parent's `limit`. An
  acquire for more units than the gate's own `limit` throws `RangeError`,
  and the message echoes the actual limit.
- A request is admitted all-or-nothing against the family pool. When the
  pool has room but the child's own slice is exhausted, the shortfall is
  borrowed from the parent side for that one occupation.
- Borrowed units are force-reclaimed when the window ends. Reclaiming only
  resets the accounting; occupations already admitted keep running and are
  never split or revoked mid-flight. The corresponding `inFlight` change is
  visible in the same `stats()` snapshot as anything re-granted at the flip.

### Waiting and weighted aging

- Requests that do not fit wait in family-wide arrival order when
  `maxWaitMs > 0`, or are rejected with `QUOTA_EXCEEDED` otherwise.
- An older request that does not fit is skipped so that younger requests
  that do fit are not blocked, but it keeps first pick of every fresh
  window, so no request can be starved. Grants remain all-or-nothing, and
  occupations arriving in the same event-loop tick settle in arrival order.
- A request still waiting at its deadline is rejected with
  `WAIT_EXPIRED`; the two error codes are `QUOTA_EXCEEDED` and
  `WAIT_EXPIRED`. Type errors throw `TypeError`, illegal values throw
  `RangeError`.
- Calling a handle's `release()` more than once is a no-op and never moves
  a statistic.

### Windows and the clock

- At a window flip, deadlines due at or before the flip moment are computed
  before any quota is handed out.
- Time is read through a clamped monotonic clock: a backwards jump of the
  system clock cannot revive an elapsed window or expire a deadline early,
  and a long event-loop block is handled as one consistent flip.

## Tests

    npm test

## Limits

Single process and a single event loop only; no distributed coordination.
All quotas and statistics start at zero on every start; nothing is read from
or written to disk.
