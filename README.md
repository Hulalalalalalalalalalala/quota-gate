# quota-gate

In-process quota gate that limits how many units of work may run in a window, refuses extra work with a distinguishable error, and reports wait statistics.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createGate({ limit, windowMs, maxWaitMs = 0, parent }) -> Gate`.
- `Gate.acquire(units = 1, options) -> Promise<{ release }>`. `options` may be
  an `AbortSignal` or `{ signal }`; aborting settles a still-queued request
  with a `QuotaExceededError` whose `code` is `'CANCELLED'`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired, cancelled }`.
- `Gate.updateLimit(limit)`, `Gate.updateWindowMs(windowMs)`,
  `Gate.updateMaxWaitMs(maxWaitMs)` — adjust the gate's configuration at
  runtime. Each adjustment is atomic: it never grants queued requests early
  and never breaks admitted occupations. Shrinking `limit` below a queued
  request's size rejects that request at once with `QUOTA_EXCEEDED`.
  `updateWindowMs` restarts the current window with the new length without
  clearing used quota or touching queued deadlines. `updateMaxWaitMs`
  re-deadlines queued waiters to enqueue-time plus the new limit; anything
  already overdue expires on the spot.
- `Gate.reparent(newParent)` — move the gate, with its descendants, under
  another gate created by this module. The subtree's draw on the shared pool
  is settled against the old family's window and re-borrowed from the new
  family's in one atomic step; if the new family's pool cannot cover it, the
  move is refused with `QUOTA_EXCEEDED` and rolled back entirely.
- `QuotaExceededError` exported class with a `code` property
  (`'QUOTA_EXCEEDED'`, `'WAIT_EXPIRED'`, or `'CANCELLED'`).

Passing `parent` (a gate created by this module) makes a child gate that
shares the parent's quota pool: every grant in the hierarchy draws from the
root pool, and a child's `limit` must not exceed its parent's. When a child's
own remaining quota cannot cover a request, the child borrows the shortfall
from the parent pool to complete that one occupation. Borrowed quota is
forcibly reclaimed when the parent's window ends; the reclaim never breaks
admitted occupations (handles stay valid) and the corresponding `inFlight`
drop is visible in the same stats snapshot. Queued requests across the whole
hierarchy wait in one arrival-ordered queue and are served all-or-nothing
before any later arrival, so no request is postponed indefinitely.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Timers use the event loop clock.
No persistence across restarts.
