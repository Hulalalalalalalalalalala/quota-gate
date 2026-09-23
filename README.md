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
- `Gate.acquire(units = 1) -> Promise<{ release }>`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired }`.
- `QuotaExceededError` exported class with a `code` property.

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
