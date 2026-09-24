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
- `Gate.acquire(units = 1, signal?) -> Promise<{ release }>`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired, cancelled }`.
- `Gate.updateLimit(limit)`, `Gate.updateWindowMs(windowMs)`,
  `Gate.updateMaxWaitMs(maxWaitMs)`, `Gate.reparent(newParent)`.
- `QuotaExceededError` exported class with a `code` property
  (`QUOTA_EXCEEDED`, `WAIT_EXPIRED`, or `CANCELLED`).

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

## Runtime adjustment

Limits, window lengths, wait caps, and parent links can be changed while the
gate is running. Every adjustment is a single synchronous cut: it never
grants queued requests early and never tears apart admitted occupations.

- `updateLimit(limit)` changes the quota cap. A limit must be a positive
  integer, must not exceed the direct parent's limit, and must not sit below
  a direct child's limit (`RangeError` otherwise; non-integers raise
  `TypeError`). After a shrink, queued requests that can never fit are
  refused immediately with `QUOTA_EXCEEDED` and leave the queue, so they
  never block later arrivals.
- `updateWindowMs(windowMs)` changes the window length. The current window
  keeps running with a fresh, full length measured from the adjustment; used
  quota is not cleared and queued deadlines do not move.
- `updateMaxWaitMs(maxWaitMs)` changes the wait cap. Each queued request's
  deadline becomes its own enqueue time plus the new cap; waiters already
  past that deadline settle as `WAIT_EXPIRED` on the spot.
- `reparent(newParent)` moves the gate with its whole subtree under another
  gate. The borrowed quota is settled in the old family's window and
  re-borrowed from the new family in one snapshot; if the new family's pool
  cannot cover it, the call throws `QuotaExceededError` (`QUOTA_EXCEEDED`)
  and nothing changes. Reparenting under the gate itself or one of its
  descendants, or with a limit above the new parent's, raises `RangeError`;
  a target that is not a gate from this module raises `TypeError`. Invalid
  calls produce no observable cut.

`acquire` also accepts an optional `AbortSignal`. Cancelling a still-queued
request settles it with a `QuotaExceededError` whose `code` is `CANCELLED`
and moves only `waiting` and the `cancelled` counter. A signal that already
fired settles the acquire as `CANCELLED` without ever entering the queue;
cancelling an already granted or already settled request does nothing. When
expiry, cancellation, and a grant fall on the same moment, they settle in
that order.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Timers use the event loop clock.
No persistence across restarts.
