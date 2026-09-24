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
- `Gate.acquireAll(members, signal?) -> Promise<{ release }>` acquires a
  cross-gate combination. `members` is a non-empty array of
  `{ gate, units }`; every gate must belong to this gate's family, no gate may
  repeat, and no two members may be ancestors of one another. The returned
  handle exposes one detached `release` that releases every member occupation;
  it is safe to destructure, call without a receiver and call repeatedly.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused,
  expired, cancelled, reserved, rolledBack }`.
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

### Cross-gate combinations

`acquireAll` reserves each member's units all-or-nothing. Every member first
draws on its own remaining quota; the shortfall is borrowed one direct parent
at a time up the ancestor chain, and every leg draws the current root-pool
window as well. A request is admitted only once every member is fully
covered; if any step cannot be covered, every reservation of the attempt is
released and the whole request rolls back.

Combinations and single requests share one arrival-ordered queue and settle
independently. A combination that cannot be gathered when it reaches the head
steps aside so a later request that fits can be admitted, but the same
combination yields at most twice; afterwards it blocks later arrivals like
any other head. While a combination waits, everything that could be gathered
immediately is reserved and held; a window flip reclaims uncommitted
reservations before it reclaims quota borrowed by admitted occupations, and
handles remain valid in both cases. A combination whose member becomes
unfittable after a shrink is refused wholesale.

`stats.reserved` is the number of units currently held by uncommitted
reservations on that gate's window; the units merge into the occupation
counters on commit and return to zero on rollback. `stats.rolledBack` counts
failed gather attempts; a rollback moves no other counter.

Invalid `acquireAll` calls raise before any counter moves: an empty member
list, non-integer or out-of-range units, a repeated gate, members from
different families, or an ancestor chain raise `RangeError`; a non-array
member list, a non-object member, a non-module gate or wrong-typed units raise
`TypeError`.


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
that order. `acquireAll` accepts the same signal with the same outcomes; a
combination that times out settles `WAIT_EXPIRED`, and one that is aborted
settles `CANCELLED`, and with `maxWaitMs` 0 a combination that cannot be
gathered immediately is refused with `QUOTA_EXCEEDED`.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Timers use the event loop clock.
No persistence across restarts.
