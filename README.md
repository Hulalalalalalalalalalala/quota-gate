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
- `Gate.acquireAll(members, signal?) -> Promise<{ release }>`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired, cancelled, reserved, rolledBack }`.
- `Gate.updateLimit(limit)`, `Gate.updateWindowMs(windowMs)`,
  `Gate.updateMaxWaitMs(maxWaitMs)`, `Gate.reparent(newParent)`.
- `QuotaExceededError` exported class with a `code` property
  (`QUOTA_EXCEEDED`, `WAIT_EXPIRED`, or `CANCELLED`).

Every acquired handle, single or combined, has exactly one own method,
`release()`; it is a closure and works the same after destructuring.

### Cross-gate combinations

`acquireAll([{ gate, units }, ...], signal?)` reserves one combined occupation
spanning several gates of the same family. Member lists must be non-empty; each
`units` must be a positive integer; a gate may appear only once; members may
not be ancestors of one another (a grandparent/grandchild loop is rejected);
and all members must belong to one family. Empty lists, non-integer or
non-positive unit counts and repeated gates raise `RangeError`; wrong argument
or member types (including foreign gates) raise `TypeError`. Invalid calls
produce no observable cut.

Each member is covered first by its own current-window quota; any shortfall
borrows its direct parent's own quota, then the grandparent's, and so on up
the chain, registering every lend as a reservation; the root pool covers
whatever remains. The whole request commits to an occupation only once every
part is reserved. If any part cannot be covered, every provisional
reservation is released and the attempt rolls back as a whole (`rolledBack`
moves once, no other counter does) and the combo gathers again at the next
pool window flip. `reserved` reports the units currently held by uncommitted
reservations (member own quota and ancestor lends on a non-root gate; pool
units on the root); on commit those units merge into the occupations and on a
whole rollback the count returns to zero.

Combined and single requests share one arrival-ordered queue and settle
independently. A head combo that cannot be gathered does not starve later
arrivals: a later request (single or combined) that can be satisfied outright
is let through, and the blocked combo yields once. After yielding twice it
stops yielding and becomes an ordinary barrier. A waiting combo may be
cancelled with its `AbortSignal` (`CANCELLED`); if its wait cap runs out it
settles as `WAIT_EXPIRED`; both reject with `QuotaExceededError` and release
the whole reservation. A signal already aborted settles immediately without
entering the queue. With `maxWaitMs` 0 a combo that cannot commit on the spot
is refused `QUOTA_EXCEEDED`.

When an ancestor window turns, borrowed quota is reclaimed uncommitted
reservations first (member own slice, then ancestor lends), then the parts
borrowed by admitted occupations; admitted handles stay valid throughout.

Passing `parent` (a gate created by this module) makes a child gate that
shares the parent's quota pool: every grant in the hierarchy draws from the
root pool, and a child's `limit` must not exceed its parent's. A request's
unit count is not capped by the entry gate's own limit: the occupation first
uses that gate's own remaining current-window quota, then borrows the
shortfall from its direct parent, then the grandparent, and so on up the
chain (each borrow registered as a reservation), with the root pool covering
whatever remains. A unit count the whole family cannot cover is not a
parameter error; with wait cap zero it is refused `QUOTA_EXCEEDED` on the
spot, otherwise the request queues. Non-positive or non-integer unit counts
raise `RangeError`; a non-numeric `units` raises `TypeError`. Borrowed quota
is forcibly reclaimed when the lender's own window ends, uncommitted
reservations first and then the parts borrowed by admitted occupations; the
reclaim never breaks admitted occupations (handles stay valid) and the
corresponding `inFlight` drop is visible in the same stats snapshot. Queued
requests across the whole hierarchy wait in one arrival-ordered queue and are
served all-or-nothing before any later arrival, so no request is postponed
indefinitely.

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
  and nothing changes. A queued combination split across the two families by
  the move is refused as a whole; a combination wholly inside the moved
  subtree releases its old-family reservation and gathers anew in the joined
  family. Reparenting under the gate itself or one of its descendants, or
  with a limit above the new parent's, raises `RangeError`; a target that is
  not a gate from this module raises `TypeError`. Invalid calls produce no
  observable cut.

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
