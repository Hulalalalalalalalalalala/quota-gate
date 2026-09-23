# quota-gate

In-process quota gate that limits how many units of work may run in a window, refuses extra work with a distinguishable error, and reports wait statistics.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createGate({ limit, windowMs, maxWaitMs = 0 }) -> Gate`.
- `Gate.acquire(units = 1) -> Promise<{ release }>`.
- `Gate.stats() -> { limit, windowMs, inFlight, waiting, granted, refused, expired }`.
- `QuotaExceededError` exported class with a `code` property.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Timers use the event loop clock.
No persistence across restarts.
