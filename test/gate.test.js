import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as mod from '../src/index.js';

test('exports the documented surface', () => {
  // ESM module namespace keys are lexicographically ordered by the spec;
  // the README only pins key order on stats().
  assert.deepEqual(Object.keys(mod).sort(), [
    'QUOTA_EXCEEDED',
    'QuotaExceededError',
    'WAIT_EXPIRED',
    'createGate',
  ]);
  assert.equal(typeof mod.createGate, 'function');
  assert.equal(typeof mod.QuotaExceededError, 'function');
});

test('stats echoes config and starts empty with README key order', () => {
  const gate = mod.createGate({ limit: 3, windowMs: 1000 });
  assert.deepEqual(Object.keys(gate.stats()), [
    'limit',
    'windowMs',
    'inFlight',
    'waiting',
    'granted',
    'refused',
    'expired',
  ]);
  assert.deepEqual(gate.stats(), {
    limit: 3,
    windowMs: 1000,
    inFlight: 0,
    waiting: 0,
    granted: 0,
    refused: 0,
    expired: 0,
  });
});

test('admits within the window and refuses overflow with QUOTA_EXCEEDED', async () => {
  const gate = mod.createGate({ limit: 2, windowMs: 60_000 });
  const a = await gate.acquire();
  const b = await gate.acquire(1);
  await assert.rejects(
    gate.acquire(),
    (err) => err instanceof mod.QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.deepEqual(
    { ...gate.stats() },
    { limit: 2, windowMs: 60_000, inFlight: 2, waiting: 0, granted: 2, refused: 1, expired: 0 },
  );
  a.release();
  b.release();
  assert.equal(gate.stats().inFlight, 0);
  // Release does not refund the spent window quota.
  await assert.rejects(gate.acquire(), (err) => err.code === 'QUOTA_EXCEEDED');
  assert.equal(gate.stats().refused, 2);
});

test('release handle is idempotent and never changes stats twice', async () => {
  const gate = mod.createGate({ limit: 1, windowMs: 60_000 });
  const handle = await gate.acquire();
  handle.release();
  handle.release();
  handle.release();
  assert.equal(gate.stats().inFlight, 0);
  assert.equal(gate.stats().granted, 1);
});

test('queue is FIFO: small requests never overtake earlier ones', async () => {
  const gate = mod.createGate({ limit: 2, windowMs: 40, maxWaitMs: 1000 });
  await gate.acquire(2); // fills the current window

  const order = [];
  const big = gate.acquire(2).then(() => order.push('big'));
  const small = gate.acquire(1).then(() => order.push('small'));
  await new Promise((r) => setTimeout(r, 5));
  assert.equal(gate.stats().waiting, 3); // 2 + 1 queued units
  // Nothing is granted outside a page turn, even though a unit could fit.
  assert.deepEqual(order, []);
  await Promise.all([big, small]);
  assert.deepEqual(order, ['big', 'small']);
  assert.equal(gate.stats().granted, 3); // initial + big + small occupancies
  assert.equal(gate.stats().refused, 0);
  assert.equal(gate.stats().expired, 0);
});

test('requests are all-or-nothing and a head that fits later blocks the line', async () => {
  const gate = mod.createGate({ limit: 5, windowMs: 40, maxWaitMs: 1000 });
  await gate.acquire(4);

  const head = gate.acquire(3); // cannot fit this window
  const tail = gate.acquire(2); // could share remainder, must wait behind head

  const [h, t] = await Promise.all([head, tail]);
  assert.equal(gate.stats().inFlight, 5); // both admitted together at the turn
  h.release();
  t.release();
  assert.equal(gate.stats().inFlight, 0);
});

test('page turn resets quota and ends outstanding occupancies', async () => {
  const gate = mod.createGate({ limit: 2, windowMs: 40, maxWaitMs: 1000 });
  const hold = await gate.acquire(2);
  const queued = gate.acquire(2);

  await new Promise((r) => setTimeout(r, 70)); // one page turn admits the waiter
  assert.equal(gate.stats().inFlight, 2); // old occupancy ended, waiter admitted
  hold.release(); // late release of a window-ended occupancy is a no-op
  assert.equal(gate.stats().inFlight, 2);
  const handle = await queued;
  handle.release();
  assert.equal(gate.stats().inFlight, 0);
});

test('waiting requests expire with WAIT_EXPIRED before a later page', async () => {
  const gate = mod.createGate({ limit: 1, windowMs: 200, maxWaitMs: 30 });
  await gate.acquire(1);

  await assert.rejects(
    gate.acquire(1),
    (err) => err instanceof mod.QuotaExceededError && err.code === 'WAIT_EXPIRED',
  );
  const s = gate.stats();
  assert.equal(s.expired, 1);
  assert.equal(s.waiting, 0);
  assert.equal(s.granted, 1);
  assert.equal(s.refused, 0);
});

test('on a page turn, expiry is judged before quota allocation', async () => {
  // Window turns at 50ms; the queued request is due at ~80ms. Block the event
  // loop past both so the two timers become due together. The interval (due
  // 50) runs before the wait timeout (due 80): at that turn the request is
  // already due and must count as expired, never consume the fresh quota.
  const gate = mod.createGate({ limit: 1, windowMs: 50, maxWaitMs: 80 });
  await gate.acquire(1);

  const startedAt = Date.now();
  const pending = gate.acquire(1);
  while (Date.now() - startedAt < 95) {
    // synchronous block so neither timer can run until both are due
  }

  await assert.rejects(pending, (err) => err.code === 'WAIT_EXPIRED');
  assert.equal(gate.stats().expired, 1);
  assert.equal(gate.stats().granted, 1);
  assert.equal(gate.stats().inFlight, 0); // page turn also ended the old occupancy
});

test('createGate validates types and ranges', () => {
  // undefined applies the documented default for maxWaitMs; every other
  // non-integer (including NaN) is a TypeError.
  for (const bad of [undefined, null, '3', 1.5, NaN, true]) {
    assert.throws(() => mod.createGate({ limit: bad, windowMs: 10 }), TypeError);
    assert.throws(() => mod.createGate({ limit: 1, windowMs: bad }), TypeError);
  }
  for (const bad of [null, '3', 1.5, NaN, true]) {
    assert.throws(() => mod.createGate({ limit: 1, windowMs: 10, maxWaitMs: bad }), TypeError);
  }
  assert.throws(() => mod.createGate({ limit: 0, windowMs: 10 }), RangeError);
  assert.throws(() => mod.createGate({ limit: -1, windowMs: 10 }), RangeError);
  assert.throws(() => mod.createGate({ limit: 1, windowMs: 0 }), RangeError);
  assert.throws(() => mod.createGate({ limit: 1, windowMs: -5 }), RangeError);
  assert.throws(() => mod.createGate({ limit: 1, windowMs: 10, maxWaitMs: -1 }), RangeError);
});

test('acquire validates units types and ranges without touching quota stats', () => {
  const gate = mod.createGate({ limit: 2, windowMs: 60_000 });
  for (const bad of [null, '1', 1.2, NaN]) {
    assert.throws(() => gate.acquire(bad), TypeError);
  }
  assert.throws(() => gate.acquire(0), RangeError);
  assert.throws(() => gate.acquire(-1), RangeError);
  assert.throws(() => gate.acquire(3), RangeError);
  const s = gate.stats();
  assert.equal(s.granted + s.refused + s.expired, 0);
});

test('default units is 1 and default maxWaitMs refuses immediately', async () => {
  const gate = mod.createGate({ limit: 1, windowMs: 60_000 });
  await gate.acquire();
  await assert.rejects(gate.acquire(), (err) => err.code === 'QUOTA_EXCEEDED');
  assert.equal(gate.stats().waiting, 0);
});
