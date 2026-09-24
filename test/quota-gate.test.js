import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createGate, QuotaExceededError } from '../src/index.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('stats exposes the documented keys in order', () => {
  const gate = createGate({ limit: 2, windowMs: 100 });
  assert.deepEqual(Object.keys(gate.stats()), [
    'limit',
    'windowMs',
    'inFlight',
    'waiting',
    'granted',
    'refused',
    'expired',
    'cancelled',
    'reserved',
    'rolledBack',
  ]);
  assert.deepEqual(gate.stats(), {
    limit: 2,
    windowMs: 100,
    inFlight: 0,
    waiting: 0,
    granted: 0,
    refused: 0,
    expired: 0,
    cancelled: 0,
    reserved: 0,
    rolledBack: 0,
  });
});

test('grants up to the limit and refuses beyond it', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000 });
  const handle = await gate.acquire();
  assert.equal(gate.stats().inFlight, 1);
  assert.equal(gate.stats().granted, 1);
  await assert.rejects(gate.acquire(), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.name, 'QuotaExceededError');
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(gate.stats().refused, 1);
  handle.release();
  assert.equal(gate.stats().inFlight, 0);
});

test('validates constructor and acquire arguments', async () => {
  assert.throws(() => createGate(), TypeError);
  assert.throws(() => createGate({ limit: 1.5, windowMs: 10 }), TypeError);
  assert.throws(() => createGate({ limit: '2', windowMs: 10 }), TypeError);
  assert.throws(() => createGate({ limit: 0, windowMs: 10 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 0 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 10, maxWaitMs: -1 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 10, maxWaitMs: 1.5 }), TypeError);

  const gate = createGate({ limit: 3, windowMs: 1000 });
  // Non-positive or non-integer unit counts are parameter errors; a count
  // above the gate's own limit is NOT — it is a quota request that borrows up
  // the chain, and is refused with QUOTA_EXCEEDED only when nothing covers it.
  assert.throws(() => gate.acquire(0), RangeError);
  assert.throws(() => gate.acquire(-2), RangeError);
  assert.throws(() => gate.acquire(1.5), RangeError);
  assert.throws(() => gate.acquire(NaN), RangeError);
  assert.throws(() => gate.acquire('1'), TypeError);
  assert.throws(() => gate.acquire(null), TypeError);
  await assert.rejects(gate.acquire(4), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(gate.stats().refused, 1);
  assert.equal(gate.stats().granted, 0); // the over-limit request never occupied anything
});

test('release is idempotent and stale handles do not corrupt stats', async () => {
  const gate = createGate({ limit: 3, windowMs: 50 });
  const h1 = await gate.acquire(2);
  h1.release();
  h1.release(); // repeated release: no error, no stat change
  assert.equal(gate.stats().inFlight, 0);

  const h2 = await gate.acquire(1);
  assert.equal(gate.stats().inFlight, 1);
  await sleep(90); // the window closes; occupations clear automatically
  assert.equal(gate.stats().inFlight, 0);
  h2.release(); // stale handle: no-op
  assert.equal(gate.stats().inFlight, 0);
});

test('queued requests are granted in arrival order at the window flip', async () => {
  const gate = createGate({ limit: 2, windowMs: 60, maxWaitMs: 1000 });
  const h = await gate.acquire(2); // window full
  const order = [];
  const p1 = gate.acquire(1).then((handle) => {
    order.push(1);
    return handle;
  });
  const p2 = gate.acquire(1).then((handle) => {
    order.push(2);
    return handle;
  });
  assert.equal(gate.stats().waiting, 2);
  const [h1, h2] = await Promise.all([p1, p2]);
  assert.deepEqual(order, [1, 2]); // same-tick arrivals settle in arrival order
  assert.equal(gate.stats().waiting, 0);
  assert.equal(gate.stats().granted, 3);
  h.release();
  h1.release();
  h2.release();
});

test('a queued request that does not fit blocks later ones (mixed sizes)', async () => {
  const gate = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(2);
  let bigResolved = false;
  let smallResolved = false;
  const pBig = gate.acquire(3).then((h) => {
    bigResolved = true;
    return h;
  });
  const pSmall = gate.acquire(1).then((h) => {
    smallResolved = true;
    return h;
  });
  await sleep(90); // first flip passed: the big head fits a fresh window
  assert.equal(bigResolved, true);
  assert.equal(smallResolved, false); // 3 of 3 used; small waits for the next flip
  await pSmall;
  assert.equal(smallResolved, true);
  await pBig;
});

test('requests due at the flip moment expire instead of being granted', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 30 });
  await gate.acquire(1);
  await assert.rejects(gate.acquire(1), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(gate.stats().expired, 1);
  assert.equal(gate.stats().granted, 1); // expiry won over allocation
});

test('a patient queued request is granted at the window flip', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(1);
  const h = await gate.acquire(1); // granted at the first flip
  assert.equal(gate.stats().granted, 2);
  assert.equal(gate.stats().inFlight, 1);
  h.release();
});

test('multiple waits expiring at the same tick all expire', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 30 });
  await gate.acquire(1);
  const results = await Promise.allSettled([gate.acquire(1), gate.acquire(1)]);
  assert.equal(results[0].status, 'rejected');
  assert.equal(results[1].status, 'rejected');
  assert.equal(results[0].reason.code, 'WAIT_EXPIRED');
  assert.equal(results[1].reason.code, 'WAIT_EXPIRED');
  assert.equal(gate.stats().expired, 2);
});

test('clock rollback does not revive expired windows or corrupt waits', async (t) => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  const stale = await gate.acquire(1);
  const queued = gate.acquire(1); // waits for the first flip
  t.mock.method(Date, 'now', () => 0); // clock jumps far backwards
  const h = await queued; // still granted at the flip, not wrongly expired
  assert.equal(gate.stats().inFlight, 1);
  stale.release(); // its window expired; rollback cannot revive it
  assert.equal(gate.stats().inFlight, 1);
  h.release();
  assert.equal(gate.stats().inFlight, 0);
});

test('child gates validate their parent and limit', async () => {
  const parent = createGate({ limit: 3, windowMs: 1000 });
  assert.throws(() => createGate({ limit: 2, windowMs: 1000, parent: {} }), TypeError);
  assert.throws(() => createGate({ limit: 2, windowMs: 1000, parent: 'gate' }), TypeError);
  assert.throws(() => createGate({ limit: 4, windowMs: 1000, parent }), RangeError);

  const child = createGate({ limit: 2, windowMs: 1000, parent });
  // A unit count above the child's own limit is not a parameter error: the
  // excess borrows up the parent chain and the pool. Here 3 units fit the
  // 3-unit family pool (2 own + 1 pool), so the occupation is granted.
  const h = await child.acquire(3);
  assert.equal(child.stats().inFlight, 3);
  h.release();
  assert.throws(() => child.acquire(0), RangeError);
  assert.throws(() => child.acquire(1.2), RangeError);
  assert.throws(() => child.acquire('3'), TypeError);
});

test('child and parent share one quota pool', async () => {
  const parent = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent });
  await child.acquire(2); // draws 2 from the shared pool
  await parent.acquire(1); // 1 left in the pool
  await assert.rejects(parent.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  await assert.rejects(child.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(parent.stats().refused, 1);
  assert.equal(child.stats().refused, 1);
});

test('child borrows the shortfall from the parent pool for one occupation', async () => {
  const parent = createGate({ limit: 5, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent });
  await child.acquire(2); // own quota
  await child.acquire(2); // borrowed from the parent pool
  assert.equal(child.stats().inFlight, 4);
  await parent.acquire(1); // 5 of 5 used; the pool is now contended
  await assert.rejects(parent.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  await assert.rejects(child.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
});

test('borrowed quota is reclaimed at the parent window end without breaking occupations', async () => {
  const parent = createGate({ limit: 5, windowMs: 60 });
  const child = createGate({ limit: 2, windowMs: 10000, parent });
  const own = await child.acquire(2);
  const borrowed = await child.acquire(2); // 2 borrowed units
  assert.equal(child.stats().inFlight, 4);
  await sleep(120); // two parent windows pass; the child window is still open
  // The borrowed 2 units were reclaimed at the parent window flip, visible in
  // the same stats snapshot; the child's own 2 units are still occupied.
  assert.equal(child.stats().inFlight, 2);
  assert.equal(parent.stats().inFlight, 0);
  // The reclaim did not break the occupations: both handles release cleanly.
  borrowed.release();
  assert.equal(child.stats().inFlight, 2); // borrowed part already reclaimed
  own.release();
  assert.equal(child.stats().inFlight, 0);
});

test('reclaim settles before contending queued requests are allocated', async () => {
  const parent = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 1000, parent });
  await child.acquire(2); // own quota, pool 2/3
  await child.acquire(1); // 1 borrowed, pool 3/3
  assert.equal(child.stats().inFlight, 3);
  const order = [];
  const pParent = parent.acquire(1).then((h) => {
    order.push('parent');
    return h;
  });
  const pChild = child.acquire(1).then((h) => {
    order.push('child');
    return h;
  });
  assert.equal(parent.stats().waiting, 1);
  assert.equal(child.stats().waiting, 1);
  await Promise.all([pParent, pChild]);
  assert.deepEqual(order, ['parent', 'child']); // arrival order across gates
  // The earlier borrow was reclaimed at the parent flip before allocation;
  // the child then borrowed again for its newly granted request.
  assert.equal(child.stats().inFlight, 3); // 2 own + 1 newly borrowed
  assert.equal(parent.stats().inFlight, 1);
  assert.equal(parent.stats().waiting, 0);
  assert.equal(child.stats().waiting, 0);
});

test('queued waiters are served before later arrivals across the hierarchy', async () => {
  const parent = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000, parent });
  await parent.acquire(1); // pool full
  const order = [];
  const pChild = child.acquire(1).then((h) => {
    order.push('child');
    return h;
  });
  const pParent = parent.acquire(1).then((h) => {
    order.push('parent');
    return h;
  });
  await sleep(90); // first flip: the earlier child request wins the pool
  assert.deepEqual(order, ['child']);
  await pParent; // served at the next flip; never starves
  assert.deepEqual(order, ['child', 'parent']);
  await pChild;
});

test('limit of one serializes the whole hierarchy', async () => {
  const parent = createGate({ limit: 1, windowMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 1000, parent });
  await child.acquire(1);
  await assert.rejects(parent.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  await assert.rejects(child.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
});

// --- Runtime adjustments ---------------------------------------------------

test('updateLimit validates value and hierarchy without producing a cut', () => {
  const parent = createGate({ limit: 5, windowMs: 1000 });
  const child = createGate({ limit: 3, windowMs: 1000, parent });
  const beforeParent = parent.stats();
  const beforeChild = child.stats();
  assert.throws(() => child.updateLimit(1.5), TypeError);
  assert.throws(() => child.updateLimit('2'), TypeError);
  assert.throws(() => child.updateLimit(0), RangeError);
  assert.throws(() => child.updateLimit(-1), RangeError);
  assert.throws(() => child.updateLimit(6), RangeError); // over the direct parent
  assert.throws(() => parent.updateLimit(2), RangeError); // below the direct child
  assert.deepEqual(parent.stats(), beforeParent);
  assert.deepEqual(child.stats(), beforeChild);
  child.updateLimit(4);
  assert.equal(child.stats().limit, 4);
  parent.updateLimit(4); // equal to the child's limit is allowed
  assert.equal(parent.stats().limit, 4);
});

test('updateLimit shrink refuses queued requests that can never fit', async () => {
  const gate = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  const h = await gate.acquire(3); // pool full
  const pBig = gate.acquire(2);
  const pSmall = gate.acquire(1);
  assert.equal(gate.stats().waiting, 3);
  gate.updateLimit(1);
  // One synchronous cut: the oversized waiter is refused on the spot and
  // leaves the queue; occupations are not torn apart.
  assert.equal(gate.stats().refused, 1);
  assert.equal(gate.stats().waiting, 1);
  assert.equal(gate.stats().inFlight, 3);
  await assert.rejects(pBig, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  // The survivor is not granted early and is not blocked by the refused one.
  const hSmall = await pSmall;
  assert.equal(gate.stats().granted, 2);
  h.release();
  hSmall.release();
});

test('updateLimit grow never grants queued requests early', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(1);
  const p = gate.acquire(1); // queued
  gate.updateLimit(5); // room now, but waiters are never released early
  assert.equal(gate.stats().waiting, 1);
  assert.equal(gate.stats().granted, 1);
  const h = await p; // granted at the window flip
  assert.equal(gate.stats().granted, 2);
  h.release();
});

test('updateLimit on a child does not refuse a waiter the pool can still cover', async () => {
  const parent = createGate({ limit: 5, windowMs: 60, maxWaitMs: 1000 });
  const child = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000, parent });
  await parent.acquire(5); // pool full
  const pChild = child.acquire(3); // queued, 3 units (above the child limit to come)
  const pParent = parent.acquire(1); // queued behind
  child.updateLimit(2); // above the child's own limit: covered by borrowing, not refused
  assert.equal(child.stats().refused, 0);
  assert.equal(child.stats().waiting, 3);
  assert.equal(parent.stats().waiting, 1);
  const hChild = await pChild; // served at the pool flip: 2 own + 1 borrowed
  assert.equal(child.stats().inFlight, 3);
  const hParent = await pParent;
  hChild.release();
  hParent.release();
});

test('acquire treats an over-own-limit count as a quota request, not a parameter error', async () => {
  const gate = createGate({ limit: 2, windowMs: 1000 });
  gate.updateLimit(3);
  await gate.acquire(3); // allowed under the new limit
  // Above the current limit: not a RangeError. With maxWaitMs 0 and a pool
  // that cannot cover it, it is refused QUOTA_EXCEEDED on the spot.
  await assert.rejects(
    () => gate.acquire(4),
    (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    },
  );
  gate.updateLimit(1);
  // A 2-unit request on a gate whose own limit is 1 still is not a parameter
  // error — but the 3-unit family pool already has 3 occupied, so it is
  // refused for lack of quota rather than rejected as a bad argument.
  await assert.rejects(
    () => gate.acquire(2),
    (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    },
  );
});

test('updateWindowMs restarts the current window with the new length', async () => {
  const gate = createGate({ limit: 1, windowMs: 40, maxWaitMs: 1000 });
  const h = await gate.acquire(1);
  gate.updateWindowMs(120); // the window now flips ~120ms from here
  assert.equal(gate.stats().windowMs, 120);
  const p = gate.acquire(1); // queued behind the full window
  await sleep(70); // the old 40ms length would have flipped; the new one has not
  assert.equal(gate.stats().waiting, 1);
  assert.equal(gate.stats().granted, 1); // not granted early
  assert.equal(gate.stats().inFlight, 1); // used quota not cleared
  const h2 = await p; // granted when the resized window flips
  assert.equal(gate.stats().granted, 2);
  h.release();
  h2.release();
});

test('updateWindowMs validates its argument without producing a cut', () => {
  const gate = createGate({ limit: 1, windowMs: 100 });
  const before = gate.stats();
  assert.throws(() => gate.updateWindowMs(0), RangeError);
  assert.throws(() => gate.updateWindowMs(-5), RangeError);
  assert.throws(() => gate.updateWindowMs(1.5), TypeError);
  assert.throws(() => gate.updateWindowMs('50'), TypeError);
  assert.deepEqual(gate.stats(), before);
});

test('updateMaxWaitMs expires already-late waiters on the spot', async () => {
  const gate = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000 });
  await gate.acquire(1); // pool full
  const p = gate.acquire(1); // queued at t0 with deadline t0+1000
  await sleep(60);
  gate.updateMaxWaitMs(30); // new deadline t0+30 is already past
  await assert.rejects(p, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(gate.stats().expired, 1);
  assert.equal(gate.stats().waiting, 0);
  assert.equal(gate.stats().granted, 1); // nothing granted early
});

test('updateMaxWaitMs extension keeps waiters alive past the old cap', async () => {
  const gate = createGate({ limit: 1, windowMs: 90, maxWaitMs: 40 });
  await gate.acquire(1);
  const p = gate.acquire(1); // would expire at t0+40
  gate.updateMaxWaitMs(1000); // deadline moves to t0+1000
  await sleep(60); // past the old cap; the waiter is still alive
  assert.equal(gate.stats().expired, 0);
  assert.equal(gate.stats().waiting, 1);
  const h = await p; // granted at the window flip (~90ms)
  assert.equal(gate.stats().granted, 2);
  h.release();
});

test('updateMaxWaitMs only retimes the gate\'s own waiters', async () => {
  const parent = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000, parent });
  await parent.acquire(1); // pool full
  const pParent = parent.acquire(1);
  const pChild = child.acquire(1);
  parent.updateMaxWaitMs(0); // every parent waiter is already late
  await assert.rejects(pParent, (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(parent.stats().expired, 1);
  assert.equal(child.stats().expired, 0);
  assert.equal(child.stats().waiting, 1); // the child's waiter is untouched
  child.updateMaxWaitMs(0);
  await assert.rejects(pChild, (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
});

test('updateMaxWaitMs validates its argument without producing a cut', () => {
  const gate = createGate({ limit: 1, windowMs: 1000 });
  const before = gate.stats();
  assert.throws(() => gate.updateMaxWaitMs(-1), RangeError);
  assert.throws(() => gate.updateMaxWaitMs(1.5), TypeError);
  assert.throws(() => gate.updateMaxWaitMs('10'), TypeError);
  assert.deepEqual(gate.stats(), before);
});

test('reparent validates its target without producing a cut', () => {
  const a = createGate({ limit: 5, windowMs: 1000 });
  const b = createGate({ limit: 3, windowMs: 1000, parent: a });
  const c = createGate({ limit: 2, windowMs: 1000, parent: b });
  const other = createGate({ limit: 1, windowMs: 1000 });
  const gates = [a, b, c, other];
  const before = gates.map((g) => g.stats());
  assert.throws(() => b.reparent({}), TypeError);
  assert.throws(() => b.reparent('gate'), TypeError);
  assert.throws(() => b.reparent(null), TypeError);
  assert.throws(() => b.reparent(b), RangeError); // under itself
  assert.throws(() => b.reparent(c), RangeError); // under its own child
  assert.throws(() => a.reparent(c), RangeError); // under its own grandchild
  assert.throws(() => b.reparent(other), RangeError); // limit 3 over the new parent's 1
  assert.deepEqual(gates.map((g) => g.stats()), before);
});

test('reparent settles the borrow in the old pool and re-borrows from the new one', async () => {
  const oldRoot = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: oldRoot });
  const newRoot = createGate({ limit: 4, windowMs: 1000 });
  await child.acquire(2); // draws 2 from the old pool
  child.reparent(newRoot);
  // Both sides of the trade are visible in the same snapshot: the old pool
  // has its 2 units back, the new pool is charged 2.
  await oldRoot.acquire(3);
  await newRoot.acquire(2);
  await assert.rejects(newRoot.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  // The reparent itself moved no counters.
  assert.equal(child.stats().granted, 1);
  assert.equal(oldRoot.stats().granted, 1);
  assert.equal(newRoot.stats().refused, 1);
});

test('reparent rejects with QUOTA_EXCEEDED and rolls back when the new pool is short', async () => {
  const oldRoot = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 1000 });
  await child.acquire(2); // subtree draw of 2
  await newRoot.acquire(1); // only 1 free in the new pool
  const before = [oldRoot.stats(), child.stats(), newRoot.stats()];
  assert.throws(
    () => child.reparent(newRoot),
    (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    },
  );
  assert.deepEqual([oldRoot.stats(), child.stats(), newRoot.stats()], before);
  // The child still belongs to the old family: its draw still counts there.
  await assert.rejects(oldRoot.acquire(2), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  await oldRoot.acquire(1); // exactly 1 free in the old pool
});

test('reparent migrates queued waiters into the new family queue', async () => {
  const oldRoot = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 60, maxWaitMs: 1000 });
  await oldRoot.acquire(1); // old pool full
  const p = child.acquire(1); // queued in the old family
  assert.equal(child.stats().waiting, 1);
  child.reparent(newRoot);
  assert.equal(child.stats().waiting, 1); // still queued, not granted early
  const h = await p; // served at the NEW family's window flip
  assert.equal(child.stats().granted, 1);
  h.release();
});

test('reparented occupations stay valid and stale out with the new pool window', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 10000 });
  const child = createGate({ limit: 2, windowMs: 10000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 60 });
  const own = await child.acquire(2);
  const borrowed = await child.acquire(2); // 2 borrowed units
  assert.equal(child.stats().inFlight, 4);
  child.reparent(newRoot);
  assert.equal(child.stats().inFlight, 4); // occupations not torn apart
  await sleep(90); // the new pool window flips: the borrowed 2 are reclaimed
  assert.equal(child.stats().inFlight, 2);
  borrowed.release(); // stale in the new pool: no double subtract
  assert.equal(child.stats().inFlight, 2);
  own.release();
  assert.equal(child.stats().inFlight, 0);
});

test('the old family does not reclaim a moved borrow at its window flip', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 50 });
  const child = createGate({ limit: 2, windowMs: 10000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 10000 });
  await child.acquire(2);
  await child.acquire(2); // 2 borrowed units
  child.reparent(newRoot);
  await sleep(80); // the old root's window flips; the moved borrow is not reclaimed there
  assert.equal(child.stats().inFlight, 4);
});

test('a root gate can be reparented and becomes an ordinary child', async () => {
  const root = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 1000, parent: root });
  const newRoot = createGate({ limit: 5, windowMs: 60 });
  await root.acquire(2); // the whole subtree draw is 2
  root.reparent(newRoot);
  // The draw moved with the subtree: 3 free in the new pool.
  await newRoot.acquire(3);
  await assert.rejects(newRoot.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  await sleep(90); // the new pool window flips; the old root's own window is still open
  const h = await root.acquire(1); // now borrows from the new pool like any child
  assert.equal(root.stats().granted, 2);
  assert.equal(child.stats().granted, 0);
  h.release();
});

test('reparent within the same family only moves the direct link', async () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  const c = createGate({ limit: 2, windowMs: 1000, parent: a });
  await c.acquire(2);
  const gates = [root, a, b, c];
  const before = gates.map((g) => g.stats());
  c.reparent(b);
  assert.deepEqual(gates.map((g) => g.stats()), before); // no cut anywhere
  await root.acquire(3); // the shared pool accounting is unchanged: 2 used of 5
  await assert.rejects(root.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
});

// --- Cancellation ----------------------------------------------------------

test('acquire validates the signal argument', () => {
  const gate = createGate({ limit: 1, windowMs: 1000 });
  assert.throws(() => gate.acquire(1, {}), TypeError);
  assert.throws(() => gate.acquire(1, 'signal'), TypeError);
  assert.throws(() => gate.acquire(1, 42), TypeError);
});

test('a pre-aborted signal settles as CANCELLED without queueing', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000 });
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(gate.acquire(1, ac.signal), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  const s = gate.stats();
  assert.equal(s.cancelled, 1);
  assert.equal(s.waiting, 0);
  assert.equal(s.granted, 0); // quota was free; still not granted
});

test('aborting a queued request cancels it and only moves waiting and cancelled', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 5000 });
  const h = await gate.acquire(1);
  const ac = new AbortController();
  const p = gate.acquire(1, ac.signal);
  assert.equal(gate.stats().waiting, 1);
  ac.abort();
  await assert.rejects(p, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  const s = gate.stats();
  assert.equal(s.cancelled, 1);
  assert.equal(s.waiting, 0);
  assert.equal(s.granted, 1);
  assert.equal(s.refused, 0);
  assert.equal(s.expired, 0);
  h.release();
});

test('cancelling the queue head does not grant later waiters early', async () => {
  const gate = createGate({ limit: 2, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(2); // pool full
  const ac = new AbortController();
  const p1 = gate.acquire(2, ac.signal); // head of the queue
  const p2 = gate.acquire(1); // queued behind
  ac.abort();
  await assert.rejects(p1, (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  await sleep(20); // well before the 60ms flip
  assert.equal(gate.stats().waiting, 1); // p2 still queued, not granted early
  assert.equal(gate.stats().granted, 1);
  const h = await p2; // granted at the window flip
  h.release();
});

test('abort after grant or after expiry is a no-op', async () => {
  const gate = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000 });
  const ac1 = new AbortController();
  const h = await gate.acquire(1, ac1.signal); // granted immediately
  ac1.abort(); // already granted: does not count
  assert.equal(gate.stats().cancelled, 0);
  assert.equal(gate.stats().inFlight, 1);
  h.release();

  const gate2 = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 30 });
  await gate2.acquire(1);
  const ac2 = new AbortController();
  const p = gate2.acquire(1, ac2.signal);
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  ac2.abort(); // already settled: does not count
  assert.equal(gate2.stats().cancelled, 0);
  assert.equal(gate2.stats().expired, 1);
});

test('an abort arriving once the deadline has passed settles as expired', async (t) => {
  const gate = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 1000 });
  await gate.acquire(1);
  const ac = new AbortController();
  const realNow = Date.now();
  const p = gate.acquire(1, ac.signal); // deadline = now + 1000
  t.mock.method(Date, 'now', () => realNow + 5000); // clock jumps past the deadline
  ac.abort(); // expiry beats cancellation at the same moment
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(gate.stats().expired, 1);
  assert.equal(gate.stats().cancelled, 0);
});

test('the four terminal counters together account for every settled request', async () => {
  const g1 = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 0 });
  await g1.acquire(); // granted
  await assert.rejects(g1.acquire(), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  }); // refused
  const g2 = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 20 });
  await g2.acquire(); // granted
  await assert.rejects(g2.acquire(), (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  }); // expired
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(g2.acquire(1, ac.signal), (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  }); // cancelled
  const s1 = g1.stats();
  assert.equal(s1.granted + s1.refused + s1.expired + s1.cancelled, 2);
  const s2 = g2.stats();
  assert.equal(s2.granted + s2.refused + s2.expired + s2.cancelled, 3);
});

// --- Cross-gate acquireAll --------------------------------------------------

test('acquireAll grants one combined occupation and returns a release-only handle', async () => {
  const root = createGate({ limit: 6, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  const handle = await root.acquireAll([{ gate: a, units: 2 }, { gate: b, units: 2 }]);
  assert.deepEqual(Object.keys(handle), ['release']);
  assert.equal(typeof handle.release, 'function');
  assert.equal(a.stats().inFlight, 2);
  assert.equal(b.stats().inFlight, 2);
  assert.equal(root.stats().granted, 1);
  // A single-gate occupation has the very same shape.
  const single = await root.acquire(1);
  assert.deepEqual(Object.keys(single), ['release']);
  // Destructuring works: release does not depend on its receiver.
  const { release } = handle;
  release();
  release(); // idempotent
  assert.equal(a.stats().inFlight, 0);
  assert.equal(b.stats().inFlight, 0);
  single.release();
});

test('a member above its own limit borrows up the direct parent chain then the pool', async () => {
  const root = createGate({ limit: 10, windowMs: 1000 });
  const p = createGate({ limit: 3, windowMs: 1000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, parent: p });
  await p.acquire(2); // occupy two of p's own window, leaving one to lend
  const handle = await root.acquireAll([{ gate: a, units: 4 }]); // own 2, parent lend 1, pool 1
  assert.equal(a.stats().inFlight, 4);
  assert.equal(p.stats().inFlight, 3); // its own 2 plus the one lent unit
  handle.release();
  assert.equal(a.stats().inFlight, 0);
  assert.equal(p.stats().inFlight, 2); // only its own occupation remains
});

test('a queued combo holds reservations until it commits at the pool flip', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000, parent: root });
  await root.acquire(4); // pool 4/5
  const pSingle = root.acquire(2); // earlier arrival; 4+2>5, queues
  const pCombo = root.acquireAll([{ gate: a, units: 1 }]); // 4+1=5 gathers, waits behind
  assert.equal(a.stats().waiting, 1); // the member carries its own slice
  assert.equal(a.stats().reserved, 1); // its unit is reserved, not yet occupied
  assert.equal(a.stats().inFlight, 0);
  assert.equal(root.stats().reserved, 0); // the unit sat in the member's own quota
  const [hSingle, hCombo] = await Promise.all([pSingle, pCombo]);
  assert.equal(a.stats().reserved, 0); // committed: reservation merges into the occupation
  assert.equal(a.stats().inFlight, 1);
  assert.equal(a.stats().waiting, 0);
  hSingle.release();
  hCombo.release();
});

test('a combo that cannot be gathered rolls back as a whole and retries at the flip', async () => {
  const root = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  const a = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 1000, parent: root });
  await root.acquire(2);
  const pSingle = root.acquire(2); // earlier arrival
  const pCombo = root.acquireAll([{ gate: a, units: 3 }]); // 2+3>3: rolls back, queues
  const s = root.stats();
  assert.equal(s.rolledBack, 1); // one failed gather
  assert.equal(s.refused, 0); // still waiting
  assert.equal(s.granted, 1); // nothing else moved
  assert.equal(a.stats().reserved, 0); // every provisional reservation was released
  const h = await pCombo; // gathered and granted at the fresh pool window
  await pSingle;
  assert.equal(a.stats().inFlight, 3);
  h.release();
});

test('a blocked head combo yields twice and then blocks later requests', async () => {
  const root = createGate({ limit: 4, windowMs: 40, maxWaitMs: 5000 });
  const a = createGate({ limit: 4, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const ac = new AbortController();
  const bigErr = root.acquireAll([{ gate: a, units: 5 }], ac.signal).then(
    () => null,
    (e) => e,
  ); // can never fit pool 4; rejection handler attached up front
  const settled = [];
  let h1;
  let h2;
  let h3;
  const p1 = root.acquire(1).then((h) => { h1 = h; settled.push('s1'); });
  const p2 = root.acquire(1).then((h) => { h2 = h; settled.push('s2'); });
  const p3 = root.acquire(1).then((h) => { h3 = h; settled.push('s3'); });
  await sleep(70); // first pool flip: two later requests overtake
  assert.deepEqual(settled, ['s1', 's2']);
  await sleep(40); // next flip: the combo already yielded twice, s3 must not pass it
  assert.deepEqual(settled, ['s1', 's2']);
  ac.abort(); // tear down the blocked combo
  assert.equal((await bigErr).code, 'CANCELLED');
  await p3; // once the combo leaves, s3 flows at the next flip
  assert.deepEqual(settled, ['s1', 's2', 's3']);
  h1.release();
  h2.release();
  h3.release();
});

test('a later combo that fits can overtake a blocked head combo once', async () => {
  const root = createGate({ limit: 4, windowMs: 40, maxWaitMs: 5000 });
  const a = createGate({ limit: 4, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const b = createGate({ limit: 4, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const ac = new AbortController();
  let bigGranted = false;
  const bigErr = a
    .acquireAll([{ gate: a, units: 5 }], ac.signal)
    .then(() => {
      bigGranted = true;
      return null;
    }, (e) => e);
  const pSmall = b.acquireAll([{ gate: b, units: 2 }]);
  const hSmall = await pSmall; // fits the fresh pool outright, overtakes the head
  assert.equal(bigGranted, false);
  hSmall.release();
  ac.abort();
  assert.equal((await bigErr).code, 'CANCELLED');
});

test('the pool flip reclaims uncommitted reservations before admitted borrows', async () => {
  const root = createGate({ limit: 5, windowMs: 60 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const h = await root.acquireAll([{ gate: a, units: 2 }]);
  await sleep(100); // the pool window flips; the combo handle stays valid
  assert.equal(a.stats().inFlight, 2); // occupation intact
  h.release(); // no double accounting on the reclaimed pool link
  assert.equal(a.stats().inFlight, 0);
});

test('a parent window flip reclaims uncommitted lends before admitted lends', async () => {
  const root = createGate({ limit: 10, windowMs: 1000, maxWaitMs: 5000 });
  const p = createGate({ limit: 2, windowMs: 40, maxWaitMs: 5000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: p });
  await root.acquire(6); // pool 6/10
  const pSingle = root.acquire(5); // 6+5>10 queues first
  // own 2 + parent lend 2 = 4; 6+4=10 gathers, waits behind the single.
  const pCombo = root.acquireAll([{ gate: a, units: 4 }]);
  assert.equal(a.stats().reserved, 2);
  assert.equal(p.stats().reserved, 2);
  await sleep(60); // p's window flips before the pool window: lend reservation reclaimed
  assert.equal(p.stats().reserved, 0);
  assert.equal(a.stats().reserved, 2); // the member's own window is still open
  const [, h] = await Promise.all([pSingle, pCombo]); // both flow at the pool flip
  assert.equal(a.stats().inFlight, 4);
  h.release();
});

test('a parent window flip reclaims admitted lends without breaking the handle', async () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const p = createGate({ limit: 2, windowMs: 40, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, parent: p });
  const h = await root.acquireAll([{ gate: a, units: 4 }]); // own 2, parent lend 2
  assert.equal(a.stats().inFlight, 4);
  assert.equal(p.stats().inFlight, 2);
  await sleep(80); // the parent window flips twice: the lend is reclaimed
  assert.equal(p.stats().inFlight, 0);
  assert.equal(a.stats().inFlight, 2); // the own part survives
  h.release(); // still a valid handle; no double subtract
  assert.equal(a.stats().inFlight, 0);
});

test('acquireAll expiry and cancellation release the whole reservation', async () => {
  const root = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  await root.acquire(2);
  root.updateMaxWaitMs(30);
  await assert.rejects(root.acquireAll([{ gate: a, units: 1 }]), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(root.stats().expired, 1);
  assert.equal(a.stats().reserved, 0);

  root.updateMaxWaitMs(5000);
  const ac = new AbortController();
  const p = root.acquireAll([{ gate: a, units: 1 }], ac.signal);
  ac.abort();
  await assert.rejects(p, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(root.stats().cancelled, 1);
  assert.equal(a.stats().reserved, 0);
  assert.equal(a.stats().waiting, 0);
});

test('a pre-aborted acquireAll settles CANCELLED without queueing or reserving', async () => {
  const root = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(root.acquireAll([{ gate: a, units: 1 }], ac.signal), (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  const s = root.stats();
  assert.equal(s.cancelled, 1);
  assert.equal(s.waiting, 0);
  assert.equal(s.granted, 0);
  assert.equal(a.stats().reserved, 0);
});

test('acquireAll with maxWaitMs 0 refuses QUOTA_EXCEEDED on the spot', async () => {
  const root = createGate({ limit: 2, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  await root.acquire(2);
  await assert.rejects(root.acquireAll([{ gate: a, units: 1 }]), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(root.stats().refused, 1);
  assert.equal(a.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
});

test('acquireAll validates members without producing a cut', () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  const child = createGate({ limit: 1, windowMs: 1000, parent: a });
  const other = createGate({ limit: 2, windowMs: 1000 });
  const before = [root.stats(), a.stats(), b.stats()];
  assert.throws(() => root.acquireAll([]), RangeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: 0 }]), RangeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: -1 }]), RangeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: 1.5 }]), RangeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: '1' }]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: a }]), TypeError);
  assert.throws(() => root.acquireAll('nope'), TypeError);
  assert.throws(() => root.acquireAll([null]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: {}, units: 1 }]), TypeError);
  assert.throws(
    () => root.acquireAll([{ gate: a, units: 1 }, { gate: a, units: 1 }]),
    RangeError,
  );
  assert.throws(() => root.acquireAll([{ gate: a, units: 1 }, { gate: other, units: 1 }]), RangeError);
  assert.throws(
    () => root.acquireAll([{ gate: a, units: 1 }, { gate: child, units: 1 }]),
    RangeError,
  );
  assert.deepEqual([root.stats(), a.stats(), b.stats()], before);
});

test('a root limit shrink refuses oversized queued combos as a whole', async () => {
  const root = createGate({ limit: 5, windowMs: 1000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: root });
  await root.acquire(3);
  const p = root.acquireAll([{ gate: a, units: 2 }, { gate: b, units: 2 }]); // total 4
  root.updateLimit(3); // the whole combo can never fit
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(root.stats().refused, 1);
  assert.equal(a.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
  assert.equal(b.stats().waiting, 0);
});

test('a member limit shrink does not refuse a combo that the pool can still cover', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 60, maxWaitMs: 5000, parent: root });
  await root.acquire(3);
  const p = root.acquireAll([{ gate: a, units: 3 }]); // above the new limit, pool still covers
  a.updateLimit(1);
  assert.equal(root.stats().refused, 0);
  const h = await p; // granted at the flip, borrowing the excess up the chain
  assert.equal(a.stats().inFlight, 3);
  h.release();
});

test('updateMaxWaitMs expires a queued combo and releases its reservation', async () => {
  const root = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: root });
  await root.acquire(2);
  const p = root.acquireAll([{ gate: a, units: 1 }]);
  root.updateMaxWaitMs(0);
  await assert.rejects(p, (err) => err.code === 'WAIT_EXPIRED');
  assert.equal(root.stats().expired, 1);
  assert.equal(a.stats().reserved, 0);
});

test('reparent refuses a combo split across the two families and moves a whole combo', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 1000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: oldRoot });
  const b = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 60, maxWaitMs: 5000 });
  await oldRoot.acquire(4);
  const pSplit = oldRoot.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }]);
  const pMoved = oldRoot.acquireAll([{ gate: a, units: 1 }]);
  a.reparent(newRoot);
  await assert.rejects(pSplit, (err) => err.code === 'QUOTA_EXCEEDED');
  const oldGranted = oldRoot.stats().granted;
  const h = await pMoved; // gathers anew and grants in the joined family
  assert.equal(newRoot.stats().granted + oldRoot.stats().granted, oldGranted + 1);
  h.release();
});

// --- Single-gate requests above the gate's own limit (chain borrowing) ------

test('a single request above its own limit borrows own -> parent -> grandparent -> pool', async () => {
  const root = createGate({ limit: 10, windowMs: 1000 });
  const p = createGate({ limit: 3, windowMs: 1000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, parent: p });
  await p.acquire(2); // leaves one unit of p's own window to lend
  const handle = await a.acquire(4); // own 2, parent lend 1, pool 1
  assert.equal(a.stats().inFlight, 4);
  assert.equal(p.stats().inFlight, 3); // its own 2 plus the one lent unit
  assert.equal(a.stats().granted, 1);
  assert.equal(a.stats().refused, 0);
  // No RangeError, no rollback accounting for a single.
  assert.equal(root.stats().rolledBack, 0);
  handle.release();
  assert.equal(a.stats().inFlight, 0);
  assert.equal(p.stats().inFlight, 2); // only p's own occupation remains
});

test('an over-limit single the family pool cannot cover is refused QUOTA_EXCEEDED', async () => {
  const root = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: root });
  await root.acquire(1); // 2 left in the pool
  await assert.rejects(child.acquire(3), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(child.stats().refused, 1);
  assert.equal(child.stats().rolledBack, 0); // singles do not move rolledBack
  assert.equal(child.stats().inFlight, 0);
  assert.equal(child.stats().reserved, 0); // the failed gather left no residue
});

test('an over-limit single queues, reserves, and borrows at the pool flip', async () => {
  const root = createGate({ limit: 10, windowMs: 60, maxWaitMs: 1000 });
  const p = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 1000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000, parent: p });
  await root.acquire(4); // pool 4/10
  await p.acquire(2); // pool 6/10, p own 2/3
  const pBlocker = root.acquire(5); // earlier arrival; 6+5>10, queues
  const hPromise = a.acquire(4); // own 2 + p lend 1 + pool 1 = 4 gathers, waits behind
  assert.equal(a.stats().waiting, 4); // the whole occupation waits
  assert.equal(a.stats().reserved, 2);
  assert.equal(p.stats().reserved, 1); // one unit promised as a lend
  assert.equal(root.stats().reserved, 1); // one unit promised from the pool
  const [hBlocker, h] = await Promise.all([pBlocker, hPromise]);
  assert.equal(a.stats().inFlight, 4);
  assert.equal(a.stats().reserved, 0);
  assert.equal(p.stats().reserved, 0);
  assert.equal(p.stats().inFlight, 3); // own 2 plus lent 1
  h.release();
  assert.equal(a.stats().inFlight, 0);
  assert.equal(p.stats().inFlight, 2);
  hBlocker.release();
});

test('an over-limit single that cannot gather queues and rolls the pool at the flip', async () => {
  const root = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000, parent: root });
  await root.acquire(3); // pool full
  const p = child.acquire(3); // own 2 + pool 1 cannot gather yet
  assert.equal(child.stats().waiting, 3);
  assert.equal(child.stats().reserved, 0); // nothing held after the failed gather
  const h = await p; // fresh pool window: own 2 + pool 1
  assert.equal(child.stats().inFlight, 3);
  assert.equal(child.stats().granted, 1);
  assert.equal(child.stats().rolledBack, 0); // rollback accounting is combo-only
  h.release();
});

test('a parent window flip reclaims a queued single\'s lend before an admitted lend', async () => {
  const root = createGate({ limit: 10, windowMs: 1000, maxWaitMs: 5000 });
  const p = createGate({ limit: 2, windowMs: 40, maxWaitMs: 5000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: p });
  await root.acquire(6); // pool 6/10
  const pSingle = root.acquire(5); // earlier arrival; 6+5>10, queues
  const pQueued = a.acquire(4); // own 2 + p lend 2 = 4 gathers (6+4=10), waits behind
  assert.equal(p.stats().reserved, 2); // the uncommitted lend
  await sleep(60); // p's window flips before the pool window
  assert.equal(p.stats().reserved, 0); // uncommitted lend reclaimed first
  const [, hQueued] = await Promise.all([pSingle, pQueued]); // both flow at the pool flip
  assert.equal(a.stats().inFlight, 4);
  hQueued.release();
});

test('a parent window flip reclaims an admitted single\'s borrowed lend without breaking the handle', async () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const p = createGate({ limit: 2, windowMs: 40, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, parent: p });
  const h = await a.acquire(4); // own 2, parent lend 2
  assert.equal(a.stats().inFlight, 4);
  assert.equal(p.stats().inFlight, 2);
  await sleep(80); // the parent window flips twice: the lend is reclaimed
  assert.equal(p.stats().inFlight, 0);
  assert.equal(a.stats().inFlight, 2); // the own part survives
  h.release(); // still a valid handle; no double subtract
  assert.equal(a.stats().inFlight, 0);
});

test('expiry and cancellation of a queued over-limit single release every reservation', async () => {
  const root = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 5000 });
  const p = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: p });
  await root.acquire(2);
  await p.acquire(1); // pool full; p own 1/3
  a.updateMaxWaitMs(20);
  await assert.rejects(a.acquire(4), (err) => {
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(a.stats().expired, 1);
  assert.equal(a.stats().reserved, 0);
  assert.equal(p.stats().reserved, 0);
  assert.equal(root.stats().reserved, 0);
  assert.equal(a.stats().waiting, 0);

  a.updateMaxWaitMs(5000);
  const ac = new AbortController();
  const pCancel = a.acquire(4, ac.signal);
  ac.abort();
  await assert.rejects(pCancel, (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(a.stats().cancelled, 1);
  assert.equal(a.stats().reserved, 0);
  assert.equal(p.stats().reserved, 0);
  assert.equal(root.stats().reserved, 0);
});

test('a pre-aborted over-limit single settles CANCELLED without reserving or queueing', async () => {
  const root = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 5000 });
  const child = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: root });
  await root.acquire(3); // pool full
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(child.acquire(3, ac.signal), (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(child.stats().cancelled, 1);
  assert.equal(child.stats().waiting, 0);
  assert.equal(child.stats().reserved, 0);
  assert.equal(root.stats().reserved, 0);
});

test('a pool limit shrink refuses an oversized queued single as a whole', async () => {
  const root = createGate({ limit: 5, windowMs: 1000, maxWaitMs: 5000 });
  const child = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 5000, parent: root });
  await root.acquire(5);
  const p = child.acquire(4); // own 2 + 2 borrowed: needs 4 pool units
  root.updateLimit(3); // 4 > 3: can never fit, refused on the spot
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(child.stats().refused, 1);
  assert.equal(child.stats().waiting, 0);
  assert.equal(child.stats().reserved, 0);
});

test('a long queue settles correctly and a single gather stays independent of queue length', async () => {
  const root = createGate({ limit: 1, windowMs: 20, maxWaitMs: 5000 });
  const child = createGate({ limit: 1, windowMs: 20, maxWaitMs: 5000, parent: root });
  await root.acquire(1); // pool full
  const N = 80;
  const promises = [];
  for (let i = 0; i < N; i += 1) {
    promises.push(i % 2 === 0 ? root.acquire(1) : child.acquire(1));
  }
  const handles = await Promise.all(promises);
  assert.equal(root.stats().granted + child.stats().granted, N + 1);
  for (const h of handles) h.release();
});
