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
  ]);
  assert.deepEqual(gate.stats(), {
    limit: 2,
    windowMs: 100,
    inFlight: 0,
    waiting: 0,
    granted: 0,
    refused: 0,
    expired: 0,
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

test('validates constructor and acquire arguments', () => {
  assert.throws(() => createGate(), TypeError);
  assert.throws(() => createGate({ limit: 1.5, windowMs: 10 }), TypeError);
  assert.throws(() => createGate({ limit: '2', windowMs: 10 }), TypeError);
  assert.throws(() => createGate({ limit: 0, windowMs: 10 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 0 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 10, maxWaitMs: -1 }), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 10, maxWaitMs: 1.5 }), TypeError);

  const gate = createGate({ limit: 3, windowMs: 1000 });
  for (const units of [0, 4]) {
    assert.throws(
      () => gate.acquire(units),
      (err) => {
        assert.ok(err instanceof RangeError);
        assert.match(err.message, /3/); // echoes the actual limit
        return true;
      },
    );
  }
  assert.throws(() => gate.acquire(1.5), TypeError);
  assert.throws(() => gate.acquire('1'), TypeError);
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

test('child gates validate their parent and limit', () => {
  const parent = createGate({ limit: 3, windowMs: 1000 });
  assert.throws(() => createGate({ limit: 2, windowMs: 1000, parent: {} }), TypeError);
  assert.throws(() => createGate({ limit: 2, windowMs: 1000, parent: 'gate' }), TypeError);
  assert.throws(() => createGate({ limit: 4, windowMs: 1000, parent }), RangeError);

  const child = createGate({ limit: 2, windowMs: 1000, parent });
  assert.throws(
    () => child.acquire(3),
    (err) => {
      assert.ok(err instanceof RangeError);
      assert.match(err.message, /2/); // echoes the child's own limit
      return true;
    },
  );
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
