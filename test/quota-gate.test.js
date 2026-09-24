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

test('updateLimit validates its argument and the hierarchy bounds', () => {
  const parent = createGate({ limit: 5, windowMs: 1000 });
  const child = createGate({ limit: 3, windowMs: 1000, parent });
  assert.throws(() => child.updateLimit('4'), TypeError);
  assert.throws(() => child.updateLimit(2.5), TypeError);
  assert.throws(() => child.updateLimit(0), RangeError);
  assert.throws(() => child.updateLimit(-2), RangeError);
  assert.throws(() => child.updateLimit(6), RangeError); // above the direct parent
  assert.throws(() => parent.updateLimit(2), RangeError); // below the direct child
  child.updateLimit(4);
  assert.equal(child.stats().limit, 4);
  parent.updateLimit(4); // now allowed: no child exceeds 4
  assert.equal(parent.stats().limit, 4);
});

test('updateLimit shrink rejects queued requests that no longer fit', async () => {
  const gate = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(3); // window full
  const big = gate.acquire(2).then((h) => h, (err) => err);
  const small = gate.acquire(1).then((h) => h);
  assert.equal(gate.stats().waiting, 3);
  gate.updateLimit(1);
  const bigResult = await big;
  assert.ok(bigResult instanceof QuotaExceededError);
  assert.equal(bigResult.code, 'QUOTA_EXCEEDED');
  assert.equal(gate.stats().refused, 1);
  assert.equal(gate.stats().waiting, 1); // only the still-fitting request remains
  const h = await small; // the dead head no longer blocks it
  assert.equal(gate.stats().granted, 2);
  h.release();
});

test('updateLimit grow does not release queued requests early', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  await gate.acquire(1);
  const p = gate.acquire(1);
  gate.updateLimit(2);
  assert.equal(gate.stats().limit, 2);
  assert.equal(gate.stats().waiting, 1); // still queued until the flip
  const h = await p;
  assert.equal(gate.stats().granted, 2);
  h.release();
});

test('updateWindowMs restarts the current window with the new length', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 2000 });
  const first = await gate.acquire(1);
  const p = gate.acquire(1);
  const start = Date.now();
  gate.updateWindowMs(50);
  assert.equal(gate.stats().windowMs, 50);
  assert.equal(gate.stats().inFlight, 1); // used quota is not cleared
  const h = await p; // granted one new-length window after the adjustment
  assert.ok(Date.now() - start < 500, 'flip happens after the new length, not the old');
  assert.equal(gate.stats().granted, 2);
  first.release();
  h.release();
  assert.throws(() => gate.updateWindowMs(0), RangeError);
  assert.throws(() => gate.updateWindowMs(-5), RangeError);
  assert.throws(() => gate.updateWindowMs(1.5), TypeError);
});

test('updateMaxWaitMs re-deadlines queued waiters and expires overdue ones', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000 });
  await gate.acquire(1);
  const p = gate.acquire(1).then((h) => h, (err) => err);
  await sleep(40);
  gate.updateMaxWaitMs(10); // deadline = enqueue time + 10, already past
  const err = await p;
  assert.equal(err.code, 'WAIT_EXPIRED');
  assert.equal(gate.stats().expired, 1);
  assert.equal(gate.stats().waiting, 0);

  const gate2 = createGate({ limit: 1, windowMs: 60, maxWaitMs: 20 });
  await gate2.acquire(1);
  const p2 = gate2.acquire(1); // would expire at +20, before the 60ms flip
  gate2.updateMaxWaitMs(1000); // re-deadlined to enqueue time + 1000
  const h2 = await p2; // survives to the flip and is granted
  assert.equal(gate2.stats().granted, 2);
  h2.release();

  assert.throws(() => gate.updateMaxWaitMs(-1), RangeError);
  assert.throws(() => gate.updateMaxWaitMs(0.5), TypeError);
});

test('reparent validates the target, ancestry and limit without a facet', () => {
  const root = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: root });
  const grand = createGate({ limit: 1, windowMs: 1000, parent: child });
  const before = child.stats();
  assert.throws(() => child.reparent({}), TypeError);
  assert.throws(() => child.reparent('gate'), TypeError);
  assert.throws(() => child.reparent(child), RangeError); // itself
  assert.throws(() => child.reparent(grand), RangeError); // its descendant
  const small = createGate({ limit: 1, windowMs: 1000 });
  assert.throws(() => child.reparent(small), RangeError); // limit above new parent
  assert.deepEqual(child.stats(), before); // failed attempts produce no facet
});

test('reparent settles the pool draw across families in one snapshot', async () => {
  const root1 = createGate({ limit: 2, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: root1 });
  const root2 = createGate({ limit: 5, windowMs: 1000 });
  await child.acquire(2); // draws the whole old family pool
  await assert.rejects(root1.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  const before = root1.stats();
  child.reparent(root2);
  assert.deepEqual(root1.stats(), before); // the old parent shows no facet
  assert.equal(child.stats().inFlight, 2); // admitted occupations are not broken
  const h1 = await root1.acquire(2); // the old pool was settled back
  await assert.rejects(root2.acquire(4), (err) => err.code === 'QUOTA_EXCEEDED'); // 2 of 5 re-borrowed
  const h2 = await root2.acquire(3);
  h1.release();
  h2.release();
});

test('reparent refuses and rolls back when the new pool cannot cover the draw', async () => {
  const root1 = createGate({ limit: 3, windowMs: 1000 });
  const child = createGate({ limit: 2, windowMs: 1000, parent: root1 });
  await child.acquire(2);
  const root2 = createGate({ limit: 3, windowMs: 1000 });
  await root2.acquire(2); // only 1 left in the new family pool
  assert.throws(
    () => child.reparent(root2),
    (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    },
  );
  // Fully rolled back: the child still belongs to the old family.
  const h1 = await root1.acquire(1); // old pool still 2 of 3
  await assert.rejects(root1.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  const h2 = await root2.acquire(1); // new pool untouched: 2 of 3 + 1
  await assert.rejects(root2.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  assert.equal(child.stats().inFlight, 2);
  h1.release();
  h2.release();
});

test('reparent carries descendants and queued waiters to the new family', async () => {
  const root1 = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000, parent: root1 });
  const grand = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000, parent: child });
  const root2 = createGate({ limit: 2, windowMs: 60 });
  await root1.acquire(1); // old pool full
  let granted = false;
  const p = grand.acquire(1).then((h) => {
    granted = true;
    return h;
  }); // queued on the old family
  child.reparent(root2);
  assert.equal(grand.stats().waiting, 1); // still queued: never granted early
  assert.equal(granted, false);
  await sleep(150); // the new family's 60ms window flips
  assert.equal(granted, true); // served by the new family's window
  const h = await p;
  h.release();
});

test('reparent within the same family keeps the pool and queue intact', async () => {
  const root = createGate({ limit: 3, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  await a.acquire(2);
  a.reparent(b); // same root: the pool draw stays put
  await root.acquire(1); // pool still 2 of 3 used
  await assert.rejects(root.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  assert.equal(a.stats().inFlight, 2);
});

test('aborting a queued acquire settles it as CANCELLED and unblocks the queue', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 1000 });
  const first = await gate.acquire(1);
  const controller = new AbortController();
  const p1 = gate.acquire(1, { signal: controller.signal });
  const p2 = gate.acquire(1);
  assert.equal(gate.stats().waiting, 2);
  controller.abort();
  await assert.rejects(p1, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(gate.stats().waiting, 1);
  assert.equal(gate.stats().cancelled, 1);
  assert.equal(gate.stats().expired, 0);
  const h2 = await p2; // granted at the next flip; the cancelled entry is gone
  assert.equal(gate.stats().granted, 2);
  first.release();
  h2.release();
});

test('an already-aborted signal settles immediately without queueing', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 1000 });
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(gate.acquire(1, controller.signal), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(gate.stats().cancelled, 1);
  assert.equal(gate.stats().waiting, 0);
  assert.equal(gate.stats().granted, 0);
});

test('cancelling a granted or settled acquire does not count', async () => {
  const gate = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000 });
  const c1 = new AbortController();
  const h = await gate.acquire(1, { signal: c1.signal }); // granted at once
  c1.abort(); // too late: the occupation is already granted
  assert.equal(gate.stats().cancelled, 0);
  assert.equal(gate.stats().inFlight, 1);
  h.release();

  const gate2 = createGate({ limit: 1, windowMs: 1000, maxWaitMs: 20 });
  await gate2.acquire(1);
  const c2 = new AbortController();
  const p = gate2.acquire(1, { signal: c2.signal }).then((h) => h, (err) => err);
  await sleep(50); // the wait already expired
  c2.abort(); // too late: already settled
  const err = await p;
  assert.equal(err.code, 'WAIT_EXPIRED');
  assert.equal(gate2.stats().cancelled, 0);
  assert.equal(gate2.stats().expired, 1);
});
