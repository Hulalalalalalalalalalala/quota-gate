import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createGate, QuotaExceededError } from '../src/index.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// file:// URL of the built module, for child-process probes that own the event
// loop and must exit (or stay alive) based solely on the gate's own timers.
const SRC_URL = new URL('../src/index.js', import.meta.url).href;

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
  // Only non-positive or non-integer counts are parameter errors; an
  // over-limit count borrows up the family chain instead of throwing.
  assert.throws(() => gate.acquire(0), RangeError);
  assert.throws(() => gate.acquire(-1), RangeError);
  assert.throws(() => gate.acquire(1.5), RangeError);
  assert.throws(() => gate.acquire('1'), TypeError);
  assert.throws(() => gate.acquire({}), TypeError);
  // 4 exceeds the gate's own limit and the family pool (3): a quota refusal,
  // not a RangeError.
  await assert.rejects(gate.acquire(4), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(gate.stats().refused, 1);
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
  // An over-limit count is not a parameter error: the child borrows the
  // excess from the parent chain and the occupation commits as one. Here the
  // parent is the root, so the excess comes out of the shared pool.
  const over = await child.acquire(3);
  assert.equal(child.stats().inFlight, 3);
  over.release();

  // With an intermediate parent the lend lands on that parent's own window.
  const root = createGate({ limit: 5, windowMs: 1000 });
  const mid = createGate({ limit: 3, windowMs: 1000, parent: root });
  const leaf = createGate({ limit: 2, windowMs: 1000, parent: mid });
  const over2 = await leaf.acquire(3); // own 2 plus one lent by mid
  assert.equal(leaf.stats().inFlight, 3);
  assert.equal(mid.stats().inFlight, 1); // the single lent unit
  over2.release();
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

// --- Over-limit single-gate occupations: own quota first, then the chain ----

test('an over-limit acquire borrows up the direct parent chain then the pool', async () => {
  const root = createGate({ limit: 10, windowMs: 1000 });
  const p = createGate({ limit: 3, windowMs: 1000, parent: root });
  const a = createGate({ limit: 2, windowMs: 1000, parent: p });
  await p.acquire(2); // two of p's own window used, one left to lend
  const handle = await a.acquire(5); // own 2 + parent lend 1 + pool 2
  assert.equal(a.stats().inFlight, 5);
  assert.equal(p.stats().inFlight, 3); // its own 2 plus the lent unit
  assert.equal(a.stats().granted, 1);
  assert.equal(a.stats().refused, 0);
  handle.release();
  assert.equal(a.stats().inFlight, 0);
  assert.equal(p.stats().inFlight, 2); // only p's own occupation remains
});

test('an over-limit request that the pool cannot cover is refused, not a RangeError', async () => {
  const root = createGate({ limit: 3, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  await a.acquire(2);
  await assert.rejects(a.acquire(2), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(a.stats().refused, 1);
  // Even a wildly over-limit count is a quota refusal: the gate limit never
  // raises RangeError.
  await assert.rejects(a.acquire(99), (err) => err.code === 'QUOTA_EXCEEDED');
});

test('a patient over-limit acquire is granted one occupation at the pool flip', async () => {
  const root = createGate({ limit: 3, windowMs: 60, maxWaitMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 1000, parent: root });
  await root.acquire(3); // pool full
  const h = await a.acquire(3); // own 2 plus one pool borrow, granted at the flip
  assert.equal(a.stats().inFlight, 3);
  assert.equal(a.stats().granted, 1);
  h.release();
});

test('a queued single holds its reservation until it commits at the pool flip', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 1000, parent: root });
  await root.acquire(4); // pool 4/5
  const pSingle = root.acquire(2); // earlier arrival; 4+2>5, queues
  const pMine = a.acquire(1); // 4+1=5 gathers its own unit, waits behind
  assert.equal(a.stats().waiting, 1);
  assert.equal(a.stats().reserved, 1);
  assert.equal(a.stats().inFlight, 0);
  const [hSingle, hMine] = await Promise.all([pSingle, pMine]);
  assert.equal(a.stats().reserved, 0); // reservation merges into the occupation
  assert.equal(a.stats().inFlight, 1);
  assert.equal(a.stats().waiting, 0);
  hSingle.release();
  hMine.release();
});

test('an over-limit head request is a hard barrier and is never overtaken', async () => {
  const root = createGate({ limit: 4, windowMs: 40, maxWaitMs: 5000 });
  const a = createGate({ limit: 4, windowMs: 10000, maxWaitMs: 5000, parent: root });
  const ac = new AbortController();
  const bigErr = a.acquire(5, ac.signal).then(() => null, (e) => e); // never fits pool 4
  let smallSettled = false;
  const pSmall = root.acquire(1).then((h) => { smallSettled = true; return h; });
  await sleep(120); // several flips: a single-gate head never yields
  assert.equal(smallSettled, false);
  ac.abort(); // tear down the blocked head
  assert.equal((await bigErr).code, 'CANCELLED');
  const hSmall = await pSmall; // the later request flows only once the head leaves
  assert.equal(smallSettled, true);
  hSmall.release();
});

test('a parent window flip reclaims an over-limit occupation\'s chain lend, handle intact', async () => {
  const root = createGate({ limit: 10, windowMs: 1000 });
  const p = createGate({ limit: 2, windowMs: 40, parent: root });
  const a = createGate({ limit: 2, windowMs: 10000, parent: p });
  const h = await a.acquire(4); // own 2, parent lends 2
  assert.equal(a.stats().inFlight, 4);
  assert.equal(p.stats().inFlight, 2);
  await sleep(80); // the parent window flips twice: the lend is reclaimed
  assert.equal(p.stats().inFlight, 0);
  assert.equal(a.stats().inFlight, 2); // the own part survives
  h.release(); // still a valid handle; no double subtract
  assert.equal(a.stats().inFlight, 0);
});

test('a parent window flip reclaims a queued single\'s uncommitted lend first', async () => {
  const root = createGate({ limit: 10, windowMs: 200, maxWaitMs: 5000 });
  const p = createGate({ limit: 2, windowMs: 40, maxWaitMs: 5000, parent: root });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: p });
  await root.acquire(4); // pool 4/10
  const pBlocker = root.acquire(7); // earlier arrival; 4+7>10, queues
  // own 2 + parent lend 2 + pool 1 = 5; 4+5<=10, so the hold gathers whole
  // and waits behind the blocker.
  const pMine = a.acquire(5);
  assert.equal(a.stats().reserved, 2);
  assert.equal(p.stats().reserved, 2);
  await sleep(60); // p's window flips: the uncommitted lend is reclaimed
  assert.equal(p.stats().reserved, 0);
  assert.equal(a.stats().reserved, 2); // the member's own reservation survives
  assert.equal(a.stats().waiting, 5);
  const hBlocker = await pBlocker; // granted at the pool flip, then released
  hBlocker.release();
  const hMine = await pMine; // re-gathers in the live chain and grants
  assert.equal(a.stats().inFlight, 5);
  assert.equal(p.stats().inFlight, 2); // lent again on commit
  hMine.release();
});

test('expiry and cancellation of an over-limit queued single release the whole hold', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 30, parent: root });
  await root.acquire(3);
  // 3+3>5, so it cannot gather; it expires without leaving anything behind.
  await assert.rejects(a.acquire(3), (err) => err.code === 'WAIT_EXPIRED');
  assert.equal(a.stats().expired, 1);
  assert.equal(a.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
  assert.equal(root.stats().reserved, 0);

  await root.acquire(1); // pool 4/5
  const pBlocker = root.acquire(2); // earlier arrival; 4+2>5, queues first
  const ac = new AbortController();
  const p = a.acquire(1, ac.signal); // own unit fits (4+1=5) and is held
  assert.equal(a.stats().reserved, 1);
  ac.abort();
  await assert.rejects(p, (err) => err.code === 'CANCELLED');
  assert.equal(a.stats().cancelled, 1);
  assert.equal(a.stats().reserved, 0);
  assert.equal(root.stats().reserved, 0);
  pBlocker.catch(() => {});
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

test('a child limit shrink does not refuse a single waiter the pool can still cover', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 1000 });
  const child = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 1000, parent: root });
  await root.acquire(5); // pool full
  const pChild = child.acquire(3); // queued, 3 units
  const pRoot = root.acquire(1); // queued behind
  child.updateLimit(2); // the request now exceeds the child's own limit
  // The child cap no longer refuses it: the family pool covers 3, so the
  // excess is borrowed up the chain; the root's waiter stays untouched.
  assert.equal(child.stats().refused, 0);
  assert.equal(child.stats().waiting, 3);
  assert.equal(root.stats().waiting, 1);
  const hChild = await pChild; // granted at the flip: own 2 plus one borrowed
  assert.equal(child.stats().inFlight, 3);
  hChild.release();
  const hRoot = await pRoot;
  hRoot.release();
});

test('a pool shrink refuses an over-limit single waiter that can never fit', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const child = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: root });
  await root.acquire(5); // pool full
  const p = child.acquire(4); // above the child limit; the pool could still cover it
  assert.equal(child.stats().waiting, 4);
  root.updateLimit(3); // now 4 can never fit anywhere in the family
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(child.stats().refused, 1);
  assert.equal(child.stats().waiting, 0);
  assert.equal(child.stats().reserved, 0);
});

test('acquire validates the shape of units but never caps it at the gate limit', async () => {
  const gate = createGate({ limit: 2, windowMs: 1000 });
  gate.updateLimit(3);
  await gate.acquire(3); // allowed under the new limit
  // 4 exceeds the gate limit but is not a parameter error: it is refused as
  // quota because the family pool (3) cannot cover it.
  await assert.rejects(gate.acquire(4), (err) => err.code === 'QUOTA_EXCEEDED');
  gate.updateLimit(1);
  assert.throws(
    () => gate.acquire(0),
    (err) => {
      assert.ok(err instanceof RangeError);
      return true;
    },
  );
  assert.throws(() => gate.acquire(2.5), RangeError);
  assert.throws(() => gate.acquire('2'), TypeError);
  // A child can borrow beyond a shrunken own limit as long as the pool covers.
  const parent = createGate({ limit: 5, windowMs: 1000 });
  const child = createGate({ limit: 1, windowMs: 1000, parent });
  const h = await child.acquire(3); // own 1, two borrowed up the chain
  assert.equal(child.stats().inFlight, 3);
  h.release();
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

test('reparent re-gathers a migrated over-limit waiter against the new chain', async () => {
  const oldRoot = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  const oldMid = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const leaf = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 5000, parent: oldMid });
  const newRoot = createGate({ limit: 4, windowMs: 60, maxWaitMs: 5000 });
  const newMid = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 5000, parent: newRoot });
  await oldRoot.acquire(2); // old pool full: the over-limit request queues
  const p = leaf.acquire(3); // own 1, the rest would have to be lent/borrowed
  assert.equal(leaf.stats().waiting, 3);
  leaf.reparent(newMid); // joins the new family; the old-family hold is dropped
  assert.equal(oldRoot.stats().reserved, 0); // nothing leaks in the old family
  assert.equal(leaf.stats().waiting, 3); // still queued, not granted early
  const h = await p; // re-gathers in the new chain and grants at the pool flip
  assert.equal(leaf.stats().inFlight, 3);
  assert.equal(newMid.stats().inFlight, 2); // own 1 plus two lent units
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

test('a wait cap beyond the 32-bit timer range still queues and grants at the flip', async () => {
  const gate = createGate({ limit: 1, windowMs: 60, maxWaitMs: 3_000_000_000 });
  await gate.acquire(1);
  // The cap overflows a raw setTimeout to 1ms; the waiter must still wait for
  // the window flip and settle as granted, not expire almost immediately.
  const h = await gate.acquire(1);
  assert.equal(gate.stats().granted, 2);
  assert.equal(gate.stats().expired, 0);
  assert.equal(gate.stats().waiting, 0);
  h.release();
});

test('a window beyond the 32-bit timer range does not flip early', async () => {
  const gate = createGate({ limit: 1, windowMs: 3_000_000_000 });
  await gate.acquire(1);
  await sleep(50); // an overflowed interval would have flipped many times by now
  await assert.rejects(gate.acquire(1), (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(gate.stats().inFlight, 1); // the window is still open
  assert.equal(gate.stats().refused, 1);
});

test('a long-capped waiter still settles on cancellation, not early expiry', async () => {
  const gate = createGate({ limit: 1, windowMs: 3_000_000_000, maxWaitMs: 3_000_000_000 });
  await gate.acquire(1);
  const ac = new AbortController();
  const p = gate.acquire(1, ac.signal);
  await sleep(50); // past any overflowed 1ms wait timer
  ac.abort();
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(gate.stats().cancelled, 1);
  assert.equal(gate.stats().expired, 0);
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

// --- Queue release after a head withdraws or is refused ----------------------

test('a zero wait cap that cannot be gathered on the spot refuses without queueing', async () => {
  const gate = createGate({ limit: 1, windowMs: 1000 }); // maxWaitMs defaults to 0
  await gate.acquire(1);
  await assert.rejects(gate.acquire(1), (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  const s = gate.stats();
  assert.equal(s.refused, 1);
  assert.equal(s.waiting, 0); // never entered the queue
  assert.equal(s.expired, 0);
  assert.equal(s.cancelled, 0);
  assert.equal(s.reserved, 0);
});

test('cancelling the blocked head releases a fully-assembled later request at once', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000, parent: root });
  await root.acquire(3); // pool 3/5
  const ac = new AbortController();
  // Head combo needs 4: 3+4>5, gathers nothing and queues first.
  const pHead = root.acquireAll([{ gate: a, units: 4 }], ac.signal);
  // Later single needs 2: 3+2=5, its whole reservation is already held.
  let laterGranted = false;
  const pLater = root.acquire(2).then((h) => {
    laterGranted = true;
    return h;
  });
  assert.equal(root.stats().reserved + a.stats().reserved, 2);
  ac.abort(); // the blocked head withdraws
  await assert.rejects(pHead, (err) => err.code === 'CANCELLED');
  // The retry releases the assembled later request in the SAME cut, with no
  // window flip in between (the window is 10s long).
  const hLater = await pLater;
  assert.equal(laterGranted, true);
  assert.equal(root.stats().granted, 2);
  assert.equal(root.stats().waiting, 0);
  assert.equal(root.stats().reserved + a.stats().reserved, 0);
  hLater.release();
});

test('an expired blocked head releases a fully-assembled later request at once', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  // The wait cap belongs to the ENTRY gate: enter the short-lived combo via a
  // gate whose cap is 20ms, while the later request is entered on the patient
  // root gate.
  const entry = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 20, parent: root });
  const a = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000, parent: root });
  await root.acquire(3); // pool 3/5
  const pHead = entry.acquireAll([{ gate: a, units: 4 }]); // 3+4>5, holds nothing
  const pLater = root.acquire(2); // 3+2=5, fully assembled and patient
  const headErr = await pHead.then(() => null, (e) => e);
  assert.equal(headErr.code, 'WAIT_EXPIRED');
  assert.equal(entry.stats().expired, 1); // booked on the entry gate
  const hLater = await pLater; // released by the retry, not by a window flip
  assert.equal(root.stats().granted, 2);
  hLater.release();
});

test('at one moment expiry beats the retry grant after a head cancellation', async (t) => {
  const root = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 5000 });
  const soon = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 30, parent: root });
  await root.acquire(1); // pool 1/3
  const ac = new AbortController();
  // Blocked head holds nothing (1+3>3).
  const pHead = root.acquire(3, ac.signal).then(() => 'granted', (e) => e.code);
  // Middle request has a 30ms cap and already holds one unit (1+1=2); the
  // patient last request holds the final unit (+1=3).
  const pMiddle = soon.acquire(1).then(() => 'granted', (e) => e.code);
  const pLast = root.acquire(1).then(() => 'granted', (e) => e.code);
  const realNow = Date.now();
  t.mock.method(Date, 'now', () => realNow + 1000); // past the middle cap, before the last
  ac.abort(); // head withdrawal triggers the retry
  assert.equal(await pHead, 'CANCELLED');
  assert.equal(await pMiddle, 'WAIT_EXPIRED'); // expiry settled before the grant
  assert.equal(await pLast, 'granted'); // the assembled later request still flows
  assert.equal(soon.stats().expired, 1); // the entry gate owns the expiry
  assert.equal(root.stats().expired, 0);
  assert.equal(root.stats().cancelled, 1);
  assert.equal(root.stats().granted, 2);
});

test('a shrink-refused head releases a later request that fits the new cap in the same cut', async () => {
  const gate = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  await gate.acquire(1); // pool 1/5
  const pBig = gate.acquire(5); // blocked head (1+5>5), holds nothing
  const pSmall = gate.acquire(3); // 1+3=4<=5, fully assembled behind the head
  assert.equal(gate.stats().reserved, 3);
  gate.updateLimit(4); // the 5-unit head can never fit and is refused as a whole
  await assert.rejects(pBig, (err) => err.code === 'QUOTA_EXCEEDED');
  // The 3-unit survivor re-assembles against the shrunken pool (1+3=4) and is
  // released by the retry in the same cut, with no window flip.
  const h = await pSmall;
  assert.equal(gate.stats().refused, 1);
  assert.equal(gate.stats().granted, 2);
  assert.equal(gate.stats().waiting, 0);
  h.release();
});

test('a blocked combo yields cumulatively across flips and then becomes a barrier', async () => {
  const root = createGate({ limit: 4, windowMs: 40, maxWaitMs: 5000 });
  const a = createGate({ limit: 4, windowMs: 1000, maxWaitMs: 5000, parent: root });
  const ac = new AbortController();
  const pBig = root.acquireAll([{ gate: a, units: 5 }], ac.signal).then(() => null, (e) => e);
  const settled = [];
  const p1 = root.acquire(1).then(() => settled.push('s1')); // yield #1 on flip 1
  await sleep(60);
  assert.deepEqual(settled, ['s1']);
  const p2 = root.acquire(1).then(() => settled.push('s2')); // yield #2 on flip 2
  await sleep(45);
  assert.deepEqual(settled, ['s1', 's2']);
  const p3 = root.acquire(1).then(() => settled.push('s3')); // barrier: no more yielding
  await sleep(45);
  assert.deepEqual(settled, ['s1', 's2']);
  ac.abort();
  assert.equal((await pBig).code, 'CANCELLED');
  await sleep(45); // the hardened barrier is gone; s3 flows at the next flip
  assert.deepEqual(settled, ['s1', 's2', 's3']);
  await Promise.all([p1, p2, p3]);
});

// --- Migration: cross-family queued combinations -----------------------------

test('a wholly-migrated combo whose entry gate stayed regathers in the new family', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 40, maxWaitMs: 5000 });
  await oldRoot.acquire(4); // old pool 4/5
  const pBlock = oldRoot.acquire(2); // earlier blocker, holds nothing
  // Entered through oldRoot (which stays), sole member a migrates: the hold
  // sits on a's own slice and charges the old pool.
  const pCombo = oldRoot.acquireAll([{ gate: a, units: 1 }]);
  assert.equal(a.stats().reserved, 1);
  a.reparent(newRoot);
  // The old-family reservation is released exactly once and the combo is not
  // granted early: it now waits in the joined family's queue.
  assert.equal(a.stats().reserved, 0);
  assert.equal(oldRoot.stats().reserved, 0);
  assert.equal(a.stats().waiting, 1);
  // The freed old-pool unit is reusable exactly once (no double deduction):
  // another 1-unit request can assemble (4+1=5) behind the blocker.
  const pExtra = oldRoot.acquire(1);
  assert.equal(oldRoot.stats().reserved, 1);
  const h = await pCombo; // new family flips at ~40ms: regathers and grants
  assert.equal(a.stats().inFlight, 1); // the member carries the occupation
  assert.equal(oldRoot.stats().granted, 2); // the entry gate owns the grant
  assert.equal(newRoot.stats().granted, 0);
  h.release();
  pBlock.catch(() => {});
  pExtra.catch(() => {});
});

test('aborting a migrated owner-stayed combo settles it on the entry gate', async () => {
  const oldRoot = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  await oldRoot.acquire(2);
  const ac = new AbortController();
  const p = oldRoot.acquireAll([{ gate: a, units: 1 }], ac.signal);
  a.reparent(newRoot);
  ac.abort();
  await assert.rejects(p, (err) => err.code === 'CANCELLED');
  assert.equal(oldRoot.stats().cancelled, 1); // booked on the entry gate
  assert.equal(newRoot.stats().cancelled, 0);
  assert.equal(a.stats().cancelled, 0);
  assert.equal(a.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
  assert.equal(newRoot.stats().reserved, 0);
});

test('the entry gate retimes an owned combo waiting in another family queue', async () => {
  const oldRoot = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  await oldRoot.acquire(2);
  const p = oldRoot.acquireAll([{ gate: a, units: 1 }]);
  a.reparent(newRoot);
  oldRoot.updateMaxWaitMs(0); // the owner expires its entry wherever it queues
  await assert.rejects(p, (err) => err.code === 'WAIT_EXPIRED');
  assert.equal(oldRoot.stats().expired, 1);
  assert.equal(a.stats().waiting, 0);
  assert.equal(newRoot.stats().waiting, 0);
});

test('a split-combo refusal does not block a later assembled request in the old family', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const b = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  await oldRoot.acquire(3); // old pool 3/5
  // Head combo straddles the move (a goes, b stays) and holds nothing.
  const pSplit = oldRoot.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 2 }]);
  // Later request assembles completely against the current old pool (3+2=5).
  const pLater = oldRoot.acquire(2);
  assert.equal(oldRoot.stats().reserved, 2);
  a.reparent(newRoot); // the straddler is refused as a whole and leaves
  await assert.rejects(pSplit, (err) => err.code === 'QUOTA_EXCEEDED');
  const h = await pLater; // unblocked in the same synchronous cut, no flip
  assert.equal(oldRoot.stats().refused, 1);
  assert.equal(oldRoot.stats().granted, 2);
  h.release();
});

test('a failed move leaves a queued cross-gate combo pending and unchanged', async () => {
  const oldRoot = createGate({ limit: 2, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const b = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 10000 });
  const hOccupation = await a.acquire(2); // subtree draw of 2 fills the old pool
  await newRoot.acquire(1); // the new pool has only 1 unit free
  // The combo would straddle the move (a goes, b stays); with the old pool
  // full it queues as the only waiter.
  const pCombo = oldRoot.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }]);
  assert.equal(oldRoot.stats().rolledBack, 1); // the failed on-the-spot gather
  const before = [oldRoot.stats(), a.stats(), b.stats(), newRoot.stats()];
  assert.throws(
    () => a.reparent(newRoot),
    (err) => err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.deepEqual([oldRoot.stats(), a.stats(), b.stats(), newRoot.stats()], before);
  // Nothing changed: still one old family, and its pool flip gathers the
  // queued combo as one occupation.
  const h = await pCombo;
  assert.equal(oldRoot.stats().granted, 1); // booked on the entry gate
  assert.equal(a.stats().inFlight, 3); // the 2 old units plus the combo member
  assert.equal(b.stats().inFlight, 1);
  h.release();
  hOccupation.release();
});

// --- Bookkeeping attribution -------------------------------------------------

test('entry-gate and member-gate counters are separately attributable for combos', async () => {
  const root = createGate({ limit: 6, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: root });
  const b = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000, parent: root });
  await root.acquire(4); // pool 4/6
  const pBlocker = root.acquire(3); // earlier single blocker (4+3>6), holds nothing
  const ac = new AbortController();
  const p = root.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }], ac.signal); // 4+2=6
  // While queued the MEMBERS carry waiting/reserved; the entry gate carries
  // only the earlier single's waiting.
  assert.equal(a.stats().waiting, 1);
  assert.equal(b.stats().waiting, 1);
  assert.equal(root.stats().waiting, 3); // the blocker's three units
  assert.equal(a.stats().reserved, 1);
  assert.equal(b.stats().reserved, 1);
  ac.abort();
  await assert.rejects(p, (err) => err.code === 'CANCELLED');
  // The terminal outcome is booked once, on the ENTRY gate.
  assert.equal(root.stats().cancelled, 1);
  assert.equal(a.stats().cancelled, 0);
  assert.equal(b.stats().cancelled, 0);
  assert.equal(a.stats().waiting, 0);
  assert.equal(b.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
  assert.equal(b.stats().reserved, 0);
  pBlocker.catch(() => {});
});

test('a whole rollback moves only rolledBack and is retried at the next flip', async () => {
  const root = createGate({ limit: 3, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 1000, maxWaitMs: 5000, parent: root });
  await root.acquire(3); // pool full
  const h = root.acquireAll([{ gate: a, units: 3 }]); // cannot gather: whole rollback
  const s = root.stats();
  assert.equal(s.rolledBack, 1); // exactly one counter moved for the failed gather
  assert.equal(s.refused, 0);
  assert.equal(s.expired, 0);
  assert.equal(s.cancelled, 0);
  assert.equal(s.reserved, 0);
  assert.equal(a.stats().reserved, 0);
  const handle = await h; // regathers against the fresh pool and commits
  assert.equal(root.stats().rolledBack, 1); // no further rollback once it fits
  assert.equal(a.stats().inFlight, 3);
  handle.release();
});

test('one head withdrawal releases the whole assembled prefix in arrival order', async () => {
  const root = createGate({ limit: 4, windowMs: 10000, maxWaitMs: 5000 });
  await root.acquire(2); // pool 2/4
  const order = [];
  const ac = new AbortController();
  const pHead = root.acquire(3, ac.signal).then(() => null, (e) => e); // holds nothing
  const p1 = root.acquire(1).then((h) => { order.push(1); return h; }); // 2+1=3 held
  const p2 = root.acquire(1).then((h) => { order.push(2); return h; }); // +1 =4 held
  assert.equal(root.stats().reserved, 2);
  ac.abort(); // one withdrawal cuts the whole prefix loose
  assert.equal((await pHead).code, 'CANCELLED');
  const handles = await Promise.all([p1, p2]);
  assert.deepEqual(order, [1, 2]); // arrival order preserved
  assert.equal(root.stats().granted, 3);
  assert.equal(root.stats().cancelled, 1);
  assert.equal(root.stats().waiting, 0);
  for (const h of handles) h.release();
});

test('head withdrawals across many window flips keep releasing later arrivals', async () => {
  const root = createGate({ limit: 4, windowMs: 30, maxWaitMs: 5000 });
  let granted = 0;
  let cancelled = 0;
  // Each window: occupy the pool, queue a blocked head then an assembled
  // request, cancel the head; the retry grants the later request. Runs over
  // many flips to exercise queue compaction and prefix accounting repeatedly.
  for (let i = 0; i < 8; i += 1) {
    const h0 = await root.acquire(2); // pool 2/4
    const ac = new AbortController();
    const pHead = root.acquire(3, ac.signal).then(() => null, (e) => e);
    const pLater = root.acquire(2).then((h) => h, () => null);
    ac.abort();
    assert.equal((await pHead).code, 'CANCELLED');
    const hLater = await pLater; // released before the next flip
    assert.ok(hLater);
    cancelled += 1;
    granted += 1;
    h0.release();
    hLater.release();
    await sleep(32); // flip the pool window for the next cycle
  }
  assert.equal(root.stats().cancelled, cancelled);
  assert.equal(root.stats().granted, granted * 2); // one h0 + one retried per cycle
  assert.equal(root.stats().waiting, 0);
});

// --- Failed-migration quota rollback -----------------------------------------

test('a rejected move leaves the old family available quota and stats identical', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 10000 });
  const child = createGate({ limit: 2, windowMs: 10000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 10000 });
  await child.acquire(2); // draws 2 from the old pool
  await newRoot.acquire(1); // only 1 free in the new pool: move cannot fit
  const before = [oldRoot.stats(), child.stats(), newRoot.stats()];
  await assert.throws(
    () => child.reparent(newRoot),
    (err) => err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.deepEqual([oldRoot.stats(), child.stats(), newRoot.stats()], before);
  // The read after the rejected move is the same as before it, item by item:
  // the child still occupies 2 of the old pool's 5, leaving exactly three.
  const h = await oldRoot.acquire(3);
  assert.equal(oldRoot.stats().granted, 1);
  await assert.rejects(oldRoot.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  h.release();
});

test('a rejected move after a lender flip refunds the live draw with no over-deduction', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const tick = (ms) => t.mock.timers.tick(ms);
  // Pool window never flips; the lender's own window is short.
  const root = createGate({ limit: 5, windowMs: 1_000_000_000 });
  const p = createGate({ limit: 2, windowMs: 30, parent: root });
  const a = createGate({ limit: 2, windowMs: 1_000_000_000, parent: p });
  const newRoot = createGate({ limit: 2, windowMs: 1_000_000_000 });
  await a.acquire(4); // own 2 + p lends 2: the live pool charge starts at 4
  tick(31); // p's window flips: the 2 lent units are reclaimed, live charge is 2
  assert.equal(a.stats().inFlight, 2);
  // One free slot in the new pool: the LIVE draw (2) does not fit, so this
  // rejects. The rejection must leave every reading identical to before.
  await newRoot.acquire(1);
  const before = [root.stats(), a.stats(), newRoot.stats()];
  assert.throws(
    () => a.reparent(newRoot),
    (err) => err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.deepEqual([root.stats(), a.stats(), newRoot.stats()], before);
  // The old pool still offers the three units the live draw freed.
  root.updateMaxWaitMs(0);
  const h = await root.acquire(3);
  assert.equal(root.stats().granted, 1);
  h.release();
  t.mock.reset();
});

test('a feasible move after a lender flip migrates the live draw, not a stale over-draw', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const tick = (ms) => t.mock.timers.tick(ms);
  const root = createGate({ limit: 5, windowMs: 1_000_000_000 });
  const p = createGate({ limit: 2, windowMs: 30, parent: root });
  const a = createGate({ limit: 2, windowMs: 1_000_000_000, parent: p });
  const newRoot = createGate({ limit: 2, windowMs: 1_000_000_000 });
  await a.acquire(4); // live pool charge 4
  tick(31); // p flips: the live pool charge drops to 2
  a.reparent(newRoot); // 2 fits the empty new pool: must succeed, not be rejected
  // The old family got its full pool back; the new family is charged exactly 2.
  root.updateMaxWaitMs(0);
  newRoot.updateMaxWaitMs(0);
  const hold = await root.acquire(5); // all 5 old units reusable exactly once
  assert.equal(root.stats().granted, 1);
  hold.release();
  await assert.rejects(newRoot.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  t.mock.reset();
});

test('an equal-size request right after a failed move reads identically to before it', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const root = createGate({ limit: 6, windowMs: 1_000_000_000 });
  const p = createGate({ limit: 3, windowMs: 30, parent: root });
  const a = createGate({ limit: 2, windowMs: 1_000_000_000, parent: p });
  const b = createGate({ limit: 2, windowMs: 1_000_000_000, parent: root });
  const newRoot = createGate({ limit: 4, windowMs: 1_000_000_000 });
  await a.acquire(4); // own 2 + p lends 2
  await b.acquire(1); // one more live pool unit -> 5 of 6 used
  t.mock.timers.tick(31); // p flips: its 2 lent units are reclaimed -> 3 of 6 live
  await newRoot.acquire(3); // 3 free of 4 in the new pool: live draw 3 cannot fit
  const before = root.stats();
  assert.throws(
    () => a.reparent(newRoot),
    (err) => err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.deepEqual(root.stats(), before); // item-for-item identical
  // An equal-size (3-unit) request that fits before still fits after, in order.
  root.updateMaxWaitMs(0);
  const h = await root.acquire(3);
  assert.equal(root.stats().granted, before.granted + 1);
  await assert.rejects(root.acquire(1), (err) => err.code === 'QUOTA_EXCEEDED');
  h.release();
  t.mock.reset();
});

test('a failed move does not block fitting queued combos that arrive behind it', async () => {
  const oldRoot = createGate({ limit: 3, windowMs: 40, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 3, windowMs: 10000 });
  await a.acquire(3); // subtree draw of 3 fills the old pool
  await newRoot.acquire(3); // the new pool has no room: the move cannot fit
  // The old pool is full, so this combo queues; the rejected move must leave it
  // pending and it must still flow in arrival order at the next pool flip.
  const pCombo = oldRoot.acquireAll([{ gate: a, units: 1 }]);
  assert.equal(a.stats().waiting, 1);
  assert.throws(
    () => a.reparent(newRoot),
    (err) => err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED',
  );
  assert.equal(oldRoot.stats().refused, 0); // the queued combo was not refused
  const h = await pCombo; // unblocked in arrival order by the pool flip, not stuck
  assert.equal(oldRoot.stats().granted, 1);
  h.release();
});

// --- Wait expiry settles on real time while the loop is idle -----------------

test('a queued wait settles WAIT_EXPIRED on real time even when the process is otherwise idle', () => {
  // The gate's own timers are the only handles. The child process must stay
  // alive until the deadline, settle the request, and then exit on its own.
  const script = `
    import { createGate } from ${JSON.stringify(SRC_URL)};
    const gate = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 30 });
    await gate.acquire(1);
    const t0 = Date.now();
    gate.acquire(1).then(
      () => console.log('GRANTED'),
      (err) => console.log(err.code + ':' + (Date.now() - t0) + ':' + gate.stats().expired),
    );
  `;
  const res = spawnSync(process.execPath, ['--input-type=module', '-e', script], {
    encoding: 'utf8',
    timeout: 5000,
  });
  assert.equal(res.status, 0); // the process exited by itself, not killed as idle
  const [code, elapsedMs, expired] = res.stdout.trim().split(':');
  assert.equal(code, 'WAIT_EXPIRED');
  assert.equal(expired, '1');
  assert.ok(Number(elapsedMs) >= 25, `expired too early after ${elapsedMs}ms`);
});

test('an idle gate with no waiters does not keep the process alive', () => {
  // Only the long-lived window timer exists and it stays unref'd.
  const script = `
    import { createGate } from ${JSON.stringify(SRC_URL)};
    createGate({ limit: 1, windowMs: 10000 });
    console.log('ready');
  `;
  const res = spawnSync(process.execPath, ['--input-type=module', '-e', script], {
    encoding: 'utf8',
    timeout: 5000,
  });
  assert.equal(res.status, 0);
  assert.equal(res.stdout.trim(), 'ready');
});

test('at one moment expiry is settled before withdrawal before a queued grant', async (t) => {
  const root = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 5000 });
  const soon = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 30, parent: root });
  await root.acquire(1); // 1 of 3 used
  const ac = new AbortController();
  const pHead = root.acquire(3, ac.signal).then(() => 'granted', (e) => e.code);
  const pMiddle = soon.acquire(1).then(() => 'granted', (e) => e.code); // holds 1, cap 30ms
  const pLast = root.acquire(1).then(() => 'granted', (e) => e.code); // holds 1
  const realNow = Date.now();
  t.mock.method(Date, 'now', () => realNow + 1000); // past the middle cap
  ac.abort(); // the head withdrawal runs the same-moment settling cut
  assert.equal(await pHead, 'CANCELLED');
  assert.equal(await pMiddle, 'WAIT_EXPIRED'); // expiry beat the grant
  assert.equal(await pLast, 'granted');
});

test('a pre-aborted signal settles directly without entering the queue', async () => {
  const gate = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 5000 });
  await gate.acquire(1);
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(gate.acquire(1, ac.signal), (err) => err.code === 'CANCELLED');
  const s = gate.stats();
  assert.equal(s.cancelled, 1);
  assert.equal(s.waiting, 0);
  assert.equal(s.expired, 0);
});

test('a wait cap beyond the 32-bit timer range honours real time and settles while idle', () => {
  // Long cap with no other live handles: the chunked ref'd timer must keep the
  // process alive, and an abort settles CANCELLED rather than an early expiry.
  const script = `
    import { createGate } from ${JSON.stringify(SRC_URL)};
    const gate = createGate({ limit: 1, windowMs: 3000000000, maxWaitMs: 3000000000 });
    await gate.acquire(1);
    const ac = new AbortController();
    const p = gate.acquire(1, ac.signal).then(
      () => 'granted',
      (err) => err.code + ':' + gate.stats().expired,
    );
    setTimeout(() => ac.abort(), 40);
    console.log(await p);
  `;
  const res = spawnSync(process.execPath, ['--input-type=module', '-e', script], {
    encoding: 'utf8',
    timeout: 5000,
  });
  assert.equal(res.status, 0);
  assert.equal(res.stdout.trim(), 'CANCELLED:0'); // no early WAIT_EXPIRED
});

// --- Invalid calls produce no statistical cut --------------------------------

test('invalid reparent targets raise TypeError/RangeError without any stat cut', () => {
  const a = createGate({ limit: 3, windowMs: 10000 });
  const child = createGate({ limit: 2, windowMs: 10000, parent: a });
  const grand = createGate({ limit: 1, windowMs: 10000, parent: child });
  const foreign = createGate({ limit: 1, windowMs: 10000 }); // below child's 2
  const gates = [a, child, grand, foreign];
  const before = gates.map((g) => g.stats());
  assert.throws(() => child.reparent({}), TypeError); // not a module gate
  assert.throws(() => child.reparent('gate'), TypeError);
  assert.throws(() => child.reparent(null), TypeError);
  assert.throws(() => child.reparent(child), RangeError); // under itself
  assert.throws(() => child.reparent(grand), RangeError); // under a descendant
  assert.throws(() => child.reparent(foreign), RangeError); // limit over new parent
  assert.deepEqual(gates.map((g) => g.stats()), before);
});

test('wrong parameter types throw TypeError and illegal values throw RangeError', () => {
  const gate = createGate({ limit: 2, windowMs: 10000, maxWaitMs: 5000 });
  const other = createGate({ limit: 2, windowMs: 10000 });
  const before = gate.stats();
  assert.throws(() => gate.acquire('1'), TypeError);
  assert.throws(() => gate.acquire(1, 42), TypeError);
  assert.throws(() => gate.acquire(0), RangeError);
  assert.throws(() => gate.acquireAll('nope'), TypeError);
  assert.throws(() => gate.acquireAll([{ gate: {}, units: 1 }]), TypeError);
  assert.throws(() => gate.acquireAll([{ gate: other, units: 1 }]), RangeError);
  assert.throws(() => createGate({ limit: 1, windowMs: '10' }), TypeError);
  assert.throws(() => createGate({ limit: 1, windowMs: 0 }), RangeError);
  assert.deepEqual(gate.stats(), before); // no observable cut
});
