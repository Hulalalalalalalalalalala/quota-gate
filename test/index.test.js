import { test, describe, mock, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createGate, QuotaExceededError } from '../src/index.js';

const settle = () => new Promise((resolve) => setImmediate(resolve));

describe('single gate - retained behavior', () => {
  test('admits up to the limit, all-or-nothing, then refuses with QUOTA_EXCEEDED', async () => {
    const gate = createGate({ limit: 3, windowMs: 60_000 });
    const a = await gate.acquire(2);
    const b = await gate.acquire(1);
    assert.equal(gate.stats().inFlight, 3);

    // A request of 2 with only 1 left is not partially granted.
    await assert.rejects(gate.acquire(2), (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    });
    const s = gate.stats();
    assert.equal(s.granted, 2);
    assert.equal(s.refused, 1);
    a.release();
    b.release();
  });

  test('default unit is 1 and limit of one admits a single unit per window', async () => {
    const gate = createGate({ limit: 1, windowMs: 60_000 });
    await gate.acquire();
    assert.equal(gate.stats().inFlight, 1);
    await assert.rejects(gate.acquire(), (err) => err.code === 'QUOTA_EXCEEDED');
  });

  test('release decrements inFlight; repeated release is a silent no-op', async () => {
    const gate = createGate({ limit: 2, windowMs: 60_000 });
    const h = await gate.acquire(2);
    h.release();
    assert.equal(gate.stats().inFlight, 0);
    h.release();
    h.release();
    assert.equal(gate.stats().inFlight, 0);
    assert.equal(gate.stats().granted, 1);
  });

  test('stats key shape and order are unchanged', () => {
    const gate = createGate({ limit: 4, windowMs: 1234 });
    assert.deepEqual(Object.keys(gate), ['acquire', 'stats']);
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
      limit: 4,
      windowMs: 1234,
      inFlight: 0,
      waiting: 0,
      granted: 0,
      refused: 0,
      expired: 0,
    });
  });

  test('window flip frees the pool and grants queued requests in arrival order', async () => {
    const gate = createGate({ limit: 2, windowMs: 40, maxWaitMs: 1_000 });
    await gate.acquire(2);
    const order = [];
    const p1 = gate.acquire(1).then(() => order.push('p1'));
    const p2 = gate.acquire(1).then(() => order.push('p2'));
    assert.equal(gate.stats().waiting, 2);
    await new Promise((r) => setTimeout(r, 70));
    assert.deepEqual(order, ['p1', 'p2']);
    assert.equal(gate.stats().inFlight, 2);
    await Promise.all([p1, p2]);
  });

  test('a queued request that times out is rejected with WAIT_EXPIRED', async () => {
    const gate = createGate({ limit: 1, windowMs: 60_000, maxWaitMs: 25 });
    await gate.acquire(1);
    const p = gate.acquire(1);
    await assert.rejects(p, (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'WAIT_EXPIRED');
      return true;
    });
    assert.equal(gate.stats().expired, 1);
    assert.equal(gate.stats().waiting, 0);
  });

  test('multiple occupations arriving in one tick settle in arrival order', async () => {
    const gate = createGate({ limit: 1, windowMs: 25, maxWaitMs: 1_000 });
    const order = [];
    const ps = [];
    // Same event-loop tick: arrivals 1..5 queue behind the one granted unit.
    for (let i = 1; i <= 5; i += 1) {
      ps.push(gate.acquire(1).then(() => order.push(i)));
    }
    await new Promise((r) => setTimeout(r, 150));
    assert.deepEqual(order, [1, 2, 3, 4, 5]);
    await Promise.all(ps);
  });
});

describe('parameter validation and error contract', () => {
  test('wrong types throw TypeError', () => {
    assert.throws(() => createGate({ limit: '2', windowMs: 10 }), TypeError);
    assert.throws(() => createGate({ limit: 2, windowMs: 1.5 }), TypeError);
    assert.throws(() => createGate({ limit: 2, windowMs: 10, maxWaitMs: true }), TypeError);
    const gate = createGate({ limit: 3, windowMs: 10 });
    assert.throws(() => gate.acquire('1'), TypeError);
    assert.throws(() => gate.acquire(NaN), TypeError);
  });

  test('illegal values throw RangeError', () => {
    assert.throws(() => createGate({ limit: 0, windowMs: 10 }), RangeError);
    assert.throws(() => createGate({ limit: -2, windowMs: 10 }), RangeError);
    assert.throws(() => createGate({ limit: 2, windowMs: 0 }), RangeError);
    assert.throws(() => createGate({ limit: 2, windowMs: 10, maxWaitMs: -1 }), RangeError);
  });

  test('units out of range throws RangeError echoing the actual limit', () => {
    const gate = createGate({ limit: 7, windowMs: 10 });
    assert.throws(
      () => gate.acquire(8),
      (err) => err instanceof RangeError && err.message.includes('7'),
    );
    const child = createGate({ limit: 2, windowMs: 10, parent: gate });
    assert.throws(
      () => child.acquire(3),
      (err) => err instanceof RangeError && err.message.includes('2'),
    );
  });

  test('invalid parent argument throws TypeError', () => {
    assert.throws(() => createGate({ limit: 1, windowMs: 10, parent: {} }), TypeError);
    assert.throws(() => createGate({ limit: 1, windowMs: 10, parent: 'no' }), TypeError);
  });
});

describe('parent-child shared pool and borrowing', () => {
  test('parent and child draw from one pool bounded by the parent limit', async () => {
    const parent = createGate({ limit: 5, windowMs: 60_000 });
    const child = createGate({ limit: 5, windowMs: 60_000, parent });
    assert.equal(child._family, parent._family);
    await parent.acquire(3);
    await child.acquire(2);
    assert.equal(parent.stats().inFlight, 3);
    assert.equal(child.stats().inFlight, 2);
    await assert.rejects(parent.acquire(1), (e) => e.code === 'QUOTA_EXCEEDED');
    await assert.rejects(child.acquire(1), (e) => e.code === 'QUOTA_EXCEEDED');
  });

  test('child limit must not exceed the parent limit', () => {
    const parent = createGate({ limit: 5, windowMs: 10 });
    assert.throws(
      () => createGate({ limit: 6, windowMs: 10, parent }),
      (e) => e instanceof RangeError,
    );
    const mid = createGate({ limit: 4, windowMs: 10, parent });
    assert.throws(
      () => createGate({ limit: 5, windowMs: 10, parent: mid }),
      (e) => e instanceof RangeError,
    );
    // Grandchildren still share the root pool.
    const leaf = createGate({ limit: 2, windowMs: 10, parent: mid });
    assert.equal(leaf._family, parent._family);
  });

  test('child borrows the shortfall beyond its own slice up to the parent limit', async () => {
    const parent = createGate({ limit: 10, windowMs: 60_000 });
    const child = createGate({ limit: 2, windowMs: 60_000, parent });
    await child.acquire(2); // own slice
    const h = await child.acquire(2); // 2 borrowed from the parent side
    assert.equal(child.stats().inFlight, 4);
    assert.equal(parent.stats().inFlight, 0); // parent itself granted nothing
    await parent.acquire(6); // pool now fully used: 4 + 6 = 10
    await assert.rejects(parent.acquire(1), (e) => e.code === 'QUOTA_EXCEEDED');
    h.release();
    // Release detaches the occupation but does not replenish the pool
    // mid-window, so the parent still cannot grant.
    assert.equal(child.stats().inFlight, 2);
    await assert.rejects(parent.acquire(1), (e) => e.code === 'QUOTA_EXCEEDED');
  });

  test('borrowed units are force-reclaimed at window end without splitting occupations', async () => {
    const parent = createGate({ limit: 10, windowMs: 60_000, maxWaitMs: 2_000 });
    const child = createGate({ limit: 2, windowMs: 60_000, maxWaitMs: 2_000, parent });
    const borrowed = await child.acquire(2);
    await child.acquire(2); // child sits at 4, 2 of them borrowed
    await parent.acquire(6); // concurrent parent traffic pre-empts the rest
    const queued = parent.acquire(3); // cannot fit until the window resets

    // Synchronously within the flip: the reclaim and the re-grant are both
    // visible in one stats snapshot.
    parent._family._flip();
    assert.equal(child.stats().inFlight, 0);
    assert.equal(parent.stats().inFlight, 3); // queued request admitted at once
    // The pre-flip borrowed occupation still holds a usable handle; calling
    // release after the reclaim is harmless and changes no statistic.
    borrowed.release();
    borrowed.release();
    assert.equal(child.stats().inFlight, 0);
    const handle = await queued;
    assert.equal(typeof handle.release, 'function');
  });

  test('per-gate counters stay independent across the family', async () => {
    const parent = createGate({ limit: 6, windowMs: 60_000 });
    const c1 = createGate({ limit: 3, windowMs: 60_000, parent });
    const c2 = createGate({ limit: 3, windowMs: 60_000, parent });
    await c1.acquire(3);
    await c2.acquire(2);
    await parent.acquire(1);
    await assert.rejects(c1.acquire(1), () => true);
    assert.equal(c1.stats().granted, 1);
    assert.equal(c1.stats().refused, 1);
    assert.equal(c2.stats().granted, 1);
    assert.equal(parent.stats().granted, 1);
    assert.equal(c1.stats().inFlight + c2.stats().inFlight + parent.stats().inFlight, 6);
  });

  test('a grandchild borrows within the shared pool but never past its parent limit', async () => {
    const root = createGate({ limit: 10, windowMs: 60_000, maxWaitMs: 2_000 });
    const mid = createGate({ limit: 4, windowMs: 60_000, maxWaitMs: 2_000, parent: root });
    const leaf = createGate({ limit: 2, windowMs: 60_000, maxWaitMs: 2_000, parent: mid });
    await leaf.acquire(2); // own slice
    await leaf.acquire(2); // borrows 2, reaching its parent's limit of 4
    assert.equal(leaf.stats().inFlight, 4);
    // A further 2 would take the leaf to 6 > mid's limit of 4, even though
    // the root pool (10) still has room, so it must wait rather than borrow.
    const overflow = leaf.acquire(2);
    await settle();
    assert.equal(leaf.stats().waiting, 2);
    assert.equal(leaf.stats().inFlight, 4);

    // The request is admitted only in the next window, where the leaf's
    // window usage has been reset - still never exceeding its own limit.
    root._family._flip();
    const h = await overflow;
    assert.equal(typeof h.release, 'function');
    assert.equal(leaf.stats().inFlight, 2);
    h.release();
  });
});

describe('weighted aging and starvation freedom', () => {
  test('a blocked large request is first at the flip; the rest follow in order', async () => {
    const gate = createGate({ limit: 5, windowMs: 60_000, maxWaitMs: 2_000 });
    await gate.acquire(5); // window full
    const order = [];
    const big = gate.acquire(5).then(() => order.push('big'));
    const s1 = gate.acquire(2).then(() => order.push('s1'));
    const s2 = gate.acquire(3).then(() => order.push('s2'));
    await settle();
    assert.deepEqual(order, []); // nothing fits before the flip

    gate._family._flip(); // big takes the whole fresh window first
    await settle();
    assert.deepEqual(order, ['big']);
    assert.equal(gate.stats().waiting, 5);

    gate._family._flip(); // s1 then s2 fill the next window together
    await settle();
    assert.deepEqual(order, ['big', 's1', 's2']);
    assert.equal(gate.stats().waiting, 0);
    await Promise.all([big, s1, s2]);
  });

  test('younger fitting requests pass an older blocked one, which is admitted next window', async () => {
    const gate = createGate({ limit: 6, windowMs: 60_000, maxWaitMs: 2_000 });
    await gate.acquire(6);
    const order = [];
    // Oldest wants 4; a second 4 cannot share a window with it; the two
    // 1-unit requests can share with the first one. Arrival order is fixed.
    const a = gate.acquire(4).then(() => order.push('a4'));
    const b = gate.acquire(4).then(() => order.push('b4'));
    const x = gate.acquire(1).then(() => order.push('x1'));
    const y = gate.acquire(1).then(() => order.push('y1'));

    gate._family._flip();
    await settle();
    // a4 is oldest and wins first; b4 does not fit in the remaining 2, so
    // it is skipped and the younger x1/y1 are admitted ahead of it.
    assert.deepEqual(order, ['a4', 'x1', 'y1']);
    assert.equal(gate.stats().inFlight, 6);
    assert.equal(gate.stats().waiting, 4);

    gate._family._flip(); // b4 gets the first pick it was waiting for
    await settle();
    assert.deepEqual(order, ['a4', 'x1', 'y1', 'b4']);
    assert.equal(gate.stats().waiting, 0);
    await Promise.all([a, b, x, y]);
  });

  test('a child borrow-waiter and later traffic settle by age at the flip', async () => {
    const parent = createGate({ limit: 10, windowMs: 60_000, maxWaitMs: 2_000 });
    const child = createGate({ limit: 3, windowMs: 60_000, maxWaitMs: 2_000, parent });
    await parent.acquire(7);
    await child.acquire(3); // family pool full; child is on its own slice
    const order = [];
    // Oldest needs 3 borrowed units; a later 1-unit parent request is
    // younger. Both fit in a fresh window, strictly in arrival order.
    const cBig = child.acquire(3).then(() => order.push('child3'));
    const pSmall = parent.acquire(1).then(() => order.push('parent1'));

    parent._family._flip();
    await settle();
    assert.deepEqual(order, ['child3', 'parent1']);
    assert.equal(child.stats().inFlight, 3);
    assert.equal(parent.stats().inFlight, 1);
    assert.equal(parent.stats().waiting, 0);
    await Promise.all([cBig, pSmall]);
  });

  test('mixed sizes across gates never partially grant and keep arrival age', async () => {
    const parent = createGate({ limit: 6, windowMs: 60_000, maxWaitMs: 2_000 });
    const child = createGate({ limit: 6, windowMs: 60_000, maxWaitMs: 2_000, parent });
    await parent.acquire(6);
    const order = [];
    const reqs = [
      [parent, 4, 'p4'],
      [child, 2, 'c2a'],
      [child, 2, 'c2b'],
    ].map(([g, u, tag]) => g.acquire(u).then(() => order.push(tag)));

    parent._family._flip();
    await settle();
    // p4 then c2a consume the window; c2b cannot fit and takes nothing.
    assert.deepEqual(order, ['p4', 'c2a']);
    assert.equal(child.stats().waiting, 2);

    parent._family._flip();
    await settle();
    assert.deepEqual(order, ['p4', 'c2a', 'c2b']);
    assert.equal(child.stats().waiting, 0);
    await Promise.all(reqs);
  });

  test('a large request is never starved by a stream of younger small requests', async () => {
    const gate = createGate({ limit: 4, windowMs: 60_000, maxWaitMs: 80 });
    await gate.acquire(4);
    const order = [];
    const big = gate.acquire(4).then(() => order.push('big'));
    const smalls = [];
    // Round after round of newer small traffic arrives behind big.
    for (let round = 0; round < 3; round += 1) {
      for (let i = 0; i < 3; i += 1) smalls.push(gate.acquire(1).then(() => order.push(`s${round}${i}`)));
      gate._family._flip();
      await settle();
      // The very first flip must take the oldest request, which is big.
      if (round === 0) assert.deepEqual(order.slice(0, 1), ['big']);
    }
    assert.equal(order[0], 'big');
    assert.equal(gate.stats().inFlight, 4);
    await big;
    await Promise.allSettled(smalls);
  });
});

describe('window and deadline judgement at clock edges', () => {
  afterEach(() => {
    mock.timers.reset();
  });

  test('deadline equal to the flip moment expires before quota is allocated', async () => {
    mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
    const gate = createGate({ limit: 1, windowMs: 100, maxWaitMs: 100 });
    await gate.acquire(1); // fills window at t=0
    const settled = Promise.allSettled([gate.acquire(1)]); // deadline t=100
    mock.timers.tick(100); // wait timer and window timer due in the same tick
    await settle();
    const [w] = await settled;
    assert.equal(w.status, 'rejected');
    assert.equal(w.reason.code, 'WAIT_EXPIRED');
    assert.equal(gate.stats().expired, 1);
    assert.equal(gate.stats().granted, 1); // the waiter was never granted
  });

  test('several wait timers due in the same tick as the window expire once each', async () => {
    mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
    const gate = createGate({ limit: 2, windowMs: 100, maxWaitMs: 100 });
    await gate.acquire(2);
    const settled = Promise.allSettled([gate.acquire(1), gate.acquire(1)]);
    mock.timers.tick(100);
    await settle();
    const [a, b] = await settled;
    assert.equal(a.status, 'rejected');
    assert.equal(b.status, 'rejected');
    assert.equal(a.reason.code, 'WAIT_EXPIRED');
    assert.equal(b.reason.code, 'WAIT_EXPIRED');
    assert.equal(gate.stats().expired, 2);
    assert.equal(gate.stats().granted, 1); // the single initial acquire(2)
  });

  test('a long event-loop block: overdue waiters expire, then the pool is reallocated', async () => {
    mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
    const gate = createGate({ limit: 2, windowMs: 100, maxWaitMs: 150 });
    await gate.acquire(1);
    await gate.acquire(1); // window full
    mock.timers.tick(20);
    const shortLived = gate.acquire(1); // deadline t=170
    mock.timers.tick(20);
    const survivor = gate.acquire(1); // deadline t=190
    const settled = Promise.allSettled([shortLived, survivor]);

    // The loop blocks until t=180; timers deliver in one batched catch-up.
    mock.timers.setTime(180);
    mock.timers.tick(1);
    await settle();
    const [gone, kept] = await settled;
    assert.equal(gone.status, 'rejected');
    assert.equal(gone.reason.code, 'WAIT_EXPIRED');
    assert.equal(kept.status, 'fulfilled'); // admitted from the fresh pool
    assert.equal(gate.stats().expired, 1);
    assert.equal(gate.stats().inFlight, 1);
    assert.equal(gate.stats().granted, 3);
    kept.value.release();
  });

  test('clock rollback never revives an elapsed window', async () => {
    mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
    const gate = createGate({ limit: 1, windowMs: 100 });
    const old = await gate.acquire(1);
    mock.timers.setTime(100);
    gate._family._flip(); // window genuinely elapsed
    assert.equal(gate.stats().inFlight, 0);

    // Wall clock jumps backwards before the flip moment.
    mock.timers.setTime(40);
    old.release(); // handle belongs to the closed window: no statistic moves
    assert.equal(gate.stats().inFlight, 0);
    assert.equal(gate.stats().granted, 1);

    // New work is accounted in the current window, never in the revived old
    // one, and the monotonic counters are not reset by the rollback.
    const fresh = await gate.acquire(1);
    assert.equal(gate.stats().inFlight, 1);
    assert.equal(gate.stats().granted, 2);
    fresh.release();
  });

  test('rollback cannot un-expire a deadline the clock already reached', async () => {
    mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: 0 });
    const gate = createGate({ limit: 1, windowMs: 100, maxWaitMs: 100 });
    await gate.acquire(1); // window full at t=0
    const settled = Promise.allSettled([gate.acquire(1)]); // deadline t=100

    // Real time advances past the deadline and the loop processes it.
    mock.timers.setTime(120);
    mock.timers.tick(1); // the clamped clock observes t=121, deadline expires
    await settle();
    const [w] = await settled;
    assert.equal(w.status, 'rejected');
    assert.equal(w.reason.code, 'WAIT_EXPIRED');
    assert.equal(gate.stats().expired, 1);

    // The wall clock then jumps backwards to t=20; a later flip must not
    // resurrect the expired request or reset the monotonic counters.
    mock.timers.setTime(20);
    gate._family._flip();
    await settle();
    assert.equal(gate.stats().expired, 1);
    assert.equal(gate.stats().granted, 1);
    assert.equal(gate.stats().waiting, 0);
  });
});
