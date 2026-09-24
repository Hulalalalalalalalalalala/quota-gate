import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createGate, QuotaExceededError } from '../src/index.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Immediate grants, handles and stats -----------------------------------

test('acquireAll grants a combo across sibling gates immediately', async () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  const h = await root.acquireAll([{ gate: a, units: 2 }, { gate: b, units: 2 }]);
  assert.equal(a.stats().inFlight, 2);
  assert.equal(b.stats().inFlight, 2);
  assert.equal(root.stats().granted, 1);
  assert.equal(a.stats().granted, 0);
  assert.equal(a.stats().reserved, 0);
  assert.equal(b.stats().reserved, 0);
  h.release();
  assert.equal(a.stats().inFlight, 0);
  assert.equal(b.stats().inFlight, 0);
});

test('the combo handle exposes only a detached, idempotent release', async () => {
  const root = createGate({ limit: 4, windowMs: 1000 });
  const a = createGate({ limit: 2, windowMs: 1000, parent: root });
  const b = createGate({ limit: 2, windowMs: 1000, parent: root });
  const h = await root.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }]);
  assert.deepEqual(Object.keys(h), ['release']);
  const { release } = h; // a destructured call has no receiver dependency
  release();
  release(); // idempotent
  assert.equal(a.stats().inFlight, 0);
  assert.equal(b.stats().inFlight, 0);
});

test('a combo whose own quota is short borrows up the direct-parent chain', async () => {
  const root = createGate({ limit: 10, windowMs: 1000 });
  const p = createGate({ limit: 5, windowMs: 1000, parent: root });
  const d = createGate({ limit: 5, windowMs: 1000, parent: p });
  const h0 = await root.acquireAll([{ gate: d, units: 5 }]); // fills d's own quota
  // Nothing own left at d: the direct parent p lends all 5.
  const h = await root.acquireAll([{ gate: d, units: 5 }]);
  assert.equal(d.stats().inFlight, 10);
  assert.equal(p.stats().inFlight, 0); // p is a lender, not the occupant
  h.release();
  assert.equal(d.stats().inFlight, 5);
  h0.release();
  assert.equal(d.stats().inFlight, 0);
});

test('a combo too big for the pool window is refused when maxWaitMs is 0', async () => {
  const root = createGate({ limit: 3, windowMs: 1000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 3, windowMs: 1000, parent: root });
  await assert.rejects(
    root.acquireAll([{ gate: a, units: 2 }, { gate: b, units: 2 }]),
    (err) => {
      assert.ok(err instanceof QuotaExceededError);
      assert.equal(err.code, 'QUOTA_EXCEEDED');
      return true;
    },
  );
  const s = root.stats();
  assert.equal(s.refused, 1);
  assert.equal(s.rolledBack, 1); // the failed gather rolled back
  assert.equal(s.granted, 0);
  assert.equal(a.stats().reserved, 0); // nothing left reserved
  assert.equal(b.stats().reserved, 0);
});

// --- Validation -------------------------------------------------------------

test('acquireAll validates its member list without producing a cut', () => {
  const root = createGate({ limit: 5, windowMs: 1000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 3, windowMs: 1000, parent: root });
  const child = createGate({ limit: 2, windowMs: 1000, parent: a });
  const foreign = createGate({ limit: 2, windowMs: 1000 });
  const gates = [root, a, b, child, foreign];
  const before = gates.map((g) => g.stats());

  assert.throws(() => root.acquireAll(), TypeError);
  assert.throws(() => root.acquireAll('nope'), TypeError);
  assert.throws(() => root.acquireAll([]), RangeError);
  assert.throws(() => root.acquireAll([null]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: 1.5 }]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: '1' }]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: {}, units: 1 }]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: 'gate', units: 1 }]), TypeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: 0 }]), RangeError);
  assert.throws(() => root.acquireAll([{ gate: a, units: 4 }]), RangeError);
  assert.throws(
    () => root.acquireAll([{ gate: a, units: 1 }, { gate: a, units: 1 }]),
    RangeError,
  ); // duplicate gate
  assert.throws(
    () => root.acquireAll([{ gate: a, units: 1 }, { gate: foreign, units: 1 }]),
    RangeError,
  ); // different families
  assert.throws(
    () => root.acquireAll([{ gate: a, units: 1 }, { gate: child, units: 1 }]),
    RangeError,
  ); // ancestor chain inside one combo
  assert.throws(() => root.acquireAll([{ gate: a, units: 1 }], 'signal'), TypeError);
  assert.deepEqual(gates.map((g) => g.stats()), before);
});

test('a pre-aborted signal settles an acquireAll as CANCELLED without queueing', async () => {
  const root = createGate({ limit: 2, windowMs: 1000, maxWaitMs: 1000 });
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

// --- Queueing, reservations and rollback ------------------------------------

test('a queued combo holds a full reservation and commits in arrival order at the flip', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 3, windowMs: 1000, parent: root });
  await root.acquire(3); // 3 of 5 used
  const pHead = root.acquire(3); // 3 do not fit the 2 free: queued
  const pCombo = root.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }]);
  // The combo fully reserved the 2 free pool units while waiting behind head.
  assert.equal(a.stats().reserved, 1);
  assert.equal(b.stats().reserved, 1);
  assert.equal(root.stats().waiting, 5); // head 3 + combo 2
  assert.equal(a.stats().waiting, 1);
  const order = [];
  const hHead = await pHead.then((h) => {
    order.push('head');
    return h;
  });
  const hCombo = await pCombo.then((h) => {
    order.push('combo');
    return h;
  });
  assert.deepEqual(order, ['head', 'combo']); // arrival order, no overtaking
  assert.equal(a.stats().reserved, 0);
  assert.equal(a.stats().inFlight, 1);
  assert.equal(root.stats().rolledBack, 0);
  hHead.release();
  hCombo.release();
});

test('a combo that cannot gather while queueing rolls back and retries at the flip', async () => {
  const root = createGate({ limit: 5, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 1000, parent: root });
  const b = createGate({ limit: 3, windowMs: 1000, parent: root });
  await root.acquire(4); // only 1 free: the 2-unit combo cannot reserve yet
  const pCombo = root.acquireAll([{ gate: a, units: 1 }, { gate: b, units: 1 }]);
  assert.equal(root.stats().rolledBack, 1); // eager gather failed and rolled back
  assert.equal(a.stats().reserved, 0);
  const h = await pCombo; // re-gathered from scratch at the first pool flip
  assert.equal(root.stats().granted, 2); // the occupant and the combo
  assert.equal(a.stats().inFlight, 1);
  assert.equal(b.stats().inFlight, 1);
  h.release();
});

test('a queued combo expires WAIT_EXPIRED before the flip and leaves no reservation', async () => {
  const root = createGate({ limit: 1, windowMs: 60, maxWaitMs: 30 });
  const a = createGate({ limit: 1, windowMs: 1000, parent: root });
  await root.acquire(1); // pool full
  const p = root.acquireAll([{ gate: a, units: 1 }]);
  await assert.rejects(p, (err) => {
    assert.ok(err instanceof QuotaExceededError);
    assert.equal(err.code, 'WAIT_EXPIRED');
    return true;
  });
  assert.equal(root.stats().expired, 1);
  assert.equal(root.stats().rolledBack, 1);
  assert.equal(root.stats().waiting, 0);
  assert.equal(a.stats().reserved, 0);
});

test('cancelling a fully reserved queued combo releases every leg', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 10000, parent: root });
  const b = createGate({ limit: 3, windowMs: 10000, parent: root });
  await root.acquire(3);
  const headAc = new AbortController();
  const head = root.acquire(3, headAc.signal); // queued head: only 2 free
  const ac = new AbortController();
  const p = root.acquireAll(
    [{ gate: a, units: 1 }, { gate: b, units: 1 }],
    ac.signal,
  );
  assert.equal(a.stats().reserved, 1);
  ac.abort();
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  assert.equal(root.stats().cancelled, 1);
  assert.equal(root.stats().waiting, 3); // only the head remains; no early grant
  assert.equal(root.stats().granted, 1);
  assert.equal(a.stats().reserved, 0);
  assert.equal(b.stats().reserved, 0);
  headAc.abort(); // clean up the head without waiting for its 5s deadline
  await assert.rejects(head, () => true);
});

// --- Fairness: yielding twice, then blocking --------------------------------

test('a combo yields at most twice and then blocks later arrivals', async () => {
  const root = createGate({ limit: 10, windowMs: 60, maxWaitMs: 5000 });
  const a = createGate({ limit: 6, windowMs: 10000, parent: root });
  const b = createGate({ limit: 6, windowMs: 10000, parent: root });
  const xAc = new AbortController();
  // 12 units can never gather from a pool of 10.
  const pX = root.acquireAll(
    [{ gate: a, units: 6 }, { gate: b, units: 6 }],
    xAc.signal,
  );
  assert.equal(root.stats().rolledBack, 1); // the eager gather failed once
  const y1 = root.acquire(2);
  const y2 = root.acquire(2);
  await sleep(90); // flip 1: X yields once; both later singles overtake it
  const [h1, h2] = await Promise.all([y1, y2]);
  assert.equal(root.stats().granted, 2);
  assert.equal(root.stats().rolledBack, 2);

  const y3 = root.acquire(2);
  await sleep(80); // flip 2: X yields a second time; y3 overtakes
  const h3 = await y3;
  assert.equal(root.stats().granted, 3);
  assert.equal(root.stats().rolledBack, 3);

  const y4 = root.acquire(2);
  await sleep(25); // flip 3 happened; X now blocks
  assert.equal(root.stats().granted, 3);
  assert.equal(root.stats().rolledBack, 4);
  assert.equal(root.stats().waiting, 14); // X (12) and y4 (2) still queued

  xAc.abort(); // once the blocking combo leaves, y4 is served at flip 4
  const xSettled = assert.rejects(pX, (err) => {
    assert.equal(err.code, 'CANCELLED');
    return true;
  });
  await xSettled;
  await sleep(70);
  const h4 = await y4;
  assert.equal(root.stats().granted, 4);
  for (const h of [h1, h2, h3, h4]) h.release();
});

// --- Window reclaim ordering -------------------------------------------------

test('a lender window reclaims uncommitted reservations before admitted lends', async () => {
  const root = createGate({ limit: 10, windowMs: 10000, maxWaitMs: 5000 });
  const p = createGate({ limit: 5, windowMs: 60, parent: root });
  const d = createGate({ limit: 2, windowMs: 10000, parent: p });
  const hOwn = await root.acquireAll([{ gate: d, units: 2 }]); // 2 own at d
  const hBorrowed = await root.acquireAll([{ gate: d, units: 2 }]); // 2 lent by p
  assert.equal(d.stats().inFlight, 4);
  await root.acquire(5); // 9 of 10 pool units used: 1 free
  const headAc = new AbortController();
  const head = root.acquire(3, headAc.signal); // queued head
  // d's own quota is full, so the queued combo reserves 1 unit lent by p.
  const comboAc = new AbortController();
  const pCombo = root.acquireAll([{ gate: d, units: 1 }], comboAc.signal);
  assert.equal(p.stats().reserved, 1);

  await sleep(90); // p flips; the root pool window stays open
  // Uncommitted reservation reclaimed first...
  assert.equal(p.stats().reserved, 0);
  assert.equal(root.stats().rolledBack, 0); // a reclaim is not a rollback
  // ...then the admitted lend: d keeps its 2 own units, loses the 2 lent.
  assert.equal(d.stats().inFlight, 2);
  hBorrowed.release(); // already reclaimed on d's side: no further drop
  assert.equal(d.stats().inFlight, 2);
  hOwn.release(); // the own occupation still releases normally
  assert.equal(d.stats().inFlight, 0);

  headAc.abort();
  comboAc.abort();
  await assert.rejects(head, () => true);
  await assert.rejects(pCombo, () => true);
});

test('a root window reclaims a root-lent combo leg without breaking the handle', async () => {
  const root = createGate({ limit: 6, windowMs: 60 });
  const d = createGate({ limit: 2, windowMs: 10000, parent: root });
  const hOwn = await root.acquireAll([{ gate: d, units: 2 }]); // 2 own
  const hBorrowed = await root.acquireAll([{ gate: d, units: 2 }]); // 2 root-lent
  assert.equal(d.stats().inFlight, 4);
  await sleep(90); // root flips; the own window of d is still open
  assert.equal(d.stats().inFlight, 2);
  hBorrowed.release(); // the borrowed part was already reclaimed
  assert.equal(d.stats().inFlight, 2);
  hOwn.release();
  assert.equal(d.stats().inFlight, 0);
});

test('an intermediate lend survives the root flip and is reclaimed at the lender flip', async () => {
  const root = createGate({ limit: 6, windowMs: 60 });
  const p = createGate({ limit: 4, windowMs: 120, parent: root });
  const d = createGate({ limit: 2, windowMs: 10000, parent: p });
  const hOwn = await root.acquireAll([{ gate: d, units: 2 }]); // 2 own at d
  const hBorrowed = await root.acquireAll([{ gate: d, units: 2 }]); // 2 lent by p
  await sleep(90); // root flips, p's window is still open: the lend survives
  assert.equal(d.stats().inFlight, 4);
  await sleep(60); // p flips: its lend is reclaimed now
  assert.equal(d.stats().inFlight, 2);
  hBorrowed.release();
  assert.equal(d.stats().inFlight, 2);
  hOwn.release();
  assert.equal(d.stats().inFlight, 0);
});

// --- Shrink ------------------------------------------------------------------

test('shrinking a member limit refuses the queued combo wholesale', async () => {
  const root = createGate({ limit: 3, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 10000, parent: root });
  await root.acquire(3); // pool full
  const p = root.acquireAll([{ gate: a, units: 2 }]);
  assert.equal(a.stats().waiting, 2);
  a.updateLimit(1); // the combo can never fit its own member anymore
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(root.stats().refused, 1);
  assert.equal(root.stats().waiting, 0);
  assert.equal(a.stats().waiting, 0);
});

test('shrinking the root pool refuses a queued combo whose total no longer fits', async () => {
  const root = createGate({ limit: 5, windowMs: 10000, maxWaitMs: 5000 });
  const a = createGate({ limit: 3, windowMs: 10000, parent: root });
  const b = createGate({ limit: 3, windowMs: 10000, parent: root });
  await root.acquire(5); // pool full
  const p = root.acquireAll([{ gate: a, units: 2 }, { gate: b, units: 2 }]);
  root.updateLimit(3); // the 4-unit combo can never fit the 3-unit pool
  await assert.rejects(p, (err) => {
    assert.equal(err.code, 'QUOTA_EXCEEDED');
    return true;
  });
  assert.equal(root.stats().refused, 1);
  assert.equal(a.stats().waiting, 0);
  assert.equal(b.stats().waiting, 0);
});

// --- Reparent ----------------------------------------------------------------

test('a reparented queued combo is served by the new family pool', async () => {
  const oldRoot = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 5000 });
  const child = createGate({ limit: 1, windowMs: 10000, maxWaitMs: 5000, parent: oldRoot });
  const newRoot = createGate({ limit: 2, windowMs: 60, maxWaitMs: 5000 });
  await oldRoot.acquire(1); // old pool full
  const p = child.acquireAll([{ gate: child, units: 1 }]); // owner moves with the subtree
  assert.equal(child.stats().waiting, 1);
  child.reparent(newRoot);
  assert.equal(child.stats().waiting, 1); // still queued, not granted early
  const h = await p; // re-gathered and served at the NEW family's flip
  assert.equal(child.stats().granted, 1);
  assert.equal(oldRoot.stats().granted, 1);
  assert.equal(newRoot.stats().granted, 0);
  h.release();
});

test('a reparented combo occupation keeps a valid handle and reclaims in the new pool', async () => {
  const oldRoot = createGate({ limit: 5, windowMs: 10000 });
  const child = createGate({ limit: 2, windowMs: 10000, parent: oldRoot });
  const newRoot = createGate({ limit: 5, windowMs: 60 });
  const hOwn = await oldRoot.acquireAll([{ gate: child, units: 2 }]); // 2 own
  const hBorrowed = await oldRoot.acquireAll([{ gate: child, units: 2 }]); // 2 borrowed
  assert.equal(child.stats().inFlight, 4);
  child.reparent(newRoot);
  assert.equal(child.stats().inFlight, 4); // occupations are never torn apart
  await sleep(90); // the new pool window flips: the borrowed 2 are reclaimed
  assert.equal(child.stats().inFlight, 2);
  hBorrowed.release();
  assert.equal(child.stats().inFlight, 2);
  hOwn.release();
  assert.equal(child.stats().inFlight, 0);
});
