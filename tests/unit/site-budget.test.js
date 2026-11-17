import test from "node:test";
import assert from "node:assert/strict";
import { SiteRequestLimiter } from "../../src/acquisition/site-budget.js";

class ManualClock {
  constructor() { this.millis = 0; }
  now() { return this.millis; }
  advance(ms) { this.millis += ms; }
}

function gate() {
  let release;
  const pending = new Promise((resolve) => { release = resolve; });
  return { pending, release };
}

async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not reached before the timeout");
    await new Promise((resolve) => setImmediate(resolve));
  }
}

test("limits concurrent requests per site while leaving an independent site untouched", async () => {
  const clock = new ManualClock();
  const limiter = new SiteRequestLimiter({ perSiteBudget: 2, now: () => clock.now() });
  const busy = [gate(), gate(), gate()];
  const calls = { alpha: 0, beta: 0 };
  const tasks = busy.map((g, index) => limiter.run("alpha.test", {
    feedId: `feed-${index}`,
    task: async () => { calls.alpha += 1; await g.pending; return index; }
  }));
  await waitFor(() => calls.alpha === 2);
  assert.equal(calls.alpha, 2, "only the budgeted number of tasks start");
  assert.deepEqual(limiter.snapshot().find((site) => site.host === "alpha.test"), {
    host: "alpha.test",
    budget: 2,
    active: 2,
    queued: 1,
    maxQueued: 200,
    feedOrder: ["feed-0", "feed-1", "feed-2"],
    totalScheduled: 2,
    totalRejected: 0,
    entries: [{ id: 3, feedId: "feed-2", reason: "feed_refresh", waitedMs: 0 }]
  });
  // A different site has its own budget and starts immediately.
  const beta = limiter.run("beta.test", { feedId: "other", task: async () => { calls.beta += 1; return "ok"; } });
  assert.equal(await beta, "ok");
  assert.equal(calls.beta, 1);
  busy.forEach((g) => g.release());
  assert.deepEqual(await Promise.all(tasks), [0, 1, 2]);
  assert.deepEqual(limiter.snapshot(), []);
});

test("schedules round-robin across feeds so a failing, retrying feed cannot starve peers", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  const order = [];
  const started = { a1: gate(), a2: gate(), b1: gate(), b2: gate() };
  const done = { a1: gate(), a2: gate(), b1: gate(), b2: gate() };
  const task = (name) => async () => { order.push(name); started[name].release(); await done[name].pending; };
  // feed-a enqueues twice before feed-b enqueues twice.
  const a1 = limiter.run("shared.test", { feedId: "feed-a", task: task("a1") });
  const a2 = limiter.run("shared.test", { feedId: "feed-a", task: task("a2") });
  const b1 = limiter.run("shared.test", { feedId: "feed-b", task: task("b1") });
  const b2 = limiter.run("shared.test", { feedId: "feed-b", task: task("b2") });
  await started.a1.pending;
  assert.deepEqual(order, ["a1"]);
  // Releasing a1 lets exactly one more task start; fairness requires it to be feed-b's.
  done.a1.release();
  await started.b1.pending;
  assert.deepEqual(order, ["a1", "b1"]);
  done.b1.release();
  await started.a2.pending;
  assert.deepEqual(order, ["a1", "b1", "a2"]);
  done.a2.release();
  await started.b2.pending;
  assert.deepEqual(order, ["a1", "b1", "a2", "b2"]);
  done.b2.release();
  await Promise.all([a1, a2, b1, b2]);
});

test("cancels a queued entry without starting its task and keeps the slot free", async () => {
  const clock = new ManualClock();
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1, now: () => clock.now() });
  const blocker = gate();
  let blockedTaskRan = false;
  const active = limiter.run("alpha.test", { feedId: "feed-a", task: async () => { blockedTaskRan = true; await blocker.pending; } });
  await Promise.resolve();
  const controller = new AbortController();
  let queuedTaskRan = false;
  const waiting = limiter.run("alpha.test", {
    feedId: "feed-b",
    signal: controller.signal,
    task: async () => { queuedTaskRan = true; }
  });
  clock.advance(150);
  const snapshot = limiter.snapshot()[0];
  assert.equal(snapshot.entries[0].waitedMs, 150);
  controller.abort();
  await assert.rejects(waiting, { code: "queued_cancelled" });
  assert.equal(queuedTaskRan, false, "a cancelled queue entry never reaches the network");
  assert.equal(limiter.snapshot()[0].queued, 0);
  blocker.release();
  await active;
  assert.equal(blockedTaskRan, true);
});

test("rejects new entries once the per-site queue is full instead of growing without bound", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1, maxQueuedPerSite: 1 });
  const blocker = gate();
  const active = limiter.run("alpha.test", { feedId: "feed-a", task: () => blocker.pending });
  limiter.run("alpha.test", { feedId: "feed-b", task: () => "ok" }).catch(() => {});
  await waitFor(() => limiter.snapshot()[0]?.active === 1);
  await assert.rejects(
    limiter.run("alpha.test", { feedId: "feed-c", task: () => "ok" }),
    { code: "site_queue_full" }
  );
  const snapshot = limiter.snapshot()[0];
  assert.equal(snapshot.totalRejected, 1);
  blocker.release();
  await active.catch(() => {});
});

test("an already-aborted signal rejects before touching the queue", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  const controller = new AbortController();
  controller.abort();
  let ran = false;
  await assert.rejects(
    limiter.run("alpha.test", { feedId: "feed-a", signal: controller.signal, task: () => { ran = true; } }),
    { code: "queued_cancelled" }
  );
  assert.equal(ran, false);
  assert.deepEqual(limiter.snapshot(), []);
});

test("releases the budget when a task throws and surfaces the original error", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  const failure = Object.assign(new Error("boom"), { code: "upstream_down" });
  await assert.rejects(
    limiter.run("alpha.test", { feedId: "feed-a", task: async () => { throw failure; } }),
    { code: "upstream_down" }
  );
  let secondRan = false;
  await limiter.run("alpha.test", { feedId: "feed-b", task: async () => { secondRan = true; } });
  assert.equal(secondRan, true, "the slot is released after a failed task");
});

test("validates the configured budgets", () => {
  assert.throws(() => new SiteRequestLimiter({ perSiteBudget: 0 }), /perSiteBudget/);
  assert.throws(() => new SiteRequestLimiter({ perSiteBudget: 65 }), /perSiteBudget/);
  assert.throws(() => new SiteRequestLimiter({ maxQueuedPerSite: 0 }), /maxQueuedPerSite/);
});
