import test from "node:test";
import assert from "node:assert/strict";
import { PageFetcher } from "../../src/acquisition/page-fetcher.js";
import { SiteRequestLimiter } from "../../src/acquisition/site-budget.js";

const feed = {
  feedId: "daily-main",
  baseUrl: "https://news.example.test/list.html",
  allowedHosts: ["news.example.test", "cdn.example.test"]
};

const htmlResponse = (html = "<main>ok</main>", options = {}) => new Response(html, {
  status: options.status ?? 200,
  headers: { "content-type": "text/html; charset=utf-8", ...(options.headers ?? {}) }
});

async function waitFor(predicate, timeoutMs = 1000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not reached before the timeout");
    await new Promise((resolve) => setImmediate(resolve));
  }
}

test("charges every attempt against the budget of the host that receives it", async () => {
  const hosts = [];
  const limiter = new SiteRequestLimiter({ perSiteBudget: 4 });
  const fetcher = new PageFetcher({
    limiter,
    fetchImpl: async (url) => { hosts.push(new URL(url).hostname); return htmlResponse(); }
  });
  await fetcher.fetchFeed(feed);
  assert.deepEqual(hosts, ["news.example.test"]);
  const snapshot = limiter.snapshot();
  assert.deepEqual(snapshot, [], "slots are released once the response is read");
});

test("a legal redirect to another allowed host consumes the target host budget, not the origin's", async () => {
  const seen = [];
  const originLimiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  const fetcher = new PageFetcher({
    limiter: originLimiter,
    fetchImpl: async (url) => {
      const parsed = new URL(url);
      seen.push(parsed.hostname);
      return parsed.hostname === "news.example.test"
        ? new Response(null, { status: 302, headers: { location: "https://cdn.example.test/latest.html" } })
        : htmlResponse("<main>from cdn</main>");
    }
  });
  const result = await fetcher.fetchFeed(feed);
  assert.equal(result.finalUrl, "https://cdn.example.test/latest.html");
  assert.deepEqual(seen, ["news.example.test", "cdn.example.test"]);
});

test("queues behind a saturated redirect target instead of borrowing the origin site slot", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  let releaseCdn;
  const cdnBlocked = new Promise((resolve) => { releaseCdn = resolve; });
  const cdnOccupant = limiter.run("cdn.example.test", { feedId: "occupant", task: () => cdnBlocked });
  await Promise.resolve();
  const requested = [];
  const fetcher = new PageFetcher({
    limiter,
    fetchImpl: async (url) => {
      requested.push(new URL(url).hostname);
      return requested.length === 1
        ? new Response(null, { status: 302, headers: { location: "https://cdn.example.test/latest.html" } })
        : htmlResponse();
    }
  });
  const fetching = fetcher.fetchFeed(feed);
  await waitFor(() => requested.length === 1 && limiter.pending("news.example.test") === 0);
  assert.deepEqual(requested, ["news.example.test"], "origin hop completed and released its slot");
  assert.equal(limiter.snapshot().find((site) => site.host === "cdn.example.test").queued, 1);
  assert.equal(limiter.pending("news.example.test"), 0);
  releaseCdn();
  await cdnOccupant;
  const result = await fetching;
  assert.equal(result.html, "<main>ok</main>");
  assert.deepEqual(requested, ["news.example.test", "cdn.example.test"]);
});

test("each retry re-enters the site queue and is charged as a separate request", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  let calls = 0;
  const waits = [];
  const fetcher = new PageFetcher({
    limiter,
    backoffBaseMs: 10,
    sleep: async (duration) => waits.push(duration),
    fetchImpl: async () => {
      calls += 1;
      return calls < 3 ? new Response("busy", { status: 503 }) : htmlResponse("ready");
    }
  });
  const result = await fetcher.fetchFeed(feed);
  assert.equal(result.attemptCount, 3);
  assert.equal(calls, 3);
  assert.deepEqual(waits, [10, 20]);
  assert.deepEqual(limiter.snapshot(), []);
});

test("external abort while queued is reported as cancellation without any fetch call", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1 });
  let release;
  const blocker = new Promise((resolve) => { release = resolve; });
  limiter.run("news.example.test", { feedId: "occupant", task: () => blocker });
  let calls = 0;
  const fetcher = new PageFetcher({ limiter, fetchImpl: async () => { calls += 1; return htmlResponse(); } });
  const controller = new AbortController();
  const waiting = fetcher.fetchFeed(feed, { signal: controller.signal });
  controller.abort();
  await assert.rejects(waiting, { code: "queued_cancelled" });
  assert.equal(calls, 0);
  release();
});

test("external abort during an in-flight request is a cancellation, not a fetch failure, and is not retried", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 2 });
  const controller = new AbortController();
  let calls = 0;
  const fetcher = new PageFetcher({
    limiter,
    backoffBaseMs: 10,
    sleep: async () => { throw new Error("retry sleep must not run after abort"); },
    fetchImpl: async (_url, options) => {
      calls += 1;
      controller.abort();
      options.signal.dispatchEvent(new Event("abort"));
      const error = new Error("The operation was aborted");
      error.name = "AbortError";
      throw error;
    }
  });
  await assert.rejects(fetcher.fetchFeed(feed, { signal: controller.signal }), { code: "fetch_aborted" });
  assert.equal(calls, 1, "aborted requests are not retried");
  assert.deepEqual(limiter.snapshot(), [], "the site slot is released after cancellation");
});

test("aborting while backing off stops the retry without fabricating a network failure", async () => {
  const limiter = new SiteRequestLimiter({ perSiteBudget: 2 });
  const controller = new AbortController();
  const fetcher = new PageFetcher({
    limiter,
    backoffBaseMs: 10,
    sleep: async () => { controller.abort(); },
    fetchImpl: async () => new Response("busy", { status: 503 })
  });
  await assert.rejects(
    fetcher.fetchFeed(feed, { signal: controller.signal }),
    { code: "fetch_aborted" }
  );
});

test("still works without a limiter, preserving direct fetch behaviour", async () => {
  const fetcher = new PageFetcher({ fetchImpl: async () => htmlResponse("<main>direct</main>") });
  const result = await fetcher.fetchFeed(feed);
  assert.equal(result.html, "<main>direct</main>");
});

test("rejects a limiter that does not implement the queue contract", () => {
  assert.throws(() => new PageFetcher({ limiter: {} }), /limiter/);
});
