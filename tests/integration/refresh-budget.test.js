import test from "node:test";
import assert from "node:assert/strict";
import { makeArchive, page, standardItems } from "../helpers.js";

function feed(feedId, hostname, extraHosts = []) {
  return {
    feedId,
    baseUrl: `https://${hostname}/index.html`,
    allowedHosts: [hostname, ...extraHosts],
    minimumItems: 3,
    missingThreshold: 3
  };
}

class ManualClock {
  constructor() { this.millis = Date.parse("2026-11-16T02:00:00Z"); }
  date() { return new Date(this.millis); }
  advance(ms) { this.millis += ms; }
}

function html() {
  // A fresh stream per call: Response bodies can only be read once.
  return new Response(page(standardItems(4)), {
    status: 200,
    headers: { "content-type": "text/html; charset=utf-8" }
  });
}

// Deterministic fetch mock whose in-flight requests can be parked and released.
// respondAll() latches: requests that reach the network after it was called
// resolve immediately, so a release can never be lost to a not-yet-started fetch.
function controllableFetch(responseFactory = html) {
  const requests = [];
  const parked = [];
  let released = false;
  const fetchImpl = async (url, options) => {
    const record = { url: new URL(url), options, release: null, response: null };
    const gate = new Promise((resolve) => { record.release = resolve; });
    requests.push(record);
    parked.push(record);
    if (released) record.release();
    await gate;
    return record.response ?? responseFactory();
  };
  return {
    fetchImpl,
    requests,
    respond(index, response) { parked[index].response = response; parked[index].release(); },
    respondAll() {
      released = true;
      parked.forEach((record) => { record.response = null; record.release(); });
    },
    settle() { return new Promise((resolve) => setImmediate(resolve)); }
  };
}

test("site budget bounds concurrent fetches across feeds while independent sites proceed", async (t) => {
  const clock = new ManualClock();
  const network = controllableFetch();
  const { archive } = await makeArchive(t, { clock: () => clock.date() });
  // Rebuild with a controlled fetch implementation sharing the archive limiter.
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  archive.refreshes.fetcher = new PageFetcher({
    fetchImpl: network.fetchImpl, limiter: archive.siteBudget, now: () => clock.millis
  });
  archive.registerFeed(feed("alpha-1", "shared.example.test"));
  archive.registerFeed(feed("alpha-2", "shared.example.test"));
  archive.registerFeed(feed("alpha-3", "shared.example.test"));
  archive.registerFeed(feed("solo", "independent.example.test"));
  const jobs = [
    archive.refreshFeed("alpha-1"),
    archive.refreshFeed("alpha-2"),
    archive.refreshFeed("alpha-3"),
    archive.refreshFeed("solo")
  ];
  await network.settle();
  const queue = archive.refreshQueue();
  const shared = queue.sites.find((site) => site.host === "shared.example.test");
  assert.equal(shared.active, 3, "all three same-site feeds share the default four-slot budget");
  assert.equal(shared.queued, 0);
  assert.equal(shared.budget, 4);
  const independent = queue.sites.find((site) => site.host === "independent.example.test");
  assert.equal(independent.active, 1);
  const queuedHosts = network.requests.map((record) => record.url.hostname).sort();
  assert.deepEqual(queuedHosts, [
    "independent.example.test",
    "shared.example.test",
    "shared.example.test",
    "shared.example.test"
  ]);
  network.respondAll();
  const results = await Promise.all(jobs);
  assert.equal(results.every((result) => result.outcome === "accepted"), true);
  assert.deepEqual(archive.refreshQueue().sites, []);
});

test("round-robin scheduling prevents one failing feed from starving same-site peers", async (t) => {
  const clock = new ManualClock();
  const { archive } = await makeArchive(t, { perSiteBudget: 1, clock: () => clock.date() });
  const started = [];
  const gates = [];
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  archive.refreshes.fetcher = new PageFetcher({
    limiter: archive.siteBudget,
    now: () => clock.millis,
    fetchImpl: async (url) => {
      const hostname = new URL(url).hostname;
      const release = gate();
      gates.push(release);
      started.push(hostname);
      await release.pending;
      return html();
    }
  });
  // Three distinct feeds share one slot; the first two enqueue before the third.
  archive.registerFeed(feed("greedy-a", "shared.example.test"));
  archive.registerFeed(feed("greedy-b", "shared.example.test"));
  archive.registerFeed(feed("peer", "shared.example.test"));
  const first = archive.refreshFeed("greedy-a");
  const second = archive.refreshFeed("greedy-b");
  const peerJob = archive.refreshFeed("peer");
  await waitFor(() => started.length === 1);
  assert.deepEqual(started, ["shared.example.test"]);
  gates[0].release();
  await waitFor(() => started.length === 2);
  // Second slot goes to the last enqueued feed; neither enqueuer can monopolize.
  const queue = archive.refreshQueue();
  const activeFeed = queue.jobs.find((job) => job.phase === "active");
  const queuedFeed = queue.jobs.find((job) => job.phase === "queued");
  assert.ok(activeFeed && queuedFeed);
  assert.notEqual(activeFeed.feedId, queuedFeed.feedId);
  gates[1].release();
  await waitFor(() => started.length === 3);
  gates[2].release();
  await Promise.all([first, second, peerJob]);
  assert.equal(started.length, 3);
});

test("retries occupy the site budget per attempt and are visible in the explainable queue", async (t) => {
  const clock = new ManualClock();
  const sleeps = [];
  let calls = 0;
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  const { SiteRequestLimiter } = await import("../../src/acquisition/site-budget.js");
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1, now: () => clock.millis });
  const fetcher = new PageFetcher({
    limiter,
    now: () => clock.millis,
    backoffBaseMs: 10,
    sleep: async (duration) => { sleeps.push(duration); clock.advance(duration); },
    fetchImpl: async () => {
      calls += 1;
      return calls < 3 ? new Response("busy", { status: 503 }) : html();
    }
  });
  const { archive } = await makeArchive(t, { clock: () => clock.date(), siteBudget: limiter });
  archive.refreshes.fetcher = fetcher;
  archive.registerFeed(feed("retryer", "shared.example.test"));
  const result = await archive.refreshFeed("retryer");
  assert.equal(result.outcome, "accepted");
  assert.equal(calls, 3);
  assert.deepEqual(sleeps, [10, 20]);
  const [attempt] = archive.listFetchAttempts("retryer");
  assert.equal(attempt.attemptCount, 3);
});

test("redirect to another allowed host is charged to the target host budget", async (t) => {
  const clock = new ManualClock();
  const { SiteRequestLimiter } = await import("../../src/acquisition/site-budget.js");
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  const limiter = new SiteRequestLimiter({ perSiteBudget: 1, now: () => clock.millis });
  let cdnRelease;
  const cdnBlocked = new Promise((resolve) => { cdnRelease = resolve; });
  limiter.run("cdn.example.test", { feedId: "occupant", task: () => cdnBlocked.then(() => html()) });
  const hosts = [];
  const fetcher = new PageFetcher({
    limiter,
    now: () => clock.millis,
    fetchImpl: async (url) => {
      const parsed = new URL(url);
      hosts.push(parsed.hostname);
      if (parsed.hostname === "origin.example.test") {
        return new Response(null, { status: 302, headers: { location: "https://cdn.example.test/latest.html" } });
      }
      return html();
    }
  });
  const { archive } = await makeArchive(t, { clock: () => clock.date(), siteBudget: limiter });
  archive.refreshes.fetcher = fetcher;
  archive.registerFeed(feed("mobile", "origin.example.test", ["cdn.example.test"]));
  const refresh = archive.refreshFeed("mobile");
  await waitFor(() => hosts.includes("origin.example.test"));
  await waitFor(() => limiter.snapshot().find((site) => site.host === "cdn.example.test")?.queued === 1);
  const cdn = limiter.snapshot().find((site) => site.host === "cdn.example.test");
  assert.equal(cdn.active, 1, "occupant holds the only CDN slot");
  assert.equal(cdn.queued, 1, "redirected request waits on the target host, not the origin");
  assert.equal(limiter.pending("origin.example.test"), 0, "origin slot was released after the first hop");
  cdnRelease();
  const result = await refresh;
  assert.equal(result.outcome, "accepted");
  assert.equal(result.httpStatus, 200);
  const [attempt] = archive.listFetchAttempts("mobile");
  assert.equal(attempt.finalUrl, "https://cdn.example.test/latest.html");
  assert.equal(attempt.requestedUrl, "https://origin.example.test/index.html");
});

test("cancelling a queued refresh never touches the network and records no fetch failure", async (t) => {
  const clock = new ManualClock();
  const network = controllableFetch();
  const { archive } = await makeArchive(t, { perSiteBudget: 1, clock: () => clock.date() });
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  archive.refreshes.fetcher = new PageFetcher({
    fetchImpl: network.fetchImpl, limiter: archive.siteBudget, now: () => clock.millis
  });
  archive.registerFeed(feed("holder", "shared.example.test"));
  archive.registerFeed(feed("waiting", "shared.example.test"));
  const holder = archive.refreshFeed("holder");
  await network.settle();
  const controller = new AbortController();
  const waiting = archive.refreshFeed("waiting", { signal: controller.signal });
  await waitFor(() => archive.refreshQueue().jobs.some((job) => job.feedId === "waiting" && job.phase === "queued"));
  controller.abort();
  await assert.rejects(waiting, { code: "refresh_cancelled", statusCode: 499 });
  assert.equal(network.requests.length, 1, "the cancelled refresh issued no request");
  assert.deepEqual(archive.listFetchAttempts("waiting"), [], "no fetch audit row is written for a queue cancel");
  network.respondAll();
  await holder;
  // The holder's fetch still archives exactly once.
  assert.equal(archive.listFetchAttempts("holder").length, 1);
  assert.equal(archive.listCaptures("holder").length, 1);
});

test("cancelling an in-flight refresh aborts the request without a fake network failure", async (t) => {
  const clock = new ManualClock();
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  let fetchCalls = 0;
  const fetcher = new PageFetcher({
    now: () => clock.millis,
    backoffBaseMs: 10,
    sleep: async () => { throw new Error("backoff must not run after cancellation"); },
    fetchImpl: async (_url, options) => {
      fetchCalls += 1;
      options.signal.addEventListener("abort", () => {}, { once: true });
      await new Promise((resolve, reject) => {
        options.signal.addEventListener("abort", () => {
          const error = new Error("The operation was aborted");
          error.name = "AbortError";
          reject(error);
        }, { once: true });
      });
    }
  });
  const { archive } = await makeArchive(t, { clock: () => clock.date() });
  archive.refreshes.fetcher = fetcher;
  archive.registerFeed(feed("cancelled", "shared.example.test"));
  const controller = new AbortController();
  const job = archive.refreshFeed("cancelled", { signal: controller.signal });
  await waitFor(() => fetchCalls === 1);
  controller.abort();
  await assert.rejects(job, { code: "refresh_aborted", statusCode: 499 });
  assert.equal(fetchCalls, 1, "aborted requests are not retried");
  const [attempt] = archive.listFetchAttempts("cancelled");
  assert.equal(attempt.outcome, "cancelled");
  assert.equal(attempt.errorCode, "fetch_aborted");
  assert.equal(attempt.captureId, null);
  assert.equal(archive.listCaptures("cancelled").length, 0);
});

test("one participant cancelling a coalesced refresh does not break the other participant or duplicate imports", async (t) => {
  const clock = new ManualClock();
  const network = controllableFetch();
  const { archive } = await makeArchive(t, { clock: () => clock.date() });
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  archive.refreshes.fetcher = new PageFetcher({
    fetchImpl: network.fetchImpl, limiter: archive.siteBudget, now: () => clock.millis
  });
  archive.registerFeed(feed("shared", "shared.example.test"));
  const first = archive.refreshFeed("shared");
  const controller = new AbortController();
  const second = archive.refreshFeed("shared", { signal: controller.signal });
  await network.settle();
  assert.equal(network.requests.length, 1, "coalescing still collapses same-feed refreshes into one fetch");
  controller.abort();
  // The remaining participant keeps the shared fetch alive, so the job is not
  // aborted; once it completes both callers receive the same captured result.
  network.respondAll();
  const [left, right] = await Promise.all([first, second]);
  assert.equal(left.attemptId, right.attemptId);
  assert.equal(right.outcome, "accepted");
  const result = left;
  assert.equal(result.outcome, "accepted");
  assert.equal(network.requests.length, 1);
  assert.equal(archive.listFetchAttempts("shared").length, 1);
  assert.equal(archive.listCaptures("shared").length, 1);
  assert.equal(archive.articles.stats("shared").total, 4);
});

test("exposes queue position and wait time through the explainable queue snapshot", async (t) => {
  const clock = new ManualClock();
  const network = controllableFetch();
  const { archive } = await makeArchive(t, { perSiteBudget: 1, clock: () => clock.date() });
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  archive.refreshes.fetcher = new PageFetcher({
    fetchImpl: network.fetchImpl, limiter: archive.siteBudget, now: () => clock.millis
  });
  archive.registerFeed(feed("first", "shared.example.test"));
  archive.registerFeed(feed("second", "shared.example.test"));
  const jobs = [archive.refreshFeed("first"), archive.refreshFeed("second")];
  await network.settle();
  clock.advance(250);
  const snapshot = archive.refreshQueue();
  const site = snapshot.sites.find((entry) => entry.host === "shared.example.test");
  assert.equal(site.budget, 1);
  assert.equal(site.active, 1);
  assert.equal(site.queued, 1);
  assert.equal(site.entries[0].feedId, "second");
  assert.equal(site.entries[0].waitedMs, 250);
  assert.equal(snapshot.jobs.find((job) => job.feedId === "second").phase, "queued");
  network.respondAll();
  await Promise.all(jobs);
});

test("serves the explainable queue and a 499 cancellation over HTTP", async (t) => {
  const clock = new ManualClock();
  const network = controllableFetch();
  const { archive } = await makeArchive(t, { perSiteBudget: 1, clock: () => clock.date() });
  const { PageFetcher } = await import("../../src/acquisition/page-fetcher.js");
  const { createHttpServer } = await import("../../src/http/server.js");
  archive.refreshes.fetcher = new PageFetcher({
    fetchImpl: network.fetchImpl, limiter: archive.siteBudget, now: () => clock.millis
  });
  archive.registerFeed(feed("holder", "shared.example.test"));
  archive.registerFeed(feed("waiting", "shared.example.test"));
  const app = createHttpServer(archive, { host: "127.0.0.1", port: 0, logger: { error() {} } });
  const address = await app.listen();
  t.after(() => app.close());
  const base = `http://127.0.0.1:${address.port}`;

  const holder = fetch(`${base}/v1/feeds/holder/refresh`, { method: "POST" });
  await waitFor(() => network.requests.length === 1);
  const controller = new AbortController();
  const waiting = fetch(`${base}/v1/feeds/waiting/refresh`, { method: "POST", signal: controller.signal });
  await waitFor(() =>
    archive.refreshQueue().sites.some((site) => site.entries.some((entry) => entry.feedId === "waiting")));

  const queueResponse = await fetch(`${base}/v1/refresh-queue`);
  const queue = await queueResponse.json();
  assert.equal(queueResponse.status, 200);
  const site = queue.sites.find((entry) => entry.host === "shared.example.test");
  assert.equal(site.budget, 1);
  assert.equal(site.queued, 1);
  assert.equal(site.entries[0].feedId, "waiting");

  controller.abort();
  await assert.rejects(waiting, { name: "AbortError" });
  assert.deepEqual(archive.listFetchAttempts("waiting"), []);
  network.respondAll();
  const holderResponse = await holder;
  assert.equal(holderResponse.status, 202);
});

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
