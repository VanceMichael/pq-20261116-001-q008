/**
 * Coordinates feed refreshes on top of the site-level request budget.
 *
 * Simultaneous refreshes of the same feed still share one fetch (coalescing).
 * Each caller may pass its own AbortSignal:
 *  - cancelling while the shared job is queued only detaches that caller and,
 *    when no participant remains, removes the queue entry without any network
 *    request or fetch failure record;
 *  - cancelling once the fetch has started aborts the in-flight request, which
 *    is recorded as a real cancellation (not a network failure). The import
 *    step, once reached, always completes, so an archived capture is never
 *    duplicated.
 */
export class RefreshService {
  constructor({ feeds, fetches, fetcher, imports, clock = () => new Date() }) {
    this.feeds = feeds;
    this.fetches = fetches;
    this.fetcher = fetcher;
    this.imports = imports;
    this.clock = clock;
    this.inFlight = new Map();
  }

  refresh(feedId, { signal } = {}) {
    const feed = this.feeds.get(feedId);
    if (!feed) return Promise.reject(new Error(`unknown feed: ${feedId}`));
    if (signal?.aborted) {
      return Promise.reject(cancelError("refresh_cancelled", "refresh was cancelled before it started"));
    }
    let job = this.inFlight.get(feedId);
    if (!job) {
      job = new RefreshJob({ feed, service: this });
      this.inFlight.set(feedId, job);
      job.run();
    }
    return job.join(signal);
  }

  removeJob(feedId, job) {
    if (this.inFlight.get(feedId) === job) this.inFlight.delete(feedId);
  }

  recent(feedId, limit) {
    if (!this.feeds.get(feedId)) return null;
    return this.fetches.recent(feedId, limit);
  }

  pending(feedId) {
    return this.inFlight.has(feedId);
  }

  queueSnapshot() {
    return [...this.inFlight.entries()].map(([feedId, job]) => ({
      feedId,
      phase: job.phase,
      participants: job.participants.size,
      currentUrl: job.currentUrl,
      attempts: job.attemptCount,
      queuedAt: job.queuedAt,
      startedAt: job.startedAt
    }));
  }
}

class RefreshJob {
  constructor({ feed, service }) {
    this.feed = feed;
    this.service = service;
    this.controller = new AbortController();
    this.participants = new Set();
    this.phase = "queued";
    this.currentUrl = null;
    this.attemptCount = 0;
    const now = service.clock().toISOString();
    this.queuedAt = now;
    this.startedAt = null;
    this.settled = false;
    this.completion = this.perform();
  }

  join(signal) {
    if (this.settled) {
      // The job may settle between lookup and join under synchronous cancellation.
      return this.completion ?? Promise.reject(cancelError("refresh_cancelled", "refresh already finished"));
    }
    const participant = {
      signal,
      rejected: false,
      resolve: null,
      reject: null
    };
    const detached = new Promise((promiseResolve, promiseReject) => {
      participant.resolve = promiseResolve;
      participant.reject = promiseReject;
    });
    this.participants.add(participant);
    if (signal) {
      if (signal.aborted) this.detach(participant);
      else signal.addEventListener("abort", () => this.detach(participant), { once: true });
    }
    const joined = Promise.race([this.completion, detached]);
    // A detached participant may reject after its caller stopped awaiting.
    joined.catch(() => {});
    return joined;
  }

  detach(participant) {
    if (participant.rejected) return;
    if (this.phase === "queued") {
      // Still waiting for a budget slot: reject immediately and tear the job
      // down without any network request or fetch audit record.
      participant.rejected = true;
      const lastParticipant = this.participants.size === 1;
      this.participants.delete(participant);
      participant.reject(cancelError("refresh_cancelled", "refresh was cancelled while waiting in the site queue"));
      if (lastParticipant && !this.settled) this.controller.abort();
      return;
    }
    // The request is already in flight. Only abort the shared fetch when this
    // was the last remaining participant; otherwise the job keeps running for
    // the others. The detached participant settles once the job does, and a
    // successful completion is delivered even to a late-cancelling caller.
    const alone = this.participants.size === 1;
    if (alone && !this.settled) this.controller.abort();
    this.completion.then(
      (value) => participant.resolve(value),
      () => participant.reject(cancelError("refresh_aborted", "refresh was cancelled while the fetch was in flight"))
    );
  }

  async run() {
    try {
      await this.completion;
    } catch {
      // Outcome is persisted inside perform; callers observe via their own promises.
    } finally {
      this.settled = true;
      this.service.removeJob(this.feed.feedId, this);
    }
  }

  async perform() {
    const { feedId } = this.feed;
    const startedAt = this.service.clock().toISOString();
    this.startedAt = startedAt;
    try {
      const fetched = await this.service.fetcher.fetchFeed(this.feed, {
        signal: this.controller.signal,
        onPhase: (phase, detail) => {
          if (phase === "request") {
            this.phase = "active";
            this.currentUrl = detail.url;
            this.attemptCount = Math.max(this.attemptCount, detail.attempt - 1);
          }
        }
      });
      this.attemptCount = fetched.attemptCount;
      // Cancellation can no longer interrupt archiving: a captured page is
      // imported exactly once even if every caller has gone away.
      const finishedAt = this.service.clock().toISOString();
      if (fetched.status === 304) {
        const attemptId = this.service.fetches.record({
          feedId, startedAt, finishedAt, outcome: "not_modified", httpStatus: 304,
          attemptCount: fetched.attemptCount, requestedUrl: fetched.requestedUrl,
          finalUrl: fetched.finalUrl, captureId: null, errorCode: null, errorMessage: null
        });
        return { attemptId, feedId, outcome: "not_modified", httpStatus: 304, capture: null };
      }
      const capture = this.service.imports.importHtml(feedId, fetched.html, finishedAt);
      const attemptId = this.service.fetches.record({
        feedId, startedAt, finishedAt, outcome: capture.accepted ? "accepted" : "rejected",
        httpStatus: fetched.status, attemptCount: fetched.attemptCount,
        requestedUrl: fetched.requestedUrl, finalUrl: fetched.finalUrl,
        captureId: capture.captureId ?? null, errorCode: capture.accepted ? null : "capture_rejected",
        errorMessage: capture.reason
      });
      return { attemptId, feedId, outcome: capture.accepted ? "accepted" : "rejected",
        httpStatus: fetched.status, capture };
    } catch (error) {
      const queuedCancel = error.code === "queued_cancelled" || error.code === "refresh_cancelled";
      if (queuedCancel) {
        // A queue-stage cancellation issued no request: leave no audit trail
        // and never masquerade as a network failure.
        throw Object.assign(error, { statusCode: 499 });
      }
      const finishedAt = this.service.clock().toISOString();
      const aborted = error.code === "fetch_aborted";
      const outcome = aborted ? "cancelled" : "failed";
      const attemptId = this.service.fetches.record({
        feedId, startedAt, finishedAt, outcome,
        httpStatus: error.status ?? null,
        attemptCount: error.attemptCount ?? 1,
        requestedUrl: this.feed.baseUrl, finalUrl: this.currentUrl,
        captureId: null, errorCode: error.code ?? "refresh_failed",
        errorMessage: error.message.slice(0, 500)
      });
      if (aborted) error.code = "refresh_aborted";
      throw Object.assign(error, {
        attemptId,
        statusCode: aborted ? 499 : error.statusCode ?? 502
      });
    }
  }
}

function cancelError(code, message) {
  return Object.assign(new Error(message), { code, statusCode: 499 });
}
