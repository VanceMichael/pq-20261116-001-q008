/**
 * Site-level request budget with a fair, explainable queue.
 *
 * Every HTTP attempt for a feed is charged against the budget of the host that
 * actually receives it (redirect targets acquire their own host budget). Each
 * site has a bounded concurrency budget and a per-feed FIFO queue; scheduling is
 * round-robin across feeds, so a feed whose requests keep failing cannot starve
 * the other feeds sharing that site. Sites are independent entries, so a
 * saturated site never blocks another site.
 */
export class SiteRequestLimiter {
  constructor({
    perSiteBudget = 4,
    maxQueuedPerSite = 200,
    now = () => Date.now()
  } = {}) {
    this.perSiteBudget = boundedInteger(perSiteBudget, 1, 64, "perSiteBudget");
    this.maxQueuedPerSite = boundedInteger(maxQueuedPerSite, 1, 10_000, "maxQueuedPerSite");
    this.now = now;
    this.sites = new Map();
    this.nextEntryId = 1;
  }

  siteState(host) {
    let state = this.sites.get(host);
    if (!state) {
      state = {
        host,
        entries: [],          // every entry, waiting or leased, stays until settled
        feedOrder: [],        // round-robin order of feeds with live entries
        activeFeeds: new Set(),
        lastServed: null,
        drainQueued: false,
        draining: false,
        totalScheduled: 0,
        totalRejected: 0
      };
      this.sites.set(host, state);
    }
    return state;
  }

  /**
   * Run `task` once a slot for `host` is available. Resolves/rejects with the
   * task's own outcome. Aborting `signal` while the entry is still waiting
   * rejects with `queued_cancelled` and never starts the task.
   */
  run(host, { feedId, reason = "feed_refresh", signal, task }) {
    if (typeof task !== "function") throw new TypeError("limiter entries require a task function");
    if (signal?.aborted) {
      return Promise.reject(queueError("queued_cancelled", "refresh was cancelled before it entered the site queue"));
    }
    const state = this.siteState(host);
    // Capacity already committed: leased entries plus everything waiting. A
    // drain scheduled but not yet run still counts, since its entries will
    // occupy a slot on the next microtask.
    const committed = state.entries.filter((entry) => !entry.settled).length;
    if (committed >= this.perSiteBudget + this.maxQueuedPerSite) {
      state.totalRejected += 1;
      return Promise.reject(queueError("site_queue_full", `site ${host} request queue is full`));
    }
    const entry = {
      id: this.nextEntryId++,
      host,
      feedId,
      reason,
      queuedAt: this.now(),
      signal,
      task,
      lease: null,
      settled: false,
      resolve: null,
      reject: null
    };
    entry.result = new Promise((promiseResolve, promiseReject) => {
      entry.resolve = promiseResolve;
      entry.reject = promiseReject;
    });
    state.entries.push(entry);
    if (!state.feedOrder.includes(feedId)) state.feedOrder.push(feedId);
    if (signal) {
      signal.addEventListener("abort", () => {
        if (!entry.settled && !entry.lease) this.finishQueued(state, entry, abortReason(signal));
      }, { once: true });
    }
    this.pump(state);
    return entry.result;
  }

  activeCount(state) {
    return state.entries.reduce((count, entry) => count + (entry.lease && !entry.lease.released ? 1 : 0), 0);
  }

  pump(state) {
    // Coalesce bursts of enqueue/release/abort into one microtask drain so
    // scheduling never nests inside an active drain.
    if (state.drainQueued) return;
    state.drainQueued = true;
    queueMicrotask(() => {
      state.drainQueued = false;
      this.drain(state);
    });
  }

  drain(state) {
    // Tasks start synchronously here but only settle on later ticks, so their
    // release() cannot re-enter this loop; it merely requests the next drain.
    // An explicit early lease.release() (redirect hops) can however fire inside
    // a task that resolved immediately, so track that and drain again after.
    state.draining = true;
    let releasedDuringDrain = false;
    try {
      while (this.activeCount(state) < this.perSiteBudget) {
        const entry = this.pickNext(state);
        if (!entry) break;
        const abortError = abortReason(entry.signal);
        if (abortError) {
          this.finishQueued(state, entry, abortError);
          continue;
        }
        state.totalScheduled += 1;
        state.activeFeeds.add(entry.feedId);
        entry.lease = {
          host: state.host,
          released: false,
          // Optional early release (e.g. after a redirect hop) that frees the
          // slot before the whole run() promise settles.
          release: () => {
            if (entry.lease && !entry.lease.released) {
              if (state.draining) releasedDuringDrain = true;
              this.release(state, entry);
            }
          }
        };
        Promise.resolve()
          .then(() => entry.task(entry.lease))
          .then(
            (value) => this.finishActive(state, entry, { ok: true, value }),
            (error) => this.finishActive(state, entry, { ok: false, error })
          );
      }
    } finally {
      state.draining = false;
    }
    if (releasedDuringDrain) this.pump(state);
  }

  // Round-robin over feeds: search starting just after the feed that received
  // the previous slot, wrapping around, so one feed's backlog cannot claim
  // every slot and starve its peers.
  pickNext(state) {
    const order = state.feedOrder;
    if (order.length === 0) return null;
    const lastIndex = order.indexOf(state.lastServed);
    const start = lastIndex === -1 ? 0 : (lastIndex + 1) % order.length;
    for (let offset = 0; offset < order.length; offset += 1) {
      const feedId = order[(start + offset) % order.length];
      const entry = state.entries.find((candidate) =>
        candidate.feedId === feedId && !candidate.lease && !candidate.settled);
      if (entry) {
        state.lastServed = feedId;
        return entry;
      }
    }
    return null;
  }

  finishQueued(state, entry, error) {
    entry.settled = true;
    entry.reject(error);
    this.compact(state);
    this.pump(state);
  }

  finishActive(state, entry, outcome) {
    // Release the slot on settle; an explicit early lease.release() is
    // idempotent and will not double-count.
    this.release(state, entry);
    if (entry.settled) return;
    entry.settled = true;
    if (outcome.ok) entry.resolve(outcome.value);
    else entry.reject(outcome.error);
  }

  release(state, entry) {
    if (!entry.lease || entry.lease.released) return;
    entry.lease.released = true;
    if (!state.entries.some((candidate) => candidate.feedId === entry.feedId && candidate.lease && !candidate.lease.released)) {
      state.activeFeeds.delete(entry.feedId);
    }
    this.compact(state);
    this.pump(state);
  }

  compact(state) {
    state.entries = state.entries.filter((entry) => !entry.settled);
    const liveFeeds = new Set(state.entries.map((entry) => entry.feedId));
    state.feedOrder = state.feedOrder.filter((feedId) => liveFeeds.has(feedId));
    if (!state.feedOrder.includes(state.lastServed)) state.lastServed = null;
  }

  pending(host) {
    const state = this.sites.get(host);
    return state ? state.entries.filter((entry) => !entry.settled).length : 0;
  }

  snapshot() {
    return [...this.sites.values()]
      .filter((state) => state.entries.some((entry) => !entry.settled))
      .map((state) => {
        const active = this.activeCount(state);
        return {
          host: state.host,
          budget: this.perSiteBudget,
          active,
          queued: state.entries.length - active,
          maxQueued: this.maxQueuedPerSite,
          feedOrder: [...state.feedOrder],
          totalScheduled: state.totalScheduled,
          totalRejected: state.totalRejected,
          entries: state.entries
            .filter((entry) => !entry.lease && !entry.settled)
            .map((entry) => ({
              id: entry.id,
              feedId: entry.feedId,
              reason: entry.reason,
              waitedMs: Math.max(0, this.now() - entry.queuedAt)
            }))
        };
      });
  }
}

function abortReason(signal) {
  if (!signal?.aborted) return null;
  return queueError("queued_cancelled", "refresh was cancelled while waiting for a site budget slot");
}

function queueError(code, message) {
  return Object.assign(new Error(message), { code });
}

function boundedInteger(value, minimum, maximum, name) {
  if (!Number.isInteger(value) || value < minimum || value > maximum) {
    throw new RangeError(`${name} must be between ${minimum} and ${maximum}`);
  }
  return value;
}
