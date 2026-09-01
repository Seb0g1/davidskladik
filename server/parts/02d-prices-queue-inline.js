function marketplaceJobId(name, data = {}) {
  // Each generation trigger must have a unique job ID so that a previously
  // failed/stalled entry with the same hash does not block retries.
  if (name === "supplier-cart-generate") {
    return `supplier-cart-gen-${crypto.randomUUID()}`;
  }
  if (name === "ozon-unarchive-queue-process") {
    const source = cleanText(data?.source || "ozon_unarchive_queue_auto");
    const runId = cleanText(data?.queueRunId || data?.runId || crypto.randomUUID());
    const force = data?.force === true ? "force" : "normal";
    const limit = Math.max(1, Math.round(Number(data?.limit || ozonUnarchiveQueueBatchLimit) || ozonUnarchiveQueueBatchLimit));
    return crypto
      .createHash("sha1")
      .update(`${name}|${source}|${force}|${limit}|${runId}`)
      .digest("hex");
  }
  const productIds = Array.isArray(data?.productIds)
    ? data.productIds.map((id) => String(id)).filter(Boolean).sort()
    : [];
  const scope = productIds.length ? productIds.join("|") : "all";
  const intent = cleanText(data?.priceIntentId || "");
  const eventKey = intent || [
    cleanText(data?.reason || ""),
    cleanText(data?.sourceEvent || ""),
    data?.force === true ? "force" : "normal",
    data?.livePriceMaster === true ? "live" : "snapshot",
    data?.marketplace || "all",
  ].join("|");
  return crypto
    .createHash("sha1")
    .update(`${name}|${scope}|${eventKey}`)
    .digest("hex");
}

function marketplaceJobsShouldRunInline(name = "") {
  if (isApiServer) return false;
  if (marketplaceQueue && marketplaceJobsCanEnqueue()) return false;
  if (process.env.DISABLE_BACKGROUND_JOBS === "true" || !backgroundJobsEnabled) {
    return name === "auto-price-push"
      || name === "ozon-unarchive-queue-process"
      || name === "no-supplier-automation"
      || name === "supplier-recovery-automation";
  }
  if (name === "auto-price-push" && !marketplaceQueueAutoPricePushEnabled) return true;
  return !marketplaceQueue;
}

// Queue hygiene: the price queue can silently grow to thousands of stale/duplicate
// auto-price-push jobs (each link save queues force jobs, reconciler queues batches).
// A huge backlog makes the worker grind at 100% CPU right after startup and stalls
// every other automation. Cap the backlog and drop jobs that are too old to matter.
const marketplaceQueueMaxPriceBacklog = Math.max(50, Number(process.env.MARKETPLACE_QUEUE_MAX_PRICE_BACKLOG || 400));
const marketplaceQueuePriceJobMaxAgeMs = Math.max(5 * 60_000, Number(process.env.MARKETPLACE_QUEUE_PRICE_JOB_MAX_AGE_MS || 2 * 60 * 60_000));
let marketplaceQueueBacklogCache = { count: 0, checkedAt: 0 };

async function cleanStaleMarketplacePriceJobs() {
  if (!marketplaceQueue) return { removed: 0 };
  const now = Date.now();
  let removed = 0;
  try {
    const states = ["prioritized", "waiting", "delayed"];
    const jobs = await marketplaceQueue.getJobs(states, 0, 20000);
    // Per-type caps: stale duplicates of these jobs pile up by the hundreds and starve
    // everything else; the schedulers re-cover their work anyway.
    const caps = {
      "auto-price-push": marketplaceQueueMaxPriceBacklog,
      "supplier-recovery-automation": 100,
      "ozon-unarchive-queue-process": 20,
      "no-supplier-automation": 100,
    };
    for (const [name, cap] of Object.entries(caps)) {
      const typeJobs = jobs
        .filter((job) => job && job.name === name)
        .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
      let kept = 0;
      for (const job of typeJobs) {
        const age = now - Number(job.timestamp || 0);
        if (age <= marketplaceQueuePriceJobMaxAgeMs && kept < cap) {
          kept += 1;
          continue;
        }
        await job.remove().catch(() => {});
        removed += 1;
      }
    }
    if (removed > 0) {
      logger.info("marketplace queue stale price jobs cleaned", {
        total: jobs.length,
        removed,
        maxBacklog: marketplaceQueueMaxPriceBacklog,
        maxAgeMs: marketplaceQueuePriceJobMaxAgeMs,
      });
    }
  } catch (error) {
    logger.warn("marketplace queue stale price jobs cleanup failed", { detail: error?.message || String(error) });
  }
  return { removed };
}

async function marketplaceQueuePriceBacklogExceeded() {
  if (!marketplaceQueue) return false;
  const now = Date.now();
  if (now - marketplaceQueueBacklogCache.checkedAt < 30_000) {
    return marketplaceQueueBacklogCache.count >= marketplaceQueueMaxPriceBacklog;
  }
  try {
    const counts = await marketplaceQueue.getJobCounts("prioritized", "waiting", "delayed");
    const backlog = (counts.prioritized || 0) + (counts.waiting || 0) + (counts.delayed || 0);
    marketplaceQueueBacklogCache = { count: backlog, checkedAt: now };
    return backlog >= marketplaceQueueMaxPriceBacklog;
  } catch {
    return false;
  }
}

// PLAN-HARDENING.md 1.3: surface how long the oldest pending "auto-price-push" job has
// been waiting. If recovery/unarchive jobs ever starved price pushes despite the
// priority contract (e.g. a misconfigured caller), this metric (and its alert) is the
// signal that catches it.
let marketplaceQueueOldestPriceJobCache = { ageMs: 0, checkedAt: 0 };

async function oldestPendingPriceJobAgeMs() {
  if (!marketplaceQueue) return 0;
  const now = Date.now();
  if (now - marketplaceQueueOldestPriceJobCache.checkedAt < 30_000) {
    return marketplaceQueueOldestPriceJobCache.ageMs;
  }
  try {
    const jobs = await marketplaceQueue.getJobs(["prioritized", "waiting", "delayed"], 0, 20000);
    let oldestAgeMs = 0;
    for (const job of jobs) {
      if (!job || job.name !== "auto-price-push") continue;
      const age = now - Number(job.timestamp || now);
      if (age > oldestAgeMs) oldestAgeMs = age;
    }
    marketplaceQueueOldestPriceJobCache = { ageMs: oldestAgeMs, checkedAt: now };
    return oldestAgeMs;
  } catch {
    return marketplaceQueueOldestPriceJobCache.ageMs;
  }
}

function queueMarketplaceJob(name, data = {}, { priority = QUEUE_PRIORITY.DEFAULT } = {}) {
  if (marketplaceJobsShouldRunInline(name)) {
    return processMarketplaceJob(name, data).catch((error) => {
      logger.warn("inline marketplace job failed", { name, detail: error?.message || String(error) });
      throw error;
    });
  }
  if (process.env.DISABLE_BACKGROUND_JOBS === "true") return Promise.resolve(null);
  if (!marketplaceQueue && !backgroundJobsEnabled) return Promise.resolve(null);
  if (name === "auto-price-push" && !marketplaceQueueAutoPricePushEnabled) {
    return processMarketplaceJob(name, data).catch((error) => {
      logger.warn("inline auto price push failed", { detail: error?.message || String(error) });
      throw error;
    });
  }
  if (marketplaceQueue) {
    const addJob = () => marketplaceQueue.add(name, data, {
      jobId: marketplaceJobId(name, data),
      priority,
      removeOnComplete: name === "auto-price-push" || name === "ozon-unarchive-queue-process" ? true : 2000,
      removeOnFail: 2000,
    });
    if (name === "auto-price-push") {
      // force jobs (recovery/unarchive/verify-retry) must never be dropped by the cap —
      // otherwise recovered products stay with stale marketplace prices.
      if (data?.force === true) {
        return addJob().catch((error) => {
          logger.warn("queue add failed, falling back to inline mode", { name, detail: error?.message || String(error) });
          return processMarketplaceJob(name, data);
        });
      }
      return marketplaceQueuePriceBacklogExceeded().then((exceeded) => {
        if (exceeded) {
          logger.warn("auto price push skipped: queue backlog limit", {
            backlog: marketplaceQueueBacklogCache.count,
            limit: marketplaceQueueMaxPriceBacklog,
            reason: cleanText(data?.reason || ""),
          });
          return null;
        }
        return addJob();
      }).catch((error) => {
        logger.warn("queue add failed, falling back to inline mode", { name, detail: error?.message || String(error) });
        return processMarketplaceJob(name, data);
      });
    }
    return addJob().catch((error) => {
      logger.warn("queue add failed, falling back to inline mode", { name, detail: error?.message || String(error) });
      return processMarketplaceJob(name, data);
    });
  }
  return processMarketplaceJob(name, data).catch((error) => {
    logger.warn("inline marketplace job failed", { name, detail: error?.message || String(error) });
    throw error;
  });
}

function initMarketplaceQueue() {
  if (!bullmqEnabled || !redisUrl) {
    logger.info("marketplace queue disabled, using inline mode", { serverRole });
    return;
  }
  try {
    const connection = { url: redisUrl, maxRetriesPerRequest: null };
    marketplaceQueue = new Queue("marketplace-tasks", { connection });
    if (isApiServer) {
      logger.info("marketplace queue producer ready", { serverRole });
      return;
    }
    if (!backgroundJobsEnabled) {
      logger.info("marketplace worker disabled until BACKGROUND_JOBS_ENABLED=true", { serverRole });
      return;
    }
    marketplaceWorker = new Worker(
      "marketplace-tasks",
      async (job) => processMarketplaceJob(job.name, job.data || {}),
      {
        connection,
        autorun: false,
        concurrency: bullmqWorkerConcurrency,
        lockDuration: bullmqLockDurationMs,
        lockRenewTime: Math.floor(bullmqLockDurationMs / 2),
        stalledInterval: bullmqStalledIntervalMs,
        maxStalledCount: bullmqMaxStalledCount,
      },
    );
    // Clean the stale price-job backlog BEFORE the worker starts consuming, otherwise a
    // multi-thousand-job backlog pins the event loop at 100% CPU right after startup.
    // Also remove accumulated failed jobs (>24 h) and completed jobs (>7 d) to keep Redis lean.
    void cleanStaleMarketplacePriceJobs()
      .catch(() => {})
      .then(() => Promise.all([
        marketplaceQueue.clean(24 * 60 * 60 * 1000, 500, "failed").catch(() => {}),
        marketplaceQueue.clean(7 * 24 * 60 * 60 * 1000, 200, "completed").catch(() => {}),
      ]))
      .then(() => {
        if (marketplaceWorker) marketplaceWorker.run().catch((error) => {
          logger.warn("marketplace worker run failed", { detail: error?.message || String(error) });
        });
        // Periodic nightly cleanup so failed jobs never accumulate beyond a day
        setInterval(() => {
          if (marketplaceQueue) {
            marketplaceQueue.clean(24 * 60 * 60 * 1000, 500, "failed").catch(() => {});
            marketplaceQueue.clean(7 * 24 * 60 * 60 * 1000, 200, "completed").catch(() => {});
          }
        }, 24 * 60 * 60 * 1000);
      });
    marketplaceWorker.on("failed", (job, error) => {
      logger.warn("marketplace job failed", { job: job?.name, detail: error?.message || String(error) });
      if (job?.name === "auto-price-push") {
        immediateAutoPushRunning = false;
        if (hasPendingImmediateAutoPricePush()) scheduleImmediateAutoPricePushFlush(immediateAutoPushFollowupDelayMs);
      }
    });
    marketplaceWorker.on("completed", (job, result) => {
      if (job?.name === "auto-price-push") {
        if (result && typeof result === "object") {
          logger.info("immediate auto price push complete", {
            reason: job.data?.reason || "bullmq",
            scope: Array.isArray(job.data?.productIds) ? job.data.productIds.length : "all",
            sent: result.sent || 0,
            failed: result.failed || 0,
            stockSent: result.stockSent || 0,
            stockFailed: result.stockFailed || 0,
            ozonSent: result.ozonSent || 0,
            ozonFailed: result.ozonFailed || 0,
            ozonSkipped: result.ozonSkipped || 0,
            yandexSent: result.yandexSent || 0,
            yandexFailed: result.yandexFailed || 0,
            yandexSkipped: result.yandexSkipped || 0,
            skipped: Array.isArray(result.skipped) ? result.skipped.length : 0,
          });
        }
        immediateAutoPushRunning = false;
        if (hasPendingImmediateAutoPricePush()) scheduleImmediateAutoPricePushFlush(immediateAutoPushFollowupDelayMs);
        return;
      }
      logger.info("marketplace job complete", { job: job?.name || "unknown" });
    });
    marketplaceWorker.on("error", (error) => {
      logger.warn("marketplace worker error", { detail: error?.message || String(error) });
    });
    // Heartbeat консьюмера в Redis: health на api-процессе читает его вместо
    // флапающего getWorkers() (CLIENT LIST) — иначе шапка показывает ложное
    // «обработчик недоступен» при живом воркере.
    const heartbeatTimer = setInterval(() => {
      if (!marketplaceQueue || !marketplaceWorker) return;
      void marketplaceQueue.client
        .then((client) => client.set(marketplaceWorkerHeartbeatKey, new Date().toISOString(), "EX", 120))
        .catch(() => {});
    }, 30_000);
    heartbeatTimer.unref?.();
    marketplaceQueue.on("error", (error) => {
      logger.warn("marketplace queue error", { detail: error?.message || String(error) });
    });
    logger.info("marketplace queue enabled", {
      mode: "bullmq",
      concurrency: bullmqWorkerConcurrency,
      lockDurationMs: bullmqLockDurationMs,
      stalledIntervalMs: bullmqStalledIntervalMs,
    });
    startAutoPricePushStallGuard();
  } catch (error) {
    marketplaceQueue = null;
    marketplaceWorker = null;
    logger.warn("marketplace queue init failed, fallback to inline mode", { detail: error?.message || String(error) });
  }
}

