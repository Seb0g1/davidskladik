function marketplaceJobId(name, data = {}) {
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
    const priceJobs = jobs
      .filter((job) => job && job.name === "auto-price-push")
      .sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
    const keep = new Set();
    for (const job of priceJobs) {
      const age = now - Number(job.timestamp || 0);
      if (age <= marketplaceQueuePriceJobMaxAgeMs && keep.size < marketplaceQueueMaxPriceBacklog) {
        keep.add(job.id);
        continue;
      }
      await job.remove().catch(() => {});
      removed += 1;
    }
    if (removed > 0) {
      logger.info("marketplace queue stale price jobs cleaned", {
        total: priceJobs.length,
        kept: keep.size,
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

function queueMarketplaceJob(name, data = {}, { priority = 5 } = {}) {
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
    void cleanStaleMarketplacePriceJobs()
      .catch(() => {})
      .then(() => {
        if (marketplaceWorker) marketplaceWorker.run().catch((error) => {
          logger.warn("marketplace worker run failed", { detail: error?.message || String(error) });
        });
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

