function scheduleAutoSync(delayMs = 10_000) {
  if (autoSyncTimer) clearTimeout(autoSyncTimer);
  autoSyncNextRunAt = new Date(Date.now() + delayMs).toISOString();
  autoSyncTimer = setTimeout(async () => {
    try {
      const settings = await readAppSettings();
      const config = settings.automation || defaultAppSettings().automation;
      if (config.autoSyncEnabled !== false) {
        const cycle = await runAutoSyncCycle("interval");
        if (cycle?.status === "deferred_under_load") {
          logger.info("auto sync deferred under load, retrying soon", { nextRetryMs: 120_000 });
          scheduleAutoSync(120_000);
          return;
        }
      } else {
        logger.info("auto sync skipped: disabled in settings");
      }
      const nextMinutes = Math.max(5, Number(config.autoSyncMinutes || autoSyncMinutes || 30) || 30);
      scheduleAutoSync(nextMinutes * 60 * 1000);
    } catch (error) {
      logger.error("auto sync failed", { detail: error.code || error.message, err: error });
      scheduleAutoSync(Math.max(5, Number(autoSyncMinutes || 30) || 30) * 60 * 1000);
    }
  }, delayMs);
}

async function nextOzonUnarchiveQueueAutoDelayMs(fallbackMs = ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000) {
  try {
    const queue = ozonUnarchiveQueuePublic(await readOzonUnarchiveQueue(), { limit: 5000 });
    if (queue.due > 0) return 1000;
    const nextScheduledMs = nextOzonUnarchiveScheduledRunAt().getTime() - Date.now();
    if (Number.isFinite(nextScheduledMs) && nextScheduledMs > 0) {
      return Math.min(fallbackMs, Math.max(30_000, nextScheduledMs));
    }
    const nextMs = queue.nextRetryAt ? new Date(queue.nextRetryAt).getTime() - Date.now() : 0;
    if (Number.isFinite(nextMs) && nextMs > 0) return Math.min(fallbackMs, Math.max(30_000, nextMs));
  } catch (error) {
    logger.warn("ozon unarchive queue auto delay check failed", { detail: error?.message || String(error) });
  }
  return fallbackMs;
}

async function rescheduleOzonUnarchiveQueueAutoSoon(reason = "queue_update") {
  if (!ozonUnarchiveQueueAutoEnabled) return;
  const delay = await nextOzonUnarchiveQueueAutoDelayMs();
  scheduleOzonUnarchiveQueueAuto(delay);
  logger.info("ozon unarchive queue auto rescheduled", {
    reason,
    nextAutoRunAt: ozonUnarchiveQueueAutoNextRunAt,
  });
}

async function rescheduleYandexUnarchiveQueueAutoSoon(reason = "queue_update") {
  if (!yandexUnarchiveQueueAutoEnabled) return;
  scheduleYandexUnarchiveQueueAuto(Math.min(yandexUnarchiveQueueAutoIntervalMinutes, 5) * 60 * 1000);
  logger.info("yandex unarchive queue auto rescheduled", {
    reason,
    nextAutoRunAt: yandexUnarchiveQueueAutoNextRunAt,
  });
}

function scheduleYandexUnarchiveQueueAuto(delayMs = yandexUnarchiveQueueAutoIntervalMinutes * 60 * 1000) {
  if (!yandexUnarchiveQueueAutoEnabled) {
    yandexUnarchiveQueueAutoNextRunAt = null;
    return;
  }
  if (yandexUnarchiveQueueAutoTimer) clearTimeout(yandexUnarchiveQueueAutoTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs || 0) || yandexUnarchiveQueueAutoIntervalMinutes * 60 * 1000);
  yandexUnarchiveQueueAutoNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  yandexUnarchiveQueueAutoTimer = setTimeout(async () => {
    try {
      const accepted = enqueueYandexUnarchiveQueueProcess({
        source: "yandex_unarchive_queue_auto",
        limit: yandexUnarchiveQueueBatchLimit,
      });
      if (!accepted) logger.info("yandex unarchive queue auto skipped", { reason: "already_running_or_queued" });
    } catch (error) {
      logger.warn("yandex unarchive queue auto tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleYandexUnarchiveQueueAuto(yandexUnarchiveQueueAutoIntervalMinutes * 60 * 1000);
    }
  }, normalizedDelay);
}

function touchAutoPricePushHeartbeat(meta = {}) {
  autoPricePushLastHeartbeatAt = Date.now();
  logger.info("auto_price_push_heartbeat", {
    ...meta,
    at: new Date().toISOString(),
  });
}

async function maybeUnstickStalledAutoPricePushJobs() {
  if (!marketplaceQueue || !marketplaceWorker) return;
  const waiting = await marketplaceQueue.getWaitingCount().catch(() => 0);
  if (!waiting) return;
  const activeJobs = await marketplaceQueue.getJobs(["active"], 0, 10).catch(() => []);
  const activeReprice = activeJobs.filter((job) => job.name === "auto-price-push");
  if (!activeReprice.length) return;
  const staleMs = autoPricePushLastHeartbeatAt
    ? Date.now() - autoPricePushLastHeartbeatAt
    : autoPricePushStallGuardMs + 1;
  if (staleMs < autoPricePushStallGuardMs) return;
  for (const job of activeReprice) {
    try {
      logger.warn("auto_price_push_stall_guard", {
        jobId: job.id,
        staleMs,
        waiting,
        reason: job.data?.reason || "auto-price-push",
        scope: Array.isArray(job.data?.productIds) ? job.data.productIds.length : "all",
      });
      await job.moveToFailed(new Error("auto_price_push_stall_guard"), job.token, false);
    } catch (error) {
      logger.warn("auto_price_push_stall_guard_failed", {
        jobId: job.id,
        detail: error?.message || String(error),
      });
    }
  }
}

function startAutoPricePushStallGuard() {
  if (autoPricePushStallGuardTimer || (!isWorkerServer && !isMonolithServer)) return;
  if (!bullmqEnabled || !redisUrl || !backgroundJobsEnabled) return;
  autoPricePushStallGuardTimer = setInterval(() => {
    void maybeUnstickStalledAutoPricePushJobs();
  }, 60_000);
  autoPricePushStallGuardTimer.unref?.();
}

function scheduleOzonUnarchiveQueueAuto(delayMs = ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000) {
  if (!ozonUnarchiveQueueAutoEnabled) {
    ozonUnarchiveQueueAutoNextRunAt = null;
    logger.info("ozon unarchive queue auto disabled");
    return;
  }
  if (ozonUnarchiveQueueAutoTimer) clearTimeout(ozonUnarchiveQueueAutoTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs || 0) || ozonUnarchiveQueueAutoIntervalMinutes * 60 * 1000);
  ozonUnarchiveQueueAutoNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  ozonUnarchiveQueueAutoTimer = setTimeout(async () => {
    try {
      const accepted = enqueueOzonUnarchiveQueueProcess({
        source: "ozon_unarchive_queue_auto",
        limit: ozonUnarchiveQueueBatchLimit,
        force: false,
      });
      if (!accepted) logger.info("ozon unarchive queue auto skipped", { reason: "already_running_or_queued" });
    } catch (error) {
      logger.warn("ozon unarchive queue auto tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleOzonUnarchiveQueueAuto(await nextOzonUnarchiveQueueAutoDelayMs());
    }
  }, normalizedDelay);
}
