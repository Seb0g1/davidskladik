function backgroundMarketplaceJobsBlocked() {
  return process.env.DISABLE_BACKGROUND_JOBS === "true" || !backgroundJobsEnabled;
}

function marketplaceJobsCanEnqueue() {
  return process.env.DISABLE_BACKGROUND_JOBS !== "true" && Boolean(marketplaceQueue);
}

function serverHeapLimitBytes() {
  const fromEnv = Number(process.env.SERVER_HEAP_LIMIT_BYTES || 0);
  if (Number.isFinite(fromEnv) && fromEnv > 0) return fromEnv;
  const match = String(process.env.NODE_OPTIONS || "").match(/--max-old-space-size=(\d+)/);
  if (match) return Number(match[1]) * 1024 * 1024;
  return 2 * 1024 * 1024 * 1024;
}

function serverHeapPressureRatio() {
  const limit = serverHeapLimitBytes();
  if (!limit) return 0;
  return process.memoryUsage().heapUsed / limit;
}

function serverUnderMemoryPressure() {
  return serverHeapPressureRatio() >= serverHeapPressureRatioLimit;
}

function serverUnderHttpLoad() {
  if (activeHttpRequests >= httpLoadActiveRequestThreshold) return true;
  const cutoff = Date.now() - httpLoadSlowRequestWindowMs;
  return recentSlowRequests.some((entry) => {
    const elapsedMs = Number(entry?.elapsedMs || 0);
    const atMs = Date.parse(entry?.at || "");
    return elapsedMs >= httpLoadSlowRequestThresholdMs && Number.isFinite(atMs) && atMs >= cutoff;
  });
}

function serverHasRecentSlowHttpActivity() {
  const cutoff = Date.now() - marketplaceMaintenanceRecentSlowWindowMs;
  return recentSlowRequests.some((entry) => {
    const elapsedMs = Number(entry?.elapsedMs || 0);
    const atMs = Date.parse(entry?.at || "");
    return elapsedMs >= slowRequestThresholdMs && Number.isFinite(atMs) && atMs >= cutoff;
  });
}

function marketplaceMaintenanceShouldDefer(trigger = "") {
  if (process.uptime() < marketplaceMaintenanceMinUptimeSec) {
    logger.warn("marketplace maintenance deferred: process uptime", {
      trigger: cleanText(trigger) || "maintenance",
      uptimeSec: Math.round(process.uptime()),
      minUptimeSec: marketplaceMaintenanceMinUptimeSec,
    });
    return { defer: true, status: "deferred_uptime" };
  }
  if (activeHttpRequests > 0) {
    logger.warn("marketplace maintenance deferred: active http requests", {
      trigger: cleanText(trigger) || "maintenance",
      activeHttpRequests,
    });
    return { defer: true, status: "deferred_active_http" };
  }
  if (serverHasRecentSlowHttpActivity()) {
    logger.warn("marketplace maintenance deferred: recent slow http", {
      trigger: cleanText(trigger) || "maintenance",
    });
    return { defer: true, status: "deferred_recent_slow_http" };
  }
  if (heavyBackgroundWorkShouldDefer(`marketplace_maintenance:${trigger}`)) {
    return { defer: true, status: "deferred_under_load" };
  }
  return { defer: false, status: "ok" };
}

function heavyBackgroundWorkShouldDefer(reason = "") {
  if (serverUnderMemoryPressure()) {
    logger.warn("heavy background work deferred: memory pressure", {
      reason: cleanText(reason) || "unspecified",
      heapRatio: Number(serverHeapPressureRatio().toFixed(3)),
    });
    return true;
  }
  if (serverUnderHttpLoad()) {
    logger.warn("heavy background work deferred: http load", {
      reason: cleanText(reason) || "unspecified",
      activeHttpRequests,
    });
    return true;
  }
  if (marketplaceMaintenanceRunning || autoSyncRunning || manualWarehouseSyncPromise || dailySyncPromise) {
    logger.warn("heavy background work deferred: other job running", {
      reason: cleanText(reason) || "unspecified",
      marketplaceMaintenanceRunning,
      autoSyncRunning,
      hasDailySyncPromise: Boolean(dailySyncPromise),
      hasManualSyncPromise: Boolean(manualWarehouseSyncPromise),
    });
    return true;
  }
  return false;
}

