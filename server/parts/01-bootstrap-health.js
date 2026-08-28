function healthTimeout(promise, timeoutMs = 2500) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("health check timeout")), timeoutMs)),
  ]);
}

function cleanBuildVersion(value) {
  return String(value || "").trim().replace(/[^\w.-]/g, "").slice(0, 80) || "dev";
}

function readGitCommit() {
  try {
    return execFileSync("git", ["rev-parse", "--short", "HEAD"], {
      cwd: __dirname,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
  } catch (_error) {
    return "";
  }
}

async function collectHealthDetails({ deep = false } = {}) {
  const requiredPostgresTables = [
    "sales_automation_sku_states",
    "ozon_unarchive_queue",
    "supplier_cart_drafts",
    "supplier_cart_draft_rows",
    "supplier_picking_rows",
    "supplier_blocks",
    "brand_index_items",
    "finance_orders",
    "finance_expenses",
    "supplier_ledger_entries",
  ];
  const components = {
    storage: {
      mode: shouldUsePostgresStorage() ? "postgres" : "json",
      postgresMode: postgresModeEnabled(),
      jsonFallback: jsonFallbackEnabled(),
    },
    postgresTables: {
      required: requiredPostgresTables,
      missing: [],
      ok: shouldUsePostgresStorage() ? null : true,
    },
    postgres: {
      configured: Boolean(cleanText(process.env.DATABASE_URL)),
      enabled: shouldUsePostgresStorage(),
      ok: shouldUsePostgresStorage() ? null : true,
    },
    pricemaster: {
      configured: Boolean(cleanText(process.env.PM_DB_HOST) && cleanText(process.env.PM_DB_NAME)),
      ok: null,
    },
    redis: {
      configured: Boolean(redisUrl),
      enabled: bullmqEnabled,
      queueMode: bullmqEnabled && redisUrl && marketplaceQueue ? "bullmq" : "inline",
      ok: bullmqEnabled ? null : true,
    },
    ozon: {
      configured: getOzonAccounts().length > 0,
      accounts: getOzonAccounts().length,
      warehouseListEnabled: ozonWarehouseListEnabled,
    },
    yandex: {
      configured: getYandexShops().length > 0,
      shops: getYandexShops().length,
    },
    runtime: {
      ok: true,
      pid: process.pid,
      node: process.version,
      uptimeSec: Math.round(process.uptime()),
      memory: (() => {
        const memory = process.memoryUsage();
        return {
          rssMb: Math.round(memory.rss / 1024 / 1024),
          heapUsedMb: Math.round(memory.heapUsed / 1024 / 1024),
          heapTotalMb: Math.round(memory.heapTotal / 1024 / 1024),
          externalMb: Math.round(memory.external / 1024 / 1024),
        };
      })(),
      slowRequestThresholdMs,
      recentSlowRequests: recentSlowRequests.slice(-10),
      slowRequests: summarizeRecentSlowRequests(20),
      stateWarnings: recentStateWarnings.slice(-20),
    },
    automation: {
      pricePush: {
        mode: marketplaceQueue && marketplaceQueueAutoPricePushEnabled ? "bullmq" : "inline",
        running: immediateAutoPushRunning,
        scheduled: Boolean(immediateAutoPushTimer),
        pendingScope: immediateAutoPushAll ? "all" : immediateAutoPushIds.size,
        pendingReasons: Array.from(immediateAutoPushReasons).slice(0, 8),
        delayMs: immediateAutoPushDelayMs,
        followupDelayMs: immediateAutoPushFollowupDelayMs,
      },
    },
  };

  if (components.postgres.enabled && deep) {
    try {
      await healthTimeout(getPrisma().$queryRaw`SELECT 1 AS ok`);
      components.postgres.ok = true;
      const rows = await healthTimeout(getPrisma().$queryRaw`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
      `);
      const present = new Set((Array.isArray(rows) ? rows : []).map((row) => cleanText(row.table_name)));
      components.postgresTables.present = Array.from(present);
      components.postgresTables.missing = requiredPostgresTables.filter((name) => !present.has(name));
      components.postgresTables.ok = components.postgresTables.missing.length === 0;
    } catch (error) {
      components.postgres.ok = false;
      components.postgres.error = error?.message || String(error);
      components.postgresTables.ok = false;
      components.postgresTables.error = error?.message || String(error);
    }
  }

  if (components.pricemaster.configured && deep) {
    try {
      const [rows] = await healthTimeout(pool.query("SELECT VERSION() AS version, NOW() AS serverTime"));
      components.pricemaster.ok = true;
      components.pricemaster.version = rows?.[0]?.version || null;
      components.pricemaster.serverTime = rows?.[0]?.serverTime || null;
    } catch (error) {
      components.pricemaster.ok = false;
      components.pricemaster.error = error?.message || String(error);
    }
  } else if (!components.pricemaster.configured) {
    components.pricemaster.ok = false;
  }

  if (bullmqEnabled && redisUrl && deep) {
    try {
      if (!marketplaceQueue) throw new Error("BullMQ is enabled but queue is not initialized");
      components.redis.counts = await healthTimeout(marketplaceQueue.getJobCounts("waiting", "active", "delayed", "failed"));
      components.redis.ok = true;
      components.redis.jobs = components.redis.counts;
    } catch (error) {
      components.redis.ok = false;
      components.redis.error = error?.message || String(error);
    }
  }

  const liveness = eventLoopHeartbeatStatus();
  const required = [
    components.postgres.enabled ? components.postgres : null,
    components.postgres.enabled ? components.postgresTables : null,
    components.pricemaster.configured ? components.pricemaster : null,
    components.redis.enabled ? components.redis : null,
    { ok: liveness.healthy },
  ].filter(Boolean);
  const ok = required.every((component) => component.ok !== false);
  const memory = process.memoryUsage();
  const heapLimit = serverHeapLimitBytes();
  const bullmq = deep ? await marketplaceQueueCounts().catch((error) => ({
    enabled: bullmqEnabled,
    mode: "bullmq",
    ok: false,
    error: error?.message || String(error),
  })) : null;
  const queue = {
    enabled: bullmqEnabled && Boolean(redisUrl),
    degraded: bullmqEnabled && Boolean(redisUrl) && (!marketplaceQueue || bullmq?.ok === false),
    producerReady: marketplaceJobsCanEnqueue(),
    consumerReady: Boolean(marketplaceWorker) || Number(bullmq?.workers || 0) > 0 || Boolean(bullmq?.consumerHeartbeatAt),
    counts: bullmq?.counts || components.redis?.counts || components.redis?.jobs || null,
    error: bullmq?.error || components.redis?.error || null,
  };
  return {
    ok,
    service: "magic-vibes-warehouse",
    serverRole,
    version: buildVersion,
    time: new Date().toISOString(),
    memory: {
      heapUsedMb: Math.round(memory.heapUsed / 1024 / 1024),
      heapLimitMb: Math.round(heapLimit / 1024 / 1024),
      heapPressureRatio: heapLimit ? memory.heapUsed / heapLimit : 0,
    },
    activeHttpRequests,
    recentSlowRequests: recentSlowRequests.slice(-10),
    liveness,
    bullmq,
    queue,
    components,
  };
}

app.get("/health", async (request, response) => {
  const deep = ["1", "true", "yes"].includes(String(request.query.deep || "").toLowerCase());
  response.json(await collectHealthDetails({ deep }));
});

app.get("/health/ready", async (_request, response) => {
  const startedAt = Date.now();
  try {
    await pool.query("SELECT 1");
    response.json({ ok: true, service: "magic-vibes-warehouse", mysql: "ok", latencyMs: Date.now() - startedAt, time: new Date().toISOString() });
  } catch (error) {
    response.status(503).json({ ok: false, service: "magic-vibes-warehouse", mysql: "fail", detail: error.code || error.message, time: new Date().toISOString() });
  }
});
