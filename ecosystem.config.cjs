/**
 * PM2 production config (16 GB RAM) — api + worker + health-watchdog.
 *
 * Запуск: cd /var/www/davidsklad/davidskladik && pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker,davidsklad-health-watchdog
 * REDIS_URL и секреты берутся из .env на сервере.
 */
const sharedStabilityEnv = {
  // glibc malloc grows up to 8 arenas per core (64 on this 8-core box), 64MB each;
  // Prisma's threaded engine pushes big JSONB rows through them and freed memory never
  // returns to the OS — the api idled at ~3.5GB RSS with a 34MB JS heap and pm2 kept
  // killing it against max_memory_restart. Two arenas cap that native bloat.
  MALLOC_ARENA_MAX: "2",
  WAREHOUSE_WARM_ON_STARTUP: "false",
  WAREHOUSE_FULL_MEMORY_LOAD_ENABLED: "false",
  WAREHOUSE_SUMMARY_WARM_ENABLED: "false",
  WAREHOUSE_FULL_SUMMARY_ENABLED: "false",
  WAREHOUSE_GROUP_COUNT_WARM_ENABLED: "false",
  WAREHOUSE_GROUP_COUNT_WAIT_MS: "0",
  WAREHOUSE_PAGE_CACHE_TTL_MS: "45000",
  WAREHOUSE_FAST_PAGE_CACHE_MS: "45000",
  WAREHOUSE_PAGE_BUILD_MAX_CONCURRENT: "2",
  HTTP_LOAD_ACTIVE_REQUEST_THRESHOLD: "4",
  SERVER_HEAP_PRESSURE_RATIO: "0.88",
};

const apiOnlyEnv = {
  ...sharedStabilityEnv,
  SERVER_ROLE: "api",
  NODE_OPTIONS: "--max-old-space-size=3072",
  BACKGROUND_JOBS_ENABLED: "false",
  BULLMQ_ENABLED: "true",
  AUTO_SYNC_INITIAL_DELAY_SECONDS: "3600",
  AUTO_SYNC_MINUTES: "180",
  YANDEX_STOCK_CAMPAIGN_IDS: "128820967",
  DAILY_FULL_IMPORT_ENABLED: "false",
  DAILY_SYNC_TIME: "23:00",
  DAILY_SYNC_SEND_PRICES: "true",
  MARKETPLACE_MAINTENANCE_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_PM_SYNC_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_PAIR_BACKFILL_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_AUTOMATION_ENABLED: "false",
  OZON_UNARCHIVE_QUEUE_AUTO_ENABLED: "false",
  PRICE_RETRY_AUTO_ENABLED: "false",
};

const workerOnlyEnv = {
  ...sharedStabilityEnv,
  SERVER_ROLE: "worker",
  // 5120 → 3072: GC triggers at ~2.7GB instead of ~4.5GB; pauses drop from 11s to ~3s.
  // warehouse-write + auto-price-push batches + WB sync together pushed heap to 4GB+
  // causing stop-the-world full GC that pinned the event loop and stalled BullMQ workers.
  NODE_OPTIONS: "--max-old-space-size=3072",
  WORKER_HEALTH_PORT: "3001",
  BACKGROUND_JOBS_ENABLED: "true",
  BULLMQ_ENABLED: "true",
  BULLMQ_WORKER_CONCURRENCY: "3",
  AUTO_SYNC_MINUTES: "10",
  AUTO_SYNC_INITIAL_DELAY_SECONDS: "120",
  AUTO_SYNC_STOCK_RECONCILE_ENABLED: "true",
  AUTO_SYNC_STOCK_RECONCILE_MAX_PRODUCTS: "15000",
  YANDEX_STOCK_CAMPAIGN_IDS: "128820967",
  DAILY_FULL_IMPORT_ENABLED: "false",
  DAILY_SYNC_TIME: "23:00",
  DAILY_SYNC_SEND_PRICES: "true",
  PRICE_RETRY_AUTO_ENABLED: "true",
  OZON_UNARCHIVE_QUEUE_AUTO_ENABLED: "true",
  YANDEX_UNARCHIVE_QUEUE_AUTO_ENABLED: "true",
  AUTO_RESTORE_ON_SUPPLIER_RETURN: "true",
  LINKED_RECONCILER_ENABLED: "true",
  LINKED_RECONCILER_BATCH_SIZE: "150",
  LINKED_RECONCILER_MAX_PRODUCTS_PER_TICK: "600",
  LINKED_RECONCILER_INTERVAL_MINUTES: "5",
  LINKED_RECONCILER_INITIAL_DELAY_SECONDS: "120",
  LINKED_RECONCILER_SEND_TARGET_STOCK: "true",
  MARKETPLACE_MAINTENANCE_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_PM_SYNC_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_PAIR_BACKFILL_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_AUTOMATION_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_HOURS: "3",
  MARKETPLACE_MAINTENANCE_MIN_UPTIME_SEC: "900",
  MARKETPLACE_MAINTENANCE_INITIAL_DELAY_MS: "900000",
  MARKETPLACE_MAINTENANCE_DEFER_RETRY_MS: "1800000",
  // Полная цепочка «новый товар Ozon → Yandex/Avito» не должна превышать ~3 ч:
  // discovery новых офферов Ozon — каждый час (дефолт), дальше импорт на Яндекс
  // и синк фида Avito каждые 3 часа вместо дефолтных 6/4.
  OZON_YANDEX_AUTO_IMPORT_INTERVAL_HOURS: "3",
  AVITO_AUTO_SYNC_HOURS: "3",
  OZON_PRICE_BATCH_SIZE: "500",
  OZON_PRICE_BATCH_DELAY_MS: "300",
  // 200 → 100: halves per-job memory footprint for auto-price-push jobs,
  // reducing concurrent heap pressure when multiple jobs run alongside warehouse write.
  AUTHORITATIVE_REPRICE_BATCH_SIZE: "100",
};

module.exports = {
  apps: [
    {
      name: "davidsklad-api",
      script: "api-entry.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      // glibc retains ~3GB of freed native memory from big Prisma result sets even with
      // MALLOC_ARENA_MAX=2 (it only trims the top of the heap). Steady-state RSS is
      // ~3.4GB + request spikes ~0.7GB; a 4096M limit sat inside that band and pm2
      // kill-looped the api every 30s. 5120M clears the band with RAM to spare
      // (16GB box; worker peaks well under its 6144M limit).
      max_memory_restart: "5120M",
      kill_timeout: 15000,
      env: {
        NODE_ENV: "production",
        ...apiOnlyEnv,
      },
    },
    {
      name: "davidsklad-worker",
      script: "worker-entry.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "6144M",
      kill_timeout: 15000,
      env: {
        NODE_ENV: "production",
        ...workerOnlyEnv,
      },
    },
    {
      // PLAN-HARDENING.md 2.1: polls /health on the api + worker ports every
      // HEALTH_WATCHDOG_INTERVAL_MS (default 45s) and `pm2 restart`s a process that
      // fails HEALTH_WATCHDOG_FAILURE_THRESHOLD (default 3) checks in a row — pm2 alone
      // does not restart a process whose event loop is pinned (status stays "online").
      name: "davidsklad-health-watchdog",
      script: "scripts/health-watchdog.cjs",
      args: "--loop",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "256M",
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
