/**
 * PM2 production config (16 GB RAM) — api + worker split.
 *
 * Запуск: cd /var/www/davidsklad/davidskladik && pm2 start ecosystem.config.cjs --only davidsklad-api,davidsklad-worker
 * REDIS_URL и секреты берутся из .env на сервере.
 */
const sharedStabilityEnv = {
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
  NODE_OPTIONS: "--max-old-space-size=5120",
  WORKER_HEALTH_PORT: "3001",
  BACKGROUND_JOBS_ENABLED: "true",
  BULLMQ_ENABLED: "true",
  BULLMQ_WORKER_CONCURRENCY: "1",
  AUTO_SYNC_MINUTES: "180",
  AUTO_SYNC_INITIAL_DELAY_SECONDS: "300",
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
  LINKED_RECONCILER_BATCH_SIZE: "80",
  LINKED_RECONCILER_MAX_PRODUCTS_PER_TICK: "160",
  LINKED_RECONCILER_INTERVAL_MINUTES: "5",
  LINKED_RECONCILER_INITIAL_DELAY_SECONDS: "420",
  LINKED_RECONCILER_SEND_TARGET_STOCK: "true",
  MARKETPLACE_MAINTENANCE_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_PM_SYNC_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_PAIR_BACKFILL_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_AUTOMATION_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_HOURS: "3",
  MARKETPLACE_MAINTENANCE_MIN_UPTIME_SEC: "900",
  MARKETPLACE_MAINTENANCE_INITIAL_DELAY_MS: "900000",
  MARKETPLACE_MAINTENANCE_DEFER_RETRY_MS: "1800000",
  OZON_PRICE_BATCH_SIZE: "500",
  OZON_PRICE_BATCH_DELAY_MS: "300",
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
      max_memory_restart: "4096M",
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
  ],
};
