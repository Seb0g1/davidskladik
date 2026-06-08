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
  AUTO_SYNC_INITIAL_DELAY_SECONDS: "3600",
  PRICE_RETRY_AUTO_ENABLED: "true",
  OZON_UNARCHIVE_QUEUE_AUTO_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_PM_SYNC_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_PAIR_BACKFILL_ENABLED: "false",
  MARKETPLACE_MAINTENANCE_AUTOMATION_ENABLED: "true",
  MARKETPLACE_MAINTENANCE_MIN_UPTIME_SEC: "3600",
  MARKETPLACE_MAINTENANCE_INITIAL_DELAY_MS: "3600000",
  MARKETPLACE_MAINTENANCE_DEFER_RETRY_MS: "1800000",
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
