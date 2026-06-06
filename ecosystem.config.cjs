/**
 * PM2 production config (15 GB RAM).
 *
 * Запуск: cd /var/www/davidsklad/davidskladik && pm2 start ecosystem.config.cjs --only davidsklad
 * Не используйте api+worker на 15 GB — удвоит RAM.
 */
module.exports = {
  apps: [
    {
      name: "davidsklad",
      script: "server.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "7000M",
      env: {
        NODE_ENV: "production",
        NODE_OPTIONS: "--max-old-space-size=5120",
        WAREHOUSE_WARM_ON_STARTUP: "false",
        WAREHOUSE_FULL_MEMORY_LOAD_ENABLED: "false",
        WAREHOUSE_SUMMARY_WARM_ENABLED: "false",
        WAREHOUSE_FULL_SUMMARY_ENABLED: "false",
        BACKGROUND_JOBS_ENABLED: "false",
        AUTO_SYNC_INITIAL_DELAY_SECONDS: "3600",
      },
    },
    {
      name: "davidsklad-api",
      script: "api-entry.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "4500M",
      env: {
        NODE_ENV: "production",
        SERVER_ROLE: "api",
        NODE_OPTIONS: "--max-old-space-size=4096",
        WAREHOUSE_WARM_ON_STARTUP: "false",
        WAREHOUSE_FULL_MEMORY_LOAD_ENABLED: "false",
        BACKGROUND_JOBS_ENABLED: "false",
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
      max_memory_restart: "6500M",
      env: {
        NODE_ENV: "production",
        SERVER_ROLE: "worker",
        NODE_OPTIONS: "--max-old-space-size=6144",
        WAREHOUSE_WARM_ON_STARTUP: "false",
        WAREHOUSE_FULL_MEMORY_LOAD_ENABLED: "false",
        BACKGROUND_JOBS_ENABLED: "true",
      },
    },
  ],
};
