/**
 * PM2: API + worker processes (queue producer vs background consumer).
 *
 * Запуск: cd /path/to/app && pm2 start ecosystem.config.cjs
 * После правок: pm2 reload ecosystem.config.cjs && pm2 save
 */
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
      max_memory_restart: "1500M",
      env: {
        NODE_ENV: "production",
        SERVER_ROLE: "api",
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
      max_memory_restart: "1500M",
      env: {
        NODE_ENV: "production",
        SERVER_ROLE: "worker",
      },
    },
  ],
};
