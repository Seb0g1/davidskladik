/**
 * PM2: фиксированный cwd, чтобы подтягивался ./node_modules рядом с server.js
 * (иначе возможен MODULE_NOT_FOUND при старте не из каталога приложения).
 *
 * Запуск: cd /home/appuser/app && pm2 start ecosystem.config.cjs
 * или после правок: pm2 delete davidsklad && pm2 start ecosystem.config.cjs && pm2 save
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
      max_memory_restart: "1G",
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
