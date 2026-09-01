module.exports = {
  apps: [{
    name: 'magicvibes-bot',
    script: 'index.js',
    env_production: { NODE_ENV: 'production' },
    max_memory_restart: '256M',
    restart_delay: 5000,
    error_file: './logs/error.log',
    out_file: './logs/out.log',
  }],
};
