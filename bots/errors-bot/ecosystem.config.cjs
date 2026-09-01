module.exports = {
  apps: [{
    name: 'errors-bot',
    script: 'index.js',
    env_production: { NODE_ENV: 'production' },
    max_memory_restart: '256M',
    restart_delay: 5000,
    log_file: './logs/combined.log',
    error_file: './logs/error.log',
    time: true,
  }]
};
