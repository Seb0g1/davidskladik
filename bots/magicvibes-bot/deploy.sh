#!/bin/bash
set -e

SERVER=5.129.238.210
REMOTE_DIR=/opt/bots/magicvibes-bot
BOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Deploying magicvibes-bot to $SERVER..."

# Sync files (exclude node_modules, logs, data, .env)
rsync -avz --delete \
  --exclude node_modules \
  --exclude logs \
  --exclude data \
  --exclude .env \
  "$BOT_DIR/" "root@$SERVER:$REMOTE_DIR/"

# Install deps + restart
ssh root@$SERVER bash << 'ENDSSH'
  set -e
  cd /opt/bots/magicvibes-bot
  npm install --production --silent
  pm2 restart magicvibes-bot 2>/dev/null || pm2 start ecosystem.config.cjs --env production
  pm2 save
  echo "==> magicvibes-bot deployed OK"
ENDSSH
