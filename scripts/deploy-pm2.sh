#!/usr/bin/env bash
# Деплой на сервере (Linux): обновление кода, зависимости, pm2.
# Использование: из каталога приложения или задать APP_DIR:
#   APP_DIR=/home/appuser/app ./scripts/deploy-pm2.sh
set -euo pipefail

APP_DIR="${APP_DIR:-/home/appuser/app}"
PM2_NAME="${PM2_NAME:-davidsklad}"

cd "$APP_DIR"

echo "==> $(date -Iseconds) deploy in $APP_DIR"

echo "==> git pull origin main"
git pull origin main

echo "==> npm ci --omit=dev"
npm ci --omit=dev

echo "==> pm2 restart or start ($PM2_NAME)"
if pm2 describe "$PM2_NAME" &>/dev/null; then
  pm2 restart "$PM2_NAME"
else
  pm2 start server.js --name "$PM2_NAME"
fi

echo "==> pm2 save"
pm2 save

echo "==> pm2 logs ($PM2_NAME, last 80 lines)"
pm2 logs "$PM2_NAME" --lines 80
