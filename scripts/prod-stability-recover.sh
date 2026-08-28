#!/usr/bin/env bash
# Восстановление после ребута. Запускать сразу: bash scripts/prod-stability-recover.sh
set -euo pipefail
cd /var/www/davidsklad/davidskladik

pm2 delete all 2>/dev/null || true

if [ -f ecosystem.config.cjs ]; then
  pm2 start ecosystem.config.cjs --only davidsklad
else
  pm2 start server.js --name davidsklad --node-args='--max-old-space-size=5120'
fi

for kv in \
  'WAREHOUSE_WARM_ON_STARTUP=false' \
  'WAREHOUSE_FULL_MEMORY_LOAD_ENABLED=false' \
  'WAREHOUSE_SUMMARY_WARM_ENABLED=false' \
  'WAREHOUSE_FULL_SUMMARY_ENABLED=false' \
  'BACKGROUND_JOBS_ENABLED=false' \
  'AUTO_SYNC_INITIAL_DELAY_SECONDS=3600' \
  'NODE_OPTIONS=--max-old-space-size=5120' \
  'WAREHOUSE_IN_MEMORY_PAGE_MAX=5000' \
  'WAREHOUSE_FAST_PAGE_CACHE_MAX=30'; do
  key="${kv%%=*}"
  if grep -q "^${key}=" .env 2>/dev/null; then
    sed -i "s|^${key}=.*|${kv}|" .env
  else
    echo "$kv" >> .env
  fi
done

pm2 restart davidsklad --update-env
pm2 save
sleep 10
echo "=== PM2 ==="
pm2 list
echo "=== MEM ==="
free -h | head -2
echo "=== HEALTH ==="
curl -sS -m 15 http://127.0.0.1:3000/health | head -c 400 || true
echo
