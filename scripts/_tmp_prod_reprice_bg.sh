#!/bin/bash
set -e
cd /var/www/davidsklad/davidskladik

APP_PASS=$(grep '^APP_PASSWORD=' .env | cut -d= -f2-)
APP_USER=$(grep '^APP_USER=' .env | cut -d= -f2-)

echo "Logging in as $APP_USER..."
curl -s -c /tmp/reprice_cookies.txt \
  -X POST http://127.0.0.1:3000/api/login \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$APP_USER\",\"password\":\"$APP_PASS\"}" \
  > /tmp/reprice_login.json
echo "Login response: $(cat /tmp/reprice_login.json | head -c 200)"

echo ""
echo "Starting reprice in background (nohup)..."
nohup curl -s -b /tmp/reprice_cookies.txt \
  -X POST http://127.0.0.1:3000/api/sales-automation/run \
  -H 'Content-Type: application/json' \
  --max-time 600 \
  -d '{"marketplace":"all","force":true,"onlyChanged":false,"limit":50000,"reason":"inna_bug_fix"}' \
  > /tmp/reprice_result.json 2>/tmp/reprice_result.err &

REPRICE_PID=$!
echo "Reprice curl PID: $REPRICE_PID"
echo "Result will be in /tmp/reprice_result.json"
echo "Started OK"
