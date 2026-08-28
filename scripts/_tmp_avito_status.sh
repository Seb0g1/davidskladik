#!/bin/bash
set -a
source /var/www/davidsklad/davidskladik/.env
set +a
TOKEN=$(curl -s -X POST https://api.avito.ru/token \
  -d "grant_type=client_credentials&client_id=${AVITO_CLIENT_ID}&client_secret=${AVITO_CLIENT_SECRET}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))")
echo "token_len=${#TOKEN}"
echo "=== last_report_info ==="
curl -s -H "Authorization: Bearer $TOKEN" https://api.avito.ru/autoload/v2/uploads/last_report_info
echo ""
echo "=== profile ==="
curl -s -H "Authorization: Bearer $TOKEN" "https://api.avito.ru/autoload/v2/profile"
