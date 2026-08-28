#!/bin/bash
set -a
source /var/www/davidsklad/davidskladik/.env
set +a
TOKEN_JSON=$(curl -sf -X POST https://api.avito.ru/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${AVITO_CLIENT_ID}&client_secret=${AVITO_CLIENT_SECRET}" 2>/dev/null)
ACCESS=$(echo "$TOKEN_JSON" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('access_token',''))" 2>/dev/null)
TOKEN_LEN=${#ACCESS}
echo "token_len=$TOKEN_LEN"
if [ "$TOKEN_LEN" -lt 10 ]; then
  echo "Failed to get token: $TOKEN_JSON"
  exit 1
fi
echo "=== autoload profile ==="
curl -s -H "Authorization: Bearer $ACCESS" "https://api.avito.ru/autoload/v2/profile"
echo ""
echo "=== last upload info ==="
curl -s -H "Authorization: Bearer $ACCESS" "https://api.avito.ru/autoload/v2/uploads/last_report_info"
echo ""
