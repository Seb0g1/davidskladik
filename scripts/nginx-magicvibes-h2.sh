#!/usr/bin/env bash
# magicvibes.ru nginx: HTTP/2 (the site was on HTTP/1.1 — slow with 60–150 requests per page on phones),
# HSTS + security headers, www.magicvibes.ru → magicvibes.ru (one address for search engines).
# Backs up the config, runs `nginx -t`, reloads; on a failed test restores the backup.
# Run from the repo root:  bash scripts/nginx-magicvibes-h2.sh
set -euo pipefail
ssh -i ~/.ssh/davidsklad_deploy -o ConnectTimeout=20 root@81.17.154.153 'set -e
F=/etc/nginx/sites-available/magicvibes; B=/root/nginx-magicvibes-$(date +%Y%m%d-%H%M%S).bak
cp -a $F $B
python3 - "$F" <<"EOF"
import sys
p = sys.argv[1]; s = open(p).read()
if "http2" not in s:
    s = s.replace("    listen 6443 ssl;\n", "    listen 6443 ssl http2;\n", 1)
if "Strict-Transport-Security" not in s:
    s = s.replace("    server_tokens off;\n", """    server_tokens off;

    # one address for search engines: www -> magicvibes.ru
    if ($host = www.magicvibes.ru) { return 301 https://magicvibes.ru$request_uri; }
    add_header Strict-Transport-Security "max-age=15552000" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
""", 1)
s = s.replace("    return 301 https://$host$request_uri;\n", "    return 301 https://magicvibes.ru$request_uri;\n")
open(p, "w").write(s)
EOF
if nginx -t 2>&1 | grep -q "test is successful"; then systemctl reload nginx; echo "nginx reloaded; backup: $B"; else nginx -t || true; cp -a $B $F; echo "nginx test FAILED - config restored from $B"; exit 1; fi'
sleep 2
curl -s -o /dev/null -w "www -> %{http_code} %{redirect_url}\n" https://www.magicvibes.ru/
curl -sI https://magicvibes.ru/ | grep -i strict-transport || echo "HSTS header missing"
