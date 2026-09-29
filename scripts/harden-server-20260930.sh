#!/usr/bin/env bash
# Server hardening 2026-09-30 (magicvibes.ru + davidsklad.ru on 81.17.154.153).
#  1. nginx: PROXY protocol stream → sites, so nginx/Express see real client IPs (everything was 127.0.0.1:
#     per-IP limits were global, consent IPs useless, fail2ban blind);
#     per-IP request/connection limits (DDoS), short client timeouts (slowloris), TLS 1.2+ only, no version.
#     Server itself, localhost and the owner's IP are exempt. Auto-rollback if a site stops answering.
#  2. fail2ban: sshd (password brute force), nginx limit_req floods, recidive.
#  3. x-ui (VPN) stopped; the daily cron line that restarted it → plain nginx reload (certificates).
#  4. Ubuntu security updates.
# Run from the repo root:  bash scripts/harden-server-20260930.sh
set -euo pipefail
OWNER_IP="${OWNER_IP:-185.42.125.116}"
ssh -i ~/.ssh/davidsklad_deploy -o ConnectTimeout=20 root@81.17.154.153 "OWNER_IP=$OWNER_IP bash -s" <<'REMOTE'
set -euo pipefail
TS=$(date +%Y%m%d-%H%M%S)
BK=/root/harden-$TS
mkdir -p $BK
cp -a /etc/nginx $BK/nginx
crontab -l > $BK/crontab.txt 2>/dev/null || true
echo "backup: $BK"

echo "== 1. nginx"
cat > /etc/nginx/conf.d/mv-security.conf <<EOF
# magicvibes / davidsklad: DDoS and abuse limits (scripts/harden-server-20260930.sh)
server_tokens off;
client_header_timeout 15s;
client_body_timeout 20s;
send_timeout 30s;
keepalive_timeout 30s;
large_client_header_buffers 4 16k;

# never limit the server itself (Next.js SSR → API), localhost and the owner
geo \$mv_exempt {
    default 0;
    127.0.0.1 1;
    ::1 1;
    81.17.154.153 1;
    $OWNER_IP 1;
}
map \$mv_exempt \$mv_limit_key { 0 \$binary_remote_addr; 1 ""; }
limit_req_zone \$mv_limit_key zone=mv_page:20m rate=15r/s;
limit_req_zone \$mv_limit_key zone=mv_api:20m rate=25r/s;
limit_conn_zone \$mv_limit_key zone=mv_conn:20m;
limit_req_status 429;
limit_conn_status 429;
limit_req_log_level warn;
EOF

python3 - <<'PY'
import re
def edit(path, fn):
    s = open(path).read(); n = fn(s)
    if n != s: open(path, "w").write(n)

# stream: send the client address to the sites
def stream(s):
    if "proxy_protocol on;" in s: return s
    return s.replace("      ssl_preread     on;", "      ssl_preread     on;\n      proxy_protocol  on;", 1)
edit("/etc/nginx/stream-enabled/stream.conf", stream)

REAL = "\n    # client address from the stream proxy (PROXY protocol)\n    set_real_ip_from 127.0.0.1;\n    real_ip_header proxy_protocol;\n"

def shop(s):
    s = s.replace("    listen 6443 ssl http2;\n", "    listen 6443 ssl http2 proxy_protocol;\n", 1)
    if "real_ip_header" not in s:
        s = s.replace("    server_tokens off;\n", "    server_tokens off;\n" + REAL, 1)
    if "zone=mv_page" not in s:
        # pages and API proxy: 15 r/s per IP, bursts of a page load pass
        s = s.replace("    location / {\n        proxy_pass http://127.0.0.1:3002;",
                      "    location / {\n        limit_req zone=mv_page burst=60 nodelay;\n        limit_conn mv_conn 40;\n        proxy_pass http://127.0.0.1:3002;", 1)
        s = s.replace("    location /_next/image {\n        proxy_pass http://127.0.0.1:3002;",
                      "    location /_next/image {\n        limit_req zone=mv_page burst=120 nodelay;\n        proxy_pass http://127.0.0.1:3002;", 1)
    return s
edit("/etc/nginx/sites-available/magicvibes", shop)

def sklad(s):
    s = s.replace("    listen 9443 ssl http2;\n", "    listen 9443 ssl http2 proxy_protocol;\n", 1)
    if "real_ip_header" not in s:
        s = s.replace("    client_max_body_size 100M;\n", "    client_max_body_size 100M;\n" + REAL, 1)
    if "zone=mv_api" not in s:
        s = s.replace("    location / {\n        proxy_pass http://127.0.0.1:3000;",
                      "    location / {\n        limit_req zone=mv_api burst=150 nodelay;\n        limit_conn mv_conn 60;\n        proxy_pass http://127.0.0.1:3000;", 1)
    return s
edit("/etc/nginx/sites-available/davidsklad.ru", sklad)

# TLS 1.0/1.1 are broken and unused
edit("/etc/nginx/nginx.conf", lambda s: s.replace("ssl_protocols TLSv1 TLSv1.1 TLSv1.2 TLSv1.3;", "ssl_protocols TLSv1.2 TLSv1.3;"))
PY

restore() { echo "!! restoring nginx from $BK"; rm -rf /etc/nginx; cp -a $BK/nginx /etc/nginx; nginx -t && systemctl reload nginx; }
if ! nginx -t 2>/tmp/nginx-t.log; then cat /tmp/nginx-t.log; restore; exit 1; fi
systemctl reload nginx
sleep 3
ok=1
for u in https://magicvibes.ru/ https://davidsklad.ru/health "https://davidsklad.ru/api/shop/catalog?pageSize=1"; do
  c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 "$u"); echo "$c $u"; [ "$c" = 200 ] || ok=0
done
if [ $ok = 0 ]; then restore; exit 1; fi
tail -2 /var/log/nginx/access.log | awk '{print "access log client:", $1}'

echo "== 2. fail2ban"
DEBIAN_FRONTEND=noninteractive apt-get install -y -q fail2ban python3-systemd >/dev/null
cat > /etc/fail2ban/jail.local <<EOF
[DEFAULT]
ignoreip = 127.0.0.1/8 ::1 81.17.154.153 $OWNER_IP
bantime  = 1h
findtime = 10m
maxretry = 5

[sshd]
enabled = true
backend = systemd

[nginx-limit-req]
enabled  = true
port     = http,https
logpath  = /var/log/nginx/error.log
findtime = 1m
maxretry = 40
bantime  = 30m

[recidive]
enabled  = true
bantime  = 1w
findtime = 1d
maxretry = 3
EOF
systemctl enable --now fail2ban >/dev/null 2>&1
systemctl restart fail2ban
sleep 3
fail2ban-client status | sed -n 2,3p

echo "== 3. x-ui"
systemctl stop x-ui 2>/dev/null || true
systemctl disable x-ui 2>/dev/null || true
crontab -l | sed 's#^@daily x-ui restart > /dev/null 2>&1 && nginx -s reload;#@daily nginx -s reload#' | crontab -
crontab -l | grep -n "nginx -s reload"
ss -tlnp | grep -c xray || true

echo "== 4. security updates"
DEBIAN_FRONTEND=noninteractive unattended-upgrade 2>&1 | tail -2 || true
apt list --upgradable 2>/dev/null | grep -ci security || true
echo "done; backup $BK"
REMOTE
