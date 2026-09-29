#!/usr/bin/env bash
# Deploy selected shop-next files to magicvibes.ru with a staged build + auto-rollback.
# Usage (repo root, Git Bash):
#   bash scripts/deploy-shop-files.sh app/page.tsx components/X.tsx ...
#   REMOVE="components/Old.tsx" bash scripts/deploy-shop-files.sh ...   # also delete files on the server
set -euo pipefail
[ $# -gt 0 ] || { echo "usage: $0 <shop-next relative files...>"; exit 1; }

HOST=root@81.17.154.153
KEY=~/.ssh/davidsklad_deploy
APP=/var/www/davidsklad/davidskladik
TS=$(date +%Y%m%d-%H%M%S)
SSH="ssh -i $KEY -o ConnectTimeout=20 $HOST"
REMOVE="${REMOVE:-}"
SHOP_FILES=("$@")

tar -czf /tmp/mv-shop-$TS.tgz -C shop-next "${SHOP_FILES[@]}"
scp -i "$KEY" -q /tmp/mv-shop-$TS.tgz "$HOST:/tmp/"
$SSH "mkdir -p /root/backup-$TS"

echo "== shop (staged build)"
$SSH "set -e
  cd $APP
  tar -czf /root/backup-$TS/shop-src.tgz -C shop-next app components lib public
  rm -rf /tmp/shop-stage && mkdir /tmp/shop-stage
  # hard-link copy: fast, node_modules shared, sources replaced below
  cp -al shop-next/. /tmp/shop-stage/
  rm -rf /tmp/shop-stage/.next
  cd /tmp/shop-stage
  for f in \$(tar -tzf /tmp/mv-shop-$TS.tgz); do rm -f \"\$f\"; done   # break hard links before overwrite
  tar -xzf /tmp/mv-shop-$TS.tgz -C /tmp/shop-stage
  for f in $REMOVE; do rm -f \"/tmp/shop-stage/\$f\"; done
  npx next build > /tmp/shop-build-$TS.log 2>&1 || { tail -30 /tmp/shop-build-$TS.log; echo 'BUILD FAILED — prod untouched'; exit 1; }
  test -f .next/BUILD_ID
  cd $APP/shop-next
  tar -xzf /tmp/mv-shop-$TS.tgz -C .
  for f in $REMOVE; do rm -f \"\$f\"; done
  rm -rf .next-prev
  pm2 stop shop-next >/dev/null
  mv .next .next-prev
  cp -a /tmp/shop-stage/.next .next
  pm2 start shop-next >/dev/null
  sleep 8
  bad=0
  for p in / /login /catalog '/catalog?q=%D0%BD%D0%B8%D1%88%D0%B5%D0%B2%D0%B0%D1%8F' /find; do
    c=\$(curl -s -o /dev/null -w '%{http_code}' \"http://127.0.0.1:3002\$p\"); echo \"\$c \$p\"; [ \"\$c\" = 200 ] || bad=1
  done
  if [ \$bad = 1 ]; then
    echo 'SHOP smoke failed — rolling back'
    pm2 stop shop-next >/dev/null; rm -rf .next; mv .next-prev .next
    tar -xzf /root/backup-$TS/shop-src.tgz -C .
    pm2 start shop-next >/dev/null
    exit 1
  fi
  echo \"shop ok, BUILD_ID \$(cat .next/BUILD_ID); rollback: $APP/shop-next/.next-prev + /root/backup-$TS\""
echo "== done"
