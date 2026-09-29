#!/usr/bin/env bash
# Backend hotfix: uploads server/parts files, restarts API + worker, smoke-tests the catalog,
# rolls every file back on failure. Run from the repo root:
#   bash scripts/deploy-api-hotfix.sh                                  # 02d-shop-api-routes.js only
#   bash scripts/deploy-api-hotfix.sh server/parts/a.js server/parts/b.js
set -euo pipefail
HOST=root@81.17.154.153
KEY=~/.ssh/davidsklad_deploy
APP=/var/www/davidsklad/davidskladik
TS=$(date +%Y%m%d-%H%M%S)
FILES=("$@")
[ ${#FILES[@]} -gt 0 ] || FILES=(server/parts/02d-shop-api-routes.js)

for f in "${FILES[@]}"; do node --check "$f"; done
tar -czf /tmp/api-$TS.tgz "${FILES[@]}"
# sshd throttles rapid reconnects now and then — retry instead of failing silently
for try in 1 2 3 4; do scp -i "$KEY" -q /tmp/api-$TS.tgz "$HOST:/tmp/" && break; echo "scp failed (try $try), retrying…"; sleep 5; [ $try = 4 ] && exit 1; done
ssh -i "$KEY" -o ConnectTimeout=20 "$HOST" "set -e
  cd $APP
  mkdir -p /tmp/api-$TS && tar -xzf /tmp/api-$TS.tgz -C /tmp/api-$TS
  for f in ${FILES[*]}; do node --check /tmp/api-$TS/\$f; done
  # back up only files that already exist; remember new ones so a rollback removes them
  # trailing true: under set -e a loop whose last test fails (last file is new) would abort silently
  EXIST=\$(for f in ${FILES[*]}; do [ -e \$f ] && echo \$f; done; true); NEW=\$(for f in ${FILES[*]}; do [ -e \$f ] || echo \$f; done; true)
  tar -czf /root/api-before-$TS.tgz \$EXIST
  cp -r /tmp/api-$TS/. ./   # every uploaded path (server/parts, lib, …), not only server/
  pm2 restart davidsklad-api davidsklad-worker --update-env >/dev/null
  sleep 10
  code=\$(curl -s -o /dev/null -w '%{http_code}' 'http://127.0.0.1:3000/api/shop/catalog?q=%D0%B4%D1%80%D0%B5%D0%B2%D0%B5%D1%81%D0%BD%D1%8B%D0%B9&pageSize=1')
  if [ \"\$code\" != 200 ]; then
    echo \"smoke failed (\$code) — rolling back\"
    tar -xzf /root/api-before-$TS.tgz -C $APP
    for f in \$NEW; do rm -f \$f; done
    pm2 restart davidsklad-api davidsklad-worker >/dev/null
    exit 1
  fi
  echo \"API ok; previous version: /root/api-before-$TS.tgz\""
