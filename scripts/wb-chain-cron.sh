#!/bin/sh
# Ночная досылка карточек WB: лимит WB — 1000 новых карточек/сутки, поэтому
# остаток кандидатов доливается порциями по расписанию (crontab root).
#
# Guard от параллельного запуска живёт в отдельном файле не случайно: строка
# прямо в crontab исполняется через `sh -c '<вся строка>'`, и pgrep находил в
# cmdline этого шелла хвост собственной команды с именем скрипта — «цепочка
# уже запущена» каждую ночь (грабли 2026-07-17). У sh, исполняющего файл,
# в cmdline только путь к файлу — самоматча нет.

cd /var/www/davidsklad/davidskladik || exit 1

log() { echo "$(date -Is) $1" >> data/wb-chain-cron.log; }

running=$(pgrep -af 'prod-wb-chai[n].cjs' | head -3)
if [ -n "$running" ]; then
  log "skip: цепочка уже запущена: $running"
  exit 0
fi

log "start chain-nomedia"
MALLOC_ARENA_MAX=2 NODE_OPTIONS='--max-old-space-size=4096' WB_REQUEST_MAX_ATTEMPTS=4 \
  /usr/bin/node scripts/prod-wb-chain.cjs chain-nomedia >> data/wb-chain-cron.log 2>&1
log "finish code=$?"
