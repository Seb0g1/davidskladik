# DavidSklad — чеклист деплоя (PLAN-HARDENING.md 6)

Прод: `root@81.17.154.153`, приложение в `/var/www/davidsklad/davidskladik`.
Процессы PM2: `davidsklad-api` (порт 3000), `davidsklad-worker` (3001, все фоновые
джобы/свипы), `davidsklad-health-watchdog`.

## 1. Перед коммитом (локально)

```bash
# Полная проверка: линты (pretest) + 254 теста
npm test

# Если менялся фронтенд — пересобрать ассеты (app-modern закоммичен в git!)
npm run frontend:check        # tsc --noEmit, без ошибок типов
npm run frontend:build        # обновляет public/app-modern/*

# Если менялся server/parts/*.js — проверить сборку монолита и манифест
node scripts/check-source-manifest.cjs          # каждый part зарегистрирован в source.js
node -e "require('fs').writeFileSync('._c.js', require('./server/source').readServerSource())" && node --check ._c.js && rm ._c.js
```

Линты, входящие в `pretest` (падают деплой при нарушении инварианта):
`check-source-manifest`, `check-computed-field-sql`, `check-queue-priorities`,
`check-warehouse-cache`, `check-regex-exec`.

## 2. Коммит и пуш

- Ветка фичи (не `main`). Сообщение коммита заканчивается `Co-Authored-By`.
- `git push origin <branch>`.

## 3. Деплой на прод

```bash
cd /var/www/davidsklad/davidskladik
git pull origin <branch>
# Бэкенд собирается в памяти при старте — отдельный build не нужен.
pm2 restart davidsklad-api davidsklad-worker
# Только если менялись standalone-скрипты (health-watchdog/pg-backup/prod-alert):
#   pm2 restart davidsklad-health-watchdog
```

Если на проде есть локальные правки конфигов (`git status` показывает `M
ecosystem.config.cjs`): убедиться, что значения уже внесены в коммит, затем
`git checkout -- ecosystem.config.cjs` перед `git pull` (иначе pull конфликтует).

## 4. Проверка после деплоя (обязательно)

```bash
curl -s -o /dev/null -w '%{http_code}' http://localhost:3000/health   # ждём 200
curl -s -o /dev/null -w '%{http_code}' http://localhost:3001/health   # ждём 200
pm2 jlist | ...    # все процессы online, restart_time не растёт в цикле
# Проверить, что целевой свип отработал (для изменений в свипах):
pm2 logs davidsklad-worker --nostream --lines 300 | grep -E "stock_sweep_complete|price_sweep_complete|health_alert_check"
```

Для backend-изменений, наблюдаемых в дашборде — открыть `/api/dashboard/summary`
(админ-сессия) и проверить `priceHealth` / `stockHealth` / `sweepHealth`.

## 5. Откат

```bash
git -C /var/www/davidsklad/davidskladik reset --hard <предыдущий-коммит>
pm2 restart davidsklad-api davidsklad-worker
```
Восстановление БД из бэкапа (если нужно):
`pg_restore --clean --no-owner -d "$DATABASE_URL" /var/backups/davidsklad/davidsklad-<stamp>.dump`

## Особенности (грабли)

- **PM2 не перезапускает зависший event-loop** (статус остаётся `online`) — это делает
  `health-watchdog` (3 провала /health подряд → restart + Telegram-алерт).
- `ecosystem.config.cjs` переопределяет `.env` на процессе (dotenv не перезаписывает уже
  заданное) — `check-env-ecosystem-divergence` предупреждает о расхождении.
- `readWarehouse()` в PG-режиме возвращает только недавно тронутые товары — полному
  каталогу нужна постраничная выборка `prisma.warehouseProduct` напрямую.
- Бэкап БД: cron `30 3 * * *` → `scripts/pg-backup.sh` (ротация 7 дней).
