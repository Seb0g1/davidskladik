# Production runbook (davidsklad.ru)

## Архитектура

- **davidsklad-api** — только HTTP (порт 3000, nginx)
- **davidsklad-worker** — фон: BullMQ, price-retry, maintenance (health: порт 3001)
- **Redis** — очередь BullMQ (привязки идут api → Redis → worker)

## Быстрое восстановление (2 минуты)

```powershell
$env:DEPLOY_PASSWORD = "..."
node scripts/run-prod-emergency-recover.cjs
```

Скрипт: убивает тяжёлые one-off процессы, деплоит `server.js` + `ecosystem.config.cjs`, перезапускает api+worker, прогоняет post-deploy check.

## Полный деплой

```powershell
$env:DEPLOY_PASSWORD = "..."
node scripts/deploy-prod.cjs
```

Перед деплоем локально: `npm test` + `npm run build`. После деплоя — блокирующий `prod-post-deploy-check.cjs` (exit 1 при сбое).

## Что НЕ включать на api

- `BACKGROUND_JOBS_ENABLED=true`
- `MARKETPLACE_MAINTENANCE_*` (кроме false)
- `PRICE_RETRY_AUTO_ENABLED=true`
- `WAREHOUSE_GROUP_COUNT_WARM_ENABLED=true`
- Monolith `davidsklad` (удалён из ecosystem)

## Симптомы и действия

| Симптом | Действие |
|---------|----------|
| 502 / таймаут login | `run-prod-emergency-recover.cjs`, проверить `pm2 list` |
| Привязка не срабатывает | `redis-cli ping`, `pm2 logs davidsklad-worker`, `/api/live-status` → `queue.degraded`, `inspect-bullmq-failed-jobs.cjs` |
| Каталог «Без привязок» медленный | Cold до 15 с допустимо; Ctrl+F5 после deploy |
| Старый UI после deploy | Ctrl+F5 (cache-bust в `index.html`) |

## Мониторинг

```bash
# на сервере (cron каждые 5 мин, с алертом при fail)
node scripts/prod-post-deploy-check.cjs || node scripts/prod-alert-on-failure.cjs prod-post-deploy-check

# установка cron + pm2-logrotate (с локальной машины)
node scripts/setup-prod-monitoring.cjs
```

Опционально Telegram: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` в `.env` на сервере — алерт после 2 подряд fail (debounce 15 мин).

Алерт: 3 slow requests > 10 s за 5 мин или login > 5 s (см. `/api/health?deep=true`).

## BullMQ failed jobs

```bash
node scripts/inspect-bullmq-failed-jobs.cjs        # разбор
node scripts/inspect-bullmq-failed-jobs.cjs --retry # повторить failed
```

## Merge в main

1. 48–72 ч без emergency recover
2. Login < 2 с, unlinked 200, привязка → цена/остаток
3. PR `codex/restore-4dfc0cb` → `main`
