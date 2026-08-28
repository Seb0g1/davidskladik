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

## Watchdog воркера (davidsklad-health-watchdog)

PM2 не перезапускает процесс с зависшим event loop — статус остаётся `online`. Для этого есть `davidsklad-health-watchdog` (`scripts/health-watchdog.cjs`), запущенный как третий PM2 процесс.

**Как работает**: каждые 45 с проверяет `/health` на api (3000) и worker (3001). При 3 подряд неудачах — `pm2 restart` + запись в `data/health-watchdog-incidents.jsonl` + Telegram-алерт. Обнаруживает не только упавший процесс, но и зависший event loop (через `liveness.ageMs` в `/health`).

```bash
# Статус watchdog
pm2 show davidsklad-health-watchdog

# Лог инцидентов (перезапуски)
cat data/health-watchdog-incidents.jsonl

# Запустить разово (проверить оба процесса)
node scripts/health-watchdog.cjs
```

**Настройка** (через `.env` на сервере):
- `HEALTH_WATCHDOG_INTERVAL_MS` — интервал проверки (дефолт 45 000 мс)
- `HEALTH_WATCHDOG_FAILURE_THRESHOLD` — порог перезапуска (дефолт 3)
- `HEALTH_WATCHDOG_TIMEOUT_MS` — таймаут HTTP-запроса (дефолт 8 000 мс)

**Если watchdog сам завис**: `pm2 restart davidsklad-health-watchdog`

## База данных: бэкап и восстановление

### Как работает бэкап

`scripts/pg-backup.sh` — ежедневный cron в 03:30 МСК, сохраняет compressed custom-format dump (`pg_dump -Fc`) в `/var/backups/davidsklad/`. Ротация: хранить 14 дней, старые удаляются автоматически. Размер дампа проверяется — при пустом файле скрипт завершается с ошибкой.

### Установка cron (один раз на сервере)

```bash
chmod +x scripts/pg-backup.sh scripts/db-restore.sh
( crontab -l 2>/dev/null; echo "30 3 * * * /var/www/davidsklad/davidskladik/scripts/pg-backup.sh >> /var/backups/davidsklad/backup.log 2>&1" ) | crontab -
```

### Ручной запуск

```bash
# из корня проекта на сервере
bash scripts/pg-backup.sh
```

### Просмотр бэкапов

```bash
ls -lh /var/backups/davidsklad/
tail -20 /var/backups/davidsklad/backup.log
```

### Восстановление

**Перед восстановлением остановить все процессы:**

```bash
pm2 stop davidsklad-api davidsklad-worker davidsklad-health-watchdog
```

**Восстановить из дампа:**

```bash
bash scripts/db-restore.sh /var/backups/davidsklad/davidsklad-20260828-030001.dump
```

Скрипт попросит подтверждение, затем выполнит `pg_restore --clean`. После восстановления:

```bash
pm2 start davidsklad-api davidsklad-worker davidsklad-health-watchdog
```

**Конфигурация через env:**
- `BACKUP_DIR` — директория (дефолт `/var/backups/davidsklad`)
- `RETENTION_DAYS` — сколько дней хранить (дефолт `14`)
- `MIN_BACKUP_BYTES` — минимальный размер для проверки (дефолт `10240`)

## BullMQ failed jobs

```bash
node scripts/inspect-bullmq-failed-jobs.cjs        # разбор
node scripts/inspect-bullmq-failed-jobs.cjs --retry # повторить failed
```

## Merge в main

1. 48–72 ч без emergency recover
2. Login < 2 с, unlinked 200, привязка → цена/остаток
3. PR `codex/restore-4dfc0cb` → `main`
