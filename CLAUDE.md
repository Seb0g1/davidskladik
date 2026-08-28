# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Magic Vibes — Склад** (`davidsklad.ru`): веб-панель управления личным складом с интеграцией PriceMaster (MySQL), Ozon и Yandex Market. Сервис считает рублёвые цены по формуле `USD × курс × коэффициент`, синхронизирует склад с маркетплейсами и отправляет обновления цен/остатков по команде оператора.

## Commands

```bash
# Development
npm start                        # запустить сервер (порт 3000)
npm run frontend:dev             # Vite dev-сервер для frontend

# Build
npm run build                    # собрать frontend (vite build)
npm run frontend:check           # type-check frontend без сборки

# Tests
npm test                         # lint + smoke-тесты
npm run test:smoke               # только smoke (без lint)

# Linters (запускаются автоматически перед test)
npm run lint:computed-fields     # SQL в computed-полях
npm run lint:source-manifest     # порядок файлов в server/source.js
npm run lint:queue-priorities    # литералы приоритетов очередей
npm run lint:warehouse-cache     # инвалидация кэша склада
npm run lint:regex-exec          # глобальные regex без .exec() pattern

# Database
npm run db:generate              # prisma generate
npm run db:migrate               # prisma migrate deploy (prod)
npm run db:migrate:dev           # prisma migrate dev (dev)

# Ops / diagnostics
npm run ops:diagnose             # диагностика БД и среды
npm run ops:predeploy            # предеплойная проверка
npm run inspect:db               # inspect соединения с PM MySQL + Postgres

# Deploy (production)
DEPLOY_PASSWORD=... node scripts/deploy-prod.cjs
node scripts/run-prod-emergency-recover.cjs   # аварийный откат
```

## Architecture

### Server — модульная сборка через `server/source.js`

Сервер (`server.js`) запускает `server/source.js`, который последовательно `require`-ит файлы из `server/parts/` в строго заданном порядке. Файлы именуются по схеме `NN-описание.js`:

| Префикс | Слой |
|---------|------|
| `01-bootstrap-*` | Express-приложение, middleware, auth, upload, health |
| `02a-*` | Утилиты, нормалайзеры, API-клиенты Ozon/Yandex, логика склада |
| `02b/02c-*` | Регистрация маршрутов legacy-каталога и core-регистров |
| `02d-*` | Роуты: операции, цены, поставщики, склад, AI |
| `02e/02f-*` | Планировщики, фоновые задачи, BullMQ workers |
| `03-lifecycle-*` | Запуск, graceful shutdown, экспорт `app` для тестов |

**Важно**: порядок файлов в `server/source.js` — это манифест зависимостей. При добавлении нового файла его нужно вставить в правильное место в этом списке, иначе линтер (`lint:source-manifest`) упадёт.

### Двухпроцессная production-схема (PM2)

- **davidsklad-api** — `SERVER_ROLE=api`, `BACKGROUND_JOBS_ENABLED=false`, только HTTP. BullMQ producer.
- **davidsklad-worker** — `SERVER_ROLE=worker`, фоновые задачи, BullMQ consumer (concurrency=3).

Конфиг: `ecosystem.config.cjs`.

### Базы данных

- **PostgreSQL** (`DATABASE_URL`) — основная БД приложения. Схема в `prisma/schema.prisma`. Клиент: `lib/postgres.js` (Prisma).
- **MySQL PriceMaster** (`PM_DB_*`) — read-only источник цен (`OfferRows`). Клиент: mysql2 pool.
- **Redis** (`REDIS_URL`) — BullMQ очереди и кэш.

Ключевые модели Prisma: `WarehouseProduct`, `ProductLink`, `PriceHistory`, `PriceRetryQueueItem`, `AppUser`.

### Frontend

React 19 + Vite + TailwindCSS + TanStack Query. Точка входа: `frontend/src/main.tsx`.

- Роуты (`frontend/src/routes/`): страницы приложения (WarehousePage, PricesPage, SuppliersPage, FinancePage и др.)
- API-слой: `frontend/src/api.ts` — все запросы к серверу
- Типы: `frontend/src/types.ts`

В dev-режиме Vite проксирует API-запросы на `localhost:3000`. В production Express отдаёт статику из `frontend/dist/`.

### Ценообразование

Формула: `цена PriceMaster (USD) × курс USD/RUB × коэффициент наценки`.

- Курс кэшируется 6 ч, берётся из внешнего API или `DEFAULT_USD_RATE`.
- Коэффициенты по умолчанию: `DEFAULT_OZON_MARKUP`, `DEFAULT_YANDEX_MARKUP`.
- На каждый товар склада можно задать свой коэффициент.
- Отправка цен: только ручная, только для выбранных товаров, с подтверждением.

### Ключевые env-переменные

```
DATABASE_URL           PostgreSQL DSN
REDIS_URL              Redis DSN
PM_DB_HOST/PORT/USER/PASSWORD/NAME  MySQL PriceMaster
OZON_CLIENT_ID / OZON_API_KEY
YANDEX_SHOPS_JSON      JSON-массив магазинов ЯМ
AVITO_CLIENT_ID / AVITO_CLIENT_SECRET  OAuth-ключи Avito (client_credentials)
WB_API_TOKEN           API-токен продавца Wildberries (или кабинет через UI)
WB_WAREHOUSE_ID        ID склада WB FBS для остатков (опционально)
WB_MAX_PRICE_RUB       лимит итоговой цены WB — выше не продаём (деф. 20000)
WB_MIN_SUPPLIER_PRICE_RUB  мин. закупка поставщика для WB (деф. 0 — отключено)
WB_MEDIA_BACKFILL_ENABLED  фоновая досылка фото WB на worker (деф. true; лимит WB ~1 фото/15 мин)
WB_SYNC_ENABLED / WB_SYNC_INTERVAL_HOURS  автосинк цен/остатков WB (деф. true / 3 ч)
WB_SYNC_ENRICH_DESCRIPTIONS  дозабор описаний Ozon за тик автосинка (деф. 300, 0 — выкл.)
DEFAULT_WB_MARKUP      наценка WB по умолчанию (деф. 1.6)
APP_USER / APP_PASSWORD / APP_SESSION_SECRET
NODE_ENV               production требует APP_SESSION_SECRET
SERVER_ROLE            api | worker (production)
BACKGROUND_JOBS_ENABLED  true только для worker
DAILY_SYNC_ENABLED / DAILY_SYNC_TIME
MARKETPLACE_IMPORT_HOURS  периодический импорт данных Ozon/Yandex в автосинке (деф. 3 ч; 0 — только суточный)
```

### Runbook и дополнительная документация

- `docs/PROD_RUNBOOK.md` — production runbook (davidsklad.ru)
- `PLAN.md`, `PLAN-HARDENING.md` — планы разработки
- `DEPLOY.md` — процедура деплоя
