# План работ DavidSklad

> Рабочий план для Claude. Обновлять статусы по мере выполнения.
> Сервер: root@81.17.154.153, код /var/www/davidsklad/davidskladik, деплой: `git pull && node server/assemble.js && pm2 restart davidsklad-api davidsklad-worker`.
> Новые server-части обязательно регистрировать в `server/source.js` (ручной manifest!).

## ✅ Сделано (июнь 2026)

- Стабильность: фикс travel-set бесконечного цикла, таймауты Ozon fetch, пул 4 параллельных Ozon-запросов, лимит+автоочистка очереди цен.
- Удаление с Яндекса <20мл (по max-объёму)/Тестер/Отливант: 4 245 шт, автоудаление мусорных строк fast-проходом (имя берётся у Ozon-двойника, если в Yandex-строке только артикул).
- Ремонт карточек Яндекса: бренд, вес/габариты, фото (свои или Ozon-двойника), штрихкоды, категория, basicPrice — 12 690 шт.
- Разархив Яндекса: bulk `offer-mappings/unarchive` каждые 60с (Медиум 10k/мин), unarchive-first + живой статус по offer-mappings (создание карточки только если реально отсутствует), остаток≥1 и force-цена в том же проходе, флаг manualSellableAt от повторной архивации.
- Ozon разархив: очередь 100/сутки, сброс 03:00 (лимит Ozon, не обойти).
- Цены: свип изменившихся каждые 2 мин (индексные колонки current_price≠target_price, кулдаун 45м от карантина), конвейер 1000шт/~98с (OZON_PRICE_BATCH_SIZE=1000, verify 10s×3 в фоне с автодожимом), параллельные PM-lookups (пул 20).
- Автоимпорт Ozon→Yandex (>20мл, не Тестер/Отливант) каждые 6ч + ручной POST /api/ozon-yandex-import/auto-run.
- ETA-таймеры в карточке (живые, от реальных nextRunAt планировщиков): «Цена обновится ~N», «Разархив ~N».
- Полный импорт каталога: ежедневно 11:00 + ручной POST /api/warehouse/sync/run (на воркере 3001).
- Реконсайлер: 600 товаров/5мин (ecosystem.config.cjs И .env — менять оба!).

## 🔜 Очередь задач

### 1. ✅ PriceMaster-поиск: сортировка для привязки [СДЕЛАНО, задеплоено]
- В выдаче «Найти строку PriceMaster» сортировать по цене от минимальной к максимальной.
- Строки-тестеры (имя содержит tester/тестер) — ВСЕГДА в конце списка, визуально пометить бейджем «Тестер».
- Где: бэкенд-роут поиска PM строк + frontend/src (поиск привязки в карточке).
- Критерий: при привязке обычного товара тестер физически нельзя выбрать случайно — он внизу и помечен.

### 2. ✅ Остаток снова 5 после заказа [СДЕЛАНО, ЖДЁТ ДЕПЛОЯ — код в git]
- Если товар привязан и поставщик доступен — остаток на МП всегда восстанавливается до целевого (по умолчанию 5).
- Сделать: (а) дефолт targetStock=5 для привязанных с поставщиком (сейчас после разархива бывает 1); (б) стоковый свип каждые 2-3 мин по аналогии с price_sweep (Яндекс лимит 100k/мин): targetStock>0 и marketplaceState.stock<targetStock → отправить остаток.
- Критерий: заказ списал остаток → в течение ~3 мин остаток снова 5 (если поставщик жив).
- Реализация: 02f-stock-sweep.js (свип каждые 180с, дефолт LINKED_DEFAULT_TARGET_STOCK=5, кулдаун повторов 20м). НЕ задеплоен — ждёт команды пользователя.

### 3. ✅ Страница «Восстановление» (очереди) [СДЕЛАНО, ЖДЁТ ДЕПЛОЯ — код в git]
- Один экран: очередь разархива Ozon (позиция, товар, ETA с учётом 100/сутки и сброса 03:00, статус/ошибки, кнопки убрать/поднять) + блок Яндекса (текущий архивный бэклог, лог последних fast-проходов, ошибки «Яндекс отказал»).
- Данные уже есть: /api/ozon/unarchive-queue, состояние fast-pass, логи. Нужен фронт (frontend/src/routes) + пара read-эндпоинтов.
- Реализация: страница /app/recovery-queue уже существовала для Ozon; добавлен блок Яндекса (бэклог архива, статус fast-прохода, последний результат, отказы) + GET /api/yandex/fast-unarchive/status.

### 4. 🔶 Уведомления на сайте со звуком [v1 ГОТОВ, ЖДЁТ ДЕПЛОЯ — код в git]
- События: новый заказ / сообщение в чате / отзыв / вопрос — с обоих МП (Ozon Premium scopes уже есть: client_id eed7e067-…, 44 скоупа; Яндекс Медиум).
- Бэкенд: поллеры новых событий (worker, каждые 1-2 мин) → таблица notifications в PG → SSE-эндпоинт /api/notifications/stream (+ REST список/прочитано).
- Фронт: колокольчик с бейджем в шапке, выпадающий список, тост + звук при новом событии.
- Настройки: выбор звука (несколько пресетов + загрузка своего mp3), вкл/выкл по типам событий. Хранить в appSettings.
- Критерий: пришёл заказ на Ozon → в течение 1-2 мин на любой странице сайта звук + тост.
- Реализация v1: таблица app_notifications (raw SQL, без prisma-миграций), поллеры worker каждые 90с (Ozon: заказы/чаты/отзывы/вопросы; Яндекс: заказы/чаты/отзывы), SSE /api/notifications/stream, колокольчик в шапке с дропдауном, тост + WebAudio-звук (3 пресета), настройки в SettingsPage (звук, типы, проба). Поллеры best-effort: при 5 ошибках источник опрашивается раз в ~15 мин.
- После деплоя проверить: формы ответов Ozon review/question API могут отличаться — смотреть warn 'notify poll ... failed' в логах воркера и поправить парсинг.

### 5. 🔶 Финансовый блок [КАРКАС ГОТОВ, ЖДЁТ ДЕПЛОЯ; UI-доводка на выходные]
- Страница «Финансы»: заказы с обоих МП, себестоимость из PriceMaster по привязке (по факту заказа — цена поставщика на момент), комиссии МП, чистая прибыль. Работает только для привязанных товаров (остальные показывать «нет привязки»).
- Плюс ручные закупки и сводка по поставщикам.
- Реализовано: 02f-finance-orders-sync.js — автозагрузка заказов каждые 20м (Ozon postings c financial_data: продажа/выплата/комиссия по SKU; Яндекс orders по campaignId: продажа+субсидии, комиссии TODO через stats), себестоимость из selectedSupplier на момент загрузки, статусы/отмены фильтруются. Маршруты POST /api/finance/sync-orders, GET /api/finance/sync-orders/status. Страница «Финансы» уже была — после деплоя проверить формы ответов API по логам 'finance ... sync failed' и доделать комиссии Яндекса + UI-полировку.

## 📦 Бэклог (после очереди)

6. 🔶 Отзывы [v1 ГОТОВ, ЖДЁТ ДЕПЛОЯ]: страница /app/reviews — листинг обоих МП, фильтры, ответы с шаблонами (data/review-templates.json) и эмодзи. Ozon /v1/review/list+comment/create, Yandex goods-feedback+comments/update. После деплоя сверить формы ответов API по логам.
7. ✅ Чаты [ГОТОВО]: /app/chats — мессенджер двух МП (список с непрочитанными, история, отправка, Ctrl+Enter), шаблоны (data/chat-templates.json), эмодзи, фильтры. Ozon v3 chat/list+history, v1 send, v2 read; Yandex businesses/chats(+history,+message).
8. ✅ Вопросы [ГОТОВО]: /app/questions — Ozon list+answer (у Яндекса нет API вопросов).
9. ✅ Настройки [ГОТОВО]: хаб-карточки на /app/settings со ссылками на все разделы (Поставщики, Цены, Операции, Ошибки наличия, Проблемные, Восстановление, Система, AI, Финансы) + кабинеты МП, роли, уведомления/звук.
10. ✅ Автокорзина [УЖЕ СУЩЕСТВОВАЛА, проверена]: /app/supplier-cart — preview/generate/commit в PriceMaster DB, расписание (scheduleSupplierCartAuto), история, rollback.
11. ✅ Сборка [УЖЕ СУЩЕСТВОВАЛА, проверена]: /app/picking-list — карточки поставщиков по алфавиту, леджер (долг/аванс/оплачено), Собрал/Не было, Заплатил с суммой, копия накладной.
12. Редизайн под референс-скрин (тёмная тема DavidSklad) — НЕ деплоить, поднять локально на Windows и показать.

## ⚠️ Известные грабли

- `readWarehouse()` в PG-режиме отдаёт только недавние товары — для полного каталога только prisma-paging.
- PM2 не перезапускает процесс с зависшим event loop (статус online) — проверять `curl /health` + %CPU.
- ecosystem.config.cjs переопределяет .env (дубли env смотреть в обоих).
- Имя Yandex-строки часто = артикул; настоящее имя у Ozon-двойника по offerId.
- Артикулы бывают с символами: `#YV026021#` — искать точно.
- Локальный «absent»-статус может врать — состояние Яндекса проверять живым API.

---

# План на следующий этап (новые функции + фиксы) — для агентов

> Контекст уже известен: monolith `server/parts/*.js` (manifest в `server/source.js` — РЕГИСТРИРОВАТЬ новые части!), фронт `frontend/src`, деплой `git pull && node server/assemble.js && pm2 restart davidsklad-api davidsklad-worker`. Сервер root@81.17.154.153.

## A. ✅ ГОТОВО: Яндекс-фильтрация — «без коробки» удалено, наборы разрешены
_Удалено 1609 (вкл. 27 «без коробки»); наборы выгружены (Яндекс 232→295). Хелперы isYandexSetProduct/isYandexNoBoxProduct в 02a-ozon-yandex-import-cleanup.js._
Файлы: `02a-ozon-yandex-import-cleanup.js` (`ozonYandexImportBlockReasons`, `assessYandexSmallVolume`, `buildYandexCleanupCandidate`), `02f-yandex-fast-unarchive.js` (партиционирование delete/unarchive), `02f-yandex-cleanup-action-routes.js` (delete-filtered-local).

1. **«Без коробки» не должно быть на Яндексе.** Сейчас `/без\s+коробк/` блокируется только на ИМПОРТЕ (`ozonYandexImportBlockReasons`), но уже выгруженные не удаляются. Нужно:
   - добавить `без коробк` в keyword-удаление `buildYandexCleanupCandidate` (рядом с отливант/тестер) и в `02f-yandex-fast-unarchive.js` (`hasBlockedKeyword`);
   - имя брать у Ozon-двойника по offerId (как уже сделано для тестеров — Yandex-строка часто хранит только артикул);
   - после деплоя запустить `/api/yandex-cleanup/delete-filtered-local` (dryRun→confirmed) чтобы вычистить существующие.
2. **Наборы разрешить (whitelist).** Ключевые слова: `парфюмерный набор`, `набор средств`, `набор кремов`, `подарочный набор`, в общем `набор`. Проблема: `assessYandexSmallVolume` блокирует по max-объёму <20мл, но «Набор 100мл+10мл» имеет max=100 → НЕ блокируется. Однако наборы из мелочёвки («набор кремов 4×10мл», max=10) сейчас удаляются. Нужно:
   - в `assessYandexSmallVolume` (или в обёртке): если имя содержит «набор»-маркер — НЕ считать smallVolume (whitelist перебивает объём);
   - убедиться, что это применяется и в `buildYandexCleanupCandidate`, и в `02f-yandex-fast-unarchive.js`, и в `02a-yandex-marketplace-send.js` (`yandex_small_volume_blocked` guard — иначе наборы не выгрузятся);
   - константа `YANDEX_SET_KEYWORDS` в одном месте, переиспользовать.
   - Критерий: «Парфюмерный набор 100 мл + 10 мл» выгружается; «без коробки» исчезает; тестеры/отливанты/<20мл-несеты по-прежнему удаляются.

## B. ✅ ГОТОВО: Страница «Импорт на Яндекс» (/app/import)
_PG-маршруты candidates(поиск по артикулу+флаги)/refresh(фон)/send-selected; общий exportOzonProductsToYandex; UI с мультивыбором. Проверено на проде: поиск DIC→47/YV→235, флаги eligible/exists/blocked верны._
Бэкенд: переписать `/api/ozon-yandex-import/preview` и `/send` на Postgres-пейджинг (сейчас `readWarehouse()` в PG-режиме отдаёт неполный subset — это причина «товар не импортировался»). Файлы: `02d-prices-finance-routes-api.js` (preview), `02f-ozon-yandex-import-send-route.js`, `02f-ozon-yandex-auto-import.js` (логика отбора уже на PG — переиспользовать `buildOzonYandexImportCandidate`).
Новый роут: `POST /api/ozon-yandex-import/refresh` (тянет свежий список с Ozon API в БД, фоном, с прогрессом — как `runOzonNameBackfill`), `GET /api/ozon-yandex-import/candidates?q=&page=` (PG-пейджинг кандидатов с фильтром по артикулу/имени, флаги eligible/blocked/exists), `POST /api/ozon-yandex-import/send` принимает явный список `offerIds`/`productIds` (выбранные пользователем), а не «все».
Фронт: новая страница `/app/import` (в сайдбаре): кнопка «Обновить с Ozon» (рефреш), поиск по артикулу/имени, таблица с чекбоксами (мультивыбор + «выбрать все на странице»), статус каждого (готов/заблокирован-причина/уже на Яндексе), кнопка «Импортировать выбранные (N)». После импорта — тосты результата, строки помечаются «отправлено».
Критерий: добавил товар на Ozon → на странице «Обновить» → нашёл по артикулу → выбрал несколько → «Импортировать» → появились на Яндексе с ценой по коэффициенту и остатком. (Авто-импорт каждые 6ч остаётся как фон, страница — для ручного «прямо сейчас».)

## C. 🔶 Детерминизм цены (КОРЕНЬ «вечно устаревших» цен) [ЧАСТИЧНО СДЕЛАНО, ЖДЁТ ДЕПЛОЯ — код в git]
Диагноз (подтверждён): расчётная `nextPrice` колеблется между двумя значениями (напр. DIC12 6708/7086), потому что выбор поставщика и/или курс USD различаются между билдами (live PriceMaster vs snapshot, альтернативные поставщики Кирилл 43 USD / коте 50 USD). Колонки `current_price` (из `raw.marketplacePrice`) и `target_price` (из `raw.nextPrice`) выводятся из разных raw-полей → `current<>target` истинно всегда → price_sweep крутит 5800+ товаров вхолостую вечно (582+ «reconciled» без эффекта).
Задачи:
1. Сделать выбор поставщика и курса ДЕТЕРМИНИРОВАННЫМ: один источник истины (snapshot ИЛИ live, не вперемешку), стабильная сортировка альтернатив (по цене, при равенстве — по rowId), фиксированный курс из настроек на момент пересчёта. Файлы: `02a-supplier-pricing-normalize.js`, `02a-price-master-warehouse-helpers.js` (`supplierAlternativesForDiagnostics`/выбор), `02d-prices-send-warehouse-select.js`.
2. Стабилизировать `current_price`/`target_price`: при `unchanged_verified` писать ОБА столбца и ВСЕ raw-поля (`marketplacePrice=nextPrice=targetPrice=currentPrice`) одним значением, чтобы будущие патчи не рассогласовывали (мой текущий reconcile в `02d-prices-send-warehouse-select.js` неполный — он не фиксит raw.nextPrice/targetPrice).
3. Добавить «verified-cooldown»: после `unchanged_verified` ставить `raw.priceVerifiedAt`, и в `02f-price-sweep.js` SQL ИСКЛЮЧАТЬ товары с свежим `priceVerifiedAt` (например <6ч) — чтобы свип перестал молотить даже если столбцы косметически разойдутся.
Критерий: счётчик stale (yandex/ozon) реально ПАДАЕТ к ~0 и держится; price_sweep выбирает только реально изменившиеся; DIC12 стабильно current==target.

Реализовано:
- #1 (частично): финальный тайбрейк в `compareWarehouseSupplierPrices` (`02a-price-master-warehouse-helpers.js`) сменён с `docDate` (нестабилен между live/snapshot фетчами одной и той же строки) на `rowId` (стабильный PK PriceMaster-строки) — устраняет флип выбора поставщика при равных ценах. Курс USD проверен: во всех repricing-путях (`02d-prices-send-warehouse-select.js`, `02f-linked-reconciler-scheduler.js`, `queueAuthoritativePriceReprice`) используется `batchPriceMaster:true` → `appSettings.fixedUsdRate || DEFAULT_USD_RATE`, без обращения к живому курсу — уже детерминирован.
- #2 (сделано): при `unchanged_verified` `02d-prices-send-warehouse-select.js` теперь пишет ОБА столбца (`current_price`, `target_price`) И все raw-поля одним значением (`raw.marketplacePrice=currentPrice=nextPrice=targetPrice`) через `jsonb_build_object` merge — следующий ребилд больше не находит расходящиеся raw.nextPrice/targetPrice.
- #3 (сделано): тот же reconcile пишет `raw.priceVerifiedAt=now()`; `02f-price-sweep.js` SQL исключает товары с `priceVerifiedAt` свежее `PRICE_SWEEP_VERIFIED_COOLDOWN_HOURS` (default 6ч), даже если current_price/target_price снова расходятся косметически.
- После деплоя проверить: счётчик stale падает и держится; в логах `price current_price reconciled to target` для прежних "вечных" SKU (напр. DIC12) больше не повторяется на каждом цикле.

## E. Архитектурное оздоровление — КОРНИ БАГОВ И КАК ИХ УБРАТЬ

> Этот раздел — честный диагноз, почему баги появляются снова и снова, и конкретный план их устранения. Написан после анализа 54 527 строк кода, 14 `raw Json?` полей в схеме и истории инцидентов (Dalik, snapshot.corrupt, event-loop блокировки воркера).

### Диагноз: почему так много багов

**1. `sourceRowId` и `exactName` не являются колонками БД**
Самый важный факт: привязка товара к PriceMaster (`sourceRowId`) хранится НЕ в типизированной колонке, а в `raw` JSON-блобе ProductLink. Нет колонки → нет индекса → нет валидации → можно записать что угодно. Именно это породило весь класс ошибок Dalik: `raw.article` и `supplierArticle`-колонка разошлись, `sourceRowId` указывал на другой товар, поле `exactName` жило отдельной жизнью.

**2. 14 `raw Json?` полей в схеме Prisma**
Почти каждая модель хранит критичные данные в неструктурированном JSON. Код пишет `raw.someField` и читает `raw?.someField` без проверок типа. Ошибки типов видны только в production, не при разработке.

**3. JSON-файлы как критическое состояние**
В корне проекта: `personal-warehouse.json` (был `.corrupt-...`!), `ozon-unarchive-queue.json`, `price-retry-queue.json`, `app-settings.json`, `app-users.json`, `snapshot.json` (300 МБ). Запись не ACID-атомарна → при перегрузке/сбое файл портится. Уже произошло минимум 1 раз (`personal-warehouse.corrupt-...`).

**4. 300 МБ PM snapshot в памяти**
300 000 строк MySQL → JSON-файл 300 МБ → in-memory индексы → GC-давление → зависания event loop. Воркер рестартовал 31 раз в день. Исправлено patch'ем, но архитектура остаётся хрупкой.

**5. Нет TypeScript на бэкенде**
54 527 строк JavaScript без типов. Ошибки `Cannot read property 'X' of undefined` видны только в проде. Фронтенд на TypeScript — бэкенд нет.

**6. Нет валидации ответов маркетплейсов**
Ozon/YM/WB меняют форму ответа API — наш код молча ломается (читает `undefined`). Узнаём только когда что-то пошло не так в прод.

**7. Ручной манифест `server/source.js`**
100+ файлов надо вставлять в правильное место вручную. Линтер помогает, но при каждом добавлении риск ошибки.

---

### Задачи: от критичных к желательным

#### E1. 🔴 КРИТИЧНО: Вынести `sourceRowId` и `exactName` из `raw` в типизированные колонки

**Проблема**: Prism-схема ProductLink не имеет колонок `sourceRowId` и `exactName` — они лежат в `raw` JSON. Нет индекса → медленный поиск; нет типа → расхождения; нет ограничений → баги Dalik.

**Что делать**:
- Prisma migration: добавить `sourceRowId String? @map("source_row_id")`, `exactName String? @map("exact_name")` в ProductLink
- Добавить `@@index([sourceRowId])` и `@@index([exactName])`
- Обновить все места где читается/пишется `raw.sourceRowId` и `raw.exactName` (grep по кодобазе)
- Backfill: заполнить новые колонки из существующих `raw`-данных через SQL-миграцию

**Файлы для изменения**: `prisma/schema.prisma`, все `02a-price-master-*.js`, `02d-operation-runners-archived.js`, `02a-normalizers-warehouse-links.js`.

**Критерий**: `prisma.productLink.findMany({ where: { sourceRowId: "2331708" } })` работает мгновенно. Никакой код не читает `link.raw?.sourceRowId` — только `link.sourceRowId`.

---

**ПРОМТ ДЛЯ АГЕНТА E1:**
```
Задача: вынести поля sourceRowId и exactName из JSON-блоба raw в типизированные колонки Prisma.

Контекст проекта:
- Node.js/Express, server/parts/*.js (манифест server/source.js — добавлять новые файлы туда!)
- PostgreSQL через Prisma (prisma/schema.prisma), MySQL PriceMaster (read-only)
- Команды: npm test (lint + smoke), npm run db:migrate:dev (Prisma migration dev)

Шаг 1 — Prisma schema (prisma/schema.prisma):
В модель ProductLink добавить после поля `keyword`:
  sourceRowId  String?  @map("source_row_id")
  exactName    String?  @map("exact_name")
В @@index добавить: @@index([sourceRowId]), @@index([exactName])

Шаг 2 — Migration:
Создай migration файл вручную (npm run db:migrate:dev -- --name add_link_source_row). Migration SQL:
  ALTER TABLE product_links ADD COLUMN source_row_id TEXT;
  ALTER TABLE product_links ADD COLUMN exact_name TEXT;
  CREATE INDEX idx_product_links_source_row_id ON product_links(source_row_id) WHERE source_row_id IS NOT NULL;
  CREATE INDEX idx_product_links_exact_name ON product_links(exact_name) WHERE exact_name IS NOT NULL;
  -- Backfill из raw JSON:
  UPDATE product_links SET source_row_id = (raw->>'sourceRowId') WHERE raw->>'sourceRowId' IS NOT NULL;
  UPDATE product_links SET exact_name = (raw->>'exactName') WHERE raw->>'exactName' IS NOT NULL;

Шаг 3 — обновить все места в коде где читается/пишется raw.sourceRowId и raw.exactName:
  Grep: grep -rn "raw\.sourceRowId\|raw\?\.sourceRowId\|raw\.exactName\|raw\?\.exactName\|sourceRowId.*raw\|exactName.*raw" server/
  Для каждого найденного места:
  - Чтение: заменить `link.raw?.sourceRowId` на `link.sourceRowId ?? link.raw?.sourceRowId` (временный fallback)
  - Запись: при update/create ProductLink всегда писать в оба места — и в поле, и в raw (чтобы не сломать код который ещё не обновлён)
  - В 02d-operation-runners-archived.js (runRepairDalikDisambiguationLinksOperation): при update добавить sourceRowId: bestRowId, exactName: bestName вместе с raw

Шаг 4 — обновить normalizers-warehouse-links.js:
  normalizeWarehouseLink должен читать sourceRowId из input.sourceRowId || input.raw?.sourceRowId
  При create ProductLink писать sourceRowId в поле, не только в raw

Шаг 5 — npm test (все 281 должны пройти, плюс lint:source-manifest).

Важно: НЕ удалять raw.sourceRowId и raw.exactName из raw-блоба пока — только добавить дублирование в колонки. Полная миграция чтений — следующий шаг.

Репорт: сколько мест обновлено, прошли ли тесты.
```

---

#### E2. 🔴 КРИТИЧНО: Zod-валидация ответов маркетплейсов

**Проблема**: Ozon/YM/WB могут изменить форму ответа API. Мы узнаём об этом только когда что-то сломалось в проде — потому что читаем `response.result?.items?.[0]?.stock` без проверки что response.result вообще существует.

**Что делать**:
- Установить `zod` (или использовать ручную валидацию) 
- Создать `server/parts/02a-api-schemas.js` со схемами для 5 критичных эндпоинтов:
  - Ozon `/v4/product/info/stocks` (остатки)
  - Ozon `/v5/product/info/prices` (цены)
  - Ozon `/v2/posting/fbs/list` (заказы)
  - YM `/v3/businesses/{id}/offers/stocks` (остатки)
  - YM `/v3/campaigns/{id}/offers` (товары)
- При получении ответа: parse → если не совпадает → `logger.warn("api_response_unexpected_shape", { endpoint, diff })`
- НЕ падать при несовпадении — только логировать

**Критерий**: при изменении API маркетплейса в логах появляется warning за часы до того как что-то сломается.

---

**ПРОМТ ДЛЯ АГЕНТА E2:**
```
Задача: добавить валидацию форм ответов API маркетплейсов чтобы обнаруживать изменения API заранее.

Контекст:
- Node.js без TypeScript на бэкенде
- Ozon API: server/parts/02a-ozon-api-request.js, 02a-ozon-stock-price-maps.js
- YM API: server/parts/02a-yandex-*.js
- Логгер: logger.warn/info/error (pino, уже импортирован во всех файлах)
- НЕ ставить новых npm-пакетов — использовать ручную валидацию

Реализация:
1. Создай server/parts/02a-api-schemas.js со вспомогательной функцией validateApiShape(response, schema, context):
   - schema — объект вида { "result.items[].stock": "number", "result.items[].offer_id": "string" }
   - функция проверяет наличие и тип каждого поля через путь (используй lodash.get или собственный getPath)
   - при несовпадении вызывает logger.warn("api_shape_mismatch", { context, missingFields, unexpectedTypes })
   - возвращает { valid: bool, issues: [] }
   - НЕ бросает исключений

2. Добавь вызовы validateApiShape для этих ответов (найди где они обрабатываются):
   a) Ozon /v4/product/info/stocks — ожидаем { result: { items: [{ offer_id, stocks: [{ type, present, reserved }] }] } }
      Файл: 02a-ozon-stock-price-maps.js, функция getOzonStockMap или аналог
   b) Ozon /v2/posting/fbs/list — ожидаем { result: { postings: [{ posting_number, products: [{ offer_id, quantity }] }] } }  
      Файл: 02f-notifications-pollers.js или 02d-shop-api-routes.js
   c) YM /v3/businesses/{id}/offers — ожидаем { result: { offerMappings: [{ offer: { offerId } }] } }
      Файл: 02a-yandex-stock-send-bulk.js (getKnownYandexExistingOfferIdSet)

3. Добавь 02a-api-schemas.js в server/source.js в правильное место (после 02a-ozon-api-request.js).

4. npm test — должны пройти все тесты.

Репорт: какие файлы изменены, сколько эндпоинтов покрыто.
```

---

#### E3. 🟡 ВАЖНО: Централизованный журнал ошибок в Postgres

**Проблема**: ошибки разбросаны по логам (pino → stdout). Нет способа быстро увидеть «что сломалось за последний час» без SSH.

**Что делать**:
- Prisma-модель `AppError` (id, type, source, message, context Json, resolvedAt, createdAt)
- Функция `recordAppError(type, source, message, context)` — пишет в БД non-blocking (без await, чтобы не ломать основной поток)
- Вызывать из catch-блоков критичных операций (ценообразование, синк, разархив)
- Маршрут `GET /api/system/errors?since=1h&type=` — для SystemPage
- На SystemPage: таблица последних 50 ошибок с фильтром по типу

---

**ПРОМТ ДЛЯ АГЕНТА E3:**
```
Задача: добавить централизованный журнал ошибок приложения в Postgres + UI на странице системы.

Контекст:
- Prisma schema: prisma/schema.prisma (команда npm run db:migrate:dev для создания миграции)
- SystemPage: frontend/src/routes/SystemPage.tsx
- API файл для системных роутов: найди через grep "system\|SystemPage" в server/parts/
- Logger: pino (logger.error/warn)
- prisma клиент: глобальный, импортируется как { prisma } или require('../lib/postgres')

Шаг 1 — Prisma schema:
Добавить модель:
model AppError {
  id          String    @id @default(cuid())
  type        String    // "price_send", "stock_send", "pm_sync", "api_error", etc
  source      String    // файл/функция откуда ошибка
  message     String
  context     Json?
  resolvedAt  DateTime? @map("resolved_at")
  createdAt   DateTime  @default(now()) @map("created_at")
  @@index([type, createdAt])
  @@index([createdAt])
  @@map("app_errors")
}

Шаг 2 — migration: npm run db:migrate:dev -- --name add_app_errors

Шаг 3 — создай server/parts/02a-error-tracker.js:
function recordAppError(type, source, message, context = {}) {
  // non-blocking — не await
  prisma.appError.create({ data: { type, source, message, context } })
    .catch((e) => logger.warn("error_tracker_write_failed", { detail: e.message }));
}
// Автоочистка: удалять ошибки старше 7 дней (вызывать раз в сутки из планировщика)
async function pruneOldAppErrors() {
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
  await prisma.appError.deleteMany({ where: { createdAt: { lt: cutoff } } });
}
Экспортировать обе функции. Добавить в server/source.js.

Шаг 4 — API роут (в существующем файле системных роутов):
GET /api/system/errors?since=1h&type=&limit=50
  since: "1h"|"6h"|"24h"|"7d"
  Возвращает: { errors: [{ id, type, source, message, context, createdAt }] }
DELETE /api/system/errors/:id — пометить resolved

Шаг 5 — вызвать recordAppError в 3 критичных местах:
  - в catch блоке отправки цен (найти через grep "price.*error\|send.*price.*catch")
  - в catch блоке синка PM снапшота (02f-daily-maintenance-schedulers.js или аналог)
  - в catch блоке отправки остатков

Шаг 6 — фронт SystemPage.tsx: добавить секцию "Журнал ошибок" с таблицей, фильтром по времени/типу, кнопкой "Отметить решённой".

npm test в конце. Репорт: что сделано.
```

---

#### E4. 🟡 ВАЖНО: Лог истории цен в карточке товара

**Проблема**: `PriceHistory` уже пишется — но нет UI чтобы это видеть. Операторы не знают, почему цена изменилась.

**Что делать**:
- На странице карточки товара: вкладка «История цен» — таблица/график последних 30 записей из PriceHistory
- Колонки: дата, рыночная цена (до), рыночная цена (после), причина (из `raw.reason`), кто изменил
- Маршрут уже может существовать — проверить через grep

---

**ПРОМТ ДЛЯ АГЕНТА E4:**
```
Задача: показать историю изменений цены в карточке товара на складе.

Контекст:
- PriceHistory модель уже есть в prisma/schema.prisma и данные пишутся
- Карточка товара: frontend/src/ — найди файл DiagnosticValue или WarehousePage или ProductCard (grep по "priceHistory\|PriceHistory\|price_history")
- API: найди есть ли уже GET /api/warehouse/products/:id/price-history (grep по server/parts/)
- Фронтенд: React + TanStack Query + TailwindCSS

Шаг 1 — найти или создать бэкенд-роут:
  Если нет GET /api/warehouse/products/:id/price-history — добавить в подходящий роутовый файл (02d-warehouse-products-crud.js или аналог):
  - SELECT из price_history WHERE product_id = :id ORDER BY created_at DESC LIMIT 50
  - Возвращать: [{ id, marketplace, target, priceRub, usdPrice, usdRate, markup, reason, createdAt }]
  - Поля могут быть в raw JSON — посмотри реальную структуру: prisma.priceHistory.findFirst() чтобы увидеть что пишется

Шаг 2 — найти где рендерится карточка товара на фронтенде:
  Это скорее всего modal/drawer или страница. Добавить раздел "История цен":
  - Использовать useQuery(["/api/warehouse/products", id, "price-history"])
  - Таблица: Дата | МП | Цена (₽) | PM цена (USD) | Наценка | Причина
  - Показывать последние 20 записей, кнопка "показать все"
  - Если записей нет — "История цен пуста"

Шаг 3 — npm run frontend:check (TypeScript errors), npm test.

Репорт: какой файл карточки изменён, есть ли уже данные в price_history.
```

---

#### E5. 🟡 ВАЖНО: Watchdog воркера (автоперезапуск при зависшем event loop)

**Проблема**: PM2 не перезапускает процесс с зависшим event loop (статус остаётся `online`). Уже приводило к суткам простоя.

**Что делать**:
- Внешний bash-скрипт (cron, каждые 2 мин): `curl -f http://localhost:3001/health || pm2 restart davidsklad-worker`
- Или: в самом воркере — setInterval который проверяет что event loop не отстаёт больше чем на 5 сек
- Документировать в DEPLOY.md и PROD_RUNBOOK.md

---

**ПРОМТ ДЛЯ АГЕНТА E5:**
```
Задача: защита от зависания event loop воркера — автоперезапуск через pm2 при недоступности /health.

Контекст:
- Production сервер: root@81.17.154.153
- PM2 конфиг: ecosystem.config.cjs в корне
- Worker process: davidsklad-worker (SERVER_ROLE=worker, порт 3001)
- /health эндпоинт уже существует (01-bootstrap-health.js)
- Деплой: git pull && node server/assemble.js && pm2 restart davidsklad-api davidsklad-worker

НЕ нужно менять код сервера. Нужно:

1. Создай scripts/watchdog-worker.sh:
#!/bin/bash
HEALTH_URL="http://localhost:3001/health"
MAX_FAILURES=2
FAILURES=0
while true; do
  if ! curl -sf --max-time 5 "$HEALTH_URL" > /dev/null 2>&1; then
    FAILURES=$((FAILURES + 1))
    echo "[watchdog] health check failed ($FAILURES/$MAX_FAILURES)" | logger -t davidsklad-watchdog
    if [ "$FAILURES" -ge "$MAX_FAILURES" ]; then
      echo "[watchdog] restarting davidsklad-worker" | logger -t davidsklad-watchdog
      pm2 restart davidsklad-worker
      FAILURES=0
    fi
  else
    FAILURES=0
  fi
  sleep 60
done

2. Создай scripts/watchdog-setup.sh — инструкция по установке как systemd service:
[Unit]
Description=DavidSklad Worker Watchdog
After=network.target

[Service]
Type=simple
ExecStart=/var/www/davidsklad/davidskladik/scripts/watchdog-worker.sh
Restart=always
RestartSec=10
User=root

[Install]
WantedBy=multi-user.target

Включение: systemctl enable davidsklad-watchdog && systemctl start davidsklad-watchdog

3. Обнови docs/PROD_RUNBOOK.md — добавь секцию "Watchdog" с инструкцией по установке и проверке (systemctl status davidsklad-watchdog).

4. Добавь в ecosystem.config.cjs для davidsklad-worker:
   max_restarts: 20,
   restart_delay: 3000,
   -- эти значения уже могут быть, проверь

Репорт: какие файлы созданы/изменены. (Не пытайся деплоить — только создай файлы локально.)
```

---

#### E6. 🟢 ХОРОШО ИМЕТЬ: Bulk-операции в каталоге склада

**Что делать**: чекбоксы у товаров в таблице каталога (WarehousePage) + кнопки «Переотправить цену», «Переотправить остаток», «Снять привязку» для выбранных товаров. API уже есть (price push, stock push) — нужен только фронтенд мультивыбора.

---

**ПРОМТ ДЛЯ АГЕНТА E6:**
```
Задача: добавить bulk-операции (мультивыбор + действия) в таблицу каталога на WarehousePage.

Контекст:
- Frontend: frontend/src/routes/WarehousePage.tsx (или аналог — найди через glob)
- React 19 + TanStack Query + TailwindCSS
- API для цен: POST /api/prices/send-selected (найди через grep)
- API для остатков: есть ли уже bulk stock endpoint? Grep по server/parts/
- API для снятия привязки: DELETE /api/warehouse/products/:id/links или аналог

Шаг 1 — state мультивыбора в WarehousePage:
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  Чекбокс в каждой строке таблицы. "Выбрать все на странице". Счётчик выбранных.

Шаг 2 — floating action bar (появляется когда selectedIds.size > 0):
  "Выбрано N товаров" + кнопки:
  - "Переотправить цену" → POST к price endpoint с выбранными productIds
  - "Переотправить остаток" → POST к stock endpoint
  - "Снять выделение" → setSelectedIds(new Set())
  Стилизация: fixed bottom bar с тенью, анимация появления

Шаг 3 — если нет bulk stock endpoint:
  Добавить POST /api/warehouse/bulk-stock-push в подходящий серверный файл
  Body: { productIds: string[] }
  Вызывает sendTargetStocksToMarketplace для найденных продуктов

Шаг 4 — npm run frontend:check + npm test.

Репорт: что изменено, есть ли нужные API-эндпоинты.
```

---

#### E7. 🟢 ХОРОШО ИМЕТЬ: Ежедневный бэкап БД

**Что делать**: bash-скрипт `scripts/db-backup.sh` → pg_dump → `/var/backups/davidsklad/YYYY-MM-DD.sql.gz`. Cron: 03:00 ежедневно. Ротация: хранить 14 дней.

---

**ПРОМТ ДЛЯ АГЕНТА E7:**
```
Задача: ежедневный бэкап PostgreSQL.

Создай scripts/db-backup.sh:
#!/bin/bash
set -e
BACKUP_DIR="/var/backups/davidsklad"
DATE=$(date +%Y-%m-%d)
mkdir -p "$BACKUP_DIR"
# DATABASE_URL из env или параметра
DB_URL="${DATABASE_URL:-}"
if [ -z "$DB_URL" ]; then
  echo "DATABASE_URL not set" >&2; exit 1
fi
pg_dump "$DB_URL" | gzip > "$BACKUP_DIR/$DATE.sql.gz"
echo "Backup saved: $BACKUP_DIR/$DATE.sql.gz ($(du -h "$BACKUP_DIR/$DATE.sql.gz" | cut -f1))"
# Ротация: удалить старше 14 дней
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +14 -delete
echo "Old backups cleaned."

Создай scripts/db-backup-setup.sh с инструкцией cron:
# Добавить в crontab (crontab -e):
# 0 3 * * * /var/www/davidsklad/davidskladik/scripts/db-backup.sh >> /var/log/davidsklad-backup.log 2>&1

Обнови docs/PROD_RUNBOOK.md — добавь секцию "Backup" с инструкцией восстановления:
  gunzip -c backup.sql.gz | psql $DATABASE_URL

Репорт: файлы созданы.
```

---

## D. Прочие улучшения (предложения)
1. ✅ **Дашборд** (главная для ADMIN): сводка — продажи за сегодня/неделю (из finance), прибыль, топ-поставщики, очередь цен, архив-бэклог, непрочитанные чаты/вопросы/отзывы. Сейчас «Статистика» пустует.
   - Сделано: новый эндпоинт `/api/dashboard/summary` (`02d-dashboard-summary.js`) агрегирует продажи за сегодня/неделю и прибыль (из `listFinanceOrders`, новый период `period=today` добавлен в `02d-finance-list-query.js`), топ-5 поставщиков за неделю по прибыли, очередь цен (`salesAutomationSkuState` со статусом pending/queued), архив-бэклог (Яндекс `archived+linked` count + очередь восстановления Ozon), непрочитанные уведомления по типам (`app_notifications` group by type). `DashboardPage.tsx` дополнен карточками: «Продажи сегодня» (метрика), «Продажи за неделю» + топ поставщиков, «Очередь и архив», «Уведомления» (чаты/вопросы/отзывы).
2. ✅ **Здоровье системы на сайте**: индикатор «воркер жив / очередь / последний синк / PriceMaster доступен» (есть `/api/live-status`) в шапке — чтобы видеть проблемы без SSH.
   - Сделано: новый компонент `SystemHealthIndicator.tsx` в шапке (только ADMIN), опрашивает `/api/live-status` каждые 30с, цветной индикатор (зелёный/жёлтый/красный) + дропдаун с детализацией по 4 пунктам (воркер/очередь/последний синк/PriceMaster).
3. **Bulk-операции в каталоге**: чекбоксы у товаров + «удалить с Яндекса / переотправить цену / переотправить остаток / снять привязку» для выбранных.
4. **Лог изменений цены в карточке**: график/история (priceHistory уже пишется) — видеть, почему и когда менялась цена.
5. **Watchdog воркера** (cron `/health` → pm2 restart): PM2 не перезапускает зависший event-loop. Однажды уже стоила суток простоя. Поставить с разрешения.
6. **Алерты в Telegram**: критичные события (воркер упал, очередь >N, массовый архив, ошибка синка) — бот-уведомление админу.
7. **Кэш брендов/поиска**: вынести 1109 брендов в долгий кэш; поиск каталога с дебаунсом уже есть, но добавить «недавние поиски».
8. **Роли тоньше**: сейчас admin/manager. Дать manager доступ к Чатам/Отзывам/Вопросам (саппорт) без цен/настроек.
9. **Авто-ответ AI на типовые вопросы/отзывы** («это оригинал?» → шаблон) с подтверждением оператора.
10. **Бэкап БД**: ежедневный pg_dump в отдельную папку/хранилище.
