# MASTER FIX PROMPT — DavidSklad полная готовность

> Используй этот файл как стартовый контекст для любой сессии.
> Все задачи разделены по приоритету. Работай сверху вниз.

---

## Окружение

- **Прод-сервер**: `root@81.17.154.153`, пароль в `DEPLOY_PASSWORD`
- **Код на сервере**: `/var/www/davidsklad/davidskladik`
- **Деплой**: `DEPLOY_PASSWORD=... node scripts/deploy-prod.cjs`
- **App credentials**: `APP_USER=david`, `APP_PASSWORD=CGJ-Ge-90`
- **Запуск скрипта на проде**: через `run-prod-diag-multi.cjs` (SFTP + SSH)
- **Архитектура**: все `server/parts/*.js` конкатенируются в единый модуль через `server/assemble.js` — функции из любого файла доступны в последующих файлах
- **КРИТИЧЕСКИ**: никогда не трогать Ozon-наценки (`ozonMarkup`). Только Яндекс если требуется
- **Prisma**: UUID-поля — `id::text` или `id::uuid` при сравнении в raw SQL
- **run-prod-diag-multi.cjs**: шаблон для запуска любого скрипта на проде через SFTP+SSH

---

## 🔴 КРИТИЧЕСКИЙ ПРИОРИТЕТ (убыток / потеря данных)

### 1. K18001 [ozon] — продаётся в убыток 61₽

**Проблема**: 2 записи K18001 (Collistar Eye Shadow, не парфюм) на Ozon с ценой 61₽ при себестоимости ~1400₽. Ozon заблокировал отправку правильной цены (ограничение ≤400₽ для этой категории). Stock=5, продажи идут в убыток.

**Что делать**:
1. Обнулить `target_stock = 0` в БД для всех warehouse_products WHERE offer_id = 'K18001' AND marketplace = 'ozon'
2. Отправить stock=0 через `/api/warehouse/prices/send` с `force:true` (это отправит и stock)
3. Проверить что stock на Ozon упал до 0

**Скрипт**: написать `scripts/fix-k18001-zero-ozon.cjs`:
- Query: `SELECT id::text AS id FROM warehouse_products WHERE offer_id='K18001' AND marketplace='ozon'`
- UPDATE target_stock=0
- POST `/api/warehouse/prices/send` с ids

### 2. Высокие цены-аномалии — проверить не ошибка ли расчёта

**Проблема**: найдены товары с ценами 1.2M₽ и 865k₽ при target_stock>0. Неизвестно: это правильная цена (дорогой товар) или ошибка расчёта.

**Что делать**: запустить диагностику:
```sql
SELECT offer_id, marketplace, current_price, target_price,
       raw->>'yandexMarkup' AS ym_markup,
       raw->>'ozonMarkup' AS oz_markup
FROM warehouse_products
WHERE current_price > 500000 AND target_stock > 0
ORDER BY current_price DESC LIMIT 10
```
Для каждого товара проверить PM-цену в `pm_snapshot_items`. Если цена расчётно неверна — сбросить `target_price=NULL`.

### 3. Oversell risk — 3545 товаров

**Проблема**: `target_stock=0` в БД, но маркетплейс показывает `stock>0`. Есть риск продажи без остатка.

**Что делать**: диагностировать root cause:
```sql
SELECT marketplace, COUNT(*) 
FROM warehouse_products
WHERE target_stock=0 
  AND (raw->'marketplaceState'->>'stock')::int > 0
GROUP BY marketplace
```
Затем проверить: это lag синка (маркетплейс ещё не обновился) или реальное расхождение. Если реальное — запустить stock sweep принудительно через POST `/api/warehouse/stock-sweep/run` или аналогичный эндпоинт.

---

## 🟠 ВЫСОКИЙ ПРИОРИТЕТ (качество привязок поставщиков)

### 4. Инна (2155 PM-строк) — 301 товар со стухшими selected_row привязками

**Проблема**: поставщик «Инна» (partner_id нужно найти) имеет 2155 активных строк в PM, но 301 товар имеет stale `selected_row` привязки (sourceRowId исчез из PM, matchType не конвертирован в article). У этих строк нет `raw->>'article'` поэтому fix-stale-row-ids.cjs их пропустил.

**Что делать**: попробовать name-matching — найти PM-строки по названию товара:
1. Для каждого stale-товара взять его warehouse_products.raw->>'name' (или Ozon name)
2. Искать в pm_snapshot_items по partner_id=Инна + нормализованное совпадение имени (ILIKE)
3. Если нашли единственное совпадение — обновить matchType='selected_row' + sourceRowId

Если name-matching невозможен (слишком много совпадений) — конвертировать в matchType='article' используя статичный артикул.

**Скрипт**: `scripts/fix-inna-stale-links.cjs`

### 5. Дмитрий Покровский — товары без PM-строк

**Проблема**: поставщик «Дмитрий Покровский» полностью отсутствует в PM. Его товары имеют product_links но не получают цену.

**Что делать**: найти все его product_links, проверить status (возможно поставщик переименован). Если поставщик действительно исчез — удалить его links чтобы zero-stock automation обнулила товары.

### 6. Дублирующие привязки (43 product+partner комбо с >2 привязками)

**Проблема**: 43 пары product_id+partner_id имеют более 2 записей в product_links. Это может вызывать неверный выбор поставщика.

**Что делать**: 
```sql
SELECT product_id, partner_id, COUNT(*) AS n, 
       array_agg(id) AS link_ids
FROM product_links
WHERE partner_id IS NOT NULL
GROUP BY product_id, partner_id
HAVING COUNT(*) > 2
ORDER BY n DESC
```
Для каждой группы — оставить одну запись (с наибольшим updated_at), остальные удалить. Делать через скрипт с предварительным preview.

### 7. Авто-конвертация selected_row→article в PM-пайплайне

**Проблема**: каждый раз когда поставщик перезаливает прайс, rowId меняется. selected_row привязки стухают. Это случилось с 242 товарами (исправлено вручную). Нужна автоматизация.

**Что делать**: в `writePriceMasterSnapshotToPostgres` (`02a-snapshot-core.js`) после записи нового снапшота:
1. Найти все product_links с matchType='selected_row' где sourceRowId не найден в новом pm_snapshot_items
2. Для каждого: если у ссылки есть article И он найден в новом PM у того же partner_id → обновить matchType='article'
3. Логировать количество авто-конверсий

**Файл**: `02a-snapshot-core.js` функция `writePriceMasterSnapshotToPostgres` (после createMany блока)

---

## 🟡 СРЕДНИЙ ПРИОРИТЕТ (автоматизация и мониторинг)

### 8. Price floor alert — уже реализован ✅

В `02f-health-alert-monitor.js` добавлена метрика `lowPriceSuspect` и алерт в Telegram когда target_price < 500₽ при stock>0.

### 9. Daily stale target_price scan — уже реализован ✅

Функция `runStalePriceTargetScan()` в `02f-daily-maintenance-schedulers.js` сбрасывает target_price=NULL для товаров где target_price < PM×rate×0.5. Запускается в daily sync.

### 10. Partner disappearance alert — уже реализован ✅

В `writePriceMasterSnapshotToPostgres` (`02a-snapshot-core.js`) — детектирует потерю ≥20% строк у поставщика и алертит в Telegram.

### 11. BullMQ UI мониторинг

**Проблема**: failed jobs в BullMQ только видны через диагностические скрипты, нет интерфейса.

**Что делать**: добавить на страницу `/app/settings` (блок «Система») секцию BullMQ:
- GET `/api/bullmq/status` — текущие counts (waiting/active/failed/delayed)
- Кнопка «Очистить failed» — вызывает `queue.clean(0, 10000, 'failed')`
- Показывать последние 5 failed jobs с причиной

**Файлы**: `02d-routes-system-warehouse.js` (новые роуты), `frontend/src/routes/SettingsPage.tsx`

### 12. Алерт в Telegram при ошибке деплоя/рестарта воркера

**Проблема**: воркер может упасть и не подняться, нет уведомления.

**Что делать**: в `02f-health-alert-monitor.js` добавить метрику «воркер не отвечает на /health дольше N минут» (уже есть `staleSweeps` для свипов, расширить на process health).

---

## 🟢 НОРМАЛЬНЫЙ ПРИОРИТЕТ (UX / продуктовые задачи)

### 13. Страница «Финансы» — доделать

**Статус**: backend готов (`02f-finance-orders-sync.js`), frontend существует. Нужно:
- Проверить логи `'finance ... sync failed'` на проде
- Доделать комиссии Яндекса (сейчас только Ozon)
- UI-полировка: фильтры по периоду, поставщику, прибыльности

### 14. Страница «Отзывы» — задеплоить

**Статус**: v1 готов (в коде). Нужно задеплоить и проверить:
- Формы ответов Ozon review/question API
- Формы ответов Yandex goods-feedback

### 15. Bulk-операции в каталоге

**Что делать**: добавить чекбоксы в `WarehousePage.tsx` + ActionBar:
- «Переотправить цену» для выбранных
- «Переотправить остаток» для выбранных
- «Снять привязку» для выбранных
- «Удалить с Яндекса» для выбранных (через `/api/yandex-cleanup/delete-filtered-local`)

### 16. График истории цены в карточке товара

**Что делать**: в `WarehousePage.tsx` (DetailPanel) добавить вкладку «История цены»:
- GET `/api/warehouse/products/:id/price-history` (данные уже пишутся в `price_history` таблицу)
- Простой линейный график (recharts уже используется)
- Показывать: дату, price sent, supplier, rate

### 17. Предупреждение в UI при подозрительной цене

**Что делать**: в карточке товара (`WarehousePage.tsx` DetailPanel) — если `current_price < 500` или `current_price > 100000` при `target_stock > 0` → показывать желтый баннер «Цена подозрительна».

### 18. Индикатор stale selected_row в UI привязки

**Что делать**: в `WarehousePage.tsx` при отображении product_links — если `matchType='selected_row'` и sourceRowId не найден в pm_snapshot_items (нужен бэкенд-флаг) → показывать бейдж «⚠ Привязка устарела» красным.

**Бэкенд**: в GET `/api/warehouse/products/:id` добавить в каждый link поле `staleLink: bool` — проверить EXISTS(sourceRowId in pm_snapshot_items).

---

## 🔵 ИНФРАСТРУКТУРА

### 19. Ротация root-пароля сервера — ручной шаг пользователя

**Причина**: пароль используется в скриптах деплоя, был в нескольких промтах. Риск компрометации.

**Что делать пользователю**:
1. SSH на сервер: `ssh root@81.17.154.153`
2. `passwd root` — ввести новый пароль
3. Обновить `DEPLOY_PASSWORD` в `.env` и `ecosystem.config.cjs`
4. Проверить деплой

### 20. Weekly backup restore drill — ручной шаг

**Причина**: pg_dump настроен, но восстановление никогда не тестировалось.

**Что делать**: на staging или локально:
```bash
pg_restore -d davidsklad_test /path/to/backup.dump
```
Проверить что данные восстановились корректно.

### 21. BullMQ startup cleanup

**Проблема**: при рестарте воркера old jobs остаются в active без lock (orphaned). Через 15 минут стейлятся.

**Что делать**: в `03-lifecycle-start.js` при запуске воркера — запускать `fix-bullmq-clean.cjs` логику (clean orphaned active jobs + clean failed). Уже есть скрипт `scripts/fix-bullmq-clean.cjs`, нужно встроить в startup.

---

## 📋 СПРАВОЧНИК КЛЮЧЕВЫХ ФАЙЛОВ

```
server/parts/
  02a-snapshot-core.js          — writePriceMasterSnapshotToPostgres, partner drop alert
  02a-price-master-warehouse-build.js — pickWarehouseSupplier, pinnedRow logic
  02a-price-master-warehouse-helpers.js — compareWarehouseSupplierPrices
  02f-price-sweep.js            — runChangedPriceSweep (каждые 120с)
  02f-stock-sweep.js            — runStockSweep (каждые 180с)
  02f-zero-stock-sweep.js       — runZeroStockSweep (каждые 180с)
  02f-health-alert-monitor.js   — evaluateHealthAlerts, sendHealthAlertTelegram
  02f-daily-maintenance-schedulers.js — runDailyRefresh, runStalePriceTargetScan

scripts/
  fix-stale-row-ids.cjs         — групповое исправление stale selected_row → article
  fix-pinned-over-cheaper.cjs   — исправление pinned поставщика дороже артикла
  fix-low-prices-send.cjs       — отправка цен для товаров с price<500₽
  diag-low-prices.cjs           — диагностика низких цен
  audit-critical.cjs            — комплексный аудит (9 секций)
  run-prod-diag-multi.cjs       — шаблон SFTP+SSH для запуска скрипта на проде

frontend/src/
  routes/WarehousePage.tsx       — основная страница склада + карточка товара
  components/PmChipInput.tsx     — глобальный chip-поиск PM (key=draftScopeKey для сброса)
  lib/pmSearchStore.ts           — store фишек поиска
```

## 📋 СПРАВОЧНИК КЛЮЧЕВЫХ ПАТТЕРНОВ

### Запуск скрипта на проде
```javascript
// В run-prod-diag-multi.cjs:
await sftpPut(conn, path.join(root, "scripts/MY_SCRIPT.cjs"), `${remoteRoot}/scripts/MY_SCRIPT.cjs`);
await exec(conn, `cd ${remoteRoot} && node scripts/MY_SCRIPT.cjs`, 120000);
```

### Правильный UPDATE в raw SQL с UUID
```sql
UPDATE warehouse_products SET target_stock = 0, updated_at = now()
WHERE id::text = ANY($1)  -- передать массив строк
-- НЕ использовать 'id'::uuid — поля id бывают строками типа 'ozon-...'
```

### Проверка PM-цены через JOIN
```sql
SELECT pm.price, pm.partner_name, pm.active
FROM product_links pl
JOIN pm_snapshot_items pm ON (
  (pl.raw->>'matchType' = 'selected_row' AND pm.row_id = (pl.raw->>'sourceRowId'))
  OR
  (pl.raw->>'matchType' = 'article'
   AND pm.partner_id::text = pl.partner_id::text
   AND pm.article = COALESCE(NULLIF(pl.raw->>'article',''), pl.supplier_article))
)
WHERE pl.product_id = $1
```

### price sweep: почему может не ловить
- Если `current_price == target_price` — sweep не трогает (нет смысла)
- Ошибка была: target_price записан с rate≈1 → равен current → sweep молчит
- Фикс: `runStalePriceTargetScan` сбрасывает target_price=NULL → sweep пересчитывает

### selectedSupplier — НИКОГДА не хранится в raw
- `selectedSupplier` — вычисляемое поле, результат `pickWarehouseSupplier()`
- В SQL запросах `raw->>'selectedSupplier'` всегда NULL → false alarm в диагностиках
- Для проверки наличия поставщика: `EXISTS (SELECT 1 FROM product_links pl WHERE pl.product_id=wp.id::text)`

---

## ✅ ЧТО УЖЕ СДЕЛАНО И РАБОТАЕТ

- **Price sweep** (каждые 120с) — авто-отправка цен при current≠target
- **Stock sweep** (каждые 180с) — восстановление остатков после заказа  
- **Zero-stock sweep** (каждые 180с) — обнуление при потере поставщика
- **PM snapshot** (каждые 12 мин) — обновление цен PM, partner drop alert
- **WB sync** (каждые 3ч) — синхронизация Wildberries
- **Daily stale target scan** (ежедневно) — сброс неверных target_price
- **Health alert monitor** (каждые 5 мин) — Telegram алерты по порогам
- **BullMQ cleanup** — стухшие и failed jobs очищены
- **PmChipInput** — сбрасывается при смене карточки (`key=draftScopeKey`)
- **242 stale selected_row** — исправлены на matchType='article'
- **105 товаров** с более дешёвым артикульным поставщиком — исправлены ($5+ разница)
- **Цены ЯМ** — отправлены для всех исправленных товаров
- **WCAG AA** — контраст, keyboard navigation, адаптив
- **Оповещения на сайте** — колокольчик + звук + SSE
- **Дашборд** — продажи, прибыль, health indicators
- **Чаты, Вопросы, Отзывы** — обе маркетплейсы
- **Восстановление** (recovery queue) — Ozon и ЯМ
