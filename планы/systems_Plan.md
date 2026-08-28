# SystemPage — Plan

## Current State

Страница технического состояния системы. Показывает: health-карточки (PostgreSQL, Redis, PriceMaster, Memory), BullMQ очереди, daily sync, auto prices, Ozon recovery-очередь, медленные endpoints, state warnings, журнал поставщиков (supplierLedger). Данные обновляются каждые 30 секунд. Страница только для чтения — нет мутаций.

---

## Bugs / Issues Found

### 1. `components` читается из `health.components`, но сервер возвращает компоненты и в `status.data.queues` — данные дублируются в разных полях

**Строки 35–41 (frontend), 61–97 (backend).**
```tsx
const health = asRecord(status.data?.health);
const components = asRecord(health.components);
const queues = asRecord(status.data?.queues);
```
Сервер в `response.json` возвращает `queues.marketplace = bullmq.counts || components.redis?.counts || null` (строка backend 69). Frontend также читает `marketplaceQueue = asRecord(queues.marketplace)`. Поле `bullmq` на сервере содержит `counts`, которые дублируются в `queues.marketplace`. Если `bullmq` вернёт данные, а `components.redis` нет — `StatusCard` "BullMQ jobs" и "Redis/BullMQ" могут показывать разные данные об одной и той же системе.

### 2. `StatusCard "Redis/BullMQ"` не показывает детали очереди — пустое поле `detail`

**Строка 75.**
```tsx
<StatusCard label="Redis/BullMQ" value={asRecord(components.redis).ok === false ? "error" : "ok"} tone={...} />
```
Нет `detail` пропа — при проблемах с Redis пользователь видит только "error" без контекста (количество failed jobs, ошибка подключения). Сравним с "BullMQ jobs" (строка 78), который показывает waiting/delayed/failed детально.

### 3. Дублирование данных: "Retry цен" показывается дважды — в `Stat` (строка 70) и в `StatusCard` (строка 82) с идентичными значениями

**Строки 70 и 82.**
```tsx
// В Stat (dashboard metrics):
<Stat label="Retry цен" value={Number(priceRetry.total || 0)} .../>
// В StatusCard:
<StatusCard label="Retry цен" value={Number(priceRetry.total || 0)} />
```
Одна и та же метрика показывается дважды без дополнительной информации во втором месте. `StatusCard` для retry не имеет ни `detail`, ни `tone`.

### 4. `stateWarnings` список реверсируется в `.slice().reverse()` на каждый рендер без мемоизации

**Строка 122.**
```tsx
{stateWarnings.slice().reverse().map((row, index) => ...)}
```
`slice().reverse()` создаёт новый массив при каждом рендере. При 30 записях это незначительно, но паттерн стоит заменить на `useMemo` или хранить уже реверсированный список.

### 5. `operationsPage` frontend запрашивает `/api/system/status`, но системный роут — `requireAdmin`, а `SystemPage` не проверяет роль

**Backend строка 1, frontend строка 30–34.** Сервер защищает `GET /api/system/status` через `requireAdmin`. Если пользователь без роли admin откроет страницу (напрямую по URL) — получит 401/403. Frontend не обрабатывает этот сценарий специально: `status.error` покажет generic `inline-error`, не поясняя что нужны права admin.

### 6. Поле `status.data?.supplierLedger` показывает `missingDebtEntries` как ошибку — но на сервере это расчётное число, не реальная ошибка

**Строки 132–146 (frontend), backend 22–24.**
Сервер считает `missingDebtEntries = Math.max(0, picked.length - linkedDebts)` — это разница между выборкой из 5000 строк и реально связанными долгами. Это не всегда ошибка: если `picked.length > sampledPickedRows` (запрос ограничен `take: 5000`), то расчёт будет неточным. Frontend показывает это число без контекста о лимите выборки.

### 7. `slowRequests` используется нестабильно: берётся как `slowEndpoints.recent` или `status.data?.slowRequests`

**Строки 49.**
```tsx
const slowRequests = list(slowEndpoints.recent || status.data?.slowRequests);
```
Сервер возвращает `slowRequests: slowEndpoints.recent` (строка backend 95) и `slowEndpoints: summarizeRecentSlowRequests(30)` (строка 55). Frontend обращается к обоим. При изменении структуры ответа — одно из двух перестанет работать, но ошибки не будет, просто пустой массив.

### 8. `StatusCard "Daily sync"` не показывает `tone` — нет цветовой индикации при ошибке или пропуске синхронизации

**Строка 79.**
```tsx
<StatusCard label="Daily sync" value={text(daily.status) || "-"} detail={dateText(daily.lastRunAt)} />
```
`tone` не передаётся. Если `daily.status === "error"` — карточка останется нейтрально серой. Нужно `tone={daily.status === "error" ? "danger" : daily.status === "running" ? "warn" : "neutral"}`.

### 9. Таблица `components` отображает `row.accounts || row.shops` как детали — может показать длинную JSON-строку

**Строка 102.**
```tsx
text(row.error || row.mode || row.queueMode || row.accounts || row.shops)
```
`row.accounts` или `row.shops` могут быть массивами или объектами. `text()` вызовет `String(value)`, что для массива даст `"[object Object],[object Object]"` или `"value1,value2"`. Нет явного форматирования.

### 10. Backend: `supplierLedgerDiagnostics` делает 4 параллельных Prisma-запроса включая `findMany` с `take: 5000` — тяжёлый запрос при каждом GET /api/system/status

**Backend строки 7–12.**
```js
const [pickedRows, debtEntries, recentPayments, picked] = await Promise.all([
  prisma.supplierPickingRow.count(...),
  prisma.supplierLedgerEntry.count(...),
  prisma.supplierLedgerEntry.findMany(...take: 10),
  prisma.supplierPickingRow.findMany({...take: 5000}),
]);
```
`findMany` с `take: 5000` запускается при каждом запросе к `/api/system/status`, который обновляется каждые 30 секунд. Это 5000 строк из БД каждые 30 секунд только для диагностики. Стоит кэшировать результат или выполнять реже.

---

## Improvement Ideas

- **[HIGH]** `supplierLedgerDiagnostics` backend: добавить кэш (например 5 минут) для `findMany take: 5000`, не делать тяжёлый запрос при каждом обращении.
- **[HIGH]** `StatusCard "Daily sync"`: добавить `tone` по значению `daily.status` для визуальной индикации ошибок.
- **[MEDIUM]** `StatusCard "Redis/BullMQ"`: добавить `detail` с числом failed/waiting jobs, как у "BullMQ jobs".
- **[MEDIUM]** Убрать дублирующий `StatusCard "Retry цен"` (строка 82) или добавить ему уникальные данные (detailAt, lastError из priceRetry).
- **[MEDIUM]** Добавить `tone` для `StatusCard "Auto prices"` — если `salesAutomation.pmTimeout > 0`, показывать warn.
- **[MEDIUM]** `row.accounts || row.shops` в таблице компонентов: форматировать как `count` или первый элемент, а не сырой `String()`.
- **[MEDIUM]** Добавить обработку 401/403 для `status.error` — показывать сообщение «Доступ только для администратора» вместо generic ошибки.
- **[LOW]** `stateWarnings.slice().reverse()` — заменить на `useMemo(() => [...stateWarnings].reverse(), [stateWarnings])`.
- **[LOW]** Добавить кнопку "Очистить state warnings" (POST /api/system/clear-warnings) — сейчас накопленные предупреждения не очищаются вручную.
- **[LOW]** Показать `missingDebtEntries` с пояснением: «Из выборки 5000 строк — может быть неточным при большом объёме».

## Code Notes

- Frontend: `frontend/src/routes/SystemPage.tsx`
- Backend: `server/parts/02d-routes-system-warehouse.js` — роут `GET /api/system/status`
- Backend (health): функция `collectHealthDetails`
- Backend (queues): функция `marketplaceQueueCounts`, `readSalesAutomationSystemSummary`
