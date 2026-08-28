# PricesPage — Plan

## Current State

Страница контроля цен и остатков. Показывает сводку автоматизации (summary), таблицу SKU со статусами, фильтры по маркетплейсу/причине/статусу отправки, быстрые фильтры (chips), и набор кнопок для принудительного пересчёта/отправки цен. Данные обновляются каждые 45 секунд. Страница содержит кнопки "Force" с `danger-action` классом.

---

## Bugs / Issues Found

### 1. `useEffect` при монтировании автоматически запускает мутацию `run` — без флага загрузки

**Строки 96–99.**
```tsx
useEffect(() => {
  run.mutate({ marketplace: "all", force: true, onlyChanged: false, reason: "prices_page_open_auto" });
}, []);
```
При каждом открытии страницы немедленно запускается `run.mutate` с `force: true, onlyChanged: false` — это пересчёт ВСЕХ товаров на всех маркетплейсах. Если пользователь открыл страницу случайно или несколько раз быстро — каждый раз запускается тяжёлая операция. Нет дебаунса, нет проверки «уже запущено», нет предупреждения пользователю.

### 2. Один shared `run`-мутация на все кнопки — `isPending` блокирует весь UI

**Строки 78–94, 193–207.** Единственный экземпляр `useMutation` (`run`) используется для 5 разных кнопок: "Пересчитать выбранное", "ОТПРАВИТЬ ВСЕ СЕЙЧАС", "Force Ozon", "Force Yandex", "Retry ошибки". Когда одна кнопка нажата и `run.isPending === true` — все остальные кнопки блокируются через `disabled={run.isPending}`. Пользователь не может запустить "Retry ошибки" пока ждёт другого запроса.

### 3. `run.data` отображает результат последней мутации — при нажатии другой кнопки показывает устаревший результат

**Строки 212–218.** `{run.data ? (<div ...>...)}` — `run.data` хранит результат последней успешной мутации. Если нажать "Force Ozon" (queued 50 SKU), потом "Retry ошибки" (sent 3), то показывается результат "Retry", но предыдущий "Force Ozon" уже в строке без подтверждения.

### 4. `yandexIssues` и `ozonIssues` считаются по полному списку items (до 500 строк) — тяжёлые useMemo

**Строки 113–114.**
```tsx
const yandexIssues = useMemo(() => items.filter(...).length, [items]);
const ozonIssues = useMemo(() => items.filter(...).length, [items]);
```
`items` может содержать до 500 элементов (`limit: "500"` в строке 72). При каждом изменении `items` оба `useMemo` полностью перефильтровывают массив дважды. Лучше объединить в один проход.

### 5. Limit захардкожен в 500, не контролируется пользователем, не показан в UI

**Строка 72.** `params.set("limit", "500")` — пользователь видит таблицу максимум из 500 строк без пагинации и без возможности изменить лимит. При > 500 SKU часть данных теряется из вида.

### 6. `formatDate` локализует время для `ru-RU`, но поле "последний расчет" показывает дату `summary.data?.updatedAt` — может быть `undefined`

**Строки 25–29, 139.** `formatDate(summary.data?.updatedAt)` — если `updatedAt` отсутствует, `formatDate` получает `undefined`, возвращает `"-"`. Не критично, но для пустого состояния ("сводка ещё не загружена") стоит показывать скелет или "Ещё не запускалось", а не просто дефис.

### 7. Кнопка "ОТПРАВИТЬ ВСЕ СЕЙЧАС" не имеет подтверждения `window.confirm`

**Строка 196.** Кнопка с классом `danger-action` и `force: true, onlyChanged: false` для `marketplace: "all"` не требует подтверждения. В `OperationsPage` аналогичные кнопки `ozon-linked-unarchive` и `repair-pricemaster-group-links` имеют `window.confirm`. Это самое опасное действие на странице (пересчёт и отправка всех цен) — без защиты от случайного клика.

### 8. `quickFilters` — статически объявленный массив с захардкоженными reason/status строками

**Строки 103–110.** Быстрые фильтры объявлены вне компонента с литеральными строками вроде `"ozon_price_not_applied"`, `"pm_live_timeout"`. Если на сервере появится новый reason — его не будет в quickFilters. Список `reasonOptions` строится динамически из `summary.data?.reasons`, но quickFilters не обновляется.

### 9. Таблица строк не имеет сортировки и не показывает общее количество

**Строки 244–265.** `<div className="table-panel ...">` — заголовок таблицы есть, но нет кликабельных колонок для сортировки, нет счётчика «Показано X из Y». При 500 строках без сортировки найти нужный SKU очень трудно.

### 10. `supplierName` функция игнорирует `item.raw.selectedSupplier` если `item.selectedSupplier` пустой

**Строки 15–18.**
```js
const supplierName = (item) => {
  const supplier = asRecord(itemValue(item, "selectedSupplier"));
  return text(supplier.partnerName || supplier.supplierName || supplier.name) || "-";
};
```
`itemValue(item, key)` смотрит сначала в `item[key]`, потом в `asRecord(item.raw)[key]`. Однако для вложенных объектов `selectedSupplier` может содержать разные поля (`partnerName` vs `supplierName`). Если сервер вернул данные в `item.raw.selectedSupplier` под другим ключом — функция вернёт `"-"` вместо имени.

---

## Improvement Ideas

- **[HIGH]** Убрать автоматический `run.mutate` при открытии страницы (`useEffect` строки 96–99) или заменить на passive refetch сводки без force-пересчёта. Если нужен прогрев — добавить флаг `initialRunDone` и не повторять при повторных открытиях.
- **[HIGH]** Добавить `window.confirm` на кнопку "ОТПРАВИТЬ ВСЕ СЕЙЧАС" (строка 196), аналогично другим опасным действиям в системе.
- **[HIGH]** Разделить мутацию `run` на несколько (или использовать `variables` из `useMutation`) чтобы разные кнопки не блокировали друг друга.
- **[MEDIUM]** Объединить два `useMemo` `yandexIssues` и `ozonIssues` в один проход по `items`.
- **[MEDIUM]** Добавить элемент управления лимитом (select 100/500/1000) и показать счётчик "Показано X из Y" в шапке таблицы.
- **[MEDIUM]** Добавить клик по колонкам таблицы для сортировки (хотя бы по offerId, targetPrice, reason).
- **[MEDIUM]** `run.data` — сбрасывать при переключении между кнопками или хранить результаты по action-type, чтобы не показывать устаревший результат.
- **[LOW]** `quickFilters` — генерировать динамически из `reasonOptions`, добавляя только те фильтры, у которых `count > 0`.
- **[LOW]** Добавить поиск по `offerId` прямо в таблице (строка фильтрации без запроса к серверу — просто `items.filter`).

## Code Notes

- Frontend: `frontend/src/routes/PricesPage.tsx`
- Backend (сводка): `server/parts/02d-prices-stats-automation.js` — функция `readSalesAutomationSystemSummary`
- Backend (отправка): `server/parts/02d-prices-send-warehouse-marketplace.js`
- Backend (роут /run): `app.post("/api/sales-automation/run", ...)` в prices-stats-automation
