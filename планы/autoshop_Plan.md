# SupplierCartPage (Автокорзина) — Plan

## Current State

Страница управления автоматической закупкой. Три вкладки: «Корзина» (основная панель из `OperationsPage`), «Готовы к отгрузке» (`ReadyToShipPanel`) и «История PM». Включает/выключает авторежим, настраивает маркетплейсы и расписание, показывает PM-статус/диагностику, позволяет откатить корзину. Встроенная панель PM-поиска позволяет вручную найти товар в снапшоте PriceMaster и добавить заявку. Вкладка «Готовы к отгрузке» синхронизирует активные заказы с маркетплейсов (Ozon/Yandex/WB) со списком уже собранных товаров.

---

## Bugs / Issues Found

### 1. `PmHistoryPanel` не перезапрашивает данные при открытии вкладки

**Файл:** `SupplierCartPage.tsx`, строки 767–811

```tsx
const historyQuery = useQuery({
  queryKey: ["supplier-cart", "pm-history"],
  queryFn: () => fetchJson("/api/supplier-cart/pm-history?limit=20", PmHistorySchema),
  staleTime: 30_000,
});
```

`PmHistoryPanel` — компонент без пропсов, рендерится только при `tab === "pm-history"` (строка 1043). При `staleTime: 30_000` данные могут быть стейлными в момент первого открытия вкладки. При монтировании TanStack Query автоматически рефетчит только если данных ещё нет или они старше `staleTime`. Но `PmHistoryPanel` монтируется и демонтируется при каждом переключении вкладок, поэтому кэш сохраняется между посещениями. Пользователь может видеть устаревший список (до 30 секунд) без понимания, что он неактуален. При этом нет ни индикатора времени последнего обновления, ни авторефреш-интервала.

### 2. Ошибки rollback отображаются через `String(error)` вместо `errorMessage()`

**Файл:** `SupplierCartPage.tsx`, строки 1026–1027

```tsx
{rollbackDryRun.error ? <div className="inline-error">{String(rollbackDryRun.error)}</div> : null}
{rollbackApply.error ? <div className="inline-error">{String(rollbackApply.error)}</div> : null}
```

Везде в проекте используется хелпер `errorMessage(error)` (импортирован в строке 12). Здесь вместо него — `String(error)`, что даёт `"[object Object]"` для объектов-ошибок. Это приведёт к нечитаемому сообщению в UI при любой серверной ошибке отката.

То же самое для `pmStatus.error` (строка 1004):

```tsx
{pmStatus.error ? <div className="inline-error">{String(pmStatus.error)}</div> : null}
```

### 3. `selectedKeys` в `ReadyToShipPanel` не сбрасывается при смене поискового запроса

**Файл:** `SupplierCartPage.tsx`, строки 167–169, 330–337

```tsx
const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
...
<button onClick={() => setSelectedKeys(selectedKeys.size === filteredMpLines.length ? new Set() : new Set(filteredMpLines.map(l => l.key)))} >
  {selectedKeys.size === filteredMpLines.length ? "Снять всё" : "Выбрать всё"}
</button>
```

При вводе нового поискового запроса `q` список `filteredMpLines` меняется, но `selectedKeys` не сбрасывается. Если выбрано «Выбрать всё» с 10 позициями, а потом поиск отфильтровал список до 3 — в `selectedKeys` остаются 10 ключей. Кнопка «В PM — N шт.» (строка 326) отправляет только `filteredMpLines.filter(l => selectedKeys.has(l.key))`, то есть реально отправит 3 позиции, но счётчик на кнопке показывает `10 шт.` — вводит в заблуждение. Нужно сбрасывать `selectedKeys` при изменении `q` через `useEffect`.

### 4. `batch-order` endpoint теряет `accountName` при конфликте ключей

**Файл:** `02f-supplier-cart-routes.js`, строки 608–631

```js
if (state.processed?.[lineKey]) { failed.push({ key: lineKey, offerId, reason: "already_committed" }); continue; }
```

При пакетном заказе (`/api/ready-to-ship/batch-order`) ключ строки берётся из `line.key`, которое в `ReadyToShipPanel` формируется на клиенте как `line.key` из маркетплейс-ответа. Если тот же заказ уже закоммичен, он попадает в `failed` с reason `already_committed`. На фронтенде `batchOrderMutation.data` показывает `failed: N` (строка 343), но **нет** раскрытия деталей о том, какие именно заказы упали и почему. Оператор не узнает, какой из N позиций уже в PM.

### 5. `PmSearchPanel` — `sortedItems` применяет двойную сортировку тестеров

**Файл:** `SupplierCartPage.tsx`, строки 567–573

```tsx
const sortedItems = useMemo(() => {
  let list = supplierFilter ? allItems.filter(...) : allItems;
  if (sortMode === "price_asc") list = [...list].sort((a, b) => priceRub(a) - priceRub(b));
  // ...
  return [...list].sort((a, b) => Number(isTester(a)) - Number(isTester(b)));
}, [...]);
```

Сначала список сортируется по выбранному `sortMode`, а затем **поверх** применяется сортировка тестеров через второй `sort()`. Второй `sort()` нестабилен в разных движках JS (ES2019+ гарантирует stable sort в V8, но порядок внутри не-тестеров сохраняется). Проблема в том, что `isTester` на фронтенде (строка 563) и `isTesterName` на бэкенде (строки 329–332) используют **разные** regex-паттерны:

- Фронтенд: `/\btest(?:er|ep|or|r)?\b|тестер/i`
- Бэкенд: `/\btest(?:er|ep|or|r)?\b/` + проверка `n.includes("отливант")` и `n.includes("тест")`

Фронтенд не проверяет `"отливант"` и `n.includes("тест")` без границ слова. Товар с названием «Тестер Chanel» не будет помечен как tester на фронтенде (regex требует `\b` перед `тестер`), но будет на бэкенде. Это приводит к тому, что в PM-поиске такой товар **не** уедет вниз списка.

### 6. `pm-search` endpoint: `limit` вычисляется через `cleanLimit(request.query.limit, 80, 200)`, но клиент запрашивает `limit=150`

**Файл:** `02f-supplier-cart-routes.js`, строка 265

```js
const limit = cleanLimit(request.query.limit, 80, 200);
```

Клиент (строка 543 в SupplierCartPage.tsx) посылает `limit=150`. Если `cleanLimit(150, 80, 200)` интерпретирует `80` как default и `200` как max, то `150` пройдёт. Но нужно проверить реализацию `cleanLimit` — если сигнатура `(value, default, max)`, всё ок; если `(value, max, default)` — результат будет `80` вместо `150`. Потенциальный silent bug в зависимости от порядка аргументов.

### 7. `ReadyToShipPanel` — `revertMutation` не инвалидирует `supplier-cart-history`

**Файл:** `SupplierCartPage.tsx`, строки 238–244

```tsx
const revertMutation = useMutation({
  ...
  onSuccess: () => {
    void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
  },
});
```

При возврате строки к сборке (revert) инвалидируется только `supplier-picking-list`. Для сравнения: `revertAndReplaceMutation` (строки 254–263) дополнительно инвалидирует `supplier-cart-history`, `supplier-cart-draft` и `suppliers`. Простой revert без замены поставщика этого не делает, хотя тоже меняет состояние корзины.

### 8. Откат (`rollback-all`) — кнопка «Откатить» активируется только по `rollbackCount(dryRun) === 0`

**Файл:** `SupplierCartPage.tsx`, строки 1009–1017

```tsx
<button
  disabled={rollbackApply.isPending || rollbackCount(dryRun) === 0}
  onClick={() => rollbackApply.mutate()}
>
```

Условие `rollbackCount(dryRun) === 0` означает: кнопка активна только если **есть что откатывать**. Но после успешного `rollbackApply` данные `dryRun` не сбрасываются (они из `rollbackDryRun.data`), и кнопка остаётся активной (count > 0 из устаревшего dry-run). Повторное нажатие запустит второй rollback. `rollbackApply.onSuccess` вызывает `rollbackDryRun.reset()` (строка 861), но это происходит **после** ренедера, то есть между успешным откатом и сбросом dry-run есть окно, когда кнопка активна. Низкая вероятность двойного нажатия, но существует.

---

## Improvement Ideas

- **[HIGH]** Исправить `String(error)` → `errorMessage(error)` для ошибок rollback и pm-status (строки 1004, 1026–1027).
- **[HIGH]** Синхронизировать regex `isTester` между фронтендом (`SupplierCartPage.tsx:563`) и бэкендом (`02f-supplier-cart-routes.js:329–332`) — вынести в общую утилиту или дублировать полный паттерн.
- **[MEDIUM]** Сбрасывать `selectedKeys` в `ReadyToShipPanel` при изменении `q` через `useEffect([q])`.
- **[MEDIUM]** В `PmHistoryPanel` добавить `refetchInterval: 60_000` или явный timestamp последнего обновления.
- **[MEDIUM]** В результатах пакетного заказа (`batchOrderMutation.data`) показывать `failedDetails` — список упавших позиций с причиной (данные уже есть в ответе сервера, `BatchOrderResultSchema` содержит `failedDetails`).
- **[MEDIUM]** После успешного `rollbackApply` немедленно инвалидировать `rollbackDryRun` или отключить кнопку до следующего dry-run.
- **[LOW]** `revertMutation.onSuccess` — добавить инвалидацию `supplier-cart-history` и `suppliers` по аналогии с `revertAndReplaceMutation`.
- **[LOW]** Проверить и задокументировать порядок аргументов `cleanLimit(value, default, max)` во всех вызовах в `02f-supplier-cart-routes.js`.
- **[LOW]** Кнопка «Нету у поставщика» в `ReadyToShipPanel` использует иконку `<Package size={14} />` (строка 404) — семантически неверно для действия «товар отсутствует». Лучше `AlertTriangle` или `X`.

---

## Code Notes

- Frontend: `frontend/src/routes/SupplierCartPage.tsx`
- Backend (роуты корзины): `server/parts/02f-supplier-cart-routes.js`
- Backend (состояние корзины): `server/parts/02d-supplier-cart-state.js`
- Backend (построение): `server/parts/02d-supplier-cart-build.js`
