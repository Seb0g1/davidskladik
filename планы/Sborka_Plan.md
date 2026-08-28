# PickingListPage (Сборка) — Plan

## Current State

Страница управления листом сборки. Оператор видит открытые позиции по поставщикам, отмечает «Собрал» или «Не было», указывает фактическую стоимость. Поддерживает перенос позиции на завтра, замену поставщика, просмотр листов сборки по датам, внутреннюю накладную и систему балансов сборщиков (выдача наличных + дневной итог). Администратор может выдавать/принимать деньги сборщикам, редактировать и удалять записи.

---

## Bugs / Issues Found

### 1. Дублирование `updateMutation` в `"Все собрал"` — параллельные мутации без защиты

**Файл:** `PickingListPage.tsx`, строки 1064–1066

```tsx
onClick={(e) => { e.stopPropagation(); openRows.forEach(r => updateMutation.mutate(...)); }}
```

`updateMutation` — одна общая мутация на весь компонент (`useMutation`). Когда в группе товара несколько открытых заказов, они отправляются пачкой через одну мутацию. После первого `.mutate()` TanStack Query переводит мутацию в `isPending`, а внутренние `variables` перезаписываются каждым следующим вызовом. Состояние `updateMutation.isPending` блокирует **все** кнопки сборки на странице (строка 1148: `disabled={updateMutation.isPending}`), а не только строки конкретной группы. Пока идёт массовый вызов, оператор не может отметить ни одну другую позицию.

### 2. `document.title` не восстанавливается при размонтировании компонента

**Файл:** `PickingListPage.tsx`, строки 283–288

```tsx
useEffect(() => {
  const prev = document.title.replace(/^\(\d+\)\s*/, "");
  document.title = openCount > 0 ? `(${openCount}) ${prev}` : prev;
  return () => { document.title = document.title.replace(/^\(\d+\)\s*/, ""); };
}, [listQuery.data?.summary]);
```

Cleanup-функция читает `document.title` в момент размонтирования (закрытие), но к тому времени значение уже может быть `"(5) Сборка"` из последнего рендера. Она убирает цифры, но только из текущего значения title. Если пользователь переключился на другую страницу, изменившую title, эффект при unmount неправильно его перезапишет (удалит числовой префикс чужой страницы). Нужно захватывать `prev` до установки и возвращать именно его.

### 3. `balanceStr` отображает $ для данных, которые могут быть в рублях

**Файл:** `PickingListPage.tsx`, строка 429

```tsx
const balanceStr = (n: number) => `$${Math.round(n).toLocaleString("ru-RU")}`;
```

Баланс сборщика (`credits`) хранится в `amount` того номинала, в каком была выдана сумма — см. `02d-picker-cash.js` строки 161–173: `amount` берётся из тела запроса без принудительного перевода в USD. Дневной итог (`dailyTotal`) в `loadDailyCartTotal` (строка 250) хранится в сырых единицах `price * qty` из поля `current.price` пикинг-строки, которое может быть в USD **или** в рублях в зависимости от `priceCurrency` поставщика. Функция `balanceStr` безусловно рисует `$`, хотя для RUB-поставщиков числа уже в рублях. Отображение вводит в заблуждение.

### 4. Picker deduct amount в `picking-routes.js` использует `row.price` (может быть RUB), но вычитается из баланса сборщика как если бы это USD

**Файл:** `02f-supplier-picking-routes.js`, строки 210–213

```js
const pickerDeductAmt = nextRow.price > 0
  ? nextRow.price * Math.max(1, Math.round(Number(nextRow.quantity || 1)))
  : 0;
```

`nextRow.price` — это закупочная цена в валюте поставщика (`priceCurrency`). Для RUB-поставщиков это рубли, но вычитается одинаковым образом. Баланс сборщика отображается с символом `$` (см. баг #3). Нет нигде конвертации через `usdRate`. В результате при работе с RUB-поставщиком сборщику вычитается из баланса сумма в рублях, хотя баланс визуально показан как долларовый. При балансе, например, `$500`, после закупки у RUB-поставщика на `₽3000` остаток показывается как `$-2500$` вместо реального `$-31$` (при курсе ≈95).

### 5. `returnCashMutation` не инвалидирует `["picker-balance", "me"]`

**Файл:** `PickingListPage.tsx`, строки 260–270

```tsx
onSuccess: (_data, vars) => {
  ...
  void queryClient.invalidateQueries({ queryKey: ["picker-balance", vars.pickerUsername] });
  void queryClient.invalidateQueries({ queryKey: ["picker-my-day"] });
},
```

Инвалидируется `["picker-balance", vars.pickerUsername]`, но **не** `["picker-balance", "me"]`. Если администратор нажал «Возврат» у сборщика, чьё имя совпадает с текущим пользователем, баланс у него в шапке (`myBalanceQuery`) не обновится. Нужно добавить `{ queryKey: ["picker-balance"] }` (широкий ключ), как сделано в `issueBalanceMutation` (строки 146–147).

### 6. `viewers` из Redis: `redis.keys()` — O(N) на продакшне без ограничения

**Файл:** `02f-supplier-picking-routes.js`, строки 419–430

```js
const keys = await redis.keys("picking:viewer:*");
```

`KEYS` в Redis блокирует сервер на время выполнения. При большом количестве ключей в Redis (даже не связанных с picking) это вызывает задержки. Нужно использовать `SCAN` с паттерном. Вызывается раз в 30 секунд с каждого клиента.

### 7. `collapsedProductGroups` / `expandedRows` не сбрасываются при смене фильтра

**Файл:** `PickingListPage.tsx`, строки 79–81

```tsx
const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());
const [collapsedProductGroups, setCollapsedProductGroups] = useState<Set<string>>(new Set());
```

При смене `status` или `supplier` список товаров полностью меняется, но множества `expandedRows` и `collapsedProductGroups` содержат ключи из старого состояния. Старые ключи безвредны (лишняя память), но при случайном совпадении ключей (маловероятно) может открыться/закрыться неправильная строка. Лучше сбрасывать при смене фильтров через `useEffect`.

### 8. `totalCost` в листах сборки считается без учёта `priceCurrency`

**Файл:** `PickingListPage.tsx`, строки 810–811

```tsx
const totalCost = sheetRows.reduce((sum, row) => sum + (row.price || 0) * (row.quantity || 1), 0);
```

Затем выводится через `moneyAmount(totalCost)` с дефолтным `currency="USD"`. Если в листе есть строки от RUB-поставщиков, их цены суммируются вместе с USD-ценами и всё показывается в `$`. Та же смешанная-валютная проблема, что в `currentGroupTotal`.

---

## Improvement Ideas

- **[HIGH]** Разбить `updateMutation` на per-row мутации (или использовать `useMutation` с отдельным state per key), чтобы кнопка «Собрал» блокировалась только у конкретной строки, а не у всех на странице.
- **[HIGH]** Добавить явную валюту к балансу сборщика: либо хранить всё в RUB (конвертировать при вычете через `usdRate`), либо явно писать в UI символ валюты из записи, а не hardcode `$`. но учти что у нас есть поставщики типо "Инна" которые принимают оплату в рублях. на странице поставщики это помечено. 
- **[MEDIUM]** `document.title` effect: захватить `prev` до установки нового значения и возвращать именно его в cleanup, чтобы не зависеть от стороннего изменения title.
- **[MEDIUM]** `redis.keys()` заменить на `redis.scan()` в `GET /api/supplier-picking-list/viewers`.
- **[MEDIUM]** При смене `status`/`supplier` фильтра сбрасывать `expandedRows` и `collapsedProductGroups` через `useEffect`.
- **[MEDIUM]** В листах сборки (`view === "sheets"`) считать `totalCost` с учётом `priceCurrency`: смешивать через `currentGroupTotalRub` или показывать отдельно USD и RUB суммы.
- **[LOW]** Добавить `["picker-balance"]` (широкий) в invalidation `returnCashMutation.onSuccess` для надёжного обновления шапки.
- **[LOW]** Кнопка «Редактировать» у записей балансов использует иконку `<Clock size={11} />` (строка 683) вместо ожидаемой иконки карандаша/редактирования. Визуальная несоответствие — заменить на `Edit3`.
- **[LOW]** Внутренняя накладная показывает предупреждение «Показаны первые 80 строк из N» (строки 1299–1301), но само предупреждение рендерится **под** `invoiceRows.slice(0, 80)` — то есть сначала идут строки, потом предупреждение внизу страницы. Лучше поставить предупреждение перед списком.
- **[LOW]** Отсутствует обработка ошибок `deferMutation` в UI: мутация объявлена (строки 241–247), но `deferMutation.error` нигде не показывается пользователю (в отличие от остальных мутаций).

---

## Code Notes

- Frontend: `frontend/src/routes/PickingListPage.tsx`
- Backend (роуты): `server/parts/02f-supplier-picking-routes.js`
- Backend (состояние): `server/parts/02d-supplier-picking-state.js`
- Backend (касса): `server/parts/02d-picker-cash.js`
- Backend (накладная): `server/parts/02f-supplier-picking-invoices-route.js`
