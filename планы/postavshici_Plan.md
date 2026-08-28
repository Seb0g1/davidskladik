# SuppliersPage (Поставщики) — Plan

## Current State

Страница полной карточки поставщика. Показывает активных/остановленных поставщиков с ледже'ром (долг, аванс, оплачено, последняя оплата), позволяет добавить/редактировать/удалить поставщика, поменять валюту закупки, остановить на срок или навсегда, раскрыть историю заказов (строки сборки со статусом «собрано») и зафиксировать возврат товара поставщику. Поддерживает ручную оплату, корректировку баланса («Свести»), управление артикулами и сортировку по долгу.

---

## Bugs / Issues Found

### 1. `historyQuery` не обновляется при переключении между поставщиками если `historySupplier` сменился быстро

**Файл:** `SuppliersPage.tsx`, строки 188–198

```tsx
const historyQuery = useQuery<SupplierProfile>({
  queryKey: ["supplier-profile", historySupplier ? supplierId(historySupplier) : "none"],
  queryFn: () => {
    const s = historySupplier;  // захватываем в момент создания queryFn
    ...
  },
  enabled: !!historySupplier,
  staleTime: 15_000,
});
```

`queryFn` использует закрытие над `historySupplier` (строка 191: `const s = historySupplier`), которое захватывается в момент создания функции. Но `queryFn` переопределяется при каждом рендере. При смене `historySupplier` React обновит `queryKey`, TanStack Query сделает новый запрос — это корректно. Однако: если пользователь быстро раскрывает двух поставщиков подряд, оба попадут в кэш с разными ключами. При повторном открытии первого поставщика (в пределах `staleTime: 15_000`) TanStack Query отдаст кэш без рефетча, хотя за это время могло измениться состояние сборки. `staleTime: 15_000` слишком велик для оперативной информации — профиль меняется при каждом «Собрал».

### 2. `anyError` содержит ошибки от всех мутаций — показывает последнюю ошибку в конце страницы, может быть не заметна

**Файл:** `SuppliersPage.tsx`, строки 326–327 и 757

```tsx
const anyError = suppliersQuery.error || refreshMutation.error || saveSupplier.error || ...;
...
{anyError ? <div className="inline-error">{errorMessage(anyError)}</div> : null}
```

Единственный блок ошибки рендерится **в самом конце страницы** (строка 757), за пределом `</section>` поставщиков. На странице с длинным списком поставщиков оператор не увидит ошибку, если не проскроллит вниз. При этом `anyError` берёт первую ненулевую ошибку из цепочки `||`, но операции `adjustBalance`, `returnSupplier`, `returnPicking` в `anyError` не включены (строка 326). Ошибки этих мутаций не попадают в общий блок совсем.

### 3. Форма «Свести баланс» не блокирует кнопку при пустом поле

**Файл:** `SuppliersPage.tsx`, строки 539–540

```tsx
disabled={adjustBalance.isPending || adjustDrafts[id] === ""}
```

Условие блокировки: `adjustDrafts[id] === ""`. Но `adjustDrafts[id]` изначально `undefined` (не `""`), потому что `adjustDrafts` инициализирован как `{}` (строка 112). При первом рендере `adjustDrafts[id]` равен `undefined`, не `""`, значит `undefined === ""` → `false` — кнопка **не заблокирована**. Пользователь может нажать «Свести» с пустым полем. На сервере (`02d-suppliers-routes-read.js`, строка 383) `normalizeFinanceMoney(undefined, null)` вернёт `null`, и вернётся 400 `"targetBalance is required"`. Визуальная блокировка не работает, хотя серверная валидация защищает — но UX некорректный.

Правильное условие: `disabled={adjustBalance.isPending || !adjustDrafts[id] && adjustDrafts[id] !== "0"}` или просто `!Number.isFinite(Number(adjustDrafts[id]))`.

### 4. Корректировка баланса (`adjustBalance`) показывает результат в RUB независимо от валюты поставщика

**Файл:** `SuppliersPage.tsx`, строки 552–558

```tsx
{adjustBalance.isSuccess && adjustBalance.data && (
  <div>
    {adjustBalance.data.skipped
      ? adjustBalance.data.message
      : `Корректировка: ${moneySigned(adjustBalance.data.currentBalance ?? 0, "RUB")} → ${moneySigned(adjustBalance.data.targetBalance ?? 0, "RUB")} (запись на ${moneySigned(adjustBalance.data.delta ?? 0, "RUB")})`}
  </div>
)}
```

Жёстко задан `"RUB"` для форматирования. При этом на сервере (`02d-suppliers-routes-read.js`, строки 388–391) `currentBalance` и `targetBalance` — это всегда рублёвые числа (ledger хранится в RUB). Но пользователь вводит сумму в валюте поставщика (строки 541–543 на фронте):

```tsx
const targetBalance = supplierCurrency === "USD" ? Math.round(inputVal * usdRate) : inputVal;
```

То есть при USD-поставщике пользователь ввёл `$10`, фронтенд конвертировал в `~950 RUB`, сервер вернул `currentBalance` и `targetBalance` в RUB. Но сообщение показывает «Корректировка: +950 ₽ → +950 ₽», хотя пользователь думал в долларах. Нужно показывать в валюте ввода или добавить конвертированное значение.

### 5. `toggleHistory` обновляет два разных стейта несинхронно — возможен рассинхрон

**Файл:** `SuppliersPage.tsx`, строки 335–349

```tsx
const toggleHistory = (supplier: Supplier) => {
  const id = supplierId(supplier);
  setExpandedHistory((prev) => {
    const next = new Set(prev);
    if (next.has(id)) {
      next.delete(id);
      if (historySupplier && supplierId(historySupplier) === id) setHistorySupplier(null);
    } else {
      next.add(id);
      setHistorySupplier(supplier);
    }
    return next;
  });
};
```

`setHistorySupplier` вызывается **внутри** функции `setExpandedHistory` — это вызов стейт-сеттера внутри другого стейт-сеттера. React батчирует обновления в обработчиках событий (React 18+), но вызов `setHistorySupplier` внутри функционального апдейта `setExpandedHistory` не является типичным паттерном и может вести себя непредсказуемо. Корректнее вынести оба вызова в тело `toggleHistory` рядом, а не вкладывать.

### 6. `deleteSupplier` без подтверждения удаления артикулов — `window.confirm` заблокирован в некоторых окружениях

**Файл:** `SuppliersPage.tsx`, строки 575–579

```tsx
onClick={() => {
  if (window.confirm(`Удалить поставщика ${supplier.name || id}?`)) deleteSupplier.mutate(id);
}}
```

`window.confirm` синхронно блокирует UI и не работает в iframe (блокируется браузером/системой). В продакшн-приложении лучше использовать inline-подтверждение (как уже сделано для сброса балансов в `PickingListPage`: `resetConfirm` state). Аналогично и для `returnPicking` (строки 640–645 в SuppliersPage.tsx).

### 7. Список артикулов использует `article.id || article.article` как React key — нестабильный key при редактировании

**Файл:** `SuppliersPage.tsx`, строки 693–704

```tsx
{articles.map((article) => {
  const articleId = String(article.id || article.article || "");
  return (
    <div className="supplier-article-row" key={articleId}>
```

При редактировании артикула (кнопка Edit, строка 701) `setArticleDraft` меняет локальный draft с `id: articleId`. Когда `saveArticle.onSuccess` инвалидирует запрос и список перезагружается, порядок артикулов от сервера может быть другим. Если `article.id` меняется при сохранении, React не сможет сопоставить элементы правильно и сделает лишние DOM-обновления (или потеряет фокус).

### 8. `/api/suppliers` не требует аутентификации (`requireStaff`/`requireAdmin`)

**Файл:** `02d-suppliers-routes-read.js`, строка 1

```js
app.get("/api/suppliers", async (request, response, next) => {
```

Нет middleware `requireStaff` или `requireAdmin`. Эндпоинт доступен без сессии. Возвращает список всех поставщиков с именами, partnerId, ledger-балансами. Для сравнения, профиль поставщика `/api/suppliers/:id/profile` закрыт `requireAdmin` (строка 54). Это утечка коммерческой информации.

### 9. Сортировка по долгу в `filtered` использует `ledger.balance` из кэшированного `suppliersQuery`, который может отставать

**Файл:** `SuppliersPage.tsx`, строки 274–278

```tsx
if (sortBy === "debt") {
  const debtA = -Number(asRecord(asRecord(a).ledger).balance || 0);
  const debtB = -Number(asRecord(asRecord(b).ledger).balance || 0);
  return debtB - debtA;
}
```

Ledger-баланс берётся из ответа `/api/suppliers` (кэш `staleTime: 30_000`, строка 121). При нажатии «Оплатил» `paySupplier.onSuccess` инвалидирует `["suppliers"]` (строка 240), что вызовет рефетч. Но если пользователь быстро нажал «Оплатил» и сразу посмотрел на сортировку по долгу — он увидит старый порядок до завершения рефетча. Это ожидаемое поведение, но следует добавить skeleton/loading-индикатор в список пока идёт рефетч, чтобы визуально показать, что данные обновляются. Сейчас нет никакого UX-сигнала об обновлении.

### 10. `historyQuery` выполняется только для одного `historySupplier` за раз — при раскрытии двух поставщиков второй не загружается

**Файл:** `SuppliersPage.tsx`, строки 593–595

```tsx
const isActive = historySupplier && supplierId(historySupplier) === id;
const profile = isActive ? historyQuery.data : undefined;
```

Можно раскрыть историю сразу нескольких поставщиков: `expandedHistory` — это `Set<string>`, позволяющий несколько активных ID. Но `historyQuery` — один `useQuery` на весь компонент, загружающий данные только для последнего `historySupplier`. Если пользователь раскрыл поставщика A, потом B, история A покажет старые данные (`isActive === false`, `profile === undefined`) и отображается пустым блоком без объяснения.

---

## Improvement Ideas

- **[HIGH]** Добавить `requireStaff` к `GET /api/suppliers` — эндпоинт раскрывает коммерческие данные без аутентификации.
- **[HIGH]** Исправить условие блокировки кнопки «Свести»: `undefined === ""` → `false`, кнопка разблокирована при пустом поле. Заменить на `!adjustDrafts[id] && adjustDrafts[id] !== "0"` или проверку `Number.isFinite`.
- **[HIGH]** Включить `adjustBalance.error`, `returnSupplier.error` и `returnPicking.error` в `anyError` или добавить отдельные error-блоки у соответствующих форм (рядом с кнопками), а не только в конце страницы.
- **[MEDIUM]** Переместить или дублировать `anyError` блок в начало страницы (под `PageHeader`) — сейчас он теряется в конце длинного списка.
- **[MEDIUM]** Заменить `window.confirm` для удаления поставщика и возврата товара на inline-подтверждение (по образцу `resetConfirm` в `PickingListPage`).
- **[MEDIUM]** `toggleHistory`: вынести `setHistorySupplier` из функционального апдейта `setExpandedHistory` — не вкладывать стейт-сеттеры.
- **[MEDIUM]** Уменьшить `staleTime` в `historyQuery` до `5_000` или добавить `refetchOnWindowFocus: true`, чтобы профиль обновлялся при возвращении на вкладку.
- **[MEDIUM]** При сортировке по долгу показывать `isFetching`-индикатор в заголовке списка (спиннер рядом с «Долг»-кнопкой), чтобы пользователь видел, что данные обновляются.
- **[LOW]** Показывать в success-сообщении корректировки баланса значения в валюте ввода (USD для USD-поставщиков), а не только в RUB.
- **[LOW]** Ограничить `expandedHistory` одним активным поставщиком за раз (или перейти к per-supplier `useQuery`) — текущая реализация создаёт иллюзию возможности открыть нескольких поставщиков, хотя данные загружаются только для последнего.
- **[LOW]** Использовать стабильный `key` для артикулов — предпочтительно серверный UUID (если есть), а не `article.id || article.article`.

---

## Code Notes

- Frontend: `frontend/src/routes/SuppliersPage.tsx`
- Backend (чтение): `server/parts/02d-suppliers-routes-read.js`
- Backend (запись): `server/parts/02d-suppliers-routes-write.js`
- Backend (ledger): `server/parts/02d-finance-supplier-ledger.js`
