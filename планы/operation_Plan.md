# OperationsPage — Plan

## Current State

Страница массовых операций и автокорзины поставщиков. Содержит два раздела:

1. **OperationsPage** — список операций (jobs) с прогрессом, детальная панель (`OperationDetailPanel`), кнопки запуска операций (восстановить привязанные, вернуть autoarchive, AI качество карточек, и т.д.). Polling каждые 5 секунд.

2. **SupplierCartPanel** — автокорзина: генерация заказов из активных отправлений Ozon/Yandex/WB, просмотр черновика, коммит в PriceMaster, ручной заказ, история. Вынесен как отдельный компонент и живёт на `/app/supplier-cart`.

---

## Bugs / Issues Found

### 1. `startMutation` — одна мутация для всех 9 кнопок; `isPending` блокирует всё

**Строки 539–559, 581–590.** Единственный `useMutation` (`startMutation`) обслуживает все кнопки запуска операций. При запуске любой операции `disabled={startMutation.isPending}` блокирует все 9 кнопок. Нельзя, например, запустить "brand-index-rebuild" пока идёт "health-deep".

### 2. `limit` = 30000 используется для всех операций — но `yandex-import-send` получает `{ limit, sendLimit }`, а остальные только `{ limit }`. Некоторые операции игнорируют `limit` на сервере

**Строки 540–553.**
```tsx
payload: type === "yandex-import-send"
  ? { limit, sendLimit }
  : type === "restore-archived-stock"
    ? { limit, stock, marketplace: "yandex" }
    : ...
    : { limit }
```
`limit` по умолчанию 30000 — это применяется к "brand-index-rebuild" (реально использует `100000` по умолчанию на сервере), к "sales-automation-run" (использует отдельный лимит). Несоответствие между UI-лимитом (30000) и серверным дефолтом молча проигнорируется, если сервер установит свой лимит через `cleanLimit`.

### 3. `OperationDetailPanel`: `refetchInterval` проверяет статус через `query.state.data?.job.status`, но при первом запросе данных нет — интервал будет `false`, и детали не обновятся автоматически

**Строки 111–114.**
```tsx
refetchInterval: (query) => {
  const status = String(asRecord(query.state.data?.job).status || "");
  return status === "queued" || status === "running" ? 3000 : false;
},
```
Когда запрос впервые запускается (`data === undefined`), `status` будет пустой строкой, `refetchInterval` вернёт `false`. Данные загрузятся один раз, но если операция в этот момент queued/running — polling не начнётся автоматически. Нужно обрабатывать начальное состояние: при `query.state.data === undefined` возвращать `3000` (polling до получения первых данных).

### 4. `selectedJob` берётся как `selectedJobId || String(jobs[0]?.id || "")` — при обновлении списка автоматически показывается первая операция, даже если пользователь ничего не выбирал

**Строки 561.** `const selectedJob = selectedJobId || String(jobs[0]?.id || "")` — при первой загрузке страницы и каждые 5 секунд при обновлении `jobs`, `selectedJob` будет равен `jobs[0].id`. Это запускает `OperationDetailPanel` с этим ID и делает запрос к `/api/operations/:id` без явного выбора пользователем. Если список обновился и `jobs[0]` сменился — деталь переключится автоматически.

### 5. `SupplierCartPanel`: `useEffect(() => { void generateMutation.mutate(); }, [])` — автоматическая генерация корзины при монтировании

**Строка 211.**
```tsx
useEffect(() => { void generateMutation.mutate(); }, []);
```
При каждом открытии страницы `/app/supplier-cart` сразу делается POST `/api/supplier-cart/generate` — это запрос к Ozon/Yandex API за актуальными заказами. Нет дебаунса, нет проверки свежести данных. `draftQuery` (строки 199–203) тоже загружает черновик — но generateMutation перезаписывает `previewData`. Два параллельных источника данных.

### 6. `commitMutation` отправляет `rows: (generateMutation.data || draftQuery.data)?.rows || []` — при частичном refresh `generateMutation` может иметь устаревшие данные

**Строки 213–219.**
```tsx
mutationFn: () => fetchJson("/api/supplier-cart/commit", ..., mutationBody({
  rows: (generateMutation.data || draftQuery.data)?.rows || [],
  keys: Array.from(selected),
})),
```
После `overrideMutation.onSuccess` вызывается `generateMutation.reset()` (строка 229), поэтому `generateMutation.data` сбрасывается и commit начнёт использовать `draftQuery.data`. Но если `draftQuery` ещё не обновился (invalidate случился только что) — commit уйдёт со старыми rows. Нет синхронизации между reset и re-fetch.

### 7. `SupplierCartPanel`: `filteredRows` зависит от `rows`, но при `q === ""` возвращает исходный `rows` без мемоизации нового массива

**Строки 274–279.**
```tsx
const filteredRows = useMemo(() => {
  const needle = q.trim().toLowerCase();
  if (!needle) return rows;
  return rows.filter(...);
}, [q, rows]);
```
При `q === ""` возвращается тот же `rows` reference — это корректно. Но `rows` объявлен как `const rows = previewData?.rows || []` (строка 273) — при каждом рендере создаётся новый массив `[]` при `previewData === undefined`, что делает `useMemo` бесполезным и каждый рендер пересчитывает `filteredRows`.

### 8. `OperationDetailPanel`: `issues.slice(0, 80)` применяется дважды — при вычислении `issues` и при рендере

**Строки 57, 159.**
```tsx
// В operationIssues():
const items = [...directErrors, ...failedRows, ...warnings].slice(0, 80);
// При рендере:
{issues.slice(0, 80).map(...)}
```
`operationIssues` уже срезает до 80, потом `issues.slice(0, 80)` срезает ещё раз. Второй `slice` никогда не срежет ничего (массив уже <= 80). Хаотичный code style.

### 9. `jobSummary` функция: `String(job.error || "")` добавляется в конец `parts` — при наличии ошибки текст операции выглядит как "Описание · ошибка" без визуального разделения

**Строки 34–35.**
```tsx
String(job.error || ""),
...
return parts.join(" · ") || String(job.summary || "");
```
`job.error` попадает в одну строку с другими метриками через `·`. Ошибка не выделена визуально — нет цвета danger или отдельного элемента.

### 10. `SupplierCartPanel`: история (`historyQuery`) не имеет `refetchInterval` и показывает только 5 последних записей жёстко

**Строки 259–262, 514.**
```tsx
const historyQuery = useQuery({
  queryKey: ["supplier-cart-history"],
  queryFn: () => fetchJson("/api/supplier-cart/history", SupplierCartHistorySchema),
  // нет refetchInterval, нет staleTime
});
// ...
{(historyQuery.data?.history || []).slice(0, 5).map(...)}
```
История загружается один раз и не обновляется после commit. После `commitMutation.onSuccess` вызывается `queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] })` (строка 218) — invalidate есть, но без `refetchInterval` пользователь не увидит новые записи без ручного refresh. Кнопка "Обновить" есть (строка 512), но invalidate уже должен был триггернуть re-fetch.

### 11. `manualOptions` в SupplierCartPanel: звёздочка `★` у поставщиков через regex `/сорин|инна/i` — захардкоженные имена поставщиков в UI-логике

**Строка 380.**
```tsx
const star = /сорин|инна/i.test(opt.supplierName) ? "★ " : "";
```
Имена конкретных поставщиков ("Сорин", "Инна") вшиты в логику рендера компонента. При изменении имён поставщиков — нужно менять фронтенд. Лучше чтобы признак "приоритетный" поставщик приходил с сервера в поле `opt.preferred` или `opt.starred`.

---

## Improvement Ideas

- **[HIGH]** Разделить `startMutation` на несколько или использовать `Map<type, useMutation>`, чтобы разные операции не блокировали друг друга.
- **[HIGH]** `OperationDetailPanel`: при первоначальной загрузке (`data === undefined`) установить `refetchInterval: 3000`, чтобы polling начинался сразу, не ждя первых данных.
- **[HIGH]** Убрать захардкоженный regex `/сорин|инна/i` (строка 380) — перенести признак приоритетности в серверный ответ (`opt.preferred: boolean`).
- **[MEDIUM]** `selectedJob` не должен автоматически переключаться на `jobs[0]` при обновлении — инициализировать как `""` и показывать детали только при явном выборе. Или автоматически выбирать только при первой загрузке (через ref-флаг).
- **[MEDIUM]** `SupplierCartPanel`: устранить двойной источник данных (`generateMutation.data || draftQuery.data`). После override делать `await queryClient.refetchQueries(["supplier-cart-draft"])` перед commit вместо reset+invalidate.
- **[MEDIUM]** `rows = previewData?.rows || []`: вынести в `useMemo` с зависимостью `[previewData]` чтобы не создавать новый `[]` при каждом рендере.
- **[MEDIUM]** Убрать второй `issues.slice(0, 80)` при рендере в `OperationDetailPanel` (строка 159) — уже ограничено в `operationIssues()`.
- **[MEDIUM]** `jobSummary`: добавить поле `error` отдельно от `parts.join(" · ")`, рендерить с `tone-danger` класом.
- **[LOW]** `historyQuery`: добавить `staleTime: 60_000` — при invalidate после commit данные обновятся автоматически без бесконечного polling.
- **[LOW]** Добавить фильтр по типу операции в списке jobs (dropdown "Все / Только running / Только failed") — при 80 операциях в списке нет навигации.

## Code Notes

- Frontend: `frontend/src/routes/OperationsPage.tsx`
- Backend (роуты операций): `server/parts/02d-run-operation-payload.js` — `runOperationPayload`
- Backend (runners): `server/parts/02d-operation-runners-core.js`
- Backend (автокорзина): поищи роут `/api/supplier-cart` в `server/parts/`
