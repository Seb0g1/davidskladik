# WarehousePage — Plan

## Current State

Главная страница склада. Отображает виртуализированный список карточек товаров (групп), с детальной панелью справа. Включает: фильтры (поиск, маркетплейс, статус, бренд, сортировка), поддержку группировки Ozon/Yandex в одну строку, блок привязок PriceMaster (LinksPanel), строки маркетплейсов (MarketplaceRows, AvitoRows, WbRows), быстрые действия, AI-фото панель, диагностику, историю цен. Демо-режим при пустом реальном каталоге. Размер файла: 2732 строки.

---

## Bugs / Issues Found

### 1. QuickActions: три кнопки вызывают одну и ту же мутацию `recover`

**Строки 2237–2249.** Кнопки "Проверить и починить товар", "Восстановить товар" и "Отправить остаток" все вызывают `recover.mutate()`. Кнопка "Отправить остаток" явно должна отправлять остаток (эндпоинт `/api/warehouse/links/recover-stale-stocks`), а "Отправить цену" уже правильно вызывает `sendPrices.mutate()`. Но дублирование первых трёх кнопок — это лишний UI без смысловых отличий: три кнопки делают одно и то же. Пользователь не понимает, в чём разница.

```tsx
// строки 2237-2244: все три — recover.mutate()
<button ... onClick={() => recover.mutate()}>Проверить и починить товар</button>
<button ... onClick={() => recover.mutate()}>Восстановить товар</button>
<button ... onClick={() => recover.mutate()}>Отправить остаток</button>
```

### 2. `useEffect` в LinksPanel зависит от вычисляемого `draftScopeKey`, но внутри вызывает `.reset()` на мутации — пропущены зависимости линтера

**Строки 596–611.** `useEffect` с зависимостью `[draftScopeKey]` вызывает `saveMutation.reset()`, `deleteMutation.reset()` и т.д. Объекты мутаций не в массиве зависимостей (есть комментарий `eslint-disable-line react-hooks/exhaustive-deps` в другом useEffect). Каждый рендер пересоздаёт объект мутации — если `draftScopeKey` не изменился, reset не вызовется, но если версия TanStack Query обновит ссылки — эффект получит устаревшую ссылку.

### 3. `EtaChips` использует `fetch` напрямую без abort controller при смене `productId`

**Строки 2019–2027.** При быстром переключении между карточками `productId` меняется, новый fetch запускается, но старый не отменяется. Флаг `alive` есть, но он только игнорирует данные — fetch-запрос всё равно завершится и займёт ресурсы/слот HTTP. При медленной сети это приводит к очереди незакрытых запросов.

### 4. `resetPriceLimitsMutation` делает последовательные `await fetch` в цикле — нет параллелизма и нет транзакции

**Строки 583–594.** Для каждого товара с `autoPriceMin`/`autoPriceMax` выполняется отдельный PATCH-запрос по очереди (`for...of` с `await`). Если в группе 10 карточек — 10 последовательных запросов. Серверный эндпоинт поддерживает `productIds`-массив (это тот же `/api/warehouse/products/:id`), поэтому лучше один bulk-запрос.

### 5. `AiImagesPanel`: `useEffect` с зависимостью `[jobQuery.data, queryClient, onSaved]` — `onSaved` нестабильная ссылка

**Строки 1163–1182.** `onSaved` передаётся как prop — это `refreshDetail` из `DetailPanel` (строка 2287), которая каждый рендер создаётся заново (`const refreshDetail = () => void queryClient.invalidateQueries(...)`). При каждом рендере `DetailPanel` эффект будет перезапускаться и вызывать `onSaved()` / `queryClient.invalidateQueries`, что приводит к лишним инвалидациям кэша при любом ре-рендере карточки.

### 6. `LinksPanel`: `stockOnlyManualPrices`-блок намеренно скрыт через `{false && ...}`, но код остался и создаёт мертвый путь (строка 679)

**Строка 679.** Весь UI-блок «Ручная цена складского fallback» завёрнут в `{false && <div ...>}`. Мутация `manualPricesMutation` объявлена, состояния `manualPrices`/`setManualPrices` живут в хуках, но никогда не используются. Это хуки вхолостую на каждый рендер.

### 7. `buildDemoBreakdown`: поле `ozonMinPrice` вычисляется как `targetPrice - 450` — магическая константа без объяснения

**Строка 173.** `ozonMinPrice: ... Math.max(0, Number(product.targetPrice || 0) - 450)` — откуда 450? Возможно, это комиссия Ozon. В реальном breakdown это значение берётся с сервера, а в демо-режиме — захардкожено. Если логика изменится — демо-данные будут показывать неправильный минимум цены Ozon.

### 8. `mergeMutation` делает GET `/api/warehouse/products/page` для поиска карточки — это не поиск по SKU, а запрос страницы каталога

**Строки 2103–2109.** При объединении карточек через ввод SKU запрос идёт на `/api/warehouse/products/page?q=...&pageSize=20&page=1&grouped=false`. Этот эндпоинт возвращает paginated список, а не конкретный продукт. Если SKU совпадает с 20+ товарами — объединятся лишние. Нет явного эндпоинта поиска-по-точному-SKU.

### 9. `DiagnosticsPanel`: вычисление `staleDays` с неправильной проверкой — смотрит на `selectedSupplier` продукта, а не на диагностический элемент

**Строки 1521–1527.** `if (staleDays !== null && staleDays >= 14 && !(item as unknown as Record<string, unknown>).selectedSupplier)` — проверяет `item.selectedSupplier` (поле диагностики, а не Product). Диагностическая запись содержит `selectedSupplier` как отдельный объект. Если `selectedSupplier` — пустой объект `{}` (truthy), условие не выполнится и предупреждение о устаревшей PM-цене не будет показано.

### 10. `detailQuery` показывает `retry: 2` — при 404 будет 2 лишних запроса

**Строки 2526–2529.** `retry: 2` включён для детальной карточки. Если группа не найдена (например, только что удалили) — TanStack Query выполнит 3 запроса вместо 1 перед показом ошибки. Нужен `retry: false` или условный `retry: (count, err) => err.status !== 404`.

### 11. `writeWarehouseLocation` всегда пишет `replaceState` при изменении фильтров

**Строки 452–454.** `useEffect(() => { writeWarehouseLocation(filters, selectedGroup, true); }, [filters, selectedGroup])` — параметр `replace=true` всегда. Это значит, что все изменения фильтров не попадают в историю браузера и кнопка «Назад» не возвращает к предыдущему фильтру, а уходит с страницы.

---

## Improvement Ideas

- **[HIGH]** `QuickActions`: разделить кнопки по смыслу — одна для repair (POST /repair), одна для send-stock, одна для send-price. Убрать дубли `recover.mutate()`.
- **[HIGH]** `resetPriceLimitsMutation`: заменить последовательный цикл `await` на одновременный `Promise.all` или создать один bulk-эндпоинт.
- **[HIGH]** `AiImagesPanel`/`DetailPanel`: мемоизировать `refreshDetail` через `useCallback`, чтобы не пересоздавать при каждом рендере и не дёргать `onSaved`-зависимый эффект.
- **[MEDIUM]** `EtaChips`: добавить `AbortController` для отмены fetch при размонтировании / смене `productId`.
- **[MEDIUM]** `LinksPanel`: убрать `stockOnlyManualPrices` состояния и `manualPricesMutation` из хуков, пока блок скрыт `{false && ...}`.
- **[MEDIUM]** `detailQuery`: добавить `retry: (count, err) => (err as any)?.status !== 404` чтобы не делать 3 запроса при отсутствующей группе.
- **[MEDIUM]** `writeWarehouseLocation`: добавлять в history при навигации (не replace), только при сбросе к первой странице использовать replace.
- **[MEDIUM]** `mergeMutation`: добавить серверный эндпоинт поиска-по-точному-SKU (`/api/warehouse/products/find?offerId=...`) вместо использования `/products/page`.
- **[LOW]** `buildDemoBreakdown`: заменить магическую константу `450` именованной переменной `OZON_MIN_PRICE_GAP_RUB = 450` с комментарием.
- **[LOW]** Страница называется «Новый каталог» в заголовке (`PageHeader title="Новый каталог"`), но это финальная страница. Переименовать в «Каталог товаров».

## Code Notes

- Frontend: `frontend/src/routes/WarehousePage.tsx`
- Backend: `server/parts/02d-routes-warehouse-catalog.js`
- Backend (диагностика): функция `buildWarehouseSkuDiagnostics`
- Backend (page): функция `buildFastWarehousePage`
