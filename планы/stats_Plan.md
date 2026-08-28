# StatisticsPage — Plan

## Current State

Страница Статистика показывает активность сотрудников за выбранный период (таблица по сотрудникам), финансовую сводку, показатели склада и очередь сборки. Всё в одном экране: большая таблица слева + три карточки справа. Период выбирается через SelectField.

---

## Bugs / Issues Found

**1. Только `users.error` отображается на странице (строка 45, 151)**

```tsx
const error = users.error;
...
{error ? <div className="inline-error">{errorMessage(error)}</div> : null}
```
Ошибки запросов `finance`, `warehouse` и `picking` не выведены в общем блоке ошибок внизу страницы. Для них есть локальные `inline-error` внутри карточек (строки 118, 128, 140), но они отображают только текст «Данные склада недоступны» без реального сообщения — пользователь не видит причину.

**2. `warehouseUrl` hardcoded с устаревшими параметрами (строка 17)**

```ts
const warehouseUrl = "/api/warehouse/products/page?page=1&pageSize=1&q=&marketplace=all&linked=all&state=all&autoOnly=false&grouped=true";
```
URL объявлен как константа вне компонента. Параметр `autoOnly=false` — захардкожен, параметр `grouped=true` может в будущем изменить формат ответа. Если API изменится, ошибка проявится молча, а не через TypeScript.

**3. `warehouse.data?.groupTotal || warehouse.data?.total` — неправильный порядок (строка 120)**

```tsx
<span>Карточки <b>{warehouse.data?.groupTotal || warehouse.data?.total || 0}</b></span>
<span>SKU <b>{warehouse.data?.rowTotal || warehouse.data?.totalAll || 0}</b></span>
```
`WarehousePageSchema` может вернуть `groupTotal = 0` (реально пустой склад), тогда `0 || warehouse.data?.total` подставит значение `total`, которое включает дубли. Нужно использовать `?? 0` вместо `|| 0`.

**4. `picking.data?.total` — поле `total` в `SupplierPickingListSchema` может быть `undefined` (строка 133)**

```tsx
<strong>{picking.data?.total || 0}</strong>
```
Запрос идёт с `limit=1`, что означает ответ содержит только 1 строку, но поле `total` в схеме — реальный общий счётчик. Если API не возвращает `total` при `limit=1` (только для листа сборки), показывается `0` без предупреждения. Нужно убедиться, что `SupplierPickingListSchema` гарантирует поле `total`.

**5. `sortedRows` обрабатывает `actionsTotal` без числового преобразования в сортировке (строка 41)**

```tsx
() => [...rows].sort((a, b) => numberValue(b.actionsTotal, 0) - numberValue(a.actionsTotal, 0)),
```
Это технически правильно (`numberValue` конвертирует), но `rows` содержит данные типа `Record<string, unknown>` согласно `UsersStatsResponseSchema`. Если `actionsTotal` приходит как строка из API (legacy), `numberValue` должна справиться — но стоит убедиться в типах.

**6. Нет индикатора загрузки для `finance`, `warehouse`, `picking` (строки 109–110)**

Индикатор загрузки (скелетон) показывается только для `users` (строка 109). Финансовые карточки и карточки склада при первой загрузке показывают пустые значения `0` / `-` без явного spinner. Пользователь не знает, что данные ещё грузятся.

**7. Бэкенд `/api/users/stats` (`routes/users.js`): функция `statsSummary` считает `affectedProducts` как сумму по пользователям, а не уникальное множество**

Строка 72:
```js
affectedProducts: users.reduce((sum, user) => sum + Number(user.affectedProducts || 0), 0),
```
`user.affectedProducts` — это `affectedProductsSet.size` для каждого пользователя отдельно. Один и тот же товар может быть изменён несколькими сотрудниками, и в summary он будет посчитан дважды. В отличие от per-user поля, summary для `affectedProducts` завышен.

**8. Бэкенд: параметр `period` не влияет на `currentLinksCreated` / `currentLinksUpdated`**

В `routes/users.js` `currentLinksCreated` и `currentLinksUpdated` вычисляются из текущего состояния склада (snapshot), а не из аудит-лога за период. При выборе «7 дней» эти поля показывают актуальное состояние на сейчас, а не активность за неделю. Это вводит в заблуждение.

---

## Improvement Ideas

- **[HIGH]** Исправить `|| 0` на `?? 0` при отображении `groupTotal`, `rowTotal`, `ready`, `withoutSupplier` в карточке склада.
- **[HIGH]** Вывести реальные сообщения ошибок в карточках (передавать `errorMessage(warehouse.error)` вместо захардкоженного текста).
- **[MEDIUM]** Добавить skeleton/spinner для финансовой карточки и карточки склада аналогично таблице сотрудников.
- **[MEDIUM]** Вынести `warehouseUrl` в компонент как `useMemo` или конструировать URL из параметров, чтобы избежать silent drift.
- **[MEDIUM]** Уточнить в документации или через отдельный API-поле, что `currentLinksCreated/Updated` не зависит от выбранного периода.
- **[LOW]** Добавить кнопку «Экспорт PDF» (аналог из `UsersSettingsPanel`) прямо на страницу статистики для быстрого экспорта без перехода в Настройки.
- **[LOW]** Добавить фильтр по сотруднику прямо в таблицу (кликабельная строка → фокус в `UsersSettingsPanel`).

---

## Code Notes

- Frontend: `frontend/src/routes/StatisticsPage.tsx`
- Backend: `routes/users.js` (функция `registerUsersRoutes`, endpoint `/api/users/stats`)
