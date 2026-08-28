# FinancePage — Plan

## Current State

Страница Финансы показывает сводку по заказам и расходам: чистая прибыль, выручка МП, долг поставщикам. Две таблицы: список заказов с inline-редактированием финансовых полей, список ручных закупок. Фильтры: период (7d/30d/90d/all) и чекбокс «только связанные со складом».

---

## Bugs / Issues Found

**1. Inline-редактирование: черновик не включает penaltiesAmount и refundsAmount в patch, но формирует их в объекте**

В `FinancePage.tsx` строка 160–167 объект `patch` включает поля `penaltiesAmount` и `refundsAmount`, однако в таблице нет `<input>` для этих двух полей (строки 177–184 — только saleAmount, payoutAmount, feesAmount, taxAmount). Таким образом `penaltiesAmount` и `refundsAmount` всегда отправляются как `Number(undefined || 0) = 0`, молча обнуляя ранее сохранённые значения при любом нажатии «Сохранить».

**2. Один глобальный updateOrder.error для всей таблицы (строка 151)**

`updateOrder.error` отображается один раз под формой закупки, а не под конкретной строкой заказа. Если при сохранении строки N возникла ошибка, а потом пользователь нажимает Сохранить в строке M (успешно), ошибка строки N остаётся видна. Визуально непонятно, к какому заказу относится ошибка.

**3. Скелетон при наличии данных (строка 188)**

Условие `!orders.data?.orders?.length && orders.isLoading` показывает скелетон только если массив пустой И идёт загрузка. При первом рендере `orders.data` равно `undefined`, поэтому `!undefined?.orders?.length` = `!undefined` = `true` — скелетон показывается. Но если TanStack Query возвращает stale данные (есть в кэше), условие ложное (`orders.data?.orders.length > 0`) — скелетон не показывается даже во время фоновой перезагрузки. Непоследовательное поведение.

**4. Форма ручной закупки: поле quantity сбрасывается неправильно**

В `form.quantity` хранится `number` (строка 47: `quantity: 1`), но `<input type="number">` при onChange (строка 143) делает `Number(event.target.value || 1)` — если пользователь стирает значение, получается `1`, а не пустое поле. Нет защиты от вводa нецелых значений (дробей) для количества.

**5. `expenseSuccess` флаг конкурирует с `createExpense.isSuccess` (строки 52, 76–78, 152)**

`expenseSuccess` сбрасывается через 3 секунды, но `createExpense.isSuccess` из TanStack Query остаётся `true` до следующего вызова мутации. Строка 152 проверяет `createExpense.isSuccess`, а не `expenseSuccess`, поэтому баннер «Закупка добавлена» остаётся виден бесконечно при переключении фильтров (period/linkedOnly), пока не запустится новая мутация.

**6. Бэкенд: `listSupplierLedgerEntries` делает дополнительный запрос на 10 000 строк без ограничения (строка 84 в `02d-finance-supplier-ledger.js`)**

Если `total > rows.length` (т.е. в таблице больше 2000 записей), выполняется `findMany` с `take: 10000` для вычисления корректного summary. Нет таймаута, нет ограничения по периоду — на большой базе это заблокирует запрос summary.

**7. Бэкенд: `normalizeFinanceOrder` в `02d-finance-normalize-helpers.js` строка 73 не поддерживает marketplace "wb" и "avito"**

```js
const marketplace = marketplaceText === "ozon" || marketplaceText === "yandex" ? marketplaceText : "";
```
WB-заказы, синхронизируемые через `02f-finance-orders-sync.js`, приходят с `marketplace: "wb"`, но при patch-запросе поле сбрасывается в пустую строку. Фильтр "Заказов" на странице показывает корректные данные только пока данные не редактируются.

---

## Improvement Ideas

- **[HIGH]** Добавить `<input>` поля для `penaltiesAmount` и `refundsAmount` в строке заказа, чтобы не обнулять эти значения при сохранении.
- **[HIGH]** Перенести `updateOrder.error` в контекст конкретной строки (или передавать `id` в мутацию и хранить `Map<id, error>`).
- **[HIGH]** Исправить `normalizeFinanceOrder`: принимать `"wb"` и `"avito"` как валидные marketplace.
- **[MEDIUM]** Заменить ручной `expenseSuccess` на прямое использование `createExpense.isSuccess` + вызов `reset()` через `onSuccess`, либо использовать `isSuccess` с коротким `staleTime`.
- **[MEDIUM]** Ограничить дополнительный запрос в `listSupplierLedgerEntries` периодом или кешировать summary отдельно.
- **[MEDIUM]** Добавить поиск по таблице заказов (бэкенд уже принимает `q`, фронтенд не передаёт).
- **[LOW]** Добавить кнопку «Удалить» для строк ручных закупок из `finance_expenses`.
- **[LOW]** Показывать `orders.isFetching` spinner в заголовке таблицы при фоновом обновлении, не скрывая существующие данные.

---

## Code Notes

- Frontend: `frontend/src/routes/FinancePage.tsx`
- Backend: `server/parts/02d-prices-finance-routes-api.js`, `server/parts/02d-finance-supplier-ledger.js`, `server/parts/02d-finance-normalize-helpers.js`
