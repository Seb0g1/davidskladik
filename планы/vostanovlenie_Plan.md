# RecoveryQueuePage — Plan

## Current State

Страница «Очередь восстановления» отображает состояние очереди Ozon autoarchive (разархивирование товаров с ограничением Ozon по дневному лимиту) и быстрого разархива Яндекс Маркета. Показывает метрики: всего в очереди, готовы к попытке, ждут проверки Ozon, в архиве Яндекс. Позволяет вручную запустить процесс, перестроить очередь и поискать конкретный SKU.

## Bugs / Issues Found

- **`data?.autoRunning` передаётся в `disabled` кнопки, но тип не гарантирован** — строка 177: `disabled={process.isPending || queue.isLoading || data?.autoRunning}`. `OzonUnarchiveQueueSchema` типизирован через Zod, но `autoRunning` может быть `undefined` при первой загрузке — React принимает `undefined` как `false`, что корректно, но TypeScript может сигнализировать о несовместимости типов в strict-режиме.

- **Поиск `queueSearch` фильтрует только первые 1000 элементов** — строка 170: запрос к бэкенду с `?limit=1000`, потом `visibleItems` обрезается до 200. Поле поиска фильтрует внутри этих 1000 строк. Если в очереди более 1000 позиций, поиск не найдёт элементы за пределами первого чанка.

- **Input поиска стоит ПОСЛЕ `table-head`** — строки 229–232: `<div className="table-head">` рендерится раньше `<input type="text" placeholder="Поиск по SKU...">`. Визуально инпут появляется внутри блока таблицы, под заголовком, что нестандартно — обычно фильтр стоит над таблицей.

- **`itemStatusLabel` и `itemWhenLabel` принимают `item` как `Record<string, unknown>`** — строки 88–104: функции используют generic cast вместо типизированного интерфейса. Любая опечатка в ключе (`item.dailyLimit` vs `item.daily_limit`) не будет поймана TypeScript.

- **`lastResultText` проверяет `row.unarchivePending`** — строки 52–62: в `lastResultText` читается `row.unarchivePending`, но в `YandexFastStatus.lastResult` (строки 22–28) поле называется `unarchived`. Поле `unarchivePending` не определено в `YandexFastStatus.lastResult` — всегда будет `0`. Текст `"ожидает 0"` будет показан всегда.

- **`rebuildMut` использует `z.unknown()` как схему** — строка 159: `fetchJson("/api/ozon/unarchive-queue/rebuild", z.unknown(), { method: "POST" })`. Это означает ответ никак не валидируется — если сервер вернёт ошибку в нестандартном формате, она не будет поймана как ошибка мутации.

- **`YandexFastBlock` рендерится всегда** — строки 188: `<YandexFastBlock status={yandexFastStatus ?? null} error={yandexFastError} />` рендерится даже когда `yandexFastQuery.isLoading` (статус `null`). Все числа отображаются как `"…"` (строка 119), что выглядит незавершённо.

- **Дублирующийся блок статистики** — `summary-grid` с «Всего в очереди / Готовы к попытке / Ждут проверки» рендерится дважды: сначала в `<section className="dashboard-metrics">` (строки 182–187) через `<Stat>`, потом снова в `<div className="summary-grid">` (строки 189–195). Данные идентичны.

## Improvement Ideas

- **[HIGH]** Исправить `lastResultText` — заменить `row.unarchivePending` на корректное поле из `YandexFastStatus.lastResult`. Судя по типу (строка 22–28), поля `unarchivePending` нет — нужно убрать или заменить на фактически возвращаемое поле.

- **[HIGH]** Убрать дубль `summary-grid` — оставить только `dashboard-metrics` с `<Stat>`, удалить повторный блок строк 189–195.

- **[MEDIUM]** Переместить input поиска выше `table-head` — перед `<div className="table-panel queue-table">` или между кнопками и таблицей.

- **[MEDIUM]** Типизировать элементы очереди — создать `interface QueueItem` вместо `Record<string, unknown>` в `itemStatusLabel` / `itemWhenLabel`, синхронизировав с `OzonUnarchiveQueueSchema`.

- **[MEDIUM]** Добавить загрузочный индикатор для YandexFastBlock — `{yandexFastQuery.isLoading ? <Loader2 /> : <YandexFastBlock ... />}`.

- **[LOW]** Добавить серверную пагинацию или поиск — `?search=` параметр в `/api/ozon/unarchive-queue` чтобы находить SKU за пределами первых 1000.

- **[LOW]** Показывать `rebuildMut.data` после успеха перестройки — сколько позиций добавлено/обновлено в очередь.

## Code Notes

- Frontend: `frontend/src/routes/RecoveryQueuePage.tsx`
- Backend: `server/parts/02d-routes-unarchive.js`, `server/parts/02f-yandex-fast-unarchive.js`
