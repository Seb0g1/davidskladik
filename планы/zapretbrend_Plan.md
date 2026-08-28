# BrandBansPage — Plan

## Current State

Страница «Запрет брендов» управляет списком брендов, чьи товары должны быть сняты с продажи на Ozon, Яндекс Маркете и WB. Возможности: добавить бренд (с автокомплитом из BrandIndex), посмотреть предпросмотр попадающих товаров, снять с продажи одним кликом, добавить/удалить конкретные артикулы в бан, удалить запрет. Данные хранятся в JSON-файле `brand-bans.json`.

## Bugs / Issues Found

- **`MpBadge` и `mpLabel` не обрабатывают WB** — строки 70–90: `mpLabel` возвращает `"ЯМ"` для `"yandex"`, `"Ozon"` для всего остального. `MpBadge` аналогично бинарный. Если `marketplace === "wb"`, оба компонента покажут `"Ozon"` с синим цветом вместо WB-значка. В предпросмотре (строка 500) для каждого товара рендерится `<span>[{mpLabel(p.marketplace)}]</span>` — WB-товары будут показаны как Ozon.

- **Предпросмотр не включает WB-товары в список продуктов** — строки 482–516: блок `isPreviewOpen && preview` показывает `preview.products`, но `preview.products` (строка 379 бэкенда) содержит только Ozon/ЯМ-товары из `resolveMatchingProductsForBan`. WB-карточки считаются через `wbCount`, но не входят в список. В UI написано `"Найдено: X товаров Ozon/ЯМ, Y карточек WB"` — это корректно, но визуально товары из WB не видны в списке предпросмотра.

- **`applyMutation.onSuccess` закрывает `openPreview`, но не очищает `previews[id]`** — строки 331–337: после успешного применения `setOpenPreview(null)`, но `previews[id]` остаётся в state. Если пользователь снова нажмёт «Просмотр» без обновления, покажется устаревший предпросмотр (до снятия с продажи).

- **`previewMutation` очищает `applyResults[id]` при новом предпросмотре** — строка 327: `setApplyResults((prev) => { const n = { ...prev }; delete n[id]; return n; })`. Это правильное поведение, но `previews[id]` при повторном просмотре перезаписывается только если `previewMutation` завершился успехом. Если он завершится ошибкой — старый предпросмотр останется, но `openPreview === id` будет true → покажется устаревший preview.

- **`OfferIdsPanel.addId()` не сообщает об ошибке дублирования** — строки 104–109: если артикул уже есть в `currentIds`, функция молча сбрасывает input и ничего не делает. Пользователь не получает никакого feedback о причине.

- **`BrandAutocomplete.onSelect` не вызывает `setOpen(false)` напрямую** — строки 217–219: `handleSelect` вызывает `onSelect(brand.displayBrand)` и `setOpen(false)`. Но `onSelect` во внешнем компоненте (строка 358) только вызывает `setBrandInput(brand)`, что тригерит `useEffect` → `setDebouncedQ` → запрос → `setOpen(brands.length > 0)`. Если бренды снова подтянулись — dropdown откроется снова сразу после выбора.

- **JSON-файл `brand-bans.json` не имеет блокировки при параллельных записях** — строки 16–21 бэкенда: запись через `tmp + rename` атомарна, но если два запроса (например, `PATCH /offer-ids` и `DELETE`) придут одновременно, оба прочитают одну версию файла, оба запишут разные версии, и одна из записей потеряется. Нет mutex/lock.

## Improvement Ideas

- **[HIGH]** Исправить `mpLabel` и `MpBadge` — добавить ветку `marketplace === "wb"` с отдельным лейблом `"WB"` и цветом.

- **[HIGH]** После `applyMutation.onSuccess` очищать `previews[id]` чтобы следующий просмотр делал свежий запрос: `setPreviews((prev) => { const n = { ...prev }; delete n[id]; return n; })`.

- **[MEDIUM]** Показывать inline feedback в `OfferIdsPanel.addId()` при попытке добавить дубликат: `"Этот артикул уже в списке"`.

- **[MEDIUM]** Добавить debounce-защиту или `previewMutation.reset()` перед открытием preview при ошибке, чтобы устаревший предпросмотр не отображался.

- **[MEDIUM]** Добавить бэкенд-мьютекс (через in-process lock-переменную) на `writeBrandBans` чтобы избежать race condition при параллельных PATCH/DELETE.

- **[LOW]** Добавить поле поиска по названию бренда в списке запретов — при большом количестве брендов список становится длинным.

- **[LOW]** Добавить дату последнего применения (apply) в карточку бана — сейчас видна только дата добавления запрета (`bannedAt`).

## Code Notes

- Frontend: `frontend/src/routes/BrandBansPage.tsx`
- Backend: `server/parts/02d-brand-bans-routes.js`
