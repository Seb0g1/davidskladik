# ImportPage (Импорт на Яндекс) — Plan

## Current State

Страница позволяет оператору вручную выбрать товары из Ozon-каталога и импортировать их на Яндекс.Маркет. Функции: обновление каталога с Ozon (async background refresh), поиск по артикулу/названию, фильтр по бренду, постраничный список кандидатов с флагами готовности, чекбоксы, «выбрать всех бренда», импорт выбранных. Дополнительно: синхронизация названий Ozon→Яндекс, добавление описаний, панель ТН ВЭД (collapsible `OzonAttributesPanel`).

---

## Bugs / Issues Found

- **`repairDescriptions.mutate()` вызывается с `dryRun: false` без подтверждения в UI — но confirm вызывается** — строка 419: `if (!window.confirm(...)) return; repairDescriptions.mutate()`. Хорошо. Но `dryRun: false` хардкожен в `mutationFn` (строка 364: `dryRun: false`). Это означает, что нет возможности запустить dry-run с UI — любое нажатие сразу применяет изменения. Между тем бэкенд (`02f-ozon-yandex-import-maint-routes.js`, строка 567) по умолчанию ставит `dryRun = request.body?.dryRun !== false`, т.е. ожидает явного `false` для реального запуска — логика на сервере обратная (default = dry), на клиенте всегда `false`.

- **`syncNames.mutate()` (строка 422) запускается без подтверждения** — операция синхронизации названий перезаписывает имена товаров на Яндексе для всех расходящихся карточек (потенциально тысячи), но вызывается немедленно по нажатию кнопки без `window.confirm`. При этом `repairDescriptions` — менее рискованная операция — имеет confirm.

- **`selected` Set не очищается при смене страницы / фильтра** — строки 386–399: `toggle` и `togglePage` работают с `selected` независимо от пагинации и фильтров. При переходе на страницу 2 или смене бренда чекбоксы страницы 1 остаются в `selected`. Пользователь может нажать «Импортировать» и отправить товары с прошлой страницы, которые уже не видны. `sendSelected.mutate` очищает `selected` только при успехе (строка 335).

- **`allPageSelected` может быть `true` при `eligibleOnPage.length === 0`** — строка 371: `const allPageSelected = eligibleOnPage.length > 0 && eligibleOnPage.every(...)`. Защита есть. Но чекбокс заголовка (строка 548) передаёт `disabled={!eligibleOnPage.length}` — ок. Однако `togglePage` (строка 393) не блокирован явно: если `eligibleOnPage` пусто, `forEach` просто не выполнится, что безопасно, но кнопка визуально не задизейблена при вызове из другого места.

- **`selectByBrand` накапливает IDs без возможности отменить выбор** — строки 339–349: мутация добавляет все eligible-id бренда в `selected` через `next.add(id)`. Нет кнопки «снять выбор бренда» — после нажатия «Выбрать все Dior» нельзя убрать именно Dior из выборки без ручного обхода по чекбоксам.

- **Дебаунс бренда (`brandInput`) сбрасывает страницу, но не очищает `selected`** — строки 308–310: при смене brandInput через `setBrandFilter` сбрасывается `page = 1`, но `selected` остаётся. Если было выбрано 50 товаров Dior, а потом набрали Chanel — выборка объединится.

- **`scanCapped` флаг в onlyEligible-режиме не учитывает поиск** — бэкенд (строка 109 `02f-ozon-yandex-import-page-routes.js`): `scanLimit = 50000`, но при активном `q` (поисковый запрос) `scanCapped` может стать `true`, хотя реальных товаров по запросу гораздо меньше — фронтенд (строка 439–442) покажет предупреждение «Показаны первые 50 000 товаров» даже когда это ложная тревога.

- **`OzonAttributesPanel`: `ozonTnvedDry.data?.candidates` условие для кнопки «Установить»** — строки 200–209 в `ImportPage.tsx`: кнопка «Установить» появляется только если `ozonTnvedDry.data?.candidates` не равен нулю. Но если dry-run вернул `candidates: 0` (уже все заполнены), пользователь не получит обратной связи — сообщение `renderResult` покажет «будет обновлено 0», но кнопка «Установить» не появится — пользователь может решить, что проверка не работает.

- **`repairYandexDescriptions` на бэкенде (строка 549) перезаписывает `raw` в Prisma полностью** — строка 549: `data: { raw: { ...(rows.find(...)?yandexRaw || {}), yandex: { ..., description } } }`. Это spread JSONB-объекта из `rows` (прочитанного в начале функции), а не актуального значения из DB. Если параллельный запрос обновил `raw` между чтением и записью, обновление затрёт новые данные.

---

## Improvement Ideas

- **[HIGH]** Добавить `window.confirm` перед `syncNames.mutate()` с указанием числа расхождений (если известно) — операция массово меняет названия на маркетплейсе.

- **[HIGH]** Сбрасывать `selected` при смене `brandFilter` или `debounced` (поискового запроса) — иначе невидимые товары остаются в выборке.

- **[HIGH]** Исправить race condition в `repairYandexDescriptions` (бэкенд) — использовать `$executeRaw` с `jsonb_set` вместо полной перезаписи `raw`, аналогично тому, как это сделано в других местах файла (строки 488–495, 509–513).

- **[MEDIUM]** Добавить кнопку «Снять выбор бренда» рядом с «Выбрать все `{brandFilter}`» — реализуется через `setSelected(prev => new Set([...prev].filter(id => !brandIds.has(id))))`.

- **[MEDIUM]** Показывать dry-run preview для «Добавить описания» перед реальным запуском — сейчас dry-run доступен только с API, из UI всегда `dryRun: false`.

- **[MEDIUM]** Добавить индикатор числа выбранных товаров «за пределами текущей страницы» — например «Выбрано 73, на этой странице 12» — чтобы оператор понимал, что в `selected` есть невидимые элементы.

- **[LOW]** Вынести магические числа `40` (pageSize), `5000` (limit в repairDescriptions) в константы.

- **[LOW]** `OzonAttributesPanel` хранит отдельные state для `ozonTnved` и `yandexTnved` — рассмотреть общий компонент `TnvedInput`, чтобы не дублировать идентичную структуру JSX трижды.

---

## Code Notes

- Frontend: `frontend/src/routes/ImportPage.tsx`
- Backend (кандидаты, refresh, send-selected): `server/parts/02f-ozon-yandex-import-page-routes.js`
- Backend (bulk send legacy): `server/parts/02f-ozon-yandex-import-send-route.js`
- Backend (sync-names, repair-descriptions, fix-categories, quarantine): `server/parts/02f-ozon-yandex-import-maint-routes.js`
