# SettingsPage — Plan

## Current State

Страница Настройки содержит таб-навигацию (Цены, Маркетплейсы, AI, Инструменты, Сотрудники, Аудит, Система). Управляет: курсом USD, базовыми наценками, гибкими правилами наценки по маркетплейсам, правилами доступности, настройками AI-провайдера, брендингом (логотипы, доп. фото), поставщиками в режиме stock_only, пользователями и их ролями, аудит-логом, retry queue цен.

---

## Bugs / Issues Found

**1. Кнопка «Сохранить» — отсутствует `type="button"` (строка 864)**

```tsx
<button className="primary-action" onClick={() => save.mutate()} disabled={save.isPending}>
```
Кнопка находится внутри `<>` (React Fragment), но на странице есть вложенные `<form>`-подобные секции с `<input>`. Если браузер интерпретирует кнопку как `type="submit"`, нажатие Enter в любом поле может вызвать преждевременное сохранение. Следует явно указать `type="button"`.

**2. Кнопка «Тест» AI без `type="button"` (строка 1060)**

```tsx
<button className="secondary-action" onClick={() => testAi.mutate()} disabled={testAi.isPending}>Тест</button>
```
Аналогично: нет `type="button"`, может срабатывать по Enter.

**3. `isDirtyRef.current` не сбрасывается при переключении таба**

`isDirtyRef.current` устанавливается в `true` при любом изменении черновика (`update`, строка 844). Флаг сбрасывается только в `onSuccess` (строка 826). При переключении таба и возврате, если настройки обновились на сервере (кто-то другой сохранил), флаг `isDirtyRef.current = true` блокирует синхронизацию черновика с новыми данными с сервера (строка 815: `if (!isDirtyRef.current)`). Пользователь никогда не узнает о внешних изменениях.

**4. `normalizeAvailabilityRule` в бэкенде игнорирует "avito" и "wb" (строка 79 в `02a-app-settings.js`)**

```js
const marketplace = rawMarketplace === "ozon" || rawMarketplace === "yandex" ? rawMarketplace : "all";
```
В UI (строки 1037–1041 в `SettingsPage.tsx`) dropdown правил доступности предлагает только "all", "ozon", "yandex" — это соответствует бэкенду, но блокирует добавление правил для WB/Avito в будущем. Стоит задокументировать ограничение или унифицировать с `normalizeMarkupRule`.

**5. Кнопка «Codex Sale preset» не сохраняет apiKey (строка 1059)**

```tsx
onClick={() => updateAi({ ...codexSaleAiPreset, apiKey: draftAi.apiKey || "" })
```
При нажатии пресета `apiKey` берётся из текущего `draftAi.apiKey`, который может быть пустой строкой (если поле не трогали) — это правильно. Но если пользователь ввёл ключ, а потом нажал пресет, ключ сохраняется. Однако `codexSaleAiPreset` содержит хардкод `providerId: "codexsale"` и `textModel: "gpt-5.4-mini"`. Если модель изменится на сервере провайдера — пресет устареет без предупреждения.

**6. `SupplierStockModePanel` — поле `trustFactor` использует `defaultValue` вместо `value` (строка 731)**

```tsx
<input type="number" ... defaultValue={supplier.trustFactor ?? 100} ... onBlur={...} />
```
Использование `defaultValue` (неконтролируемый input) означает, что при обновлении данных из сервера (после `refetch`) значение в поле не обновится — пользователь видит старое значение. То же для `orderCutoffTime` (строка 739).

**7. `UsersSettingsPanel` — `createUser.isSuccess` не показывается пользователю**

После создания пользователя `onSuccess` только сбрасывает форму и вызывает `refreshUsers()`. Никакого `<div className="success-strip">` нет — пользователь должен догадаться по появлению нового пользователя в списке.

**8. Аудит-лог: ключ строки использует `entry.id || entry.createdAt` + индекс (строка 590)**

```tsx
key={`${String(entry.id || entry.createdAt || "")}-${index}`}
```
Использование индекса как fallback в ключе — потенциальная проблема при поиске (q): при изменении фильтра q список перестраивается, React может повторно использовать DOM-узлы неправильно. Нужен стабильный id.

**9. `SystemSettingsPanel` — `retryPrices` и `clearRetry` используют один `SyncStatusSchema` для несовместимых ответов (строка 617–627)**

Оба endpoint (`/api/warehouse/prices/retry` и `/api/warehouse/prices/retry-queue DELETE`) возвращают разные структуры, но парсятся одним `SyncStatusSchema`. Если схема строгая, парсинг упадёт в runtime.

---

## Improvement Ideas

- **[HIGH]** Добавить `type="button"` на все `<button>` без явного типа на странице (строки 864, 1060 и другие).
- **[HIGH]** Исправить `defaultValue` → `value` для `trustFactor` и `orderCutoffTime` в `SupplierStockModePanel`, хранить значения в локальном state.
- **[MEDIUM]** При получении новых данных с сервера (если `isDirtyRef.current = false`) показывать toast «Настройки обновлены» вместо молчаливой замены черновика.
- **[MEDIUM]** Добавить `<div className="success-strip">` при успешном создании пользователя в `UsersSettingsPanel`.
- **[MEDIUM]** Добавить `avito` и `wb` в dropdown правил доступности и соответствующую поддержку в `normalizeAvailabilityRule`.
- **[LOW]** Вынести пресет Codex Sale в конфиг (env или settings), чтобы модель `textModel` бралась актуальная, а не была захардкожена.
- **[LOW]** Использовать стабильные id в аудит-логе (например, только `entry.id`).

---

## Code Notes

- Frontend: `frontend/src/routes/SettingsPage.tsx`
- Backend: `server/parts/02a-app-settings.js`, `routes/users.js`
