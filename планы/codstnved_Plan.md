# TnvedPage — Plan

## Current State

Страница «Коды ТН ВЭД» позволяет назначать коды ЕАЭС товарам на Ozon (по категориям/типам) и Яндекс Маркету (по offerId через Ozon-категории + fallback-код по умолчанию). Workflow: загрузить категории с Ozon → ввести 10-значные коды → сохранить → предпросмотр → отправить. Яндекс аналогично: задать дефолтный код → предпросмотр → отправить. Прогресс применения отображается через polling `/api/ozon/tnved/progress`.

## Bugs / Issues Found

- **Кнопка «Отправить на Ozon» заблокирована после `previewApply.mutate()` до ручного сохранения** — строка 251: `disabled={!hasAssignments || isApplying || (!saved && previewResult === null)}`. После успешного предпросмотра `previewResult` уже не `null`, кнопка разблокируется — это корректно. Но если пользователь изменил коды после предпросмотра (не нажав «Сохранить»), `saved` сброшен в `false`, `previewResult` не `null` → кнопка разблокирована, хотя несохранённые изменения есть. Коды, которые применятся, будут из БД (старые), а не из интерфейса.

- **`localCodes` не сбрасывается при refetch категорий** — строка 143–151: `useEffect` инициализирует `localCodes` только если `!Object.keys(localCodes).length`. При принудительном обновлении категорий (кнопка «Обновить») новые данные с бэкенда придут, но `localCodes` уже непустой → коды не обновятся. Если на бэкенде изменились `tnvedCode` (другой пользователь сохранил), клиент будет показывать устаревшие.

- **`applyMutation.onSuccess` не вызывает `setPreviewResult(null)` при async-режиме** — строка 183–193: `onSuccess: () => { setPreviewResult(null); ... }`. Бэкенд при `dryRun: false` возвращает `{ ok: true, async: true }` сразу (строка 609 бэкенда), а реальная работа в фоне. `setPreviewResult(null)` вызывается, но пользователь не видит прогресс сразу — надо дождаться polling. Это корректно, но `applyMutation.data` (строка 556) тоже вызывает `renderApplyResult` — при `async: true` покажется успех-полоска до фактического завершения.

- **`renderApplyResult` вызывается дважды для Яндекс** — строки 555–556:
  ```tsx
  {renderApplyResult(yandexPreview, yandexPreviewMutation.error as Error | null)}
  {renderApplyResult(yandexApplyMutation.data ?? null, yandexApplyMutation.error as Error | null)}
  ```
  Если одновременно есть и `yandexPreview` (результат предпросмотра) и `yandexApplyMutation.data` (результат применения), обе полоски рендерятся одновременно. Пользователь видит два блока результатов.

- **Валидация кода ТН ВЭД на клиенте — только при сохранении** — строки 382–386: проверка `!/^\d{10}$/.test(...)` происходит только в `onClick` кнопки «Сохранить», не inline при вводе. Пользователь может ввести некорректный код, нажать «Предпросмотр» (который не валидирует коды сам) и получить непонятную ошибку с бэкенда.

- **`progressQuery.refetchInterval` возвращает `0` когда `running === false`** — строка 129: `refetchInterval: (query) => (query.state.data?.running ? 2000 : 0)`. Когда `running` становится `false` (завершено), polling прекращается — это правильно. Но если страница открыта в момент завершения фоновой задачи (между двумя polling-тиками), прогресс-блок (`completedAt`) никогда не обновится. Нужен хотя бы один дополнительный fetch после `running: false`.

- **`/api/ozon/tnved/progress` не требует `requireAdmin`** — строка 583 бэкенда: `app.get("/api/ozon/tnved/progress", async ...)` — без middleware авторизации. Любой неаутентифицированный запрос получит данные о прогрессе (минимальная утечка инфо).

## Improvement Ideas

- **[HIGH]** Сбрасывать `localCodes` при обновлении категорий — в `useEffect` убрать условие `!Object.keys(localCodes).length`, чтобы при refetch данные синхронизировались.

- **[HIGH]** Добавить `requireAdmin` на `/api/ozon/tnved/progress` — согласованность с остальными tnved-эндпоинтами.

- **[MEDIUM]** Заблокировать «Отправить на Ozon» если `!saved` независимо от `previewResult` — или сбрасывать `previewResult` при изменении кодов (в `onChange` поля кода). Это предотвратит применение несохранённых данных.

- **[MEDIUM]** Добавить один дополнительный refetch после `running: false` — через `useEffect` с зависимостью `progress?.running`, чтобы поймать `completedAt`.

- **[MEDIUM]** Скрывать `yandexPreview` при успешном `yandexApplyMutation` — `onSuccess: () => { setYandexPreview(null); ... }`, чтобы не показывать два блока результатов.

- **[LOW]** Добавить inline-валидацию кода в `onChange` — подсвечивать поле красным если не 10 цифр, не ждать кнопки «Сохранить».

- **[LOW]** Добавить кнопку «Сбросить все коды» — очистить `localCodes` и вернуться к сохранённым значениям из БД без рефреша страницы.

## Code Notes

- Frontend: `frontend/src/routes/TnvedPage.tsx`
- Backend: `server/parts/02f-ozon-attribute-backfill.js` (строки 528–800)
