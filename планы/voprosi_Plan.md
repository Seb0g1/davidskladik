# QuestionsPage — Plan

## Current State

Страница вопросов по товарам агрегирует вопросы с Ozon и Wildberries (Яндекс.Маркет API вопросов не имеет — об этом явно написано в подсказке). Оператор видит список вопросов с фильтром по маркетплейсу и чекбоксом «только без ответа», отвечает прямо из UI с шаблонами и эмодзи. Отправка без `window.confirm`. Данные обновляются каждые 2 минуты. SKU-названия товаров Ozon резолвятся дополнительным API-запросом и кешируются 30 минут.

---

## Bugs / Issues Found

- **`needsAnswer` для Ozon — двойное условие с логической дырой**
  Строка 92 (`02f-questions-routes.js`): `needsAnswer: cleanText(question.status).toUpperCase() !== "PROCESSED" && !(Number(question.answers_count || 0) > 0)`. Если статус не "PROCESSED" (т.е. "NEW"), но `answers_count > 0` (вопрос уже имеет ответ, но статус не обновился на стороне Ozon) — `needsAnswer = false`. Это корректная защита. Но обратная ситуация: статус "PROCESSED" И `answers_count === 0` — вопрос не будет показан как требующий ответа, хотя ответа нет. Зависит от того, что Ozon считает истиной. Логика асимметрична и не документирована в коде.

- **`onlyNew` инвертирован относительно parameter name**
  Строка 57 (`02f-questions-routes.js`): `const onlyNew = String(request.query.unanswered ?? "true") !== "false"`. При `unanswered=false` параметр правильно даёт `onlyNew=false`. Но дефолт `?? "true"` означает что при отсутствии параметра `onlyNew=true` — то есть по умолчанию показываются только новые. Это поведение отличается от `/api/reviews`, где дефолт `?? "false"` (строка 115 `02f-reviews-routes.js`). Несогласованность API: отзывы по умолчанию все, вопросы по умолчанию только новые.

- **Фронт посылает `limit=50` жёстко, бэкенд поддерживает до 500**
  Строка 94 `QuestionsPage.tsx`: `limit=50` в URL. Бэкенд (строка 58 `02f-questions-routes.js`) принимает до 500. При большом числе вопросов оператор не видит старые.

- **`QuestionCard` в `.tsx` строка 44: лейбл маркетплейса всегда «Ozon» для не-WB**
  ```tsx
  {question.marketplace === "wb" ? "WB" : "Ozon"}
  ```
  Если в будущем добавят новый маркетплейс с вопросами, он будет подписан «Ozon». Нет ни `yandex`, ни дефолтного fallback с реальным значением.

- **`ozonSkuNameCache` — глобальный Map без ограничения размера (утечка памяти)**
  Строка 25 (`02f-questions-routes.js`): `const ozonSkuNameCache = new Map()`. Кеш никогда не очищается, только записи устаревают по времени (30 мин). Но старые записи не удаляются — при тысячах SKU Map растёт неограниченно. Аналогичной утечки нет в других модулях проекта.

- **`resolveOzonSkuNames` запрашивает `/v3/product/info/list` — это не публичный SKU-эндпоинт**
  Строка 36 (`02f-questions-routes.js`): `await ozonRequest("/v3/product/info/list", { sku: chunk.map(v => Number(v) || v) }, account)`. Если `sku` — числовые артикулы Ozon (не `offer_id`), то преобразование `Number(v) || v` корректно для строк-чисел, но если SKU пришёл как `"0"` (невалидный), `Number("0") || "0"` вернёт `"0"` (строку), что может вызвать ошибку API. Неочевидная граница.

- **Нет индикатора загрузки при первичном рендере**
  `QuestionsPage.tsx`, строка 148: `{!rows.length && !questionsQuery.isFetching ? <div className="empty-state">...</div> : null}` — пока идёт первый запрос, `.reviews-grid` пустой без скелетона.

- **Нет `useMemo` для `counters`**
  В `ReviewsPage` счётчики вычисляются через `useMemo`. В `QuestionsPage` (строки 121–122) счётчики считаются прямо в JSX при каждом рендере: `rows.filter(r => r.needsAnswer).length` и `rows.some(r => r.needsAnswer)`. При большом `rows` (50 элементов некритично, но паттерн непоследовательный).

- **WB: `skip=0` жёстко задан, нет пагинации**
  Строка 110 (`02f-questions-routes.js`): `?isAnswered=${isAnswered}&take=${take}&skip=0`. При `limit=50` и `take=50` если вопросов больше 50, всегда берётся только первая страница. Для `onlyNew=false` (показать все) делается два запроса с `skip=0` — вторая пачка дублирует первую по отвеченным, но уже не превышает `limit`.

---

## Improvement Ideas

- **[HIGH]** Исправить лейбл маркетплейса в `QuestionCard` (строка 44 `QuestionsPage.tsx`): заменить тернарный оператор на функцию `marketplaceLabel` аналогичную `ChatsPage`, которая корректно обрабатывает все значения.

- **[HIGH]** Ограничить размер `ozonSkuNameCache` (например, LRU на 500 записей) или периодически очищать устаревшие записи: `for (const [k, v] of ozonSkuNameCache) if (Date.now() - v.at > 30_60000) ozonSkuNameCache.delete(k)`.

- **[HIGH]** Согласовать дефолт параметра `unanswered` между `/api/reviews` и `/api/questions` — сейчас они разные (false vs true).

- **[MEDIUM]** Добавить скелетон-загрузчик при первичном рендере `.reviews-grid` (скопировать паттерн из `ChatsPage` с `ListSkeleton`).

- **[MEDIUM]** Добавить `useMemo` для счётчиков «Ждут ответа» (паттерн из `ReviewsPage`).

- **[MEDIUM]** Счётчик символов `{text.length}/5000` в форме ответа есть — но кнопка не блокируется при превышении. Добавить проверку `text.length <= 5000` в `disabled`.

- **[MEDIUM]** Для Ozon-вопросов добавить прямую ссылку на страницу товара в маркетплейсе — поле `productUrl` уже есть в типе и в нормализаторе.

- **[LOW]** Добавить пагинацию или «загрузить ещё» для вопросов — фронт запрашивает жёсткий `limit=50`.

- **[LOW]** WB: реализовать постраничную загрузку через `skip` вместо фиксированного `skip=0`.

---

## Code Notes

- Frontend: `frontend/src/routes/QuestionsPage.tsx`
- Backend: `server/parts/02f-questions-routes.js`
- Кеш SKU: `ozonSkuNameCache` (Module-level Map в `02f-questions-routes.js`)
- Шаблоны: `data/question-templates.json`
