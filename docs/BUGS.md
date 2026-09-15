# Аудит багов — davidsklad / magicvibes.ru

Дата аудита: 2026-09-15  
Охват: server/parts/, lib/, shop/src/  
Статус: `[ ]` — не исправлен, `[x]` — исправлен

---

## Критичные (CRITICAL)

### [x] BUG-001 · shop: реферальные баллы — несогласованные fire-and-forget операции
**Файл:** `server/parts/02d-shop-api-routes.js:1722-1729`  
**Тип:** data-inconsistency  

Два Prisma-запроса запускаются без `await` и с `.catch(() => {})`:
```js
prisma.shopPointTransaction.create({ ... }).catch(() => {});
prisma.shopCustomer.update({ ... data: { loyaltyPoints: { increment: N } } }).catch(() => {});
```
Если первый (`create`) успеет, а второй (`update`) упадёт (DB timeout, прочая ошибка) — в таблице будет `PointTransaction` без соответствующего увеличения баланса. Ошибка нигде не логируется.

**Фикс:** обернуть в `Promise.all` с `await` и логировать ошибку.

---

## Высокие (HIGH)

### [x] BUG-002 · shop: race condition применения промокода
**Файл:** `server/parts/02d-shop-api-routes.js:1692-1696`  
**Тип:** race-condition  

`readShopPromoCodes()` → изменение в памяти → `writeShopPromoCodes()` без каких-либо блокировок. При двух одновременных заказах с одним промокодом оба процесса прочитают одно состояние и запишут usageCount+1 вместо +2. Ограниченные промокоды могут использоваться сверх лимита.

**Фикс:** использовать named mutex (аналог GET_LOCK в MySQL) или переместить промокоды в Postgres с транзакционным обновлением.

---

### [x] BUG-003 · shop: NaN в totalRub из-за невалидного priceRub
**Файл:** `server/parts/02d-shop-api-routes.js:1655`  
**Тип:** wrong-logic  

```js
const baseTotal = items.reduce((s, i) => s + Number(i.priceRub || 0) * Number(i.quantity || 1), 0);
```
`Number("abc" || 0)` → `Number("abc")` → `NaN`. `NaN` распространяется через умножение и `Math.round(NaN)` = `NaN`. Значение `NaN` пишется в поле `totalRub` Postgres-записи заказа, платёж на `NaN` рублей не создаётся, заказ виснет.

**Фикс:** добавить `Number.isFinite(price) && Number.isFinite(qty)` перед суммированием; вернуть 400 если итог нечисловой.

---

### [x] BUG-004 · scheduler: unhandled rejection в finally блоке OzonUnarchiveQueue
**Файл:** `server/parts/02f-background-schedulers.js:152`  
**Тип:** crash  

```js
} finally {
  scheduleOzonUnarchiveQueueAuto(await nextOzonUnarchiveQueueAutoDelayMs());
}
```
`await` внутри `finally` в async-callback `setTimeout`. Если `nextOzonUnarchiveQueueAutoDelayMs()` выбросит исключение — это unhandled rejection внутри отдельного async-контекста. В Node.js 18+ может завершить процесс. Минимальный эффект — следующий тик OzonUnarchive не будет запланирован (очередь встаёт).

**Фикс:** обернуть `await` в try/catch с fallback-задержкой.

---

### [x] BUG-005 · yandex-fast-unarchive: ошибка одного target останавливает остальные
**Файл:** `server/parts/02f-yandex-fast-unarchive.js:101-113`  
**Тип:** wrong-logic  

```js
for (const [target, items] of groupByTarget(toUnarchive).entries()) {
  ...
  const results = await sendYandexOfferArchiveState(shop, items.map(...), false);
  // нет try-catch
}
```
Если `sendYandexOfferArchiveState` выбросит для одного target — цикл прерывается, все оставшиеся target пропускаются. В результате часть товаров может остаться в архиве, хотя фактически успешно разархивирована.

**Фикс:** обернуть тело цикла в `try/catch` с логированием, продолжать итерацию.

---

### ~~BUG-006~~ · shop: currency null — FALSE POSITIVE
**Файл:** `server/parts/02d-shop-api-routes.js:~465`  
**Тип:** wrong-logic  

Если `snapCurrency` из PM-снапшота `null`/`undefined`, условие `snapCurrency === "RUB"` возвращает `false` и рублёвая цена умножается на `usdRate × markup`, завышая её в ~200 раз. Аналогично — если `usdRate` равен 0, итоговая цена равна 0.

**Фикс:** добавить guard `if (!usdRate || usdRate < 1)` с fallback; явно обрабатывать `null` currency как `RUB` или `USD` согласно источнику.

---

## Средние (MEDIUM)

### [x] BUG-007 · shop-bot: потеря Telegram-обновлений при ошибке в обработчике
**Файл:** `server/parts/02f-shop-bot.js:327-329`  
**Тип:** data-loss  

```js
_sbotOffset = updates[updates.length - 1].update_id + 1; // ← сразу
for (const upd of updates) await _sbotHandleUpdate(upd);  // ← потом
```
Offset продвигается ДО обработки всех апдейтов. Если `_sbotHandleUpdate` выбросит на N-м сообщении из M — исключение поймается в `catch`, а сообщения N+1..M молча потеряются (offset уже прошёл мимо них).

**Фикс:** переместить `_sbotOffset = ...` ПОСЛЕ цикла; добавить `try/catch` внутри цикла.

---

### [x] BUG-008 · marketplace-sync-core: payload.stocks без null-проверки
**Файл:** `server/parts/02f-marketplace-sync-core.js:128, 193, 350`  
**Тип:** crash  

```js
const payload = { stocks: await buildOzonStockPayloadItems(...) };
if (!payload.stocks.length) continue;  // crash если stocks === null/undefined
```
Если `buildOzonStockPayloadItems` вернёт `null` или `undefined` при сетевой ошибке — `.length` бросает TypeError, прерывая весь sync-цикл.

**Фикс:** изменить на `if (!payload.stocks?.length) continue;`.

---

### BUG-009 · supplier-cart-insert: запрос к picking-state внутри критической секции
**Файл:** `server/parts/02d-supplier-cart-insert.js:580-608`  
**Тип:** race-condition  

`readSupplierPickingState()` и `writeSupplierPickingState()` вызываются внутри POST-обработчика после релиза MySQL-лока. Два параллельных commit (manual и auto) могут прочитать один и тот же picking-state и затем оба записать — второй затирает строки, добавленные первым.

**Фикс:** использовать существующий `withWarehouseProductMutationLock` или отдельный mutex для picking-state.

---

### [x] BUG-010 · shop: заказ висит в pending если платёж не создался
**Файл:** `server/parts/02d-shop-api-routes.js:1751-1756`  
**Тип:** data-loss  

Заказ сохраняется в DB (line 1708) до создания платежа (line 1751). Если `_ozonPayCreateOrder` вернёт `null` (Ozon API недоступен), статус заказа остаётся `"pending"`. Клиент уходит, заказ остаётся в базе навсегда без привязанного платежа.

**Фикс:** при `!paymentUrl` установить `status: "payment_failed"` и логировать; добавить CRON для очистки/повторной попытки старых `pending`-заказов.

---

## Низкие (LOW)

### ~~BUG-011~~ · price-master-build: ЛОЖНАЯ ТРЕВОГА
Функция уже вызывается один раз и результат сохраняется в `productMarkupOverride` (строка 86). Баг не существует.

---

### [x] BUG-012 · yandex-fast-unarchive: targetStock fragile при "0" и NaN
**Файл:** `server/parts/02f-yandex-fast-unarchive.js:~173`  
**Тип:** wrong-logic  

`Number(item.targetStock || "0")` — если `item.targetStock` равен числу `0` (falsy), выражение `0 || "0"` → `"0"` → `Number("0")` = 0, что правильно. Однако если targetStock — невалидная строка типа `"none"`, то `Number("none")` = `NaN`, и `NaN` пишется в остатки на ЯМ (API принимает числа).

**Фикс:** `const stock = Number.isFinite(Number(item.targetStock)) ? Number(item.targetStock) : 0;`

---

---

## Второй раунд аудита

### [x] BUG-013 · shop: манипуляция ценой через клиентский priceRub
**Файл:** `server/parts/02d-shop-api-routes.js:1655-1659`  
**Тип:** security  

Сервер принимает `priceRub` из тела запроса без какой-либо валидации против реальной цены в БД. Клиент может изменить `priceRub` в localStorage/DevTools с 1 000 ₽ до 1 ₽ и заказ будет создан с поддельной суммой. `_ozonPayCreateOrder` получит `totalRub = 1`, что фактически позволяет оплатить заказ за 1 рубль.

**Фикс:** добавить минимальную цену на товар + проверку против `currentPrice` из БД.

---

### BUG-014 · shop: price currency null в buildShopProductsFromDb
~~FALSE POSITIVE~~ — строка 462 имеет `String(snap.currency || "USD")`, null уже защищён.

---

### BUG-015 · price-history: записывается исходная цена, а не фактически отправленная
**Файл:** `server/parts/02d-prices-queue-inline.js` или `02f-price-retry.js:~86-101`  
**Тип:** wrong-logic  

В `historyRows` новая цена записывается из `item.price` (исходный targetItems), тогда как на ЯМ отправляется `roundPrice(item.price)` из yandexItems. Если цена округляется, история содержит неокруглённое значение — несоответствие в аудит-логе.

**Фикс:** использовать фактически отправленное значение `y.price.value` при записи в historyRows.

---

## Уже исправлено (в этой сессии)

- ~~BUG-A~~: pool null check перед `pool.getConnection()` в supplier-cart-insert → исправлено (коммит 31767fe1)
- ~~BUG-B~~: returnCoveredRows не писались в state.processed → исправлено (коммит 31767fe1)
- ~~BUG-C~~: replace-supplier продолжал работу при ошибке удаления PM-строки → исправлено (коммит 31767fe1)
- ~~BUG-D~~: belt-and-suspenders SELECT FOR UPDATE перед INSERT в supplier-cart → исправлено (коммит 6cb2e532)
