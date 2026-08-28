# NEWPLAN — Страница «Реализация»: полный редизайн

> Рабочая директория: `/var/www/davidsklad/davidskladik` (прод) / `C:/Users/Seb0g1/Documents/New project` (локально)
> Стек: Node.js/Express + PostgreSQL (Prisma) + MySQL PriceMaster (read-only, pool) + React 19 + TanStack Query + TailwindCSS
> Деплой: `git pull && npm run db:migrate && node server/assemble.js && pm2 restart davidsklad-api davidsklad-worker`
> Тесты: `npm test` (должны проходить все)
> Новые серверные файлы — регистрировать в `server/source.js`!

---

## Контекст: как работает реализация сейчас

**Модели Prisma:**
- `ConsignmentItem` — товар в реализации. Поля: id, name, article (`pm:{productId}` для PM-товаров), supplierName, partnerId, purchasePrice, salePrice, quantity, note, archived
- `ConsignmentOperation` — операция: type=receive/purchase/sale/return/writeoff/sponsor_topup/sponsor_payout/etc., sourceKey=`pm_sale_{rowId}` для PM-продаж

**MySQL PriceMaster таблицы (read-only через глобальный `pool`):**
- `Products` — номенклатура PM: ProductID, ProductName, SalePrice, Barcode и др.
- `SaleRows` — строки продаж: RowID, ProductID, Quantity, Price, DocID
- `SaleDocs` — документы продаж: DocID, DocDate, Comment
- `OfferRows` — ценовые строки: RowID, NativeID (артикул), NativeName, NativePrice, DocID

**Текущая логика PM-синка продаж:**
- `runConsignmentPmSync()` в `02d-consignment-routes.js`
- Инкрементный по `source_key = 'pm_sale_{rowId}'`
- Матч ТОЛЬКО по `article = 'pm:{productId}'` (нечёткий убран из-за ложных совпадений)
- Продажи до `item.createdAt` игнорируются

**Текущие проблемы:**
1. Нет UI для добавления товаров из PM-номенклатуры (нужно вручную вводить имя/артикул)
2. Нет приходной накладной с историей
3. Матч только по `pm:{productId}`, без проверки имени — если productId совпал случайно, данные некорректны
4. Спонсор не получает уведомлений о заработке

---

## Задачи для агентов

---

### Задача 1: PM-номенклатура браузер + добавление в реализацию

**Файл агента:** создаёт `server/parts/02d-consignment-pm-nomenclature.js` + обновляет фронт

**ПРОМТ ДЛЯ АГЕНТА:**
```
Задача: добавить браузер PM-номенклатуры для реализации — оператор видит все товары из PM.Products и добавляет нужные одним кликом.

Контекст:
- Рабочая директория: C:/Users/Seb0g1/Documents/New project
- PM MySQL (read-only): глобальный pool (mysql2), таблица Products (ProductID, ProductName, SalePrice)
- PostgreSQL: prisma, модель ConsignmentItem (article = "pm:{productId}" для PM-товаров)
- Фронт: frontend/src/routes/ConsignmentPage.tsx (или найди через glob)
- npm test + npm run frontend:check должны пройти

Шаг 1 — бэкенд: добавить 2 роута в server/parts/02d-consignment-routes.js

GET /api/consignment/pm-nomenclature?q=&page=&limit=50
Запрос к PM MySQL:
```sql
SELECT p.ProductID, p.ProductName, p.SalePrice, p.Barcode
FROM Products p
WHERE p.ProductID > 0
  AND (? = '' OR p.ProductName LIKE CONCAT('%', ?, '%'))
ORDER BY p.ProductName ASC
LIMIT ? OFFSET ?
```
Параметры: q (строка поиска), page (0-based), limit (50)
Для каждого ProductID проверить есть ли уже ConsignmentItem с article=`pm:{productId}`:
  const existingIds = await prisma.consignmentItem.findMany({
    where: { article: { in: productIds.map(id => `pm:${id}`) } },
    select: { article: true },
  });
Вернуть: { ok: true, items: [{ productId, name, salePrice, barcode, alreadyAdded: bool }], total, page }

POST /api/consignment/pm-nomenclature/add
Body: { productId: number, purchasePrice: number, quantity: number }
- Найти продукт в PM: SELECT ProductName, SalePrice FROM Products WHERE ProductID = ?
- Создать ConsignmentItem: { name: productName, article: `pm:${productId}`, purchasePrice, salePrice: pmSalePrice, quantity }
- Если quantity > 0: создать ConsignmentOperation type="receive"
- Если уже существует (article уже есть) → вернуть 409 с { error: "Товар уже добавлен", item: existing }

Шаг 2 — фронт ConsignmentPage.tsx:
Добавить кнопку "Добавить из PM" рядом с кнопкой "Добавить товар".
При нажатии — открыть modal/drawer с:
- Поле поиска (debounce 300ms) → useQuery для /api/consignment/pm-nomenclature?q=
- Список результатов: ProductName | SalePrice | [Добавить] / [Уже добавлен]
- При клике "Добавить": показать inline-форму с полями "Закупочная цена" (pre-filled из SalePrice) и "Кол-во"
- После добавления: invalidate consignment queries, toast "Добавлено: {name}"
- Пагинация: кнопка "Загрузить ещё" (page++)

npm test + npm run frontend:check в конце. Репорт: что реализовано.
```

---

### Задача 2: Приходная накладная (история поступлений)

**ПРОМТ ДЛЯ АГЕНТА:**
```
Задача: реализовать приходные накладные для реализации — оператор формирует накладную из PM-номенклатуры, указывает количество и цену закупки, накладная сохраняется в истории.

Контекст:
- Рабочая директория: C:/Users/Seb0g1/Documents/New project
- PostgreSQL: prisma (schema.prisma), prisma/migrations/ для новых миграций
- PM MySQL (read-only): pool, таблица Products
- Фронт: frontend/src/routes/ConsignmentPage.tsx
- НЕ запускать npm run db:migrate (нет соединения с БД) — создать SQL migration файл вручную
- npm test + npm run frontend:check должны пройти

Шаг 1 — Prisma schema: добавить 2 модели в prisma/schema.prisma

model ConsignmentInvoice {
  id          String    @id @default(cuid())
  number      String    // "ПН-001", "ПН-002" и т.д. (генерируется автоматически)
  supplierName String?  @map("supplier_name")
  note        String?
  totalAmount Float     @default(0) @map("total_amount") // сумма закупки
  createdBy   String?   @map("created_by")
  createdAt   DateTime  @default(now()) @map("created_at")
  items       ConsignmentInvoiceItem[]
  @@map("consignment_invoices")
}

model ConsignmentInvoiceItem {
  id          String   @id @default(cuid())
  invoiceId   String   @map("invoice_id")
  itemId      String?  @map("item_id")   // ConsignmentItem.id (null если не создан)
  name        String
  article     String?  // "pm:{productId}"
  quantity    Int
  unitPrice   Float    @map("unit_price")
  invoice     ConsignmentInvoice @relation(fields: [invoiceId], references: [id], onDelete: Cascade)
  @@index([invoiceId])
  @@map("consignment_invoice_items")
}

Шаг 2 — SQL migration:
Создай prisma/migrations/YYYYMMDDHHMMSS_add_consignment_invoices/migration.sql:
```sql
CREATE TABLE consignment_invoices (
  id TEXT PRIMARY KEY,
  number TEXT NOT NULL,
  supplier_name TEXT,
  note TEXT,
  total_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE consignment_invoice_items (
  id TEXT PRIMARY KEY,
  invoice_id TEXT NOT NULL REFERENCES consignment_invoices(id) ON DELETE CASCADE,
  item_id TEXT,
  name TEXT NOT NULL,
  article TEXT,
  quantity INTEGER NOT NULL,
  unit_price DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_consignment_invoice_items_invoice_id ON consignment_invoice_items(invoice_id);
```

Шаг 3 — бэкенд (добавить в 02d-consignment-routes.js):

GET /api/consignment/invoices?page=&limit=20
  Возвращает список накладных с items[], сортировка по createdAt DESC

POST /api/consignment/invoices
Body: {
  supplierName?: string,
  note?: string,
  items: [{ name, article, quantity, unitPrice }]  // article = "pm:{productId}" для PM-товаров
}
Логика:
1. Генерировать номер: "ПН-{count+1}" (count = всего накладных в БД)
2. Для каждой строки items: найти или создать ConsignmentItem по article (если article = "pm:{productId}") или по name
3. Создать ConsignmentInvoice + ConsignmentInvoiceItems
4. Для каждого ConsignmentItem: увеличить quantity += item.quantity
5. Создать ConsignmentOperation type="purchase" для каждой строки (balanceDelta = -unitPrice * quantity из баланса)
6. Вернуть { ok: true, invoice: {...} }

GET /api/consignment/invoices/:id — детали накладной

Шаг 4 — фронт ConsignmentPage.tsx:
Добавить кнопку "Приходная накладная" → открывает страницу/modal:

Форма накладной:
- Поставщик (текст)
- Примечание
- Таблица строк: [Поиск из PM или ввод вручную] | Кол-во | Цена закупки | Удалить
  - Поиск из PM: поле с автодополнением → /api/consignment/pm-nomenclature?q=
  - Или ввод вручную: просто текстовое поле имени
- Итого: сумма
- Кнопки: "Добавить строку" / "Провести накладную"

История накладных (отдельная вкладка):
- Таблица: Номер | Дата | Поставщик | Кол-во позиций | Сумма
- Клик → детали накладной

npm test + npm run frontend:check. Репорт: что реализовано.
```

---

### Задача 3: Улучшенный матч PM-продаж (по ProductID + имени)

**ПРОМТ ДЛЯ АГЕНТА:**
```
Задача: улучшить синхронизацию PM-продаж в реализацию — добавить матч по имени как fallback, плюс добавить поле pmName в ConsignmentItem для двойной проверки.

Контекст:
- Рабочая директория: C:/Users/Seb0g1/Documents/New project
- PM синк: функция runConsignmentPmSync() в server/parts/02d-consignment-routes.js (строки ~849-1050)
- Текущий матч: ТОЛЬКО article = "pm:{productId}" — нет имени
- Проблема: если productId совпал случайно с другим товаром → ложные продажи

Изменения только в runConsignmentPmSync():

Шаг 1 — добавить поле pmName в ConsignmentItem (через raw JSON, без новой миграции):
При создании ConsignmentItem через POST /api/consignment/pm-nomenclature/add:
  raw: { pmProductId: productId, pmName: productName }
Это позволит проверять имя при синке.

Шаг 2 — улучшить матч в runConsignmentPmSync():
После нахождения ConsignmentItem по article = "pm:{productId}":
  - Если item.raw?.pmName существует: сравнить с productName из PM (normalized)
  - Если имена сильно разные (Jaccard similarity < 0.5): пропустить продажу + logger.warn("pm_sale_name_mismatch", { productId, pmName, storedName })
  - Если pmName не задан (старые товары): продолжить как раньше (совместимость)

Шаг 3 — Fallback матч по имени:
В runConsignmentPmSync(), после основного матча по productId:
  Для SaleRows строк у которых НЕТ матча по productId:
  - Взять productName из PM
  - Попробовать найти ConsignmentItem где name.toLowerCase() === productName.toLowerCase()
  - Если нашли: использовать этот item (но НЕ обновлять article — только запись продажи)
  - logger.info("pm_sale_name_fallback_match", { productId, productName, itemId })

Важно: НЕ удалять существующую логику матча по article — только добавить проверки

Шаг 4 — npm test. Репорт: что изменено в runConsignmentPmSync.
```

---

### Задача 4: Автообнаружение новых PM-товаров

**ПРОМТ ДЛЯ АГЕНТА:**
```
Задача: фоновая задача, которая раз в день проверяет появились ли новые товары в PM.Products, которых ещё нет в реализации — и логирует их для оператора.

Контекст:
- Рабочая директория: C:/Users/Seb0g1/Documents/New project
- PM MySQL (read-only): pool, таблица Products
- PostgreSQL: prisma, ConsignmentItem (article = "pm:{productId}")
- Планировщики: server/parts/02f-daily-maintenance-schedulers.js
- Логгер: logger (pino)
- npm test должно пройти

Шаг 1 — функция checkNewPmNomenclatureItems() в 02d-consignment-routes.js:
```js
async function checkNewPmNomenclatureItems() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { skipped: true };
  
  // Все productId которые уже есть в реализации
  const existing = await prisma.consignmentItem.findMany({
    where: { article: { startsWith: 'pm:' } },
    select: { article: true },
  });
  const existingIds = new Set(existing.map(e => Number(e.article.replace('pm:', ''))));
  
  // Все активные товары из PM
  const [pmProducts] = await pool.query(`
    SELECT ProductID, ProductName, SalePrice
    FROM Products
    WHERE ProductID > 0 AND (Active IS NULL OR Active = 1)
    ORDER BY ProductID DESC
    LIMIT 1000
  `);
  
  const newItems = pmProducts.filter(p => !existingIds.has(p.ProductID));
  
  if (newItems.length > 0) {
    logger.info('pm_new_nomenclature_items', {
      count: newItems.length,
      items: newItems.slice(0, 20).map(p => ({ id: p.ProductID, name: p.ProductName })),
    });
  }
  return { newCount: newItems.length, checked: pmProducts.length };
}
```
Экспортировать функцию.

Шаг 2 — добавить в ежедневный планировщик (02f-daily-maintenance-schedulers.js):
Найти функцию ежедневного обслуживания и добавить вызов checkNewPmNomenclatureItems() с try/catch.

Шаг 3 — GET /api/consignment/pm-nomenclature/new (для фронта):
Возвращает список товаров из PM которых ещё нет в реализации (первые 100).
Используй ту же логику что в checkNewPmNomenclatureItems().
Формат: { ok: true, items: [{ productId, name, salePrice }], total }

Шаг 4 — на ConsignmentPage добавить индикатор (badge):
Если /api/consignment/pm-nomenclature/new возвращает total > 0:
Показать бейдж "Новые товары в PM: N" с кнопкой открыть список

npm test. Репорт: что реализовано.
```

---

### Задача 5: Telegram-уведомления спонсору о заработке

**ПРОМТ ДЛЯ АГЕНТА:**
```
Задача: ежедневно отправлять спонсору в Telegram сводку: сколько заработал за день и текущий баланс.

Контекст:
- Рабочая директория: C:/Users/Seb0g1/Documents/New project
- Telegram-бот: посмотри server/parts/02a-telegram-alerts.js (токен TELEGRAM_BOT_TOKEN)
- Реализация: prisma, ConsignmentOperation (type="sale", sponsorDelta = прибыль спонсора)
- Настройки: server/parts/02a-app-settings.js (appSettings, как хранятся настройки)
- Планировщик: server/parts/02f-daily-maintenance-schedulers.js
- npm test должно пройти

Шаг 1 — добавить настройки спонсора в appSettings:
Grep: grep -n "appSettings\|saveAppSettings\|SETTINGS_KEYS\|settingsKey" server/parts/02a-app-settings.js
Добавить в список настроек:
  sponsorTelegramChatId: "" // Chat ID спонсора в Telegram
  sponsorDailyReportEnabled: true // Включить ежедневный отчёт

Шаг 2 — добавить UI в SettingsPage.tsx:
Найди секцию настроек реализации/финансов. Добавить поля:
  - "Telegram Chat ID спонсора" (input type=text)
  - "Ежедневный отчёт спонсору" (checkbox)
Сохранять через существующий механизм настроек.

Шаг 3 — функция sendSponsorDailyReport() в 02d-consignment-routes.js:
```js
async function sendSponsorDailyReport() {
  const settings = getAppSettings(); // или appSettings глобальный
  const chatId = settings.sponsorTelegramChatId;
  if (!chatId || !settings.sponsorDailyReportEnabled) return { skipped: true };
  
  const prisma = getPrisma();
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  
  // Прибыль спонсора за сегодня
  const todayOps = await prisma.consignmentOperation.findMany({
    where: { createdAt: { gte: today }, type: { in: ['sale', 'return'] } },
  });
  const todaySponsorProfit = todayOps.reduce((sum, op) => sum + (op.sponsorDelta || 0), 0);
  
  // Общий баланс спонсора (все операции)
  const allOps = await prisma.consignmentOperation.findMany({
    select: { sponsorDelta: true, balanceDelta: true, type: true },
  });
  const totalBalance = allOps.reduce((sum, op) => sum + (op.balanceDelta || 0), 0);
  const totalSponsorProfit = allOps.reduce((sum, op) => sum + (op.sponsorDelta || 0), 0);
  
  const message = [
    `📊 Ежедневный отчёт реализации`,
    ``,
    `💰 Ваша прибыль за сегодня: ${todaySponsorProfit.toFixed(0)} ₽`,
    `📈 Накопленная прибыль: ${totalSponsorProfit.toFixed(0)} ₽`,
    `💳 Баланс счёта: ${totalBalance.toFixed(0)} ₽`,
    ``,
    `📅 ${new Date().toLocaleDateString('ru-RU')}`,
  ].join('\n');
  
  // Отправить через Telegram Bot API
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) return { skipped: true, reason: 'no_token' };
  await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text: message }),
    signal: AbortSignal.timeout(5000),
  });
  
  logger.info('sponsor_daily_report_sent', { chatId, todayProfit: todaySponsorProfit });
  return { ok: true, todayProfit: todaySponsorProfit, balance: totalBalance };
}
```

Шаг 4 — добавить вызов в ежедневный планировщик:
В 02f-daily-maintenance-schedulers.js добавить вызов sendSponsorDailyReport() — в конце дневного обслуживания (примерно в 20:00 или как настроено DAILY_SYNC_TIME).

Шаг 5 — POST /api/consignment/sponsor-report/send (ручной запуск):
Для тестирования. Вызывает sendSponsorDailyReport() и возвращает результат.

npm test. Репорт: что реализовано, как добавить настройки.
```

---

## Порядок запуска агентов

Запускать все параллельно:

```
Агент 1: Задача 1 (PM-номенклатура браузер)
Агент 2: Задача 2 (Приходная накладная)
Агент 3: Задача 3 (Улучшенный PM матч)
Агент 4: Задача 4 (Автообнаружение новых товаров PM)
Агент 5: Задача 5 (Telegram спонсору)
```

Задачи 1 и 2 зависят друг от друга (накладная использует PM-номенклатуру).
Задачи 3, 4, 5 независимы.
Если запускать последовательно: сначала 1, затем 2+3+4+5 параллельно.

## После реализации: деплой

```bash
ssh root@81.17.154.153
cd /var/www/davidsklad/davidskladik
git pull
npm run db:migrate    # для новых ConsignmentInvoice моделей
node server/assemble.js
pm2 restart davidsklad-api davidsklad-worker
```
