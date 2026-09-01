# Агент: Бот магазина Magic Vibes (@magicvibepafrum_bot)

## Задача
Создать Telegram-бота для покупателей магазина magicvibes.ru. Разворачивается на сервере 5.129.238.210. Функции: привязка аккаунта, рассылки, каталог, промокоды, статус заказа.

## Контекст
Сайт magicvibes.ru — это отдельный магазин парфюмерии (не davidsklad.ru). Посмотри папку `shop/` в корне проекта — там может быть код магазина. Изучи что уже реализовано для понимания API.

## Расположение файлов
```
bots/magicvibes-bot/
  index.js
  db.js           # JSON-база пользователей бота
  handlers/
    start.js      # приветствие + регистрация
    catalog.js    # каталог ароматов
    search.js     # поиск парфюма
    orders.js     # статус заказов
    broadcast.js  # рассылки (только для admin)
    promo.js      # промокоды
    profile.js    # профиль покупателя
    subscribe.js  # подписка на уведомления
  keyboards/
    main.js       # главное меню
    catalog.js    # навигация каталога
  .env.example
  package.json
  ecosystem.config.cjs
  deploy.sh
```

## Функционал бота

### Для покупателей:
- `/start` — приветствие, регистрация (имя + email для привязки аккаунта)
- `🔎 Каталог` — browse парфюмерии (категории → бренды → товары)
- `🔍 Поиск` — поиск по названию/бренду
- `📦 Мои заказы` — статус последних заказов (через API магазина)
- `👤 Профиль` — мои данные, привязанный аккаунт magicvibes.ru
- `🎁 Промокод` — ввести промокод для скидки
- `🔔 Уведомления` — подписка на новинки / акции конкретного бренда

### Привязка аккаунта:
1. Покупатель вводит email
2. Бот отправляет на email код подтверждения через API magicvibes.ru
3. Покупатель вводит код → аккаунт привязан
4. Хранится: `{ telegramId, email, accountId, linkedAt }` в `data/users.json`

### Рассылки (admin):
Администратор через бота может:
- `@admin broadcast <текст>` — рассылка всем подписчикам
- `@admin promo <код> <скидка%> <срок>` — создать промокод
- Отправлять фото + подпись

### Каталог:
Данные берутся с magicvibes.ru API (изучи какие эндпоинты есть). Если API нет — добавить в shop/ нужные роуты.
Товар в каталоге: фото + название + цена + кнопка "Купить на сайте" (ссылка)

### Inline-режим:
`@magicvibepafrum_bot dior` — быстрый поиск через inline mode, показывает карточки товаров прямо в чате

## Переменные окружения (.env)
```
TELEGRAM_BOT_TOKEN=8691183442:AAFoWdQuuNHwYdFUAoJmv3ipvFOVnRkJ83c
MAGICVIBES_API_BASE=https://magicvibes.ru
MAGICVIBES_API_SECRET=<секретный ключ>
ADMIN_TELEGRAM_IDS=<ID администраторов>
SMTP_HOST=<для отправки кодов подтверждения>
SMTP_USER=<email>
SMTP_PASS=<пароль>
NODE_ENV=production
```

## Технический стек
- Node.js + `node-telegram-bot-api` + `node-fetch` + `dotenv` + `nodemailer` (для email-кодов)
- Состояние пользователей: JSON файл (не база данных — достаточно для MVP)

## deploy.sh
Аналогично другим ботам — деплой на 5.129.238.210:
```bash
#!/bin/bash
SERVER=5.129.238.210
rsync -avz --exclude node_modules . root@$SERVER:/opt/bots/magicvibes-bot/
ssh root@$SERVER "cd /opt/bots/magicvibes-bot && npm install --production && pm2 restart magicvibes-bot || pm2 start ecosystem.config.cjs && pm2 save"
```

## Важно
- Бот должен быть дружелюбным, с эмодзи 💎🌸✨
- Inline keyboard для навигации (не текстовые команды)
- Pagination для каталога (по 5 товаров на страницу, кнопки ← →)
- Защита от спама рассылок: не более 1 broadcast в день
- Все ошибки в `logs/error.log`
