# Агент: Бот реализации (@davidskladrealizarsuya_bot)

## Задача
Создать Telegram-бота для партнёров по реализации. Бот разворачивается на сервере 5.129.238.210 (Нидерланды) как самостоятельный Node.js-процесс, берёт данные через API сайта davidsklad.ru.

## Расположение файлов
Создать папку `bots/realizatsiya-bot/` в корне проекта:
```
bots/realizatsiya-bot/
  index.js          # точка входа
  api.js            # клиент к davidsklad.ru API
  handlers/
    start.js
    balance.js
    sales.js
    products.js
    report.js
  .env.example      # шаблон переменных окружения
  package.json
  ecosystem.config.cjs  # PM2 конфиг для продакшена
  deploy.sh         # скрипт деплоя на 5.129.238.210
```

## Технический стек
- Node.js + библиотека `node-telegram-bot-api` (npm install node-telegram-bot-api node-fetch dotenv)
- Данные берутся через HTTP запросы к `https://davidsklad.ru/api/...`
- Авторизация к API: сессионная кука или API-ключ (посмотри как работает аутентификация в `server/parts/01-bootstrap-auth-core.js`)

## Функционал бота

### Команды:
- `/start` — приветствие, инструкция по привязке партнёра
- `/balance` — текущий баланс (сколько должен магазин партнёру)
- `/sales` — продажи за текущий месяц (список товаров, кол-во, сумма)
- `/report` — полный отчёт за последние 30 дней (текстовый + CSV)
- `/products` — список товаров партнёра на реализации (артикул, остаток, цена)
- `/help` — помощь

### API-запросы к davidsklad.ru:
Бот должен обращаться к следующим эндпоинтам (изучи `server/parts/02d-consignment*.js` для понимания структуры):
- `GET /api/consignment/items` — товары на реализации
- `GET /api/consignment/operations` — операции (продажи)
- `GET /api/consignment/balance` — баланс партнёра

Для авторизации к API нужен сервисный API-ключ (или передавать партнёрский идентификатор). Изучи существующие роуты и добавь эндпоинт `GET /api/consignment/partner-summary?partnerId=...&secret=...` если его нет.

### Привязка партнёра:
- При старте бот просит ввести код партнёра (или имя из базы)
- Код проверяется через API, сохраняется в `data/realizatsiya-bot-users.json` (Map: telegramId → partnerId)
- Если партнёр не привязан — доступны только `/start` и `/help`

### Форматирование ответов:
- Используй HTML-форматирование Telegram (`parse_mode: 'HTML'`)
- Числа — с разделителями тысяч, рубли — суммы в ₽
- Даты — в формате ДД.ММ.ГГГГ

## Переменные окружения (.env)
```
TELEGRAM_BOT_TOKEN=8993485518:AAG0vnkx_QbDiPEiXuWH0Srh6aINOjd64cQ
DAVIDSKLAD_API_BASE=https://davidsklad.ru
DAVIDSKLAD_API_SECRET=<создать секретный ключ для межсерверных запросов>
BOT_USERS_FILE=./data/users.json
NODE_ENV=production
```

## deploy.sh
Скрипт должен:
1. Подключиться по SSH к 5.129.238.210 (ключ или пароль через sshpass)
2. Создать папку `/opt/bots/realizatsiya-bot/`
3. Скопировать файлы через rsync или scp
4. Установить зависимости `npm install --production`
5. Запустить через PM2: `pm2 start ecosystem.config.cjs --env production`
6. Сохранить PM2 конфиг: `pm2 save`

## ecosystem.config.cjs
```js
module.exports = {
  apps: [{
    name: 'realizatsiya-bot',
    script: 'index.js',
    env_production: { NODE_ENV: 'production' },
    max_memory_restart: '256M',
    restart_delay: 5000,
  }]
}
```

## Важно
- Не хранить токен в коде — только через .env
- Добавить обработку ошибок на все API-запросы (если davidsklad.ru недоступен — вежливое сообщение партнёру)
- Логировать ошибки в файл `logs/error.log`
- Бот должен работать 24/7 через PM2
