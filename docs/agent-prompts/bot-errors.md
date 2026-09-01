# Агент: Бот мониторинга ошибок (@MagicVibeAlert_bot)

## Задача
Создать/доработать Telegram-бот для мониторинга davidsklad.ru. Разворачивается на сервере 5.129.238.210. Бот получает алерты от сайта и позволяет администратору запрашивать статус системы.

## Контекст существующего кода
В проекте уже есть `server/parts/02a-telegram-alerts.js` — функция `sendTelegramAlert(type, message)` которая отправляет уведомления через `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`. Изучи этот файл.

Также есть `data/health-watchdog-incidents.jsonl` и `data/health-watchdog-state.json` — данные watchdog'а.

## Расположение файлов
```
bots/errors-bot/
  index.js
  handlers/
    status.js     # статус сервера
    logs.js       # последние ошибки
    restart.js    # команда рестарта (с подтверждением)
    incidents.js  # история инцидентов
  .env.example
  package.json
  ecosystem.config.cjs
  deploy.sh
```

## Функционал бота

### Входящие алерты (Push от сервера):
Бот принимает POST-запросы от davidsklad.ru на webhook или polling.
Сервер davidsklad.ru отправляет алерты через `sendTelegramAlert`. Нужно добавить на сервер (в `02a-telegram-alerts.js` или отдельный файл) поддержку отправки алертов через бота `@MagicVibeAlert_bot`.

Типы алертов (уже частично реализованы, проверь `02a-telegram-alerts.js`):
- 🔴 Критические ошибки сервера (crash, OOM)
- 🟡 Предупреждения (event loop blocked > 2s)
- 🟢 Восстановление (сервер перезапустился)
- 💰 Алерты цен (цена ниже порога, цена в карантине Озона)
- 📦 Алерты склада (партнёр исчез из снапшота PM)

### Команды администратора:
- `/status` — статус PM2 процессов (api, worker, watchdog), uptime, память
- `/logs [N]` — последние N строк из PM2 логов (default 20)
- `/errors [N]` — последние N ошибок из logов (filter level=error/warn)
- `/incidents` — история инцидентов из `health-watchdog-incidents.jsonl`
- `/queue` — статус BullMQ очередей (через API davidsklad.ru)
- `/restart api` / `/restart worker` — рестарт процесса (с подтверждением кнопками Inline keyboard)
- `/settings` — текущие настройки (курс, наценки) через API

### Получение данных:
Бот запрашивает `https://davidsklad.ru/api/...` с API-ключом:
- `GET /api/health` — health-check
- `GET /api/diagnostics` — расширенная диагностика (если есть)

Для PM2-статуса: SSH на ru-сервер (81.17.154.153) и выполнить `pm2 jlist` — парсить JSON ответ.

### Авторизация:
Только whitelist Telegram ID может отдавать команды. В .env:
```
ADMIN_TELEGRAM_IDS=123456789,987654321
```

## Переменные окружения (.env)
```
TELEGRAM_BOT_TOKEN=8270081253:AAFbNra1X4VqiiGt4ag0cr_DX6Kvov3uPPY
DAVIDSKLAD_API_BASE=https://davidsklad.ru
DAVIDSKLAD_API_SECRET=<секретный ключ>
ADMIN_TELEGRAM_IDS=<список ID администраторов>
RU_SERVER_HOST=81.17.154.153
NODE_ENV=production
```

## Важно
- Бот должен работать на Netherlands сервере (5.129.238.210)
- Но запрашивает данные с RU-сервера через HTTPS API
- При рестарте PM2 процессов — SSH на RU-сервер и выполнить команду
- Защита от случайного рестарта: Inline keyboard "Подтвердить / Отмена"
- Алерты с anti-spam: не более 1 алерта одного типа в 10 минут (уже реализовано в 02a-telegram-alerts.js — сохрани это поведение)
