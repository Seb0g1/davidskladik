'use strict';

const BOT_PASSWORD = process.env.BOT_PASSWORD || 'CGJ-Ge-48A';

function fmt(n) {
  const num = Math.round(Number(n) || 0);
  return num.toLocaleString('ru-RU');
}

function fmtDate(isoString) {
  if (!isoString) return '';
  const d = new Date(isoString);
  return `Обновлено: ${d.toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' })}`;
}

function mainMenuKeyboard(role) {
  const rows = [
    [
      { text: '💰 Баланс', callback_data: 'menu_balance' },
      { text: '📈 Продажи', callback_data: 'menu_sales' },
    ],
    [
      { text: '📦 Товары', callback_data: 'menu_products' },
      { text: '📋 Отчёт', callback_data: 'menu_report' },
    ],
    [{ text: '🔄 Обновить', callback_data: 'menu_refresh' }],
  ];
  return { inline_keyboard: rows };
}

function backKeyboard() {
  return {
    inline_keyboard: [[{ text: '« Главное меню', callback_data: 'menu_back' }]],
  };
}

function roleKeyboard() {
  return {
    inline_keyboard: [
      [{ text: '👤 Я партнёр', callback_data: 'role_partner' }],
      [{ text: '👑 Я администратор', callback_data: 'role_admin' }],
    ],
  };
}

function checkPassword(input) {
  return String(input).trim() === BOT_PASSWORD;
}

module.exports = { fmt, fmtDate, mainMenuKeyboard, backKeyboard, roleKeyboard, checkPassword };
