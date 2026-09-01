'use strict';

function mainMenu(isLinked = false) {
  const rows = [
    [
      { text: '🛍 Каталог', callback_data: 'catalog_home' },
      { text: '🔍 Поиск', callback_data: 'search_start' },
    ],
  ];

  if (isLinked) {
    rows.push([
      { text: '📦 Мои заказы', callback_data: 'orders' },
      { text: '👤 Профиль', callback_data: 'profile' },
    ]);
  } else {
    rows.push([{ text: '🔗 Привязать аккаунт', callback_data: 'link_account' }]);
  }

  rows.push([
    { text: '🔔 Уведомления', callback_data: 'subscribe_menu' },
    { text: '❓ Помощь', callback_data: 'help' },
  ]);

  return { inline_keyboard: rows };
}

function backToMain() {
  return {
    inline_keyboard: [[{ text: '🏠 Главное меню', callback_data: 'main_menu' }]],
  };
}

function paginationKeyboard(page, totalPages, prefix) {
  const row = [];
  if (page > 0) row.push({ text: '← Назад', callback_data: `${prefix}_${page - 1}` });
  row.push({ text: `${page + 1}/${totalPages}`, callback_data: 'noop' });
  if (page < totalPages - 1) row.push({ text: 'Вперёд →', callback_data: `${prefix}_${page + 1}` });
  return {
    inline_keyboard: [row, [{ text: '🏠 Главное меню', callback_data: 'main_menu' }]],
  };
}

module.exports = { mainMenu, backToMain, paginationKeyboard };
