'use strict';

const { getOrders } = require('../api');
const { getUser } = require('../db');
const { backToMain } = require('../keyboards/main');

const STATUS_LABELS = {
  pending:    '⏳ Ожидает оплаты',
  paid:       '✅ Оплачен',
  processing: '🔄 В обработке',
  shipped:    '📦 Отправлен',
  delivered:  '🎉 Доставлен',
  cancelled:  '❌ Отменён',
};

function formatDate(iso) {
  try {
    const d = new Date(iso);
    return `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}.${d.getFullYear()}`;
  } catch { return '—'; }
}

async function handleOrders(bot, chatId, telegramId) {
  const user = getUser(telegramId);
  if (!user?.token) {
    await bot.sendMessage(
      chatId,
      '🔗 Для просмотра заказов привяжите аккаунт magicvibes.ru.',
      { reply_markup: { inline_keyboard: [[{ text: '🔗 Привязать аккаунт', callback_data: 'link_account' }]] } }
    );
    return;
  }

  try {
    const orders = await getOrders(user.token);
    if (!orders || orders.length === 0) {
      await bot.sendMessage(chatId, '📦 У вас пока нет заказов.', { reply_markup: backToMain() });
      return;
    }

    const recent = orders.slice(0, 5);
    const lines = recent.map((o) => {
      const status = STATUS_LABELS[o.status] || o.status;
      const total = o.totalRub ? `${Number(o.totalRub).toLocaleString('ru')} ₽` : '';
      const date = formatDate(o.createdAt);
      return `📋 <b>Заказ ${o.id}</b>\n${status}\n${date}${total ? ` · ${total}` : ''}`;
    });

    await bot.sendMessage(
      chatId,
      `📦 <b>Ваши заказы</b> (последние ${recent.length}):\n\n${lines.join('\n\n')}`,
      { parse_mode: 'HTML', reply_markup: backToMain() }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Не удалось загрузить заказы. Попробуйте позже.', { reply_markup: backToMain() });
  }
}

module.exports = { handleOrders };
