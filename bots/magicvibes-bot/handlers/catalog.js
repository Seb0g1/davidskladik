'use strict';

const { getCatalog, getBrands, getProduct } = require('../api');
const { brandListKeyboard, productListKeyboard, productDetailKeyboard, encodeShort, decodeShort } = require('../keyboards/catalog');
const { backToMain } = require('../keyboards/main');

async function handleCatalogHome(bot, chatId) {
  try {
    const brands = await getBrands();
    if (!brands || brands.length === 0) {
      await bot.sendMessage(chatId, '🌸 Каталог пока пуст. Загляните позже!', { reply_markup: backToMain() });
      return;
    }
    await bot.sendMessage(
      chatId,
      '🛍 <b>Каталог Magic Vibes</b>\n\nВыберите бренд:',
      { parse_mode: 'HTML', reply_markup: brandListKeyboard(brands, 0) }
    );
  } catch (err) {
    await bot.sendMessage(chatId, '⚠️ Не удалось загрузить каталог. Попробуйте позже.', { reply_markup: backToMain() });
  }
}

async function handleBrandsPage(bot, chatId, page) {
  try {
    const brands = await getBrands();
    await bot.sendMessage(
      chatId,
      '🛍 <b>Каталог Magic Vibes</b>\n\nВыберите бренд:',
      { parse_mode: 'HTML', reply_markup: brandListKeyboard(brands, page) }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Ошибка загрузки.', { reply_markup: backToMain() });
  }
}

async function handleBrandProducts(bot, chatId, brandEncoded, page = 0) {
  const brand = decodeShort(brandEncoded);
  try {
    const data = await getCatalog({ brand, page, pageSize: 5 });
    const products = data.items || data.products || data || [];
    const total = data.total || products.length;
    const totalPages = Math.ceil(total / 5) || 1;

    if (products.length === 0) {
      await bot.sendMessage(chatId, `🌸 У бренда <b>${brand}</b> нет товаров в наличии.`, {
        parse_mode: 'HTML',
        reply_markup: backToMain(),
      });
      return;
    }

    const lines = products.map((p, i) => {
      const price = p.priceRub ? `${Number(p.priceRub).toLocaleString('ru')} ₽` : '';
      return `${i + 1 + page * 5}. ${p.name || p.offerId}${price ? ` — ${price}` : ''}`;
    });

    await bot.sendMessage(
      chatId,
      `🌸 <b>${brand}</b> — ${total} товаров\n\n${lines.join('\n')}\n\nВыберите товар:`,
      { parse_mode: 'HTML', reply_markup: productListKeyboard(products, page, totalPages, brand) }
    );
  } catch {
    await bot.sendMessage(chatId, '⚠️ Ошибка загрузки товаров.', { reply_markup: backToMain() });
  }
}

async function handleProductDetail(bot, chatId, offerId) {
  try {
    const p = await getProduct(offerId);
    const price = p.priceRub ? `${Number(p.priceRub).toLocaleString('ru')} ₽` : '';
    const stock = p.stock > 0 ? `✅ В наличии (${p.stock} шт.)` : '❌ Нет в наличии';
    const siteUrl = `https://magicvibes.ru/product/${encodeURIComponent(offerId)}`;

    let text = `💎 <b>${p.name || offerId}</b>`;
    if (p.brand) text += `\nБренд: ${p.brand}`;
    if (price) text += `\nЦена: <b>${price}</b>`;
    text += `\n${stock}`;
    if (p.description) text += `\n\n${p.description.slice(0, 500)}`;

    if (p.mainImage || p.images?.[0]) {
      await bot.sendPhoto(chatId, p.mainImage || p.images[0], {
        caption: text,
        parse_mode: 'HTML',
        reply_markup: productDetailKeyboard(offerId, siteUrl),
      });
    } else {
      await bot.sendMessage(chatId, text, {
        parse_mode: 'HTML',
        reply_markup: productDetailKeyboard(offerId, siteUrl),
      });
    }
  } catch {
    await bot.sendMessage(chatId, '⚠️ Товар не найден.', { reply_markup: backToMain() });
  }
}

module.exports = { handleCatalogHome, handleBrandsPage, handleBrandProducts, handleProductDetail };
