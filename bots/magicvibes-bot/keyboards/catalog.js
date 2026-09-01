'use strict';

function brandListKeyboard(brands, page = 0) {
  const PAGE_SIZE = 6;
  const start = page * PAGE_SIZE;
  const slice = brands.slice(start, start + PAGE_SIZE);
  const totalPages = Math.ceil(brands.length / PAGE_SIZE);

  const rows = slice.map((b) => [
    { text: `${b.name} (${b.count})`, callback_data: `brand_${encodeShort(b.name)}` },
  ]);

  const nav = [];
  if (page > 0) nav.push({ text: '← Назад', callback_data: `brands_page_${page - 1}` });
  if (totalPages > 1) nav.push({ text: `${page + 1}/${totalPages}`, callback_data: 'noop' });
  if (page < totalPages - 1) nav.push({ text: 'Вперёд →', callback_data: `brands_page_${page + 1}` });
  if (nav.length) rows.push(nav);

  rows.push([{ text: '🏠 Главное меню', callback_data: 'main_menu' }]);

  return { inline_keyboard: rows };
}

function productListKeyboard(products, page, totalPages, brand) {
  const rows = products.map((p) => [
    { text: `${p.name || p.offerId}`, callback_data: `product_${p.offerId}` },
  ]);

  const nav = [];
  if (page > 0) nav.push({ text: '← Назад', callback_data: `catalog_brand_${encodeShort(brand)}_${page - 1}` });
  if (totalPages > 1) nav.push({ text: `${page + 1}/${totalPages}`, callback_data: 'noop' });
  if (page < totalPages - 1) nav.push({ text: 'Вперёд →', callback_data: `catalog_brand_${encodeShort(brand)}_${page + 1}` });
  if (nav.length) rows.push(nav);

  rows.push([
    { text: '◀ Бренды', callback_data: 'catalog_home' },
    { text: '🏠 Меню', callback_data: 'main_menu' },
  ]);

  return { inline_keyboard: rows };
}

function productDetailKeyboard(offerId, siteUrl) {
  const rows = [];
  if (siteUrl) {
    rows.push([{ text: '🛒 Купить на сайте', url: siteUrl }]);
  }
  rows.push([
    { text: '◀ К списку', callback_data: 'catalog_home' },
    { text: '🏠 Меню', callback_data: 'main_menu' },
  ]);
  return { inline_keyboard: rows };
}

// Shorten brand name to fit in callback_data (max 64 bytes total)
function encodeShort(name) {
  return encodeURIComponent(name).slice(0, 30);
}

function decodeShort(encoded) {
  try { return decodeURIComponent(encoded); } catch { return encoded; }
}

module.exports = { brandListKeyboard, productListKeyboard, productDetailKeyboard, encodeShort, decodeShort };
