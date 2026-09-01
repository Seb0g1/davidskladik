'use strict';

const fetch = require('node-fetch');

const BASE = process.env.MAGICVIBES_API_BASE || 'https://magicvibes.ru';
const SHOP_API = `${BASE}/api/shop`;

async function shopReq(path, init = {}, token) {
  const headers = { 'Content-Type': 'application/json', 'User-Agent': 'magicvibes-bot/1.0' };
  if (token) headers['Authorization'] = `Bearer ${token}`;
  const res = await fetch(`${SHOP_API}${path}`, { headers, timeout: 10000, ...init });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function getCatalog(params = {}) {
  const qs = new URLSearchParams();
  if (params.page) qs.set('page', String(params.page));
  if (params.pageSize) qs.set('pageSize', String(params.pageSize || 5));
  if (params.brand) qs.set('brand', params.brand);
  if (params.category) qs.set('category', params.category);
  if (params.q) qs.set('q', params.q);
  if (params.inStock) qs.set('inStock', 'true');
  if (params.sort) qs.set('sort', params.sort);
  return shopReq(`/catalog?${qs}`);
}

async function getProduct(offerId) {
  return shopReq(`/product/${encodeURIComponent(offerId)}`);
}

async function getBrands() {
  return shopReq('/brands');
}

async function getCategories() {
  return shopReq('/categories');
}

async function sendCode(email) {
  return shopReq('/auth/send-code', {
    method: 'POST',
    body: JSON.stringify({ email }),
  });
}

async function verifyCode(email, code) {
  return shopReq('/auth/verify-code', {
    method: 'POST',
    body: JSON.stringify({ email, code }),
  });
}

async function getOrders(token) {
  return shopReq('/auth/orders', {}, token);
}

async function getMe(token) {
  return shopReq('/auth/me', {}, token);
}

module.exports = { getCatalog, getProduct, getBrands, getCategories, sendCode, verifyCode, getOrders, getMe };
