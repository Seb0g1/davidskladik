'use strict';

const fetch = require('node-fetch');

const BASE = process.env.DAVIDSKLAD_API_BASE || 'https://davidsklad.ru';
const SECRET = process.env.DAVIDSKLAD_API_SECRET || '';

async function apiGet(path, params = {}) {
  const url = new URL(path, BASE);
  url.searchParams.set('secret', SECRET);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
  }
  const res = await fetch(url.toString(), {
    headers: { 'User-Agent': 'realizatsiya-bot/1.0' },
    timeout: 10000,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`API ${res.status}: ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function getPartnerSummary(partnerId) {
  return apiGet('/api/consignment/partner-summary', partnerId ? { partnerId } : {});
}

module.exports = { getPartnerSummary };
