// Background monitor: re-run TN VED apply every 2 hours, report status
// Run with: node scripts/tnved-monitor.js
const https = require('https');

function fetchProd(path, method, body, cookie) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: 'davidsklad.ru', path, method: method || 'GET',
      headers: { 'Content-Type': 'application/json', ...(data ? { 'Content-Length': Buffer.byteLength(data) } : {}), ...(cookie ? { Cookie: cookie } : {}) },
    };
    const req = https.request(opts, res => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve({ status: res.statusCode, body: d, headers: res.headers })); });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function runApply(cookie) {
  const r = await fetchProd('/api/ozon/tnved/apply', 'POST', {}, cookie);
  if (r.status !== 202) { console.error('Apply launch failed:', r.status, r.body); return; }
  for (let i = 0; i < 60; i++) {
    await sleep(5000);
    const p = JSON.parse((await fetchProd('/api/ozon/tnved/progress', 'GET', null, cookie)).body);
    if (!p.running && p.completedAt) {
      console.log(`[${new Date().toISOString()}] Ozon apply: updated=${p.updated} errors=${p.errors}`);
      return;
    }
  }
}

async function main() {
  const login = await fetchProd('/api/login', 'POST', { username: 'david', password: 'CGJ-Ge-90' });
  const cookie = login.headers['set-cookie'].map(c => c.split(';')[0]).join('; ');
  console.log(`[${new Date().toISOString()}] Monitor started. Will re-run apply every 2 hours.`);

  for (let cycle = 0; cycle < 12; cycle++) {
    console.log(`\n[${new Date().toISOString()}] Cycle ${cycle + 1}/12 — running Ozon apply...`);
    await runApply(cookie);
    if (cycle < 11) {
      console.log(`[${new Date().toISOString()}] Sleeping 2 hours...`);
      await sleep(2 * 60 * 60 * 1000);
    }
  }
  console.log(`[${new Date().toISOString()}] Monitor finished (24h).`);
}
main().catch(console.error);
