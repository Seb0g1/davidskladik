// Lightweight Telegram alert sender for critical system events.
// Uses TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (same as health-alert-monitor).
// Rate-limited: max 1 alert per type per 10 minutes to avoid spam.

const _tgAlertEnabled = () => Boolean(
  cleanText(process.env.TELEGRAM_BOT_TOKEN) && cleanText(process.env.TELEGRAM_CHAT_ID),
);
const _tgAlertCooldownMs = Math.max(60_000, Number(process.env.TELEGRAM_ALERT_COOLDOWN_MS || 10 * 60_000) || 10 * 60_000);
const _tgAlertLastSent = new Map(); // type → ms

function sendTelegramAlert(type, message, { force = false } = {}) {
  if (!_tgAlertEnabled()) return Promise.resolve(false);
  const now = Date.now();
  if (!force) {
    const last = _tgAlertLastSent.get(type) || 0;
    if (now - last < _tgAlertCooldownMs) return Promise.resolve(false);
  }
  _tgAlertLastSent.set(type, now);
  const token = cleanText(process.env.TELEGRAM_BOT_TOKEN);
  const chatId = cleanText(process.env.TELEGRAM_CHAT_ID);
  const base = cleanText(process.env.TELEGRAM_API_BASE_URL) || "https://api.telegram.org";
  const text = `🚨 DavidSklad\n${message}`;
  const payload = JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: true });
  return new Promise((resolve) => {
    try {
      const url = new URL(`${base.replace(/\/$/, "")}/bot${token}/sendMessage`);
      const req = require("https").request({
        hostname: url.hostname,
        port: url.port || 443,
        path: url.pathname,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) },
        timeout: 8_000,
      }, (res) => {
        res.on("data", () => {});
        res.on("end", () => resolve(res.statusCode >= 200 && res.statusCode < 300));
      });
      req.on("error", (e) => { logger.warn("telegram_alert_send_failed", { type, detail: e.message }); resolve(false); });
      req.on("timeout", () => { req.destroy(); resolve(false); });
      req.write(payload);
      req.end();
    } catch (e) {
      logger.warn("telegram_alert_error", { type, detail: e?.message || String(e) });
      resolve(false);
    }
  });
}
