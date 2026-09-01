// Lightweight Telegram alert sender for critical system events.
// Uses TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID (same as health-alert-monitor).
// Also supports ALERT_BOT_TOKEN + ALERT_CHAT_ID for @MagicVibeAlert_bot.
// Rate-limited: max 1 alert per type per 10 minutes to avoid spam.

const _tgAlertEnabled = () => Boolean(
  cleanText(process.env.TELEGRAM_BOT_TOKEN) && cleanText(process.env.TELEGRAM_CHAT_ID),
);
const _tgAlertBotEnabled = () => Boolean(
  cleanText(process.env.ALERT_BOT_TOKEN) && cleanText(process.env.ALERT_CHAT_ID),
);
const _tgAlertCooldownMs = Math.max(60_000, Number(process.env.TELEGRAM_ALERT_COOLDOWN_MS || 10 * 60_000) || 10 * 60_000);
const _tgAlertLastSent = new Map(); // type → ms

function _sendTelegramRaw(token, chatId, text) {
  const base = cleanText(process.env.TELEGRAM_API_BASE_URL) || "https://api.telegram.org";
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
      req.on("error", (e) => { logger.warn("telegram_alert_send_failed", { detail: e.message }); resolve(false); });
      req.on("timeout", () => { req.destroy(); resolve(false); });
      req.write(payload);
      req.end();
    } catch (e) {
      logger.warn("telegram_alert_error", { detail: e?.message || String(e) });
      resolve(false);
    }
  });
}

function sendTelegramAlert(type, message, { force = false } = {}) {
  const primaryEnabled = _tgAlertEnabled();
  const alertBotEnabled = _tgAlertBotEnabled();
  if (!primaryEnabled && !alertBotEnabled) return Promise.resolve(false);
  const now = Date.now();
  if (!force) {
    const last = _tgAlertLastSent.get(type) || 0;
    if (now - last < _tgAlertCooldownMs) return Promise.resolve(false);
  }
  _tgAlertLastSent.set(type, now);
  const text = `🚨 DavidSklad\n${message}`;
  const sends = [];
  if (primaryEnabled) {
    sends.push(_sendTelegramRaw(
      cleanText(process.env.TELEGRAM_BOT_TOKEN),
      cleanText(process.env.TELEGRAM_CHAT_ID),
      text,
    ));
  }
  if (alertBotEnabled) {
    sends.push(_sendTelegramRaw(
      cleanText(process.env.ALERT_BOT_TOKEN),
      cleanText(process.env.ALERT_CHAT_ID),
      text,
    ));
  }
  return Promise.all(sends).then(results => results.some(Boolean));
}
