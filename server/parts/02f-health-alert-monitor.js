// Periodic health-alert monitor (PLAN-HARDENING.md 4: "Алерты по порогам").
//
// The dashboard exposes alert flags (stale prices, starved price queue, stalled sweeps,
// oversold-below-target), but nothing pushes them — an admin has to be looking. This
// worker-side scheduler evaluates the same thresholds every HEALTH_ALERT_INTERVAL and
// sends a Telegram message per breached check, with a per-key cooldown so it cannot spam.
//
// Safe by construction: if TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID are not set it is a pure
// no-op (it still logs the evaluation, which is useful on its own). Worker-only (schedulers
// do not run on the api process).

const healthAlertEnabled = process.env.HEALTH_ALERT_ENABLED !== "false";
const healthAlertIntervalMs = Math.max(60_000, Number(process.env.HEALTH_ALERT_INTERVAL_SECONDS || 300) * 1000 || 300_000);
const healthAlertCooldownMs = Math.max(5 * 60_000, Number(process.env.HEALTH_ALERT_COOLDOWN_MS || 30 * 60_000) || 30 * 60_000);
const linkedSoldBelowTargetAlertThreshold = Math.max(0, Number(process.env.LINKED_SOLD_BELOW_TARGET_ALERT_THRESHOLD || 1000) || 1000);
let healthAlertTimer = null;
let healthAlertRunning = false;
const healthAlertLastSentAt = new Map(); // key -> ms

function healthAlertTelegramConfigured() {
  return Boolean(cleanText(process.env.TELEGRAM_BOT_TOKEN) && cleanText(process.env.TELEGRAM_CHAT_ID));
}

function sendHealthAlertTelegram(text) {
  const token = cleanText(process.env.TELEGRAM_BOT_TOKEN);
  const chatId = cleanText(process.env.TELEGRAM_CHAT_ID);
  if (!token || !chatId) return Promise.resolve(false);
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
        timeout: 10_000,
      }, (res) => {
        res.on("data", () => {});
        res.on("end", () => resolve(res.statusCode >= 200 && res.statusCode < 300));
      });
      req.on("error", () => resolve(false));
      req.on("timeout", () => { req.destroy(); resolve(false); });
      req.write(payload);
      req.end();
    } catch {
      resolve(false);
    }
  });
}

// Pure: turn the raw metrics into a list of breached-alert {key, message} objects, so the
// thresholds are unit-testable without DB or network.
function evaluateHealthAlerts(metrics = {}, thresholds = {}) {
  const alerts = [];
  const stalePrice = Number(metrics.stalePriceLinked || 0);
  if (stalePrice > Number(thresholds.stalePriceLinked || 0)) {
    alerts.push({ key: "stale_price_linked", message: `⚠️ Зависшие цены: ${stalePrice} привязанных SKU не сошлись с целевой ценой > ${thresholds.staleHours}ч (порог ${thresholds.stalePriceLinked}).` });
  }
  const oldestJobMs = Number(metrics.oldestPriceJobAgeMs || 0);
  if (oldestJobMs > Number(thresholds.oldestPriceJobMs || 0)) {
    alerts.push({ key: "price_queue_starved", message: `⚠️ Очередь цен: самая старая price-задача ждёт ${Math.round(oldestJobMs / 60000)} мин (порог ${Math.round(Number(thresholds.oldestPriceJobMs) / 60000)} мин).` });
  }
  const staleSweeps = Array.isArray(metrics.staleSweeps) ? metrics.staleSweeps : [];
  if (staleSweeps.length) {
    alerts.push({ key: `stale_sweeps:${staleSweeps.slice().sort().join(",")}`, message: `⚠️ Свипы не отрабатывают: ${staleSweeps.join(", ")} — нет heartbeat дольше 2.5× интервала.` });
  }
  const soldBelow = Number(metrics.linkedSoldBelowTarget || 0);
  if (soldBelow > Number(thresholds.linkedSoldBelowTarget || 0)) {
    alerts.push({ key: "sold_below_target", message: `⚠️ Остатки: ${soldBelow} привязанных SKU ниже целевого остатка (порог ${thresholds.linkedSoldBelowTarget}). Возможно, дозаказ не успевает.` });
  }
  // Only the ACTIONABLE direction is alerted: products we had as «нет в наличии/архив/неактивен»
  // that the marketplace actually sells (lost sales). The opposite (active -> out_of_stock) is
  // normal stock depletion the reconciler corrects each cycle and used to false-alarm (e.g. 177
  // over a full ~14k cycle is ~1%). Total churn stays in the dashboard as info only.
  const wronglyHidden = Number(metrics.marketplaceWronglyHidden || 0);
  if (wronglyHidden > Number(thresholds.marketplaceWronglyHidden || 0)) {
    alerts.push({ key: "marketplace_wrongly_hidden", message: `⚠️ Скрытые продаваемые: за последний цикл сверки ${wronglyHidden} товаров были у нас «нет в наличии/архив», а на Ozon/Yandex продаются (порог ${thresholds.marketplaceWronglyHidden}). Возможна потеря продаж.` });
  }
  const errorSpikes = Array.isArray(metrics.errorSpikes) ? metrics.errorSpikes : [];
  for (const spike of errorSpikes) {
    alerts.push({ key: `error_spike:${spike.class}`, message: `⚠️ Всплеск ошибок: «${spike.class}» — ${spike.count}× за последние ${spike.windowMinutes} мин (порог ${thresholds.errorSpike}).` });
  }
  return alerts;
}

async function runHealthAlertCheck({ source = "schedule" } = {}) {
  if (healthAlertRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  healthAlertRunning = true;
  try {
    const [stalePriceLinked, oldestPriceJobAgeMs, linkedSoldBelowTarget, sweeps, reconcilerState, errorSpikes] = await Promise.all([
      prisma.$queryRawUnsafe(`
        SELECT COUNT(*)::int AS n FROM warehouse_products p
        WHERE p.archived = false AND p.target_price IS NOT NULL AND p.target_price > 0
          AND (p.current_price IS NULL OR p.current_price <> p.target_price)
          AND p.updated_at < now() - interval '${stalePriceLinkedHours} hours'
          AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
      `).then((rows) => Number(rows?.[0]?.n || 0)).catch(() => 0),
      oldestPendingPriceJobAgeMs().catch(() => 0),
      prisma.$queryRawUnsafe(`
        SELECT COUNT(*)::int AS n FROM warehouse_products p
        WHERE p.archived = false AND COALESCE(p.target_stock, 0) > 0
          AND COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) < p.target_stock
          AND EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
      `).then((rows) => Number(rows?.[0]?.n || 0)).catch(() => 0),
      readSweepHeartbeats().catch(() => []),
      readLinkedReconcilerState().catch(() => null),
      readErrorSpikes().catch(() => []),
    ]);
    const staleSweeps = (sweeps || []).filter((s) => s.stale).map((s) => s.name);
    const marketplaceStateMismatches = Number(reconcilerState?.lastCycleStateMismatches || 0);
    const marketplaceWronglyHidden = Number(reconcilerState?.lastCycleWronglyHidden || 0);
    const metrics = { stalePriceLinked, oldestPriceJobAgeMs, linkedSoldBelowTarget, staleSweeps, marketplaceStateMismatches, marketplaceWronglyHidden, errorSpikes };
    const thresholds = {
      stalePriceLinked: stalePriceLinkedAlertThreshold,
      staleHours: stalePriceLinkedHours,
      oldestPriceJobMs: priceQueueOldestJobAlertMs,
      linkedSoldBelowTarget: linkedSoldBelowTargetAlertThreshold,
      marketplaceWronglyHidden: marketplaceStateMismatchAlertThreshold,
      errorSpike: errorSpikeAlertThreshold,
    };
    const alerts = evaluateHealthAlerts(metrics, thresholds);
    logger.info("health_alert_check", { source, ...metrics, breached: alerts.map((a) => a.key) });

    let sent = 0;
    if (alerts.length && healthAlertTelegramConfigured()) {
      const nowMs = Date.now();
      for (const alert of alerts) {
        const last = healthAlertLastSentAt.get(alert.key) || 0;
        if (nowMs - last < healthAlertCooldownMs) continue;
        const ok = await sendHealthAlertTelegram(`DavidSklad · мониторинг\n${alert.message}`);
        if (ok) { healthAlertLastSentAt.set(alert.key, nowMs); sent += 1; }
      }
    }
    return { status: "ok", metrics, breached: alerts.length, sent, telegram: healthAlertTelegramConfigured() };
  } catch (error) {
    logger.warn("health alert check failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    healthAlertRunning = false;
  }
}

function scheduleHealthAlertMonitor(delayMs = healthAlertIntervalMs) {
  if (!healthAlertEnabled) return;
  if (healthAlertTimer) clearTimeout(healthAlertTimer);
  const normalizedDelay = Math.max(30_000, Number(delayMs) || healthAlertIntervalMs);
  healthAlertTimer = setTimeout(async () => {
    try {
      await runHealthAlertCheck({ source: "schedule" });
    } catch (error) {
      logger.warn("health alert tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleHealthAlertMonitor(healthAlertIntervalMs);
    }
  }, normalizedDelay);
  healthAlertTimer.unref?.();
}
