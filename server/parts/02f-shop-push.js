// Web Push notifications for Magic Vibes shop.
//
// Required env vars:
//   VAPID_PUBLIC_KEY   — from `node scripts/gen-vapid.cjs`
//   VAPID_PRIVATE_KEY  — same
//
// Globals available: app, requireAdmin, getPrisma, logger, cleanText

const webpush = require("web-push");

// ─── VAPID setup ─────────────────────────────────────────────────────────────
const VAPID_PUB  = process.env.VAPID_PUBLIC_KEY  || "";
const VAPID_PRIV = process.env.VAPID_PRIVATE_KEY || "";
const VAPID_SUB  = "mailto:noreply@magicvibes.ru";

if (VAPID_PUB && VAPID_PRIV) {
  webpush.setVapidDetails(VAPID_SUB, VAPID_PUB, VAPID_PRIV);
  logger.info("web_push_ready", { pub: VAPID_PUB.slice(0, 16) + "…" });
} else {
  logger.warn("web_push_disabled", { reason: "VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY not set" });
}

// ─── Public: provide VAPID public key to frontend ────────────────────────────
app.get("/api/shop/push/vapid-key", (_req, res) => {
  if (!VAPID_PUB) return res.status(503).json({ error: "Push notifications not configured" });
  res.json({ ok: true, key: VAPID_PUB });
});

// ─── Public: save push subscription ─────────────────────────────────────────
app.post("/api/shop/push/subscribe", async (req, res, next) => {
  try {
    const { endpoint, keys } = req.body || {};
    if (!endpoint || !keys?.p256dh || !keys?.auth) {
      return res.status(400).json({ error: "Invalid subscription object" });
    }
    if (!VAPID_PUB) return res.status(503).json({ error: "Push not configured" });

    // Optionally link to customer via Bearer token
    let customerId = null;
    const auth = req.headers.authorization || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    if (token && typeof verifyShopToken === "function") {
      const payload = verifyShopToken(token);
      if (payload?.customerId) customerId = payload.customerId;
    }

    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "DB not available" });

    await prisma.shopPushSubscription.upsert({
      where:  { endpoint },
      create: { endpoint, p256dh: keys.p256dh, auth: keys.auth, customerId },
      update: { p256dh: keys.p256dh, auth: keys.auth, customerId },
    });

    logger.info("push_subscribe", { customerId: customerId || "guest" });
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Public: remove push subscription ────────────────────────────────────────
app.delete("/api/shop/push/subscribe", async (req, res, next) => {
  try {
    const { endpoint } = req.body || {};
    if (!endpoint) return res.status(400).json({ error: "endpoint required" });
    const prisma = getPrisma();
    if (!prisma) return res.json({ ok: true });
    await prisma.shopPushSubscription.deleteMany({ where: { endpoint } });
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Admin: subscriber count ──────────────────────────────────────────────────
app.get("/api/shop/admin/push/stats", requireAdmin, async (_req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.json({ ok: true, total: 0, configured: false });
    const total = await prisma.shopPushSubscription.count();
    res.json({ ok: true, total, configured: Boolean(VAPID_PUB) });
  } catch (e) { next(e); }
});

// ─── Admin: send push to all subscribers (or by customerId) ──────────────────
app.post("/api/shop/admin/push/send", requireAdmin, async (req, res, next) => {
  try {
    if (!VAPID_PUB) return res.status(503).json({ error: "Push not configured" });
    const { title, body, url, customerId } = req.body || {};
    if (!title || !body) return res.status(400).json({ error: "title and body required" });

    const prisma = getPrisma();
    if (!prisma) return res.status(503).json({ error: "DB not available" });

    const where = customerId ? { customerId } : {};
    const subs = await prisma.shopPushSubscription.findMany({ where });
    if (!subs.length) return res.json({ ok: true, sent: 0, failed: 0 });

    const payload = JSON.stringify({
      title: cleanText(title),
      body:  cleanText(body),
      url:   url ? cleanText(url) : "https://magicvibes.ru",
      icon:  "https://magicvibes.ru/icon-192.png",
      badge: "https://magicvibes.ru/badge-72.png",
    });

    let sent = 0, failed = 0;
    const staleEndpoints = [];

    for (const sub of subs) {
      try {
        await webpush.sendNotification(
          { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } },
          payload,
          { TTL: 86400 }
        );
        sent++;
      } catch (err) {
        failed++;
        // 410 Gone = subscription expired, clean up
        if (err?.statusCode === 410 || err?.statusCode === 404) {
          staleEndpoints.push(sub.endpoint);
        }
        logger.warn("push_send_error", { endpoint: sub.endpoint.slice(-20), status: err?.statusCode });
      }
      // brief pause to avoid rate-limiting
      await new Promise((r) => setTimeout(r, 80));
    }

    if (staleEndpoints.length) {
      await prisma.shopPushSubscription.deleteMany({ where: { endpoint: { in: staleEndpoints } } });
      logger.info("push_stale_cleaned", { count: staleEndpoints.length });
    }

    logger.info("push_broadcast_done", { sent, failed, total: subs.length });
    res.json({ ok: true, sent, failed, total: subs.length });
  } catch (e) { next(e); }
});
