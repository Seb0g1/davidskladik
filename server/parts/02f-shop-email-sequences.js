// Email sequences for Magic Vibes shop orders.
// Step 1 (immediate): order confirmation + fragrance story.
// Step 7 (day 7):     review request + 5% discount code REVIEW5.
// Step 30 (day 30):   fragrance of the month newsletter.
//
// Triggered: step 1 by sendOrderSequenceEmail() called from order creation route.
//            steps 7 & 30 by daily scanner (scheduleEmailSequenceScanner).
//
// Globals available: getPrisma, logger, readAppSettings, cleanText

// ─── SMTP (self-contained, mirrors shopSendEmail in 02d-shop-api-routes.js) ──
function seqSendMail({ to, subject, html }) {
  return new Promise((resolve, reject) => {
    const tls = require("tls");
    const host = process.env.SHOP_SMTP_HOST;
    const port = Number(process.env.SHOP_SMTP_PORT || 465);
    const user = process.env.SHOP_SMTP_USER;
    const pass = process.env.SHOP_SMTP_PASS;
    if (!host || !user || !pass) return reject(new Error("SMTP not configured"));

    const b64 = (s) => Buffer.from(s).toString("base64");
    const lines = [];
    let buf = "";
    let done = false;

    const sock = tls.connect({ host, port, rejectUnauthorized: false }, () => {});
    sock.setTimeout(20000);
    sock.on("timeout", () => sock.destroy(new Error("SMTP timeout")));

    function send(line) { sock.write(line + "\r\n"); }

    function onLine(line) {
      const code = parseInt(line.slice(0, 3), 10);
      if (line[3] === "-") return; // multi-line, wait for last
      if (code === 220) { send("EHLO magicvibes.ru"); return; }
      if (code === 250) {
        if (!lines.includes("auth")) { lines.push("auth"); send("AUTH LOGIN"); return; }
        if (!lines.includes("user")) { lines.push("user"); send(b64(user)); return; }
        if (!lines.includes("rcpt")) { lines.push("rcpt"); send(`RCPT TO:<${to}>`); return; }
        if (!lines.includes("data")) { lines.push("data"); send("DATA"); return; }
        if (!lines.includes("quit")) { lines.push("quit"); send("QUIT"); return; }
        return;
      }
      if (code === 334) {
        if (!lines.includes("user")) { lines.push("user"); send(b64(user)); return; }
        send(b64(pass)); return;
      }
      if (code === 235) { send(`MAIL FROM:<${user}>`); return; }
      if (code === 354) {
        const msg = [
          `From: "Magic Vibes" <${user}>`,
          `To: ${to}`,
          `Subject: =?utf-8?B?${Buffer.from(subject).toString("base64")}?=`,
          `MIME-Version: 1.0`,
          `Content-Type: text/html; charset=utf-8`,
          `X-Mailer: Magic Vibes Shop`,
          `List-Unsubscribe: <mailto:${user}?subject=unsubscribe>`,
          `List-Unsubscribe-Post: List-Unsubscribe=One-Click`,
          ``,
          html,
          `.`,
        ].join("\r\n");
        sock.write(msg + "\r\n"); return;
      }
      if (code === 221) { sock.destroy(); if (!done) { done = true; resolve(); } return; }
      if (code >= 400) { sock.destroy(new Error(`SMTP ${code}: ${line.slice(4)}`)); return; }
    }

    sock.on("data", (chunk) => {
      buf += chunk.toString();
      let idx;
      while ((idx = buf.indexOf("\r\n")) !== -1) {
        onLine(buf.slice(0, idx));
        buf = buf.slice(idx + 2);
      }
    });
    sock.on("error", (err) => { if (!done) { done = true; reject(err); } });
    sock.on("close", () => { if (!done) { done = true; resolve(); } });
  });
}

// ─── Email base layout ────────────────────────────────────────────────────────
function seqLayout(bodyContent) {
  return `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Magic Vibes</title>
</head>
<body style="margin:0;padding:0;background:#f0ede8;font-family:Georgia,'Times New Roman',serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0ede8;padding:32px 16px;">
<tr><td align="center">
<table width="540" cellpadding="0" cellspacing="0" style="background:#0b0b0b;border-radius:4px;overflow:hidden;max-width:100%;">

  <!-- Header -->
  <tr><td style="padding:32px 40px 24px;border-bottom:1px solid rgba(201,162,94,0.25);text-align:center;">
    <div style="font-family:Georgia,serif;font-style:italic;font-size:28px;color:#f5f4f0;letter-spacing:-0.5px;">Magic Vibes</div>
    <div style="font-size:10px;letter-spacing:0.28em;text-transform:uppercase;color:#5d5a54;margin-top:4px;">Оригинальная парфюмерия</div>
  </td></tr>

  <!-- Body -->
  ${bodyContent}

  <!-- Footer -->
  <tr><td style="padding:24px 40px 32px;border-top:1px solid rgba(255,255,255,0.06);text-align:center;">
    <div style="font-size:11px;color:#3d3a34;line-height:1.7;">
      Magic Vibes · magicvibes.ru<br>
      <a href="mailto:noreply@magicvibes.ru?subject=unsubscribe" style="color:#4a473f;text-decoration:underline;">Отписаться от писем</a>
    </div>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>`;
}

// ─── Template: Day 1 — order confirmation ────────────────────────────────────
function emailDay1Html({ firstName, items, orderId, totalRub }) {
  const itemRows = (items || []).slice(0, 5).map((it) =>
    `<tr>
      <td style="padding:8px 0;font-size:13px;color:#c9c5bc;border-bottom:1px solid rgba(255,255,255,0.05);">${it.name || it.offerId}</td>
      <td style="padding:8px 0;font-size:13px;color:#c9c5bc;border-bottom:1px solid rgba(255,255,255,0.05);text-align:right;">×${it.quantity}</td>
      <td style="padding:8px 0;font-size:13px;color:#e8d5a3;border-bottom:1px solid rgba(255,255,255,0.05);text-align:right;">${(it.priceRub * it.quantity).toLocaleString("ru-RU")} ₽</td>
    </tr>`
  ).join("");

  const body = `
  <tr><td style="padding:40px 40px 28px;">
    <div style="font-family:Georgia,serif;font-style:italic;font-size:32px;color:#f5f4f0;line-height:1.1;margin-bottom:12px;">
      ${firstName ? `${firstName}, с` : "С"}пасибо за заказ
    </div>
    <div style="font-size:13px;color:#7d7a73;line-height:1.7;margin-bottom:28px;">
      Мы уже готовим вашу посылку. Каждый флакон проходит проверку перед отправкой — это занимает 1–2 дня.
    </div>

    <!-- Order summary -->
    <div style="background:#111113;border-radius:3px;border:1px solid rgba(201,162,94,0.2);padding:20px 24px;margin-bottom:28px;">
      <div style="font-size:10px;letter-spacing:0.22em;text-transform:uppercase;color:#c9a25e;margin-bottom:14px;">Ваш заказ #${orderId}</div>
      <table width="100%" cellpadding="0" cellspacing="0">${itemRows}</table>
      <div style="display:flex;justify-content:space-between;padding-top:12px;margin-top:4px;border-top:1px solid rgba(201,162,94,0.2);">
        <div style="font-size:12px;color:#5d5a54;">Итого</div>
        <div style="font-size:16px;font-family:Georgia,serif;font-style:italic;color:#e8d5a3;">${(totalRub || 0).toLocaleString("ru-RU")} ₽</div>
      </div>
    </div>

    <!-- Fragrance story -->
    <div style="border-left:2px solid rgba(201,162,94,0.4);padding-left:20px;margin-bottom:32px;">
      <div style="font-size:10px;letter-spacing:0.22em;text-transform:uppercase;color:#5d5a54;margin-bottom:10px;">История аромата</div>
      <div style="font-family:Georgia,serif;font-style:italic;font-size:17px;color:#c9a25e;line-height:1.5;margin-bottom:8px;">«Аромат — это невидимый аксессуар, который оставляет самое сильное воспоминание»</div>
      <div style="font-size:12px;color:#5d5a54;text-align:right;">— Коко Шанель</div>
    </div>
    <div style="font-size:13px;color:#7d7a73;line-height:1.8;margin-bottom:32px;">
      Парфюмерия — это искусство, которое существует вне времени. Каждый флакон хранит работу парфюмера: сотни проб, тысячи ингредиентов и годы поиска идеального баланса. Ваш выбор — это не просто аромат, это история, которую вы расскажете без слов.
    </div>

    <div style="text-align:center;">
      <a href="https://magicvibes.ru/orders" style="display:inline-block;padding:14px 32px;background:rgba(201,162,94,0.12);border:1px solid rgba(201,162,94,0.4);border-radius:2px;font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#e8d5a3;text-decoration:none;">
        Мои заказы
      </a>
    </div>
  </td></tr>`;

  return seqLayout(body);
}

// ─── Template: Day 7 — review request ────────────────────────────────────────
function emailDay7Html({ firstName, items, orderId }) {
  const productName = items?.[0]?.name || "ваш аромат";
  const reviewUrl = `https://magicvibes.ru/account`;

  const body = `
  <tr><td style="padding:40px 40px 28px;">
    <div style="font-family:Georgia,serif;font-style:italic;font-size:32px;color:#f5f4f0;line-height:1.1;margin-bottom:12px;">
      Как вам аромат?
    </div>
    <div style="font-size:13px;color:#7d7a73;line-height:1.7;margin-bottom:28px;">
      ${firstName ? `${firstName}, п` : "П"}рошла неделя с вашего заказа #${orderId}. Надеемся, что <em>${productName}</em> вам понравился и уже стал частью вашего образа.
    </div>

    <!-- Review invitation -->
    <div style="background:#111113;border-radius:3px;border:1px solid rgba(201,162,94,0.2);padding:24px;margin-bottom:28px;text-align:center;">
      <div style="font-size:36px;margin-bottom:12px;">⭐⭐⭐⭐⭐</div>
      <div style="font-family:Georgia,serif;font-style:italic;font-size:18px;color:#f5f4f0;margin-bottom:8px;">Оставьте отзыв</div>
      <div style="font-size:13px;color:#7d7a73;line-height:1.6;margin-bottom:20px;">
        Ваше мнение помогает другим покупателям выбрать идеальный аромат.<br>
        За отзыв с фото — <strong style="color:#c9a25e;">+50 баллов</strong> «Золото Magic Vibes».
      </div>
      <a href="${reviewUrl}" style="display:inline-block;padding:12px 28px;background:rgba(201,162,94,0.12);border:1px solid rgba(201,162,94,0.4);border-radius:2px;font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#e8d5a3;text-decoration:none;">
        Написать отзыв
      </a>
    </div>

    <!-- Promo code -->
    <div style="background:linear-gradient(135deg,#1a1408 0%,#111113 70%);border-radius:3px;border:1px solid rgba(201,162,94,0.35);padding:20px 24px;margin-bottom:28px;text-align:center;">
      <div style="font-size:10px;letter-spacing:0.22em;text-transform:uppercase;color:#5d5a54;margin-bottom:10px;">Подарок за отзыв</div>
      <div style="font-family:Georgia,serif;font-size:28px;font-style:italic;color:#c9a25e;letter-spacing:0.1em;margin-bottom:6px;">REVIEW5</div>
      <div style="font-size:12px;color:#7d7a73;">−5% на следующий заказ · действует 30 дней</div>
    </div>

    <div style="font-size:12px;color:#3d3a34;line-height:1.7;text-align:center;">
      Промокод действителен при следующем оформлении заказа на magicvibes.ru
    </div>
  </td></tr>`;

  return seqLayout(body);
}

// ─── Template: Day 30 — fragrance of the month ───────────────────────────────
async function emailDay30Html({ firstName }) {
  // Try to get the latest Telegram news post as "fragrance of month" content
  let newsText = null;
  let newsPhotoUrl = null;
  try {
    const prisma = getPrisma();
    if (prisma) {
      const post = await prisma.telegramNewsPost.findFirst({
        where: { active: true },
        orderBy: { publishedAt: "desc" },
      });
      if (post) {
        newsText = (post.text || "").slice(0, 300);
        newsPhotoUrl = post.photoUrl || null;
      }
    }
  } catch { /* ignore */ }

  const months = ["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"];
  const month = months[new Date().getMonth()];

  const photoBlock = newsPhotoUrl
    ? `<tr><td style="padding:0 40px 24px;"><img src="${newsPhotoUrl}" alt="" width="460" style="display:block;max-width:100%;border-radius:2px;opacity:0.85;"></td></tr>`
    : "";

  const body = `
  ${photoBlock}
  <tr><td style="padding:${newsPhotoUrl ? "0" : "40px"} 40px 28px;">
    <div style="font-size:10px;letter-spacing:0.28em;text-transform:uppercase;color:#c9a25e;margin-bottom:14px;">Аромат ${month}</div>
    <div style="font-family:Georgia,serif;font-style:italic;font-size:32px;color:#f5f4f0;line-height:1.1;margin-bottom:16px;">
      ${firstName ? `${firstName}, н` : "Н"}овая история
    </div>

    ${newsText
      ? `<div style="font-size:14px;color:#9d9a94;line-height:1.85;margin-bottom:28px;border-left:2px solid rgba(201,162,94,0.3);padding-left:18px;">${newsText}</div>`
      : `<div style="font-size:14px;color:#7d7a73;line-height:1.85;margin-bottom:28px;">
          Месяц прошёл — и пришло время открыть что-то новое. Наш каталог пополнился свежими поступлениями: от классических европейских домов до редких ближневосточных ароматов. Найдите свою следующую историю.
        </div>`
    }

    <!-- CTA -->
    <div style="background:#111113;border-radius:3px;border:1px solid rgba(255,255,255,0.07);padding:24px;margin-bottom:28px;text-align:center;">
      <div style="font-family:Georgia,serif;font-style:italic;font-size:18px;color:#f5f4f0;margin-bottom:8px;">22 000+ ароматов</div>
      <div style="font-size:12px;color:#5d5a54;margin-bottom:20px;">Chanel · Dior · Tom Ford · Creed · Hermès · Byredo и другие</div>
      <a href="https://magicvibes.ru/catalog" style="display:inline-block;padding:14px 32px;background:rgba(201,162,94,0.12);border:1px solid rgba(201,162,94,0.4);border-radius:2px;font-size:12px;letter-spacing:0.14em;text-transform:uppercase;color:#e8d5a3;text-decoration:none;">
        Открыть каталог
      </a>
    </div>

    <!-- Loyalty reminder -->
    <div style="text-align:center;padding:12px 0;">
      <div style="font-size:11px;color:#3d3a34;line-height:1.7;">
        ✦ Программа «Золото Magic Vibes» — баллы за каждый отзыв и покупку<br>
        <a href="https://magicvibes.ru/account" style="color:#c9a25e;text-decoration:none;">Проверить баланс баллов →</a>
      </div>
    </div>
  </td></tr>`;

  return seqLayout(body);
}

// ─── Core: log sent + guard against duplicates ───────────────────────────────
async function seqMarkSent(prisma, orderId, email, step) {
  try {
    await prisma.shopEmailSequenceLog.create({ data: { orderId, email, step } });
    return true;
  } catch (e) {
    // unique constraint = already sent
    if (e?.code === "P2002") return false;
    throw e;
  }
}

// ─── Step 1: called right after order creation ────────────────────────────────
async function sendOrderSequenceEmail({ orderId, email, firstName, items, totalRub }) {
  if (!email || !orderId) return;
  const prisma = getPrisma();
  if (!prisma) return;
  try {
    const ok = await seqMarkSent(prisma, orderId, email, 1);
    if (!ok) return; // already sent
    const html = emailDay1Html({ firstName, items, orderId, totalRub });
    await seqSendMail({
      to: email,
      subject: `Спасибо за заказ #${orderId}! Ваш аромат уже готовится`,
      html,
    });
    logger.info("email_seq_sent", { step: 1, orderId, to: email });
  } catch (err) {
    logger.warn("email_seq_step1_failed", { orderId, detail: err?.message || String(err) });
  }
}

// ─── Scanner: find orders due for step 7 or step 30 ─────────────────────────
async function runEmailSequenceScanner() {
  const prisma = getPrisma();
  if (!prisma) return { skipped: true, reason: "no_prisma" };
  if (!process.env.SHOP_SMTP_HOST) return { skipped: true, reason: "no_smtp" };

  let sent7 = 0, sent30 = 0, errors = 0;

  try {
    const now = new Date();
    const day7Threshold  = new Date(now.getTime() - 7  * 24 * 3600 * 1000);
    const day30Threshold = new Date(now.getTime() - 30 * 24 * 3600 * 1000);

    // Orders older than 7 days that haven't received step 7
    const orders7 = await prisma.$queryRawUnsafe(`
      SELECT o.id, o.delivery, o.items, o.total_rub, o.created_at
      FROM shop_orders o
      WHERE o.created_at <= $1
        AND NOT EXISTS (
          SELECT 1 FROM shop_email_sequence_logs l
          WHERE l.order_id = o.id AND l.step = 7
        )
      LIMIT 100
    `, day7Threshold);

    for (const order of orders7) {
      try {
        const delivery = typeof order.delivery === "string" ? JSON.parse(order.delivery) : (order.delivery || {});
        const email = delivery.email;
        if (!email) continue;
        const items = typeof order.items === "string" ? JSON.parse(order.items) : (order.items || []);
        const ok = await seqMarkSent(prisma, order.id, email, 7);
        if (!ok) continue;
        const html = emailDay7Html({ firstName: delivery.firstName, items, orderId: order.id });
        await seqSendMail({ to: email, subject: "Как вам аромат? Оставьте отзыв — −5% на следующий заказ", html });
        sent7++;
        logger.info("email_seq_sent", { step: 7, orderId: order.id, to: email });
        await new Promise((r) => setTimeout(r, 1200)); // rate-limit: 1 email/1.2s
      } catch (err) {
        errors++;
        logger.warn("email_seq_step7_failed", { orderId: order.id, detail: err?.message || String(err) });
      }
    }

    // Orders older than 30 days that haven't received step 30
    const orders30 = await prisma.$queryRawUnsafe(`
      SELECT o.id, o.delivery, o.items, o.created_at
      FROM shop_orders o
      WHERE o.created_at <= $1
        AND NOT EXISTS (
          SELECT 1 FROM shop_email_sequence_logs l
          WHERE l.order_id = o.id AND l.step = 30
        )
      LIMIT 100
    `, day30Threshold);

    for (const order of orders30) {
      try {
        const delivery = typeof order.delivery === "string" ? JSON.parse(order.delivery) : (order.delivery || {});
        const email = delivery.email;
        if (!email) continue;
        const ok = await seqMarkSent(prisma, order.id, email, 30);
        if (!ok) continue;
        const html = await emailDay30Html({ firstName: delivery.firstName });
        await seqSendMail({ to: email, subject: `Аромат ${["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"][new Date().getMonth()]} от Magic Vibes`, html });
        sent30++;
        logger.info("email_seq_sent", { step: 30, orderId: order.id, to: email });
        await new Promise((r) => setTimeout(r, 1200));
      } catch (err) {
        errors++;
        logger.warn("email_seq_step30_failed", { orderId: order.id, detail: err?.message || String(err) });
      }
    }
  } catch (err) {
    logger.warn("email_seq_scanner_failed", { detail: err?.message || String(err) });
    return { ok: false, error: err?.message };
  }

  logger.info("email_seq_scan_done", { sent7, sent30, errors });
  return { ok: true, sent7, sent30, errors };
}

// ─── Admin route: stats ───────────────────────────────────────────────────────
app.get("/api/shop/admin/email-sequences/stats", requireAdmin, async (_req, res, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return res.json({ ok: true, stats: [] });
    const rows = await prisma.$queryRawUnsafe(`
      SELECT step, COUNT(*)::int AS count, MAX(sent_at) AS last_sent_at
      FROM shop_email_sequence_logs
      GROUP BY step ORDER BY step
    `);
    res.json({ ok: true, stats: rows });
  } catch (e) { next(e); }
});

app.post("/api/shop/admin/email-sequences/run-scan", requireAdmin, async (_req, res, next) => {
  try {
    const result = await runEmailSequenceScanner();
    res.json({ ok: true, result });
  } catch (e) { next(e); }
});

// ─── Scheduler: daily at 10:30 ───────────────────────────────────────────────
let _emailSeqTimer = null;

function scheduleEmailSequenceScanner() {
  if (_emailSeqTimer) clearTimeout(_emailSeqTimer);

  // Next 10:30 local time
  const now = new Date();
  const next = new Date(now);
  next.setHours(10, 30, 0, 0);
  if (next <= now) next.setDate(next.getDate() + 1);
  const delay = next.getTime() - now.getTime();

  _emailSeqTimer = setTimeout(async () => {
    try {
      await runEmailSequenceScanner();
    } catch (err) {
      logger.warn("email_seq_scheduler_error", { detail: err?.message || String(err) });
    } finally {
      scheduleEmailSequenceScanner();
    }
  }, delay);

  logger.info("email_seq_scheduled", { nextRunAt: next.toISOString() });
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
scheduleEmailSequenceScanner();
