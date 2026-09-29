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
// Delegates to shopSendEmail (02d) — one SMTP client, base64 body, proper headers.
// Order mails are transactional; review / newsletter mails are marketing (List-Unsubscribe)
// and go only to orders with delivery.consents.ads = true.
function seqSendMail({ to, subject, html, marketing = false }) {
  return shopSendEmail({ to, subject, html, marketing });
}

function seqRub(n) {
  return `${Math.round(Number(n) || 0).toLocaleString("ru-RU").replace(/ /g, "&nbsp;")}&nbsp;&#8381;`;
}

// ─── Template: Day 1 — order confirmation ────────────────────────────────────
function emailDay1Html({ firstName, items, orderId, totalRub }) {
  const rows = (items || []).slice(0, 6).map((it) => `<tr>
<td style="padding:12px 0;border-bottom:1px solid #ece8e0;font-family:${MV_MAIL.body};font-size:14px;line-height:1.45;color:${MV_MAIL.ink};">${mvEsc(it.name || it.offerId)}</td>
<td style="padding:12px 0 12px 12px;border-bottom:1px solid #ece8e0;font-family:${MV_MAIL.body};font-size:13px;color:${MV_MAIL.muted};white-space:nowrap;" align="right">&times;${mvEsc(it.quantity)}</td>
<td style="padding:12px 0 12px 14px;border-bottom:1px solid #ece8e0;font-family:${MV_MAIL.body};font-size:14px;font-weight:700;color:${MV_MAIL.ink};white-space:nowrap;" align="right">${seqRub(Number(it.priceRub) * Number(it.quantity || 1))}</td>
</tr>`).join("");

  const content = `
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:26px 0 0;"><tr>
<td bgcolor="${MV_MAIL.paper}" style="background:${MV_MAIL.paper};border-radius:22px;padding:20px 22px 16px;">
<div style="font-family:${MV_MAIL.body};font-size:11px;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;color:${MV_MAIL.muted};">Заказ №${mvEsc(orderId)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:6px;">${rows}</table>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:14px;"><tr>
<td style="font-family:${MV_MAIL.body};font-size:14px;color:${MV_MAIL.muted};">Итого</td>
<td align="right" style="font-family:${MV_MAIL.display};font-size:22px;font-weight:800;color:${MV_MAIL.ink};">${seqRub(totalRub)}</td>
</tr></table>
</td></tr></table>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:18px 0 22px;"><tr>
<td width="33%" valign="top" style="padding-right:8px;font-family:${MV_MAIL.body};font-size:13px;line-height:1.45;color:#2b2a27;"><div style="font-family:${MV_MAIL.display};font-size:18px;font-weight:800;color:${MV_MAIL.pink};">01</div>Проверяем оригинальность каждого флакона</td>
<td width="33%" valign="top" style="padding:0 4px;font-family:${MV_MAIL.body};font-size:13px;line-height:1.45;color:#2b2a27;"><div style="font-family:${MV_MAIL.display};font-size:18px;font-weight:800;color:${MV_MAIL.pink};">02</div>Бережно упаковываем за 1–2 дня</td>
<td width="33%" valign="top" style="padding-left:8px;font-family:${MV_MAIL.body};font-size:13px;line-height:1.45;color:#2b2a27;"><div style="font-family:${MV_MAIL.display};font-size:18px;font-weight:800;color:${MV_MAIL.pink};">03</div>Доставляем через Ozon за 1–5 дней</td>
</tr></table>`;

  return mvEmailLayout({
    preheader: `Заказ №${orderId} принят — ${Math.round(Number(totalRub) || 0).toLocaleString("ru-RU")} ₽`,
    kicker: "Заказ принят",
    title: `${firstName ? mvEsc(firstName) + ", с" : "С"}пасибо за заказ`,
    script: "ваш аромат уже в пути к вам",
    intro: "Мы получили заказ и уже готовим посылку. Как только передадим её в доставку — пришлём трек-номер.",
    content,
    cta: { label: "Мои заказы", href: `${MV_MAIL.site}/orders` },
    note: "«Аромат — невидимый аксессуар, который оставляет самое сильное воспоминание». — Коко Шанель",
  });
}

// ─── Template: Day 7 — review request ────────────────────────────────────────
function emailDay7Html({ firstName, items, orderId }) {
  const productName = mvEsc(items?.[0]?.name || "ваш аромат");
  const content = `
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:24px 0 8px;"><tr>
<td align="center" bgcolor="${MV_MAIL.paper}" style="background:${MV_MAIL.paper};border-radius:22px;padding:22px 18px;">
<div style="font-size:30px;letter-spacing:6px;color:${MV_MAIL.pink};line-height:1;">&#9733;&#9733;&#9733;&#9733;&#9733;</div>
<div style="font-family:${MV_MAIL.body};font-size:14px;line-height:1.55;color:#2b2a27;margin-top:12px;">Отзыв помогает другим выбрать свой аромат.<br>За отзыв с фото — <b>+50 баллов</b> «Золото Magic Vibes».</div>
</td></tr></table>`
    + mvEmailCodeTile("REVIEW5", "−5% на следующий заказ · действует 30 дней");
  return mvEmailLayout({
    preheader: "Расскажите, как вам аромат — и получите −5% на следующий заказ",
    kicker: "Неделя с ароматом",
    title: "Как вам аромат?",
    script: "нам правда важно",
    intro: `${firstName ? mvEsc(firstName) + ", п" : "П"}рошла неделя с заказа №${mvEsc(orderId)}. Надеемся, <b>${productName}</b> уже стал частью вашего образа.`,
    content,
    cta: { label: "Написать отзыв", href: `${MV_MAIL.site}/orders` },
    marketing: true,
  });
}

// ─── Template: Day 30 — fragrance of the month ───────────────────────────────
async function emailDay30Html({ firstName }) {
  // latest Telegram news post as "fragrance of the month" content (local photos only)
  let newsText = null;
  let newsPhotoUrl = null;
  try {
    const prisma = getPrisma();
    if (prisma) {
      const post = await prisma.telegramNewsPost.findFirst({ where: { active: true }, orderBy: { publishedAt: "desc" } });
      if (post) {
        newsText = (post.text || "").slice(0, 320);
        const photo = post.photoUrl || "";
        newsPhotoUrl = photo.startsWith("/uploads/") ? `https://davidsklad.ru${photo}` : null;
      }
    }
  } catch { /* ignore */ }

  const months = ["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"];
  const month = months[new Date().getMonth()];
  const photo = newsPhotoUrl
    ? `<img src="${mvEsc(newsPhotoUrl)}" alt="" width="496" style="display:block;width:100%;max-width:496px;height:auto;border:0;border-radius:20px;margin:24px 0 4px;">`
    : "";
  const text = newsText
    ? `<p style="margin:18px 0 22px;font-family:${MV_MAIL.body};font-size:15px;line-height:1.65;color:#2b2a27;border-left:3px solid ${MV_MAIL.lime};padding-left:16px;">${mvEsc(newsText)}${newsText.length >= 320 ? "…" : ""}</p>`
    : `<p style="margin:18px 0 22px;font-family:${MV_MAIL.body};font-size:15px;line-height:1.65;color:#2b2a27;">Каталог пополнился: от классических европейских домов до редких арабских ароматов. Найдите свою следующую историю.</p>`;

  return mvEmailLayout({
    preheader: `Аромат ${month}: новинки и подборки Magic Vibes`,
    kicker: `Аромат ${month}`,
    title: `${firstName ? mvEsc(firstName) + ", н" : "Н"}овая история`,
    script: "22 000 ароматов ждут",
    content: photo + text + `<p style="margin:0 0 18px;font-family:${MV_MAIL.body};font-size:13px;color:${MV_MAIL.muted};">Chanel · Dior · Tom Ford · Creed · Byredo · Montale и ещё 200+ брендов</p>`,
    cta: { label: "Открыть каталог", href: `${MV_MAIL.site}/catalog` },
    note: `&#10022; «Золото Magic Vibes» — баллы за покупки и отзывы. <a href="${MV_MAIL.site}/account" style="color:${MV_MAIL.violet};text-decoration:none;font-weight:600;">Проверить баланс &rarr;</a>`,
    marketing: true,
  });
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
        -- review / newsletter mails are advertising (38-ФЗ ст. 18): only with the opt-in ticked at checkout
        AND (o.delivery::jsonb #>> '{consents,ads}') = 'true'
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
        await seqSendMail({ to: email, subject: "Как вам аромат? Оставьте отзыв — −5% на следующий заказ", html, marketing: true });
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
        -- review / newsletter mails are advertising (38-ФЗ ст. 18): only with the opt-in ticked at checkout
        AND (o.delivery::jsonb #>> '{consents,ads}') = 'true'
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
        await seqSendMail({ to: email, subject: `Аромат ${["января","февраля","марта","апреля","мая","июня","июля","августа","сентября","октября","ноября","декабря"][new Date().getMonth()]} от Magic Vibes`, html, marketing: true });
        sent30++;
        logger.info("email_seq_sent", { step: 30, orderId: order.id, to: email });
        await new Promise((r) => setTimeout(r, 1200));
      } catch (err) {
        errors++;
        logger.warn("email_seq_step30_failed", { orderId: order.id, detail: err?.message || String(err) });
      }
    }
    if (orders7.length >= 100) logger.warn("email_seq_backlog_step7", { fetched: orders7.length, sent: sent7, hint: "LIMIT 100 hit — backlog may be larger" });
    if (orders30.length >= 100) logger.warn("email_seq_backlog_step30", { fetched: orders30.length, sent: sent30, hint: "LIMIT 100 hit — backlog may be larger" });
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
