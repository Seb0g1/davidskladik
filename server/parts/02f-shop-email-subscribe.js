// POST /api/shop/email-subscribe — capture email from popup or quiz, send promo code
// Globals: getPrisma, cleanText, logger

app.post("/api/shop/email-subscribe", shopCors, async (request, response, next) => {
  try {
    const email = cleanText(request.body?.email || "").toLowerCase().trim();
    const source = cleanText(request.body?.source || "popup").slice(0, 50);
    const quizCategory = cleanText(request.body?.quizCategory || "").slice(0, 100) || null;

    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return response.status(400).json({ error: "Укажите корректный email" });
    }

    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ error: "База данных недоступна" });

    // Upsert — if already subscribed just update quiz category if new
    const subscriber = await prisma.shopEmailSubscriber.upsert({
      where: { email },
      update: { ...(quizCategory ? { quizCategory } : {}), unsubscribed: false },
      create: { email, source, quizCategory },
    });

    // Send promo if not yet sent
    if (!subscriber.promoSent) {
      const promoCode = source === "quiz" ? "QUIZ10" : "VIBES10";
      const discount = source === "quiz" ? "10%" : "10%";
      const subjectText = "Ваш промокод Magic Vibes — " + promoCode;
      const htmlBody = `<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><style>
        body{font-family:'Helvetica Neue',Arial,sans-serif;background:#09090b;color:#f2ede6;margin:0;padding:0}
        .wrap{max-width:540px;margin:40px auto;padding:40px 36px;background:#111113;border:1px solid rgba(201,162,94,0.2);border-radius:4px}
        .brand{font-family:Georgia,serif;font-style:italic;font-size:24px;color:#c9a25e;margin-bottom:28px}
        .promo{font-family:Georgia,serif;font-style:italic;font-size:52px;color:#c9a25e;letter-spacing:0.1em;margin:16px 0}
        .muted{font-size:13px;color:#7d7a73;line-height:1.7;margin-top:8px}
        .btn{display:inline-block;margin-top:28px;padding:12px 28px;background:#c9a25e;color:#09090b;text-decoration:none;border-radius:2px;font-weight:600;font-size:13px;letter-spacing:0.06em}
      </style></head><body><div class="wrap">
        <div class="brand">Magic Vibes</div>
        <p style="font-size:15px;margin:0 0 6px">Ваш промокод на первый заказ:</p>
        <div class="promo">${promoCode}</div>
        <p class="muted">Скидка <strong style="color:#c9a25e">${discount}</strong> на первый заказ на magicvibes.ru.<br>Введите при оформлении заказа в поле «Промокод».</p>
        ${quizCategory ? `<p class="muted" style="margin-top:16px">Подборка ароматов по вашему квизу: <strong style="color:#e8d5a3">${quizCategory}</strong></p>` : ""}
        <a href="https://magicvibes.ru/catalog" class="btn">Перейти в каталог</a>
        <p class="muted" style="margin-top:28px;font-size:11px;color:#4a473f">Если вы не подписывались — просто проигнорируйте письмо.</p>
      </div></body></html>`;

      try {
        const { seqSendMail } = global.__shopEmailSeq__ || {};
        if (seqSendMail) {
          await seqSendMail({ to: email, subject: subjectText, html: htmlBody });
        }
      } catch (mailErr) {
        logger.warn({ err: mailErr }, "shop_email_subscribe: promo mail failed");
      }

      await prisma.shopEmailSubscriber.update({ where: { email }, data: { promoSent: true } });
    }

    response.json({ ok: true, alreadySubscribed: subscriber.promoSent });
  } catch (error) { next(error); }
});

// GET /api/shop/admin/email-subscribers (admin)
app.get("/api/shop/admin/email-subscribers", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const [subscribers, total] = await Promise.all([
      prisma.shopEmailSubscriber.findMany({
        orderBy: { createdAt: "desc" },
        take: 500,
        where: { unsubscribed: false },
      }),
      prisma.shopEmailSubscriber.count({ where: { unsubscribed: false } }),
    ]);
    response.json({ ok: true, subscribers, total });
  } catch (error) { next(error); }
});
