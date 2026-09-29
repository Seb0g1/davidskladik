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
    // 152-ФЗ + ст. 18 38-ФЗ: a mailing needs both ticked boxes; the fact is logged as proof
    const consents = request.body?.consents || {};
    if (consents.pd !== true || consents.ads !== true) {
      return response.status(400).json({ error: "Отметьте согласие на обработку данных и на получение рассылки" });
    }
    logger.info("shop_consent", { kind: "subscribe", email, source, version: cleanText(String(consents.version || "")).slice(0, 20), ip: String(request.ip || "").replace(/^::ffff:/, "").slice(0, 64) });

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
      const htmlBody = mvEmailLayout({
        preheader: `Промокод ${promoCode}: −${discount} на первый заказ`,
        kicker: "Подарок",
        title: `−${discount} на первый заказ`,
        script: "с любовью, Magic Vibes",
        intro: "Спасибо, что с нами! Введите промокод при оформлении заказа в поле «Промокод».",
        content: mvEmailCodeTile(promoCode, "Действует на первый заказ на magicvibes.ru")
          + (quizCategory ? `<p style="margin:0 0 18px;font-family:${MV_MAIL.body};font-size:15px;line-height:1.6;color:#2b2a27;">Ваш результат квиза: <b>${mvEsc(quizCategory)}</b> — подборка уже ждёт в каталоге.</p>` : ""),
        cta: { label: "Выбрать аромат", href: `${MV_MAIL.site}/catalog` },
        note: "Если вы не подписывались — просто проигнорируйте письмо.",
        marketing: true,
      });

      // global.__shopEmailSeq__ was never set, so this promo was silently never sent
      try {
        await shopSendEmail({ to: email, subject: subjectText, html: htmlBody, marketing: true });
      } catch (mailErr) {
        logger.warn("shop_email_subscribe: promo mail failed", { detail: mailErr?.message || String(mailErr) });
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
