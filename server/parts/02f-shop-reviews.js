// Shop reviews — authenticated buyers leave reviews, shown on homepage and product pages.
// POST /api/shop/reviews — create review (requires shop auth token)
// GET  /api/shop/reviews — list approved reviews (public)
// GET  /api/shop/reviews/my — my reviews (shop auth)
// GET  /api/shop/admin/reviews — all reviews (admin)
// PATCH /api/shop/admin/reviews/:id — approve/hide (admin)

// ─── Public: list approved reviews ───────────────────────────────────────────
app.get("/api/shop/reviews", async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const limit = Math.min(50, Number(request.query.limit || 8) || 8);
    const offerId = cleanText(request.query.offerId || "");
    const reviews = await prisma.shopReview.findMany({
      where: { approved: true, rating: { gte: 4 }, ...(offerId ? { offerId } : {}) },
      orderBy: { createdAt: "desc" },
      take: limit,
      include: { customer: { select: { firstName: true, lastName: true, email: true } } },
    });
    response.json({
      ok: true,
      reviews: reviews.map((r) => ({
        id: r.id,
        offerId: r.offerId,
        productName: r.productName,
        productImg: r.productImg,
        rating: r.rating,
        text: r.text,
        photoUrl: r.photoUrl || null,
        createdAt: r.createdAt,
        author: [r.customer?.firstName, r.customer?.lastName].filter(Boolean).join(" ") || "Покупатель",
      })),
    });
  } catch (error) { next(error); }
});

// ─── Authenticated: create review ────────────────────────────────────────────
app.post("/api/shop/reviews", requireShopAuth, async (request, response, next) => {
  try {
    const customerId = request.shopCustomer?.customerId;
    if (!customerId) return response.status(401).json({ error: "Требуется авторизация" });
    const text = cleanText(request.body?.text || "");
    const rating = Math.max(1, Math.min(5, Number(request.body?.rating || 5) || 5));
    if (!text || text.length < 10) return response.status(400).json({ error: "Текст отзыва минимум 10 символов." });

    // Require a display name to protect customer privacy
    const reviewer = await prisma.shopCustomer.findUnique({ where: { id: customerId }, select: { firstName: true } });
    if (!reviewer?.firstName) return response.status(400).json({ error: "Укажите ваше имя в профиле, чтобы опубликовать отзыв.", code: "no_name" });
    if (text.length > 2000) return response.status(400).json({ error: "Текст отзыва максимум 2000 символов." });

    const offerId = cleanText(request.body?.offerId || "");
    const productName = cleanText(request.body?.productName || "").slice(0, 200);
    const productImg = cleanText(request.body?.productImg || "").slice(0, 500);
    const photoUrl = cleanText(request.body?.photoUrl || "").slice(0, 1000) || null;

    const prisma = getPrisma();

    // Prevent duplicate reviews for same product
    if (offerId) {
      const existing = await prisma.shopReview.findFirst({ where: { customerId, offerId } });
      if (existing) return response.status(409).json({ error: "Вы уже оставили отзыв на этот товар.", code: "already_reviewed" });
    }

    const review = await prisma.shopReview.create({
      data: { customerId, offerId: offerId || null, productName: productName || null, productImg: productImg || null, rating, text, photoUrl, approved: true },
    });

    // Award loyalty points
    const pts = photoUrl ? 50 : 20;
    await prisma.$transaction([
      prisma.shopCustomer.update({ where: { id: customerId }, data: { loyaltyPoints: { increment: pts } } }),
      prisma.shopPointTransaction.create({ data: { customerId, points: pts, reason: photoUrl ? "Отзыв с фото" : "Отзыв", refId: review.id } }),
    ]);

    response.json({ ok: true, review: { id: review.id, rating: review.rating, text: review.text, createdAt: review.createdAt }, pointsEarned: pts });
  } catch (error) { next(error); }
});

// ─── Authenticated: my reviews ────────────────────────────────────────────────
app.get("/api/shop/reviews/my", requireShopAuth, async (request, response, next) => {
  try {
    const customerId = request.shopCustomer?.customerId;
    const prisma = getPrisma();
    const reviews = await prisma.shopReview.findMany({
      where: { customerId },
      orderBy: { createdAt: "desc" },
    });
    response.json({ ok: true, reviews });
  } catch (error) { next(error); }
});

// ─── Admin: list all reviews ──────────────────────────────────────────────────
app.get("/api/shop/admin/reviews", requireStaff, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const reviews = await prisma.shopReview.findMany({
      orderBy: { createdAt: "desc" },
      take: 200,
      include: { customer: { select: { email: true, firstName: true, lastName: true } } },
    });
    response.json({ ok: true, reviews });
  } catch (error) { next(error); }
});

// ─── Admin: approve/hide review ───────────────────────────────────────────────
app.patch("/api/shop/admin/reviews/:id", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const review = await prisma.shopReview.update({
      where: { id: request.params.id },
      data: { approved: request.body.approved !== false },
    });
    response.json({ ok: true, review });
  } catch (error) { next(error); }
});

// ─── Admin: delete review ─────────────────────────────────────────────────────
app.delete("/api/shop/admin/reviews/:id", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    await prisma.shopReview.delete({ where: { id: request.params.id } });
    response.json({ ok: true });
  } catch (error) { next(error); }
});

// ─── Loyalty: get balance + tier ─────────────────────────────────────────────
app.get("/api/shop/loyalty", requireShopAuth, async (request, response, next) => {
  try {
    const customerId = request.shopCustomer?.customerId;
    if (!customerId) return response.status(401).json({ error: "Требуется авторизация" });
    const prisma = getPrisma();
    const customer = await prisma.shopCustomer.findUnique({
      where: { id: customerId },
      select: { loyaltyPoints: true },
    });
    const pts = customer?.loyaltyPoints ?? 0;
    const transactions = await prisma.shopPointTransaction.findMany({
      where: { customerId },
      orderBy: { createdAt: "desc" },
      take: 20,
    });
    const tier = pts >= 300 ? "platinum" : pts >= 100 ? "gold" : "silver";
    const nextTier = tier === "platinum" ? null : tier === "gold" ? 300 : 100;
    response.json({ ok: true, points: pts, tier, nextTier, transactions });
  } catch (error) { next(error); }
});
