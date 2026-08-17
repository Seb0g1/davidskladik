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
    const reviews = await prisma.shopReview.findMany({
      where: { approved: true },
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
        createdAt: r.createdAt,
        author: [r.customer?.firstName, r.customer?.lastName].filter(Boolean).join(" ") || r.customer?.email?.split("@")[0] || "Покупатель",
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
    if (text.length > 2000) return response.status(400).json({ error: "Текст отзыва максимум 2000 символов." });

    const offerId = cleanText(request.body?.offerId || "");
    const productName = cleanText(request.body?.productName || "").slice(0, 200);
    const productImg = cleanText(request.body?.productImg || "").slice(0, 500);

    const prisma = getPrisma();

    // Prevent duplicate reviews for same product
    if (offerId) {
      const existing = await prisma.shopReview.findFirst({ where: { customerId, offerId } });
      if (existing) return response.status(409).json({ error: "Вы уже оставили отзыв на этот товар.", code: "already_reviewed" });
    }

    const review = await prisma.shopReview.create({
      data: { customerId, offerId: offerId || null, productName: productName || null, productImg: productImg || null, rating, text, approved: true },
    });
    response.json({ ok: true, review: { id: review.id, rating: review.rating, text: review.text, createdAt: review.createdAt } });
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
