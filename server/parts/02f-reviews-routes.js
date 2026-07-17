// Reviews: unified listing from all marketplaces + replies + reply templates.
//
// Ozon (Premium): POST /v1/review/list, reply POST /v1/review/comment/create.
// Yandex (Medium): POST /v2/businesses/{businessId}/goods-feedback (page_token),
//                  reply POST /v2/businesses/{businessId}/goods-feedback/comments/update.
// WB (feedbacks-api): GET /api/v1/feedbacks?isAnswered=&take=&skip=,
//                     reply POST /api/v1/feedbacks/answer { id, text } (204).
//                     Токен кабинета должен включать категорию «Вопросы и отзывы».
// Templates live in data/review-templates.json (id, title, text).

const reviewTemplatesPath = path.join(dataDir, "review-templates.json");

async function readReviewTemplates() {
  try {
    const parsed = JSON.parse(await fs.readFile(reviewTemplatesPath, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function writeReviewTemplates(templates) {
  await fs.mkdir(dataDir, { recursive: true }).catch(() => {});
  await fs.writeFile(reviewTemplatesPath, JSON.stringify(templates, null, 2), "utf8");
}

function normalizeOzonReview(review = {}, account = {}) {
  return {
    id: `ozon:${cleanText(review.id)}`,
    marketplace: "ozon",
    target: account.id || "ozon",
    externalId: cleanText(review.id),
    sku: cleanText(review.sku),
    offerId: cleanText(review.offer_id || ""),
    productName: cleanText(review.product_title || review.sku_title || ""),
    rating: Number(review.rating || 0) || 0,
    text: cleanText(review.text || ""),
    advantages: "",
    disadvantages: "",
    authorName: cleanText(review.author_name || ""),
    createdAt: cleanText(review.published_at || review.created_at || ""),
    status: cleanText(review.status || ""),
    needsReply: cleanText(review.status).toUpperCase() !== "PROCESSED",
    commentsCount: Number(review.comments_amount || 0) || 0,
    photosCount: Number(review.photos_amount || 0) || 0,
  };
}

function normalizeYandexReview(feedback = {}, shop = {}) {
  const description = feedback.description || {};
  return {
    id: `yandex:${cleanText(feedback.feedbackId || feedback.id)}`,
    marketplace: "yandex",
    target: shop.id || "yandex",
    externalId: cleanText(feedback.feedbackId || feedback.id),
    sku: cleanText(feedback.identifiers?.modelId || ""),
    offerId: cleanText(feedback.identifiers?.offerId || ""),
    productName: cleanText(feedback.identifiers?.offerName || ""),
    rating: Number(feedback.statistics?.rating || 0) || 0,
    text: cleanText(description.comment || ""),
    advantages: cleanText(description.advantages || ""),
    disadvantages: cleanText(description.disadvantages || ""),
    authorName: cleanText(feedback.author || ""),
    createdAt: cleanText(feedback.createdAt || ""),
    status: feedback.needReaction ? "NEED_REACTION" : "PROCESSED",
    needsReply: feedback.needReaction !== false,
    commentsCount: Number(feedback.statistics?.commentsCount || 0) || 0,
    photosCount: Array.isArray(feedback.media?.photos) ? feedback.media.photos.length : 0,
  };
}

function normalizeWbReview(feedback = {}, account = {}) {
  const details = feedback.productDetails || {};
  const answered = Boolean(cleanText(feedback.answer?.text || ""));
  return {
    id: `wb:${cleanText(feedback.id)}`,
    marketplace: "wb",
    target: account.id || "wb",
    externalId: cleanText(feedback.id),
    sku: cleanText(details.nmId || ""),
    // supplierArticle = наш vendorCode (offerId Ozon) — по нему находится товар.
    offerId: cleanText(details.supplierArticle || ""),
    productName: cleanText(details.productName || ""),
    rating: Number(feedback.productValuation || 0) || 0,
    text: cleanText(feedback.text || ""),
    advantages: cleanText(feedback.pros || ""),
    disadvantages: cleanText(feedback.cons || ""),
    authorName: cleanText(feedback.userName || ""),
    createdAt: cleanText(feedback.createdDate || ""),
    status: answered ? "PROCESSED" : "NEW",
    needsReply: !answered,
    commentsCount: answered ? 1 : 0,
    photosCount: Array.isArray(feedback.photoLinks) ? feedback.photoLinks.length : 0,
  };
}

// WB требует явный isAnswered: «все отзывы» — это два запроса (сначала новые).
async function listWbFeedbacks(account, { onlyUnanswered, limit }) {
  const take = Math.max(1, Math.min(1000, limit));
  const rows = [];
  for (const isAnswered of onlyUnanswered ? [false] : [false, true]) {
    if (rows.length >= limit) break;
    // attempts: 1 — bucket отзывов WB после 429 просит ждать ~60 с; ретрай
    // внутри UI-запроса упирается в таймаут nginx, лучше сразу warning.
    const data = await wbRequest(account, "feedbacks", "GET",
      `/api/v1/feedbacks?isAnswered=${isAnswered}&take=${take}&skip=0&order=dateDesc`, undefined, { attempts: 1 });
    rows.push(...(data?.data?.feedbacks || []));
  }
  return rows.slice(0, limit);
}

app.get("/api/reviews", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.query.marketplace || "all").toLowerCase();
    const onlyUnanswered = String(request.query.unanswered ?? "false") === "true";
    const limit = Math.max(1, Math.min(300, Number(request.query.limit || 100) || 100));
    const reviews = [];
    const warnings = [];

    if (marketplace === "all" || marketplace === "ozon") {
      for (const account of getOzonAccounts()) {
        try {
          const data = await ozonRequest("/v1/review/list", {
            limit: Math.max(20, Math.min(100, limit)),
            sort_dir: "DESC",
            ...(onlyUnanswered ? { status: "UNPROCESSED" } : {}),
          }, account);
          const rows = data?.reviews || data?.result?.reviews || [];
          reviews.push(...rows.map((review) => normalizeOzonReview(review, account)));
        } catch (error) {
          warnings.push(`Ozon ${account.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    if (marketplace === "all" || marketplace === "yandex") {
      const seenBusinesses = new Set();
      for (const shop of getYandexShops()) {
        if (!shop.businessId || seenBusinesses.has(String(shop.businessId))) continue;
        seenBusinesses.add(String(shop.businessId));
        try {
          let pageToken = "";
          const rows = [];
          for (let pageIndex = 0; pageIndex < 15 && rows.length < limit; pageIndex += 1) {
            const tokenPart = pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : "";
            const data = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/goods-feedback?limit=20${tokenPart}`,
              onlyUnanswered ? { reactionStatus: "NEED_REACTION" } : {});
            const batch = data?.result?.feedbacks || [];
            rows.push(...batch);
            pageToken = cleanText(data?.result?.paging?.nextPageToken || "");
            if (!batch.length || !pageToken) break;
          }
          reviews.push(...rows.slice(0, limit).map((feedback) => normalizeYandexReview(feedback, shop)));
        } catch (error) {
          warnings.push(`Yandex ${shop.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    if (marketplace === "all" || marketplace === "wb") {
      for (const account of getWbAccounts()) {
        try {
          const rows = await listWbFeedbacks(account, { onlyUnanswered, limit });
          reviews.push(...rows.map((feedback) => normalizeWbReview(feedback, account)));
        } catch (error) {
          warnings.push(`WB ${account.id}: ${error?.message || "ошибка"}`);
        }
      }
    }

    // Product names: yandex feedbacks carry only offerId/modelId — resolve via our DB.
    const prisma = getPrisma();
    if (prisma) {
      const missingNames = reviews.filter((review) => !review.productName && review.offerId);
      const offerIds = Array.from(new Set(missingNames.map((review) => review.offerId)));
      if (offerIds.length) {
        const products = await prisma.warehouseProduct.findMany({
          where: { offerId: { in: offerIds } },
          select: { offerId: true, name: true },
        }).catch(() => []);
        const nameByOffer = new Map();
        for (const product of products) {
          const key = cleanText(product.offerId);
          const name = cleanText(product.name);
          if (key && name && name !== key && !nameByOffer.has(key)) nameByOffer.set(key, name);
        }
        for (const review of missingNames) {
          review.productName = nameByOffer.get(review.offerId) || review.productName;
        }
      }
    }

    reviews.sort((a, b) => cleanText(b.createdAt).localeCompare(cleanText(a.createdAt)));
    response.json({ ok: true, rows: reviews.slice(0, limit), warnings });
  } catch (error) {
    next(error);
  }
});

app.post("/api/reviews/reply", requireAdmin, async (request, response, next) => {
  try {
    const marketplace = cleanText(request.body?.marketplace).toLowerCase();
    const externalId = cleanText(request.body?.externalId);
    const target = cleanText(request.body?.target);
    const text = cleanText(request.body?.text);
    if (!externalId || !text) return response.status(400).json({ error: "Нужны externalId и text." });

    if (marketplace === "ozon") {
      const account = getOzonAccountByTarget(target) || getOzonAccounts()[0];
      if (!account) return response.status(400).json({ error: "Ozon аккаунт не найден." });
      const result = await ozonRequest("/v1/review/comment/create", {
        review_id: externalId,
        text,
        mark_review_as_processed: true,
      }, account);
      await appendAudit(request, "reviews.reply", { entityType: "review", entityId: `ozon:${externalId}` });
      return response.json({ ok: true, marketplace, result });
    }

    if (marketplace === "yandex") {
      const shop = getYandexShopByTarget(target) || getYandexShops()[0];
      if (!shop?.businessId) return response.status(400).json({ error: "Yandex кабинет не найден." });
      const result = await yandexRequest(shop, "POST", `/v2/businesses/${shop.businessId}/goods-feedback/comments/update`, {
        feedbackId: Number(externalId) || externalId,
        comment: { text },
      });
      await appendAudit(request, "reviews.reply", { entityType: "review", entityId: `yandex:${externalId}` });
      return response.json({ ok: true, marketplace, result });
    }

    if (marketplace === "wb") {
      const account = getWbAccountByTarget(target) || getWbAccounts()[0];
      if (!account) return response.status(400).json({ error: "Кабинет WB не найден." });
      // 204 без тела при успехе; текст ответа WB ограничивает 5000 символами.
      const result = await wbRequest(account, "feedbacks", "POST", "/api/v1/feedbacks/answer", {
        id: externalId,
        text: text.slice(0, 5000),
      });
      await appendAudit(request, "reviews.reply", { entityType: "review", entityId: `wb:${externalId}` });
      return response.json({ ok: true, marketplace, result });
    }

    response.status(400).json({ error: "marketplace должен быть ozon, yandex или wb." });
  } catch (error) {
    next(error);
  }
});

app.get("/api/reviews/templates", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ ok: true, templates: await readReviewTemplates() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/reviews/templates", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readReviewTemplates();
    const title = cleanText(request.body?.title);
    const text = cleanText(request.body?.text);
    if (!title || !text) return response.status(400).json({ error: "Нужны title и text." });
    const id = cleanText(request.body?.id) || crypto.randomUUID();
    const existingIndex = templates.findIndex((item) => item.id === id);
    const template = { id, title: title.slice(0, 80), text: text.slice(0, 1500), updatedAt: new Date().toISOString() };
    if (existingIndex >= 0) templates[existingIndex] = template;
    else templates.push(template);
    await writeReviewTemplates(templates);
    response.json({ ok: true, template, templates });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/reviews/templates/:id", requireAdmin, async (request, response, next) => {
  try {
    const templates = await readReviewTemplates();
    const next_ = templates.filter((item) => item.id !== cleanText(request.params.id));
    await writeReviewTemplates(next_);
    response.json({ ok: true, templates: next_ });
  } catch (error) {
    next(error);
  }
});
