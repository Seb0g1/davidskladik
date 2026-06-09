app.get("/api/warehouse/ai-drafts", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const marketplace = cleanText(request.query.marketplace || "").toLowerCase();
    const status = cleanText(request.query.status || "pending").toLowerCase();
    const limit = cleanLimit(request.query.limit, 200, 1000);
    const rows = [];
    for (const product of warehouse.products || []) {
      const normalized = normalizeWarehouseProduct(product);
      if (marketplace && normalized.marketplace !== marketplace) continue;
      const allImageDrafts = normalized.aiImages || [];
      const latestImageDraft = allImageDrafts
        .filter((draft) => !status || draft.status === status)
        .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))[0] || null;
      const contentDrafts = (normalized.aiContentDrafts || [])
        .filter((draft) => !status || draft.status === status)
        .map((draft) => ({ type: "content", draft, relatedImageDraft: latestImageDraft }));
      const imageDrafts = allImageDrafts
        .filter((draft) => !status || draft.status === status)
        .map((draft) => ({ type: "image", draft }));
      for (const item of [...contentDrafts, ...imageDrafts]) {
        rows.push({
          product: {
            id: normalized.id,
            offerId: normalized.offerId,
            name: normalized.name,
            marketplace: normalized.marketplace,
            target: normalized.target,
            imageUrl: normalized.imageUrl,
            updatedAt: normalized.updatedAt,
            cardQuality: normalized.yandex?.extra?.cardQuality || null,
          },
          type: item.type,
          draft: item.draft,
          relatedImageDraft: item.relatedImageDraft || null,
        });
      }
    }
    rows.sort((a, b) => String(b.draft.createdAt || "").localeCompare(String(a.draft.createdAt || "")));
    response.json({ ok: true, drafts: rows.slice(0, limit), total: rows.length });
  } catch (error) {
    next(error);
  }
});


