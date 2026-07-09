app.post("/api/warehouse/products/:id/export", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Product export was not sent because manual confirmation is required." });
    }

    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    const conflict = productConflict(product, request.body.expectedUpdatedAt);
    if (conflict) return conflictResponse(response, [conflict]);
    const before = cloneAuditValue({ id: product.id, exports: product.exports || {}, updatedAt: product.updatedAt });

    const targetId = cleanText(request.body.target || product.target || product.marketplace);
    const targetMeta = targetById(targetId) || { id: targetId, marketplace: targetId, name: targetId };
    product.exports = product.exports || {};

    if (targetMeta.marketplace === "ozon") {
      const account = getOzonAccountByTarget(targetMeta.id || targetId);
      if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

      const built = buildOzonWarehouseProductItem(product, request.body);
      if (!built.ready) {
        return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
      }

      const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
      const exportState = {
        status: "sent",
        targetName: account.name || "Ozon",
        sentAt: new Date().toISOString(),
      };
      product.exports[account.id] = exportState;
      product.exports.ozon = exportState;
      product.updatedAt = new Date().toISOString();
      await writeWarehouse(warehouse);
      const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
      await appendAudit(request, "warehouse.product.export", {
        productId: product.id,
        offerId: product.offerId,
        target: account.id,
        oldValue: before,
        newValue: { id: product.id, exports: product.exports, updatedAt: product.updatedAt },
      });
      return response.json({ ok: true, target: account.id, sent: 1, item: built.item, result, product: freshProduct || normalizeWarehouseProduct(product) });
    }

    if (targetMeta.marketplace === "yandex" || targetId === "yandex") {
      const shop = getYandexShopByTarget(targetMeta.id || targetId);
      if (!shop) return response.status(400).json({ error: "Кабинет Yandex Market не найден. Добавьте его в настройках." });

      const smallVolumeCheck = assessYandexSmallVolume(product);
      if (smallVolumeCheck.blocked) {
        return response.status(400).json({
          error: smallVolumeCheck.reason,
          blocked: true,
          minVolumeMl: smallVolumeCheck.minVolumeMl,
        });
      }

      const built = buildYandexOfferMapping(product, request.body);
      if (!built.ready) {
        return response.status(400).json({ error: "Не хватает обязательных полей Yandex Market.", missing: built.missing });
      }

      const result = await yandexRequest(
        shop,
        "POST",
        `/v2/businesses/${shop.businessId}/offer-mappings/update`,
        { offerMappings: [{ offer: built.offer }] },
      );
      const exportState = {
        status: "sent",
        targetName: shop.name || "Yandex Market",
        sentAt: new Date().toISOString(),
      };
      product.exports[shop.id] = exportState;
      product.exports.yandex = exportState;
      product.updatedAt = new Date().toISOString();
      warehouse.products = mergeProducts(warehouse.products || [], [
        buildYandexWarehouseProductFromOzonExport(product, shop, exportState),
      ]);
      await writeWarehouse(warehouse);
      const [freshProduct] = await buildFreshWarehouseProducts([product.id]);
      await appendAudit(request, "warehouse.product.export", {
        productId: product.id,
        offerId: product.offerId,
        target: shop.id,
        oldValue: before,
        newValue: { id: product.id, exports: product.exports, updatedAt: product.updatedAt },
      });
      return response.json({ ok: true, target: shop.id, sent: 1, offer: built.offer, result, product: freshProduct || normalizeWarehouseProduct(product) });
    }

    return response.status(400).json({ error: "Неизвестный маркетплейс для выгрузки." });
  } catch (error) {
    next(error);
  }
});

app.post("/api/warehouse/products/:id/publish", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({ error: "Product was not published because manual confirmation is required." });
    }

    const warehouse = await readWarehouse();
    const product = warehouse.products.find((item) => item.id === request.params.id);
    if (!product) return response.status(404).json({ error: "Товар склада не найден." });
    if (product.marketplace !== "ozon") {
      return response.status(400).json({ error: "Автосоздание карточки сейчас поддержано только для Ozon." });
    }

    const rules = await readOzonProductRules();
    const built = buildOzonProductPayload(
      {
        offerId: product.offerId,
        name: request.body.name || product.name,
        price: Number(request.body.usdPrice || 0),
      },
      { ...rules, ...(request.body.ozon || {}) },
    );
    if (!built.ready) {
      return response.status(400).json({ error: "Не хватает обязательных полей Ozon.", missing: built.missing });
    }

    const account = getOzonAccountByTarget(product.target || "ozon");
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const result = await ozonRequest("/v2/product/import", { items: [built.item] }, account);
    response.json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon/products/send", requireAdmin, async (request, response, next) => {
  try {
    if (request.body.confirmed !== true) {
      return response.status(400).json({
        error: "Ozon products were not sent because manual confirmation is required.",
      });
    }

    const offerIds = new Set(
      (Array.isArray(request.body.offerIds) ? request.body.offerIds : [])
        .map((offerId) => String(offerId || "").trim())
        .filter(Boolean),
    );

    if (!offerIds.size) {
      return response.status(400).json({ error: "No selected products to send." });
    }

    const account = getOzonAccountByTarget(cleanText(request.body.target || "ozon"));
    if (!account) return response.status(400).json({ error: "Кабинет Ozon не найден. Добавьте его в настройках." });

    const rules = await readOzonProductRules();
    const existingOfferIds = await getOzonOfferIdSet(5000, account);
    const candidates = await getPriceMasterProductCandidates({ limit: 1000 });
    const selected = candidates.filter((row) => offerIds.has(row.offerId));
    const items = [];
    const blocked = [];

    for (const row of selected) {
      const built = buildOzonProductPayload(row, rules);
      if (existingOfferIds.has(row.offerId)) {
        blocked.push({ offerId: row.offerId, reason: "already_exists" });
      } else if (!built.ready) {
        blocked.push({ offerId: row.offerId, reason: "missing_fields", missing: built.missing });
      } else {
        items.push(built.item);
      }
    }

    if (!items.length) {
      return response.status(400).json({ error: "No ready products to send.", blocked });
    }

    const results = [];
    for (const chunk of chunkArray(items, 100)) {
      const data = await ozonRequest("/v2/product/import", { items: chunk }, account);
      results.push(data);
    }

    response.json({
      ok: true,
      sent: items.length,
      blocked,
      results,
    });
  } catch (error) {
    next(error);
  }
});
