app.get("/api/supplier-cart/preview", requireAdmin, async (request, response, next) => {
  try {
    const preview = await buildSupplierCartPreview({
      marketplace: request.query.marketplace,
      from: request.query.from,
      to: request.query.to,
      limit: request.query.limit,
    });
    response.json(preview);
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/draft", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
    const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
    const skipped = rows.length - ready - alreadyCommitted;
    response.json({
      ok: true,
      draftId: state.draft?.id || "",
      generatedAt: state.draft?.generatedAt || null,
      generatedBy: state.draft?.generatedBy || "",
      rows,
      total: rows.length,
      ready,
      skipped,
      alreadyCommitted,
      warnings: state.draft?.summary?.warnings || [],
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/schedule", requireAdmin, async (_request, response, next) => {
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/supplier-cart/schedule", requireAdmin, async (request, response, next) => {
  try {
    const appSettings = await readAppSettings();
    const settings = normalizeSupplierCartSettings({
      ...(appSettings.supplierCart || {}),
      ...(request.body || {}),
    });
    await writeAppSettings({
      ...appSettings,
      supplierCart: settings,
    });
    await scheduleSupplierCartAuto();
    response.json({ ok: true, settings, ...supplierCartAutomationPublic() });
  } catch (error) {
    next(error);
  }
});

// Background generate tracking — completion detected via draft generatedAt timestamp
let _cartGenerating = false;
let _cartGeneratingAt = null;
let _cartGenerateTriggeredAt = null; // draft.generatedAt at the moment we triggered generation
const _CART_GENERATE_TIMEOUT_MS = 5 * 60 * 1000; // auto-clear stale flag after 5 min

app.get("/api/supplier-cart/generating", requireAdmin, async (_request, response) => {
  if (!_cartGenerating) return response.json({ generating: false, startedAt: null });
  // Auto-clear if timed out
  if (_cartGeneratingAt && Date.now() - new Date(_cartGeneratingAt).getTime() > _CART_GENERATE_TIMEOUT_MS) {
    _cartGenerating = false;
    _cartGeneratingAt = null;
    _cartGenerateTriggeredAt = null;
    return response.json({ generating: false, startedAt: null });
  }
  // Detect completion by checking if draft's generatedAt has advanced
  try {
    const state = await readSupplierCartState();
    const draftAt = state.draft?.generatedAt || null;
    const completed = _cartGenerateTriggeredAt
      ? (draftAt && draftAt > _cartGenerateTriggeredAt)
      : Boolean(draftAt); // first-ever generation: done when any draft exists
    if (completed) {
      _cartGenerating = false;
      _cartGeneratingAt = null;
      _cartGenerateTriggeredAt = null;
      return response.json({ generating: false, startedAt: null });
    }
  } catch (_) {}
  response.json({ generating: _cartGenerating, startedAt: _cartGeneratingAt });
});

app.post("/api/supplier-cart/generate", requireAdmin, async (request, response, next) => {
  try {
    // Return current cached draft immediately; generation runs in worker via BullMQ
    const state = await readSupplierCartState();
    const rows = state.draft?.rows || [];
    const ready = rows.filter((r) => r.ready && !r.alreadyCommitted).length;
    const alreadyCommitted = rows.filter((r) => r.alreadyCommitted).length;
    response.json({
      ok: true,
      generating: true,
      draftId: state.draft?.id || "",
      generatedAt: state.draft?.generatedAt || null,
      generatedBy: state.draft?.generatedBy || "",
      rows,
      total: rows.length,
      ready,
      skipped: rows.length - ready - alreadyCommitted,
      alreadyCommitted,
      warnings: state.draft?.summary?.warnings || [],
    });

    if (_cartGenerating) return; // already running
    _cartGenerating = true;
    _cartGeneratingAt = new Date().toISOString();
    _cartGenerateTriggeredAt = state.draft?.generatedAt || null;
    const params = {
      marketplace: request.body?.marketplace || request.query.marketplace,
      from: request.body?.from || request.query.from,
      to: request.body?.to || request.query.to,
      limit: request.body?.limit || request.query.limit,
    };
    // Run inline — all I/O-bound work (marketplace APIs, DB), won't block the event loop.
    // BullMQ queue starvation made this unreliable: price-push jobs (priority 1) always
    // run ahead, so a user-triggered cart generation (priority 4) would never be scheduled.
    setImmediate(async () => {
      try {
        await generateSupplierCartDraft(params, request);
        logger.info("supplier cart inline generate complete");
      } catch (err) {
        logger.warn("supplier cart inline generate failed", { detail: err?.message || String(err) });
      } finally {
        _cartGenerating = false;
        _cartGeneratingAt = null;
        _cartGenerateTriggeredAt = null;
      }
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/commit", requireAdmin, async (request, response, next) => {
  try {
    const rows = Array.isArray(request.body?.rows) ? request.body.rows : [];
    const keys = Array.isArray(request.body?.keys) ? request.body.keys : [];
    const state = await readSupplierCartState();
    const sourceRows = rows.length
      ? rows
      : (state.draft?.rows?.length ? state.draft.rows : (await buildSupplierCartPreview(request.body || {})).rows);
    const selectedKeys = new Set(keys.map(cleanText).filter(Boolean));
    const selectedRows = selectedKeys.size ? sourceRows.filter((row) => selectedKeys.has(cleanText(row.key))) : sourceRows;
    const result = await insertSupplierCartRowsIntoPriceMaster(selectedRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      skippedDetails: result.skippedDetails || [],
      pmBlocked: result.pmBlocked || [],
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      verifiedInPriceMaster: Boolean(result.verification?.ok),
      verifiedRows: Number(result.verification?.verifiedRows || 0),
      priceMasterDb: result.verification?.db || "",
      marketplaceConfirms: result.marketplaceConfirms || [],
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/pricemaster/status", requireAdmin, async (_request, response, next) => {
  try {
    response.json(await getPriceMasterBasketStatus());
  } catch (error) {
    next(error);
  }
});

// Pending PM cart item count (badge): rows in RequestDocs WHERE Sended=0.
app.get("/api/supplier-cart/pm-pending-count", requireAdmin, async (_request, response, next) => {
  try {
    const connection = await pool.getConnection();
    try {
      const [[row]] = await connection.query(
        "SELECT COUNT(*) AS total FROM RequestRows rr JOIN RequestDocs rd ON rd.DocID = rr.DocID WHERE rd.Sended = 0",
      );
      const [[docRow]] = await connection.query(
        "SELECT COUNT(*) AS docs FROM RequestDocs WHERE Sended = 0",
      );
      response.json({ ok: true, rows: Number(row?.total || 0), docs: Number(docRow?.docs || 0) });
    } finally {
      connection.release();
    }
  } catch (error) {
    next(error);
  }
});

// PM cart history: last sent docs (Sended=1), grouped by DocID.
app.get("/api/supplier-cart/pm-history", requireAdmin, async (_request, response, next) => {
  try {
    const limit = Math.min(50, Math.max(1, Number(_request.query.limit || 20) || 20));
    const connection = await pool.getConnection();
    try {
      const [docs] = await connection.query(
        `SELECT d.DocID, d.DocDate, d.PartnerID, d.Comment, d.Sended, d.Recieved,
                p.PartnerName, COUNT(r.RowID) AS rowCount
           FROM RequestDocs d
           LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
           LEFT JOIN RequestRows r ON r.DocID = d.DocID
          WHERE d.Sended = 1 AND d.Comment LIKE 'ДавидСклад%'
          GROUP BY d.DocID, d.DocDate, d.PartnerID, d.Comment, d.Sended, d.Recieved, p.PartnerName
          ORDER BY d.DocID DESC
          LIMIT ?`,
        [limit],
      );
      response.json({ ok: true, docs: docs || [] });
    } finally {
      connection.release();
    }
  } catch (error) {
    next(error);
  }
});

// Снять блок поставщика для SKU вручную.
// Блок возникает при замене поставщика («Не было») и хранится в state.supplierBlocks и/или SupplierBlock Postgres.
app.delete("/api/supplier-cart/blocks", requireAdmin, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const partnerId = cleanText(request.body?.partnerId);
    if (!offerId || !partnerId) {
      return response.status(400).json({ error: "offerId and partnerId are required.", code: "blocks_missing_params" });
    }
    const blockKey = supplierBlockKey(offerId, partnerId);
    const state = await readSupplierCartState();
    const hadJsonBlock = Boolean(state.supplierBlocks?.[blockKey]);
    if (hadJsonBlock) {
      delete state.supplierBlocks[blockKey];
      await writeSupplierCartState(state);
    }
    let hadPgBlock = false;
    if (shouldUsePostgresStorage()) {
      const prisma = getPrisma();
      if (prisma) {
        try {
          const updated = await prisma.supplierBlock.updateMany({
            where: { blockKey, active: true },
            data: { active: false, expiresAt: new Date() },
          });
          hadPgBlock = updated.count > 0;
        } catch (pgErr) {
          logger.warn("supplier block pg deactivate failed", { blockKey, detail: pgErr?.message || String(pgErr) });
        }
      }
    }
    if (!hadJsonBlock && !hadPgBlock) {
      return response.status(404).json({ ok: false, error: "Блок не найден.", code: "block_not_found", blockKey });
    }
    await appendAudit(request, "supplier_cart.block_removed", {
      entityType: "supplier_block",
      entityId: blockKey,
      newValue: { offerId, partnerId, blockKey, hadJsonBlock, hadPgBlock },
    });
    response.json({ ok: true, unblocked: blockKey });
  } catch (error) {
    next(error);
  }
});

// Clear stale "already committed" processed entries so items can be re-queued.
// Used when PM rows were manually deleted or orders got stuck without a real PM request.
app.delete("/api/supplier-cart/processed", requireAdmin, async (request, response, next) => {
  try {
    const keys = Array.isArray(request.body?.keys) ? request.body.keys.map(cleanText).filter(Boolean) : [];
    if (!keys.length) return response.status(400).json({ error: "keys[] is required.", code: "keys_required" });
    const keySet = new Set(keys);
    const state = await readSupplierCartState();
    let cleared = 0;
    for (const key of keys) {
      if (state.processed?.[key]) {
        delete state.processed[key];
        cleared++;
      }
    }
    // Also unmark alreadyCommitted in the stored draft so the UI reflects the change immediately
    // without requiring a full re-generate.
    if (Array.isArray(state.draft?.rows)) {
      for (const row of state.draft.rows) {
        if (row.alreadyCommitted && keySet.has(cleanText(row.key || ""))) {
          row.alreadyCommitted = false;
          row.requestDocId = null;
          row.requestRowId = null;
        }
      }
    }
    await writeSupplierCartState(state);
    response.json({ ok: true, cleared, requested: keys.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/rollback-all", requireAdmin, async (request, response, next) => {
  try {
    const confirm = cleanText(request.body?.confirm);
    if (confirm !== "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART") {
      return response.status(400).json({
        error: "Confirmation is required.",
        code: "supplier_cart_rollback_confirmation_required",
      });
    }
    const result = await rollbackSupplierCartAll(request, { dryRun: request.body?.dryRun !== false });
    response.json(result);
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-cart/manual-order", requireAdmin, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const quantity = Math.max(1, Math.round(Number(request.body?.quantity || 1) || 1));
    const marketplace = cleanText(request.body?.marketplace || "ozon").toLowerCase();
    const note = cleanText(request.body?.note || "").slice(0, 200);
    const forcedPartnerId = cleanText(request.body?.partnerId || "");
    const forcedRowId = cleanText(request.body?.rowId || "");
    if (!offerId) return response.status(400).json({ error: "offerId is required.", code: "manual_order_no_offer_id" });
    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId: "" });
    if (!product) return response.status(404).json({ error: "Товар не найден на складе.", code: "product_not_found" });
    const state = await readSupplierCartState();
    const manualKey = `manual|manual|manual-${Date.now()}|${offerId}`;
    const productName = cleanText(product.productName || product.name || offerId);
    const orderId = `manual-${Date.now()}`;

    let rowWithNote;
    if (forcedPartnerId) {
      const { options } = await listSupplierCartSupplierOptions(offerId);
      const chosen = pickSupplierCartOption(options, forcedPartnerId, forcedRowId);
      const rejection = supplierCartOptionRejection(chosen);
      if (rejection) return response.status(rejection.status).json({ error: rejection.error, code: rejection.code });
      rowWithNote = normalizeSupplierCartPreviewRow({
        key: manualKey,
        marketplace: "manual",
        offerId,
        quantity,
        productName,
        orderId,
        accountId: "manual",
        accountName: "Ручной заказ",
        supplierName: chosen.supplierName,
        partnerId: chosen.partnerId,
        offerRowId: chosen.rowId,
        price: chosen.price,
        originalPrice: chosen.originalPrice,
        priceCurrency: chosen.priceCurrency,
        trustFactor: chosen.trustFactor,
        orderCutoffTime: chosen.orderCutoffTime,
        reseller: chosen.reseller,
        stockOnlyFallback: chosen.stockOnly,
        available: true,
        ready: true,
        manualNote: note,
      });
    } else {
      const manualLine = normalizeSupplierCartLine({
        key: manualKey,
        marketplace: "manual",
        offerId,
        quantity,
        productName,
        orderId,
        accountId: "manual",
        accountName: "Ручной заказ",
      });
      const row = await resolveSupplierCartRow(warehouse, manualLine, state);
      if (!row.ready) {
        return response.status(400).json({
          error: "Не удалось подобрать поставщика.",
          skipReason: row.skipReason || "supplier_not_found",
          code: "manual_order_no_supplier",
        });
      }
      rowWithNote = { ...row, quantity, manualNote: note };
    }

    const result = await insertSupplierCartRowsIntoPriceMaster([rowWithNote], request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      row: result.inserted[0] || null,
    });
  } catch (error) {
    next(error);
  }
});

// Поиск товаров PriceMaster по названию/артикулу — запрашивает live MySQL напрямую.
// Снапшот Postgres не используется: он устаревает и скрывает товары с Active=0.
app.get("/api/supplier-cart/pm-search", requireStaff, async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || "").toLowerCase();
    const partnerId = cleanText(request.query.partnerId || "");
    const limit = cleanLimit(request.query.limit, 80, 200);

    const tokenGroups = q ? pmQueryToTokenGroups(q) : null;

    // Query live MySQL directly — always fresh, includes active=0 rows (marked _unavailable).
    let items = [];
    if (pool) {
      try {
        const params = [];
        const conditions = ["r.Ignored = 0"];

        if (q) {
          if (tokenGroups && tokenGroups.length) {
            // AND across token groups, OR within each group (synonyms).
            // Compound tokens (e.g. "BOD13") are OR'd against the AND block so
            // exact article codes always reach the JS post-filter.
            const sqlGroups = tokenGroups.filter((g) => !g._compound);
            const compoundGroups = tokenGroups.filter((g) => g._compound);

            const andParts = sqlGroups.map((group) => {
              const conds = group.flatMap(() => ["r.NativeName LIKE ?", "r.NativeID LIKE ?"]);
              params.push(...group.flatMap((t) => [likeSearch(t), likeSearch(t)]));
              return `(${conds.join(" OR ")})`;
            });
            const compoundConds = compoundGroups.flatMap((g) => {
              params.push(likeSearch(g[0]), likeSearch(g[0]));
              return ["r.NativeName LIKE ?", "r.NativeID LIKE ?"];
            });

            if (andParts.length && compoundConds.length) {
              conditions.push(`((${andParts.join(" AND ")}) OR (${compoundConds.join(" OR ")}))`);
            } else if (andParts.length) {
              for (const part of andParts) conditions.push(part);
            } else if (compoundConds.length) {
              conditions.push(`(${compoundConds.join(" OR ")})`);
            }
          } else {
            conditions.push("(r.NativeName LIKE ? OR r.NativeID LIKE ?)");
            params.push(likeSearch(q), likeSearch(q));
          }
        }

        if (partnerId) {
          conditions.push("d.PartnerID = ?");
          params.push(partnerId);
        }

        // Fetch limit*10 (cap 2000) to allow de-dup and post-filter, then slice to limit.
        params.push(Math.min(limit * 10, 2000));

        const [liveRows] = await pool.query(
          `SELECT r.RowID AS rowId, r.NativeID AS article, r.NativeName AS nativeName,
                  r.NativePrice AS price, r.Active AS active, d.DocDate AS docDate,
                  d.PartnerID AS partnerId, p.PartnerName AS partnerName
           FROM OfferRows r
           JOIN OfferDocs d ON d.DocID = r.DocID
           LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
           WHERE ${conditions.join(" AND ")}
           ORDER BY d.DocDate DESC, r.RowID DESC
           LIMIT ?`,
          params,
        );

        // De-duplicate: keep only the newest row per partner+article combination.
        const seenOffer = new Set();
        for (const row of liveRows) {
          const offerKey = `${cleanText(row.partnerId)}|${cleanText(row.article || "").toLowerCase()}`;
          if (seenOffer.has(offerKey)) continue;
          seenOffer.add(offerKey);
          items.push({
            id: `live_${cleanText(row.rowId)}`,
            rowId: cleanText(row.rowId || ""),
            article: cleanText(row.article || ""),
            partnerId: cleanText(row.partnerId || ""),
            partnerName: cleanText(row.partnerName || ""),
            nativeName: cleanText(row.nativeName || ""),
            price: String(row.price ?? ""),
            currency: "USD",
            docDate: row.docDate || null,
            active: Boolean(row.active),
            _unavailable: !row.active,
          });
        }

        // Post-filter: apply quality bar (required keywords must match; numbers/units optional).
        if (tokenGroups && tokenGroups.length >= 1) {
          items = items.filter((item) => {
            const hay = [cleanText(item.nativeName || ""), cleanText(item.article || "")].join(" ");
            return pmPassesSearchFilter(hay, tokenGroups);
          });
        }
      } catch (mysqlErr) {
        logger.warn("pm-search MySQL query failed", { detail: mysqlErr?.message || String(mysqlErr) });
      }
    }

    items = items.slice(0, limit);

    const usdRate = Number((await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
    // Инна prices in PM snapshot are stored with currency="USD" (snapshot has no managed-supplier
    // awareness), but they are actually in RUB — detect by partner name so sorting is correct.
    const toRub = (price, currency, partnerName) => {
      const p = Number(price || 0);
      if (cleanText(currency || "USD").toUpperCase() === "RUB") return p;
      if (isInnaSupplierName(partnerName || "")) return p;
      return p * usdRate;
    };
    const isTesterName = (name) => {
      const n = cleanText(name || "").toLowerCase();
      return n.includes("отливант") || /\btest(?:er|ep|or|r)?\b/.test(n) || n.includes("тест");
    };
    // Relevance score for a name given the token groups:
    //   +2 per required group that matches
    //   +1 per optional group that matches (numbers/units)
    //   +1 bonus when a numeric token matches with BOTH-sides word boundary
    //      (exact volume: "5ml" scores higher than "1.5ml" or "15ml")
    const computeRelevance = (name, article) => {
      if (!tokenGroups || !tokenGroups.length) return 0;
      const hay = [name, article].join(" ").toLowerCase().replace(/ё/g, "е");
      let score = 0;
      for (const group of tokenGroups) {
        const isOptional = pmTokenGroupIsOptional(group);
        const matches = group.some((t) => pmTokenMatchesText(hay, t));
        if (!matches) continue;
        score += isOptional ? 1 : 2;
        // Extra +1 when a numeric token is a true standalone number (both boundaries)
        if (isOptional && group.some((t) => /^\d+$/.test(t))) {
          const exactMatch = group.some((t) => {
            if (!/^\d+$/.test(t)) return false;
            const esc = t.replace(/[-[\]/{}()*+?.\\^$|]/g, "\\$&");
            return new RegExp(`(?<!\\d)${esc}(?!\\d)`).test(hay);
          });
          if (exactMatch) score += 1;
        }
      }
      return score;
    };

    const mapped = items.map((item) => {
      const currency = cleanText(item.currency || "USD");
      const price = Number(item.price || 0);
      const partnerName = cleanText(item.partnerName || "");
      const name = cleanText(item.nativeName || "");
      const article = cleanText(item.article || "");
      return {
        id: item.id,
        rowId: cleanText(item.rowId || ""),
        article,
        partnerId: cleanText(item.partnerId || ""),
        supplierName: partnerName,
        name,
        price,
        currency: isInnaSupplierName(partnerName) ? "RUB" : currency,
        priceRub: toRub(price, currency, partnerName),
        isTester: isTesterName(item.nativeName || ""),
        docDate: item.docDate?.toISOString?.()?.slice(0, 10) || null,
        unavailable: Boolean(item._unavailable),
        _relevance: computeRelevance(name, article),
      };
    });
    // Sort: 0=active, 1=active+tester, 2=inactive+tester, 3=inactive-non-tester ("не в PM")
    const pmSearchRank = (item) => {
      if (item.unavailable) return item.isTester ? 2 : 3;
      return item.isTester ? 1 : 0;
    };
    mapped.sort((a, b) => {
      const ra = pmSearchRank(a), rb = pmSearchRank(b);
      if (ra !== rb) return ra - rb;
      if (b._relevance !== a._relevance) return b._relevance - a._relevance;
      return a.priceRub - b.priceRub;
    });
    mapped.forEach((item) => { delete item._relevance; });
    response.json({
      ok: true,
      total: mapped.length,
      items: mapped,
    });
  } catch (error) {
    next(error);
  }
});

// Ручной заказ из PM-поиска: выбранные строки снапшота → RequestDocs/RequestRows
app.post("/api/supplier-cart/pm-manual-commit", requireAdmin, async (request, response, next) => {
  try {
    const items = Array.isArray(request.body?.items) ? request.body.items : [];
    if (!items.length) return response.status(400).json({ ok: false, error: "No items provided." });
    const prisma = getPrisma();
    if (!prisma) return response.status(503).json({ ok: false, error: "Database not available" });
    const ids = [...new Set(items.map((i) => cleanText(i.id)).filter(Boolean))];
    // Only look up Postgres snapshot for non-live IDs (live_xxx come from MySQL fallback)
    const snapshotIds = ids.filter((id) => !id.startsWith("live_"));
    const snapshotRows = snapshotIds.length
      ? await prisma.priceMasterSnapshotItem.findMany({ where: { id: { in: snapshotIds } } })
      : [];
    const snapById = new Map(snapshotRows.map((r) => [r.id, r]));
    const cartRows = items
      .map((item) => {
        const itemId = cleanText(item.id);
        let snap = snapById.get(itemId);
        // Live-only items (from MySQL fallback, id=live_XXX): build snap from request body fields
        if (!snap && itemId?.startsWith("live_") && item.rowId && item.partnerId) {
          snap = {
            rowId: cleanText(item.rowId),
            partnerId: cleanText(item.partnerId),
            article: cleanText(item.article || ""),
            nativeName: cleanText(item.name || item.nativeName || ""),
            price: Number(item.price || 0),
            currency: cleanText(item.currency || "USD"),
            partnerName: cleanText(item.supplierName || ""),
          };
        }
        if (!snap || !snap.rowId || !snap.partnerId) return null;
        const quantity = Math.max(1, Math.round(Number(item.quantity || 1) || 1));
        const note = cleanText(item.note || "").slice(0, 200);
        return normalizeSupplierCartPreviewRow({
          key: `manual|pm|${snap.partnerId}|${snap.article}|${Date.now()}`,
          marketplace: "manual",
          offerId: snap.article,
          productName: snap.nativeName || snap.article,
          quantity,
          partnerId: cleanText(snap.partnerId),
          supplierName: cleanText(snap.partnerName || ""),
          offerRowId: cleanText(snap.rowId),
          price: Number(snap.price || 0),
          priceCurrency: cleanText(snap.currency || "USD"),
          manualNote: note,
          ready: true,
          alreadyCommitted: false,
          trustFactor: 100,
          reseller: false,
          supplierScore: 100,
        });
      })
      .filter(Boolean);
    if (!cartRows.length) return response.status(400).json({ ok: false, error: "No valid rows to commit (missing rowId or partnerId)." });
    const result = await insertSupplierCartRowsIntoPriceMaster(cartRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      skipped: result.skipped,
      skippedDetails: result.skippedDetails || [],
      pmBlocked: (result.pmBlocked || []).map((b) => ({ offerId: b.offerId, productName: b.productName, existingDocId: b.existingDocId, existingDocDate: b.existingDocDate })),
      docIds: result.docIds,
      pickingCreated: Array.isArray(result.pickingCreated) ? result.pickingCreated.length : (result.pickingCreated || 0),
      verifiedInPriceMaster: Boolean(result.verification?.ok),
      rows: result.inserted,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/ready-to-ship", requireStaff, async (request, response, next) => {
  try {
    const lookbackDays = Math.min(60, Math.max(1, Number(request.query.days || 30) || 30));
    const limit = cleanLimit(request.query.limit, 500);
    const now = new Date();
    const from = new Date(now.getTime() - lookbackDays * 24 * 60 * 60 * 1000);
    const to = now;
    const appSettings = await readAppSettings();
    const cartSettings = normalizeSupplierCartSettings(appSettings.supplierCart || {});

    const [ozonResult, yandexResult, wbResult] = await Promise.allSettled([
      fetchOzonSupplierCartLines({ from, to, limit: Math.ceil(limit * 0.6), statuses: cartSettings.includeOzonStatuses }),
      fetchYandexSupplierCartLines({ from, to, limit: Math.ceil(limit * 0.3), statuses: cartSettings.includeYandexStatuses, substatuses: cartSettings.includeYandexSubstatuses }),
      fetchWbSupplierCartLines({ limit: Math.ceil(limit * 0.3) }),
    ]);

    const lines = [
      ...(ozonResult.status === "fulfilled" ? ozonResult.value : []),
      ...(yandexResult.status === "fulfilled" ? yandexResult.value : []),
      ...(wbResult.status === "fulfilled" ? wbResult.value : []),
    ];
    const errors = [
      ozonResult.status === "rejected" ? `Ozon: ${ozonResult.reason?.message || "ошибка"}` : null,
      yandexResult.status === "rejected" ? `Yandex: ${yandexResult.reason?.message || "ошибка"}` : null,
      wbResult.status === "rejected" ? `WB: ${wbResult.reason?.message || "ошибка"}` : null,
    ].filter(Boolean);

    response.json({ ok: true, lines: lines.slice(0, limit), total: lines.length, errors });
  } catch (error) {
    next(error);
  }
});

// «Нету у поставщика» для заказа из маркетплейса: снузит привязку основного поставщика по offerId.
app.post("/api/ready-to-ship/missing", requireStaff, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const snoozeDays = Math.min(60, Math.max(1, Number(request.body?.snoozeDays || 7) || 7));
    const partnerIdHint = cleanText(request.body?.partnerId || "").toLowerCase();
    if (!offerId) return response.status(400).json({ ok: false, error: "offerId is required.", code: "missing_offer_id" });

    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId });
    if (!product) return response.status(404).json({ ok: false, error: "Товар не найден на складе.", code: "product_not_found" });

    const links = product.links || [];
    const link = (partnerIdHint
      ? links.find((l) => cleanText(l.partnerId).toLowerCase() === partnerIdHint)
      : null)
      || links.find((l) => !l.snoozeUntil || new Date(l.snoozeUntil) <= new Date())
      || links[0];

    if (!link) return response.status(404).json({ ok: false, error: "Привязка поставщика не найдена.", code: "link_not_found" });

    const result = await applyWarehouseLinkSnooze(product.id, link.id, snoozeDays, { reason: "marketplace_order_missing" });
    await appendAudit(request, "supplier_cart.marketplace_order_missing", {
      entityType: "warehouse_product",
      entityId: product.id,
      newValue: { offerId, linkId: link.id, partnerId: link.partnerId, supplierName: link.supplierName, snoozeDays, snoozedUntil: result?.snoozedUntil },
    });
    response.json({
      ok: true,
      offerId,
      productId: product.id,
      linkId: link.id,
      supplierName: link.supplierName || link.partnerId || "",
      snoozedUntil: result?.snoozedUntil || null,
      snoozeDays,
    });
  } catch (error) {
    next(error);
  }
});

// Заказать у выбранного поставщика для заказа из маркетплейса (замена поставщика).
app.post("/api/ready-to-ship/order", requireStaff, async (request, response, next) => {
  try {
    const offerId = cleanText(request.body?.offerId);
    const partnerId = cleanText(request.body?.partnerId);
    const rowId = cleanText(request.body?.rowId);
    const quantity = Math.max(1, Math.round(Number(request.body?.quantity || 1) || 1));
    const marketplace = cleanText(request.body?.marketplace || "ozon").toLowerCase();
    const orderId = cleanText(request.body?.orderId || "");
    const accountId = cleanText(request.body?.accountId || "");
    const accountName = cleanText(request.body?.accountName || "");

    if (!offerId) return response.status(400).json({ ok: false, error: "offerId is required.", code: "missing_offer_id" });
    if (!partnerId || !rowId) return response.status(400).json({ ok: false, error: "partnerId and rowId are required.", code: "missing_supplier" });

    const { options } = await listSupplierCartSupplierOptions(offerId);
    const option = pickSupplierCartOption(options, partnerId, rowId);
    const rejection = supplierCartOptionRejection(option);
    if (rejection) return response.status(rejection.status).json({ ok: false, error: rejection.error, code: rejection.code });

    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), [offerId]);
    const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId });
    if (!product) return response.status(404).json({ ok: false, error: "Товар не найден на складе.", code: "product_not_found" });

    const cartRow = normalizeSupplierCartPreviewRow({
      key: `ready-to-ship|${marketplace}|${orderId || Date.now()}|${offerId}`,
      marketplace,
      accountId: accountId || marketplace,
      accountName: accountName || marketplace,
      orderId: orderId || `mp-${Date.now()}`,
      offerId,
      productName: cleanText(product.productName || product.name || offerId),
      quantity,
      warehouseProductId: product.id,
      groupKey: warehouseProductPageGroupKey(product),
      groupOfferId: product.offerId,
      partnerId: option.partnerId,
      supplierName: option.supplierName,
      offerRowId: option.rowId,
      price: option.price,
      originalPrice: option.originalPrice,
      priceCurrency: option.priceCurrency,
      trustFactor: option.trustFactor,
      orderCutoffTime: option.orderCutoffTime,
      reseller: option.reseller,
      supplierScore: option.score || 0,
      available: true,
      ready: true,
    });

    const result = await insertSupplierCartRowsIntoPriceMaster([cartRow], request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
      supplierName: option.supplierName,
      pmBlocked: (result.pmBlocked || []).map((b) => ({ offerId: b.offerId, productName: b.productName, existingDocId: b.existingDocId, existingDocDate: b.existingDocDate })),
      row: result.inserted[0] || null,
    });
  } catch (error) {
    next(error);
  }
});

// Пакетный заказ: несколько маркетплейсовых заказов → PM, авто-поставщик, группировка по поставщику.
app.post("/api/ready-to-ship/batch-order", requireStaff, async (request, response, next) => {
  try {
    const lines = Array.isArray(request.body?.lines) ? request.body.lines : [];
    if (!lines.length) return response.status(400).json({ ok: false, error: "No lines provided.", code: "missing_lines" });

    const allOfferIds = [...new Set(lines.map((l) => cleanText(l.offerId)).filter(Boolean))];
    const warehouse = await hydrateSupplierCartWarehouse(await readWarehouse(), allOfferIds);
    const state = await readSupplierCartState();

    const cartRows = [];
    const failed = [];
    for (const line of lines) {
      const offerId = cleanText(line.offerId);
      const quantity = Math.max(1, Math.round(Number(line.quantity || 1) || 1));
      const marketplace = cleanText(line.marketplace || "ozon").toLowerCase();
      const orderId = cleanText(line.orderId || line.postingNumber || "");
      const accountId = cleanText(line.accountId || "");
      const accountName = cleanText(line.accountName || "");
      const lineKey = cleanText(line.key) || `ready-to-ship|${marketplace}|${orderId || Date.now()}|${offerId}`;
      if (!offerId) { failed.push({ key: lineKey, reason: "missing_offer_id" }); continue; }
      if (state.processed?.[lineKey]) { failed.push({ key: lineKey, offerId, reason: "already_committed" }); continue; }

      const product = findSupplierCartWarehouseProduct(warehouse, { offerId, marketplace, accountId });
      if (!product) { failed.push({ key: lineKey, offerId, reason: "product_not_found" }); continue; }

      const normalizedLine = normalizeSupplierCartLine({ marketplace, accountId, orderId, offerId, quantity, productName: cleanText(line.productName || "") });
      const row = await resolveSupplierCartRow(warehouse, normalizedLine, state);
      if (!row.ready) { failed.push({ key: lineKey, offerId, reason: row.skipReason || "no_supplier" }); continue; }

      cartRows.push(normalizeSupplierCartPreviewRow({
        ...row,
        key: lineKey,
        marketplace,
        accountId: accountId || marketplace,
        accountName: accountName || marketplace,
        orderId: orderId || `mp-${Date.now()}`,
        quantity,
        warehouseProductId: product.id,
        groupKey: warehouseProductPageGroupKey(product),
        groupOfferId: product.offerId,
        available: true,
        ready: true,
      }));
    }

    if (!cartRows.length) {
      return response.status(400).json({ ok: false, error: "No orders could be resolved.", failed });
    }

    const result = await insertSupplierCartRowsIntoPriceMaster(cartRows, request);
    response.json({
      ok: true,
      inserted: result.inserted.length,
      failed: failed.length,
      failedDetails: failed,
      docIds: result.docIds,
      pickingCreated: result.pickingCreated?.length || 0,
    });
  } catch (error) {
    next(error);
  }
});

// Re-send marketplace shipment confirmations for alreadyCommitted draft rows.
// Safe to call multiple times — Ozon/Yandex handle duplicate ship confirmations gracefully.
// Only sends for rows ordered today (soldAt within last 48h) to avoid mass-confirming old orders.
app.post("/api/supplier-cart/reconfirm-marketplace", requireAdmin, async (request, response, next) => {
  try {
    const state = await readSupplierCartState();
    const draftRows = state.draft?.rows || [];
    const cutoff = new Date(Date.now() - 48 * 60 * 60 * 1000);
    const isRecent = (row) => !row.soldAt || new Date(row.soldAt) >= cutoff;
    const toConfirm = draftRows.filter(
      (row) => row.alreadyCommitted && row.postingNumber && row.ozonProductId && row.marketplace === "ozon" && isRecent(row),
    );
    const toConfirmYandex = draftRows.filter(
      (row) => row.alreadyCommitted && row.orderId && row.campaignId && row.marketplace === "yandex" && isRecent(row),
    );

    const allToConfirm = [...toConfirm, ...toConfirmYandex];
    if (!allToConfirm.length) return response.json({ ok: true, confirmed: 0, skipped: 0, results: [] });

    const results = await confirmMarketplaceOrdersAfterInsert(allToConfirm);
    logger.info("marketplace reconfirm manual", { ozon: toConfirm.length, yandex: toConfirmYandex.length });
    response.json({ ok: true, confirmed: allToConfirm.length, skipped: 0, results });
  } catch (error) {
    next(error);
  }
});

app.get("/api/supplier-cart/history", requireAdmin, async (_request, response, next) => {
  try {
    const state = await readSupplierCartState();
    response.json({
      ok: true,
      updatedAt: state.updatedAt,
      totalProcessed: Object.keys(state.processed || {}).length,
      history: (state.history || []).slice().reverse(),
    });
  } catch (error) {
    next(error);
  }
});

