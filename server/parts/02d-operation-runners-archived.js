function productLooksArchived(product = {}) {
  const state = product.marketplaceState || {};
  const code = cleanText(state.code || product.status).toLowerCase();
  const visibility = cleanText(state.visibility || product.visibility).toUpperCase();
  return Boolean(
    product.archived
      || state.archived
      || code === "archived"
      || visibility === "ARCHIVED"
      || product.noSupplierAutomation?.archivedAt
  );
}

function pickArchivedStockRestoreCandidates(products = [], { marketplace = "all", limit = 30000 } = {}) {
  const marketplaceFilter = cleanText(marketplace || "all").toLowerCase();
  const max = Math.max(1, Math.min(50000, Math.round(Number(limit || 30000) || 30000)));
  return (Array.isArray(products) ? products : [])
    .filter((product) => {
      const productMarketplace = cleanText(product.marketplace).toLowerCase();
      if (!["ozon", "yandex"].includes(productMarketplace)) return false;
      if (marketplaceFilter !== "all" && productMarketplace !== marketplaceFilter) return false;
      if (!cleanText(product.offerId || product.offer_id)) return false;
      if (productMarketplace === "ozon" && !Number(product.productId || product.product_id || 0)) return false;
      // Skip products that were zeroed by no-supplier automation and not explicitly archived by us.
      // Restoring them would only generate unfulfillable orders while manualSellableAt blocks re-zeroing for 48h.
      if (
        Array.isArray(product.links) && product.links.length > 0
        && product.noSupplierAutomation?.stockZeroAt
        && !product.noSupplierAutomation?.archivedAt
      ) return false;
      return productLooksArchived(product);
    })
    .slice(0, max);
}

async function applyArchivedStockRestoreLocalPatch(warehouse, targetProducts, stockActions, unarchiveActions, stock, now = new Date().toISOString()) {
  const restoredStockIds = new Set((Array.isArray(stockActions) ? stockActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const unarchivedIds = new Set((Array.isArray(unarchiveActions) ? unarchiveActions : []).filter((item) => item.ok).map((item) => String(item.id)));
  const stockActionById = new Map((Array.isArray(stockActions) ? stockActions : []).map((item) => [String(item.id), item]));
  const unarchiveActionById = new Map((Array.isArray(unarchiveActions) ? unarchiveActions : []).map((item) => [String(item.id), item]));
  const touchedIds = new Set((Array.isArray(targetProducts) ? targetProducts : []).map((product) => String(product.id)));
  const changedProducts = [];
  for (const product of warehouse.products || []) {
    if (!touchedIds.has(String(product.id))) continue;
    const stockAction = stockActionById.get(String(product.id));
    const unarchiveAction = unarchiveActionById.get(String(product.id));
    if (stockAction) product.lastStockSend = marketplaceCommandFromAction(stockAction, product, now);
    if (unarchiveAction) product.lastArchiveSend = marketplaceCommandFromAction(unarchiveAction, product, now);
    product.targetStock = stock;
    product.noSupplierAutomation = product.noSupplierAutomation || {};
    product.noSupplierAutomation.stockZeroAt = null;
    product.noSupplierAutomation.archivedAt = null;
    product.noSupplierAutomation.recoveredAt = now;
    product.noSupplierAutomation.manualSellableAt = now;
    product.noSupplierAutomation.lastError = null;
    if (restoredStockIds.has(String(product.id)) || unarchivedIds.has(String(product.id))) {
      product.marketplaceState = {
        ...(product.marketplaceState || {}),
        code: "active",
        status: "active",
        archived: false,
        stock,
      };
      product.status = "active";
      product.archived = false;
    }
    product.updatedAt = now;
    changedProducts.push(product);
  }
  if (changedProducts.length) {
    await writeWarehouseProductPatch(changedProducts, { reason: "archived_stock_restore", writeLinks: false });
  }
  return changedProducts.length;
}

// For Dalik-style suppliers that store multiple different products under the same article
// code, a previously pinned (selected_row) link may point to the wrong PM row. This
// operation scans all selected_row Dalik links, runs Jaccard disambiguation against the
// warehouse product name, and updates links that point to the wrong row. Safe to run with
// dryRun=true (default) first to preview changes.
async function runRepairDalikDisambiguationLinksOperation(payload = {}, options = {}) {
  const dryRun = payload.dryRun !== false;
  const targetArticle = cleanText(payload.article || "");

  if (!shouldUsePostgresStorage()) return { ok: false, error: "Postgres storage required" };
  const prisma = getPrisma();
  if (!prisma) return { ok: false, error: "Prisma client unavailable" };

  const dalikRows = await prisma.$queryRawUnsafe(
    `SELECT pl.id AS link_id, pl.product_id, pl.supplier_article AS article,
            pl.supplier_name, pl.partner_id, pl.raw AS link_raw,
            wp.raw AS product_raw, wp.offer_id
     FROM product_links pl
     JOIN warehouse_products wp ON wp.id = pl.product_id
     WHERE pl.raw->>'matchType' = 'selected_row'
       AND (pl.supplier_name ILIKE '%алик%' OR pl.partner_id IN (
             SELECT DISTINCT partner_id FROM pm_snapshot_items
             WHERE partner_name ILIKE '%алик%' LIMIT 5))
     ${targetArticle ? "AND pl.supplier_article = $1" : ""}
     LIMIT 2000`,
    ...(targetArticle ? [targetArticle] : []),
  );

  const candidates = [];
  const changedItems = [];

  for (const row of dalikRows) {
    const linkRaw = typeof row.link_raw === "string" ? JSON.parse(row.link_raw) : (row.link_raw || {});
    const productRaw = typeof row.product_raw === "string" ? JSON.parse(row.product_raw) : (row.product_raw || {});
    const article = cleanText(row.article || "");
    const partnerId = cleanText(row.partner_id || "");
    const currentExactName = cleanText(linkRaw.exactName || "");
    const currentSourceRowId = cleanText(String(linkRaw.sourceRowId || ""));
    if (!article || !currentSourceRowId) continue;

    const productName = cleanText(productRaw.name || row.offer_id || "");
    if (!productName) continue;

    const snapshotRows = await prisma.priceMasterSnapshotItem.findMany({
      where: { article, ...(partnerId ? { partnerId } : {}) },
      orderBy: [{ docDate: "desc" }],
      take: 50,
    });

    let bestName = "";
    let bestRowId = "";
    let bestArticle = "";

    // Case A: multiple rows → run Jaccard disambiguation
    const uniqueNames = new Set(snapshotRows.map((r) => cleanText(r.nativeName || "").toLowerCase()).filter(Boolean));
    if (snapshotRows.length > 1 && uniqueNames.size > 1) {
      const matchMap = new Map([["tmp", snapshotRows.map((r) => ({ name: r.nativeName, rowId: r.rowId }))]]);
      const disambiguated = disambiguateSupplierCartMatchesByOrderName(matchMap, productName);
      const bestRows = disambiguated.get("tmp") || [];
      if (bestRows.length) {
        bestName = cleanText(bestRows[0].name || "");
        bestRowId = cleanText(String(bestRows[0].rowId || ""));
      }
    }

    // Cases B/C/D — use in-memory PM snapshot to verify and fix stale links.
    // Loaded once and reused across all sub-checks.
    const storedResolvedName = cleanText(linkRaw.resolvedPriceMasterRow?.name || "");
    const rawArticle = cleanText(linkRaw.article || "");
    if (!bestName) {
      const pmIndexes = await getPriceMasterSnapshotIndexes();

      // Case B: resolvedPriceMasterRow.name ≠ exactName — names are inconsistent, find by exactName.
      const nameIsInconsistent = currentExactName && storedResolvedName && storedResolvedName.toLowerCase() !== currentExactName.toLowerCase();
      // Case C: raw.article ≠ supplier_article DB column — article column corrected but raw JSON lagging.
      const articleDbRawMismatch = rawArticle && article && rawArticle !== article;
      // Case D: sourceRowId is no longer under the stored article — supplier renumbered the product.
      const rowForCurrentArticle = (pmIndexes.byRowId.get(currentSourceRowId) || []);
      const rowStillUnderArticle = rowForCurrentArticle.some((r) => {
        const f = priceMasterSnapshotRowFields(r);
        return cleanText(f.article) === rawArticle && (!partnerId || cleanText(f.partnerId) === partnerId);
      });
      const articleMoved = currentExactName && rawArticle && !rowStillUnderArticle;

      if (nameIsInconsistent || articleDbRawMismatch || articleMoved) {
        // Search by exactName across all articles in the snapshot
        const byNameCandidates = (pmIndexes.byName.get(currentExactName.toLowerCase()) || [])
          .filter((r) => {
            const fields = priceMasterSnapshotRowFields(r);
            return (!partnerId || cleanText(fields.partnerId) === partnerId) && Boolean(fields.active) && Number(fields.price || 0) > 0;
          });
        if (byNameCandidates.length) {
          const best = byNameCandidates[0];
          const fields = priceMasterSnapshotRowFields(best);
          bestName = cleanText(fields.name || "");
          bestRowId = cleanText(String(fields.rowId || ""));
          bestArticle = cleanText(fields.article || "");
        } else if (articleDbRawMismatch) {
          // No PM row found but raw.article is stale — fix article in raw to match DB column
          bestName = currentExactName;
          bestRowId = currentSourceRowId;
          bestArticle = article;
        }
      }
    }

    if (!bestName) continue;
    const rawArticleFinal = rawArticle || article;
    const bestArticleFinal = bestArticle || rawArticleFinal;
    if (bestName.toLowerCase() === currentExactName.toLowerCase() && bestRowId === currentSourceRowId && bestArticleFinal === rawArticleFinal) continue;

    const candidate = { linkId: row.link_id, productId: row.product_id, offerId: row.offer_id, article: rawArticleFinal || article, newArticle: bestArticleFinal !== rawArticleFinal ? bestArticleFinal : undefined, currentExactName, currentSourceRowId, storedResolvedName, newExactName: bestName, newSourceRowId: bestRowId };
    candidates.push(candidate);

    if (!dryRun) {
      const updatedRaw = {
        ...linkRaw,
        ...(bestArticleFinal !== rawArticleFinal ? { article: bestArticleFinal } : {}),
        exactName: bestName,
        sourceRowId: bestRowId,
        resolvedPriceMasterRow: { name: bestName, rowId: bestRowId },
        updatedAt: new Date().toISOString(),
      };
      await prisma.productLink.update({
        where: { id: row.link_id },
        data: {
          raw: updatedRaw,
          sourceRowId: bestRowId || null,
          exactName: bestName || null,
          ...(bestArticleFinal !== article ? { supplierArticle: bestArticleFinal } : {}),
        },
      });
      changedItems.push(candidate);
    }
  }

  if (!dryRun && changedItems.length) {
    invalidateWarehouseViewCache();
    logger.info("repair dalik disambiguation links: cache invalidated", { changed: changedItems.length });
  }

  const result = {
    ok: true,
    dryRun,
    scanned: dalikRows.length,
    candidates: candidates.length,
    changed: changedItems.length,
    items: candidates.slice(0, 100),
    summary: dryRun
      ? `Сухой прогон: найдено ${candidates.length} привязок Далик с неверным PM-товаром из ${dalikRows.length} проверенных. Для применения запустите с dryRun=false.`
      : `Исправлено ${changedItems.length} из ${candidates.length} привязок Далик.`,
  };
  logger.info("repair dalik disambiguation links complete", { dryRun, scanned: dalikRows.length, candidates: candidates.length, changed: changedItems.length });
  return result;
}

// Finds Ozon products that have PM supplier links but whose FBS stock was never
// initialized in Ozon (targetStock = 0, never sent). These show as "не обновлён"
// in the Ozon cabinet. Pushes a default stock value to activate FBS for them.
async function runInitializeLinkedOzonStockOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const stock = Math.max(1, Math.min(9999, Math.round(Number(payload?.stock || 3) || 3)));
  // force=true: also processes products previously zeroed by no-supplier automation that now
  // have links again (supplier returned). Without force, those are skipped to avoid re-activating
  // products whose supplier genuinely disappeared.
  const force = payload?.force === true;
  const warehouse = await readWarehouse();
  const candidates = (warehouse.products || []).filter((product) => {
    if (cleanText(product.marketplace).toLowerCase() !== "ozon") return false;
    if (!cleanText(product.offerId || product.offer_id) || !cleanText(product.target)) return false;
    if (!product.hasLinks) return false;
    // Skip products recently zeroed by the no-supplier automation — they lost their supplier.
    // With force=true, skip only if ALSO archived (definitely inactive).
    if (!force && product.noSupplierAutomation?.stockZeroAt && !product.noSupplierAutomation?.archivedAt) return false;
    if (force && product.noSupplierAutomation?.archivedAt) return false;
    const currentStock = Math.max(0, Math.round(Number(product.marketplaceState?.stock || 0)));
    const currentTargetStock = Math.max(0, Math.round(Number(product.targetStock || 0)));
    // Only act on products with 0 stock and 0 target — if either > 0 they were already handled.
    return currentStock === 0 && currentTargetStock === 0;
  }).slice(0, limit);

  if (!candidates.length) {
    return {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      stock,
      sent: 0,
      errors: [],
      summary: "Нет товаров с привязками и нулевым остатком.",
    };
  }

  const targetProducts = candidates.map((product) => ({
    ...normalizeWarehouseProduct({ ...product, targetStock: stock }),
    _forceStock: true,
  }));
  const stockActions = await sendTargetStocksToMarketplace(targetProducts);
  const sent = stockActions.filter((a) => a.ok).length;
  const errors = stockActions.filter((a) => !a.ok).map((a) => `${a.id}: ${a.error || "failed"}`);

  // Patch warehouse: update targetStock for successfully sent products.
  const sentIds = new Set(stockActions.filter((a) => a.ok).map((a) => String(a.id)));
  if (sentIds.size) {
    const changedProducts = [];
    for (const product of warehouse.products || []) {
      if (!sentIds.has(String(product.id))) continue;
      product.targetStock = stock;
      product.noSupplierAutomation = product.noSupplierAutomation || {};
      product.noSupplierAutomation.stockZeroAt = null;
      product.noSupplierAutomation.recoveredAt = new Date().toISOString();
      product.updatedAt = new Date().toISOString();
      changedProducts.push(product);
    }
    if (changedProducts.length) {
      await writeWarehouseProductPatch(changedProducts, { reason: "initialize_linked_ozon_stock", writeLinks: false });
    }
  }

  const result = {
    ok: errors.length === 0,
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    stock,
    sent,
    errors,
    summary: `Товаров с нулевым остатком и привязкой: ${candidates.length}; отправлено остатков: ${sent}; ошибки: ${errors.length}.`,
  };
  logger.info("initialize linked ozon stock complete", { candidates: candidates.length, stock, sent, errors: errors.length });
  return result;
}

async function runArchivedStockRestoreOperation(payload = {}, options = {}) {
  const requestedLimit = Number(payload?.limit || 30000);
  const limit = Math.max(1, Math.min(50000, Number.isFinite(requestedLimit) ? Math.round(requestedLimit) : 30000));
  const stock = Math.max(1, Math.min(9999, Math.round(Number(payload?.stock || 3) || 3)));
  const requestedMarketplace = cleanText(payload?.marketplace || "yandex").toLowerCase();
  const marketplace = ["yandex", "ozon", "all"].includes(requestedMarketplace) ? requestedMarketplace : "yandex";
  const batchSize = Math.max(20, Math.min(300, Math.round(Number(payload?.batchSize || 100) || 100)));
  const reportProgress = async (progress, summary) => {
    if (typeof options.onProgress !== "function") return;
    await options.onProgress({
      progress: Math.max(5, Math.min(99, Math.round(Number(progress || 5) || 5))),
      summary,
    });
  };
  const warehouse = await readWarehouse();
  const candidates = pickArchivedStockRestoreCandidates(warehouse.products || [], { marketplace, limit });
  if (!candidates.length) {
    const result = {
      ok: true,
      scanned: Math.min(limit, (warehouse.products || []).length),
      candidates: 0,
      marketplace,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      errors: [],
      summary: "Архивных товаров для восстановления не найдено.",
    };
    logger.info("archived stock restore complete", {
      candidates: 0,
      stock,
      restoredStocks: 0,
      unarchived: 0,
      sellableRecovered: 0,
      stockFailed: 0,
      unarchiveFailed: 0,
      errors: 0,
    });
    return result;
  }

  const targetProducts = candidates.map((product) => normalizeWarehouseProduct({
    ...product,
    targetStock: stock,
    marketplaceState: {
      ...(product.marketplaceState || {}),
      stock,
    },
  }));
  await reportProgress(8, `Найдено архивных товаров: ${targetProducts.length}. Запускаю восстановление пачками по ${batchSize}.`);
  const firstStockActions = [];
  const unarchiveActions = [];
  const secondStockActions = [];
  let localPatched = 0;
  const batches = chunkArray(targetProducts, batchSize);
  for (let index = 0; index < batches.length; index += 1) {
    const batch = batches[index];
    const processedBefore = index * batchSize;
    const processedAfter = Math.min(targetProducts.length, processedBefore + batch.length);
    logger.info("archived stock restore batch started", {
      batch: index + 1,
      batches: batches.length,
      products: batch.length,
      processed: processedBefore,
      total: targetProducts.length,
    });
    const batchFirstStockActions = await restoreStocksOnMarketplaces(batch);
    firstStockActions.push(...batchFirstStockActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor(batch.length / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Восстанавливаю остатки: ${processedBefore + Math.floor(batch.length / 3)} из ${targetProducts.length}.`,
    );
    const batchUnarchiveActions = await verifyYandexUnarchiveActions(
      batch,
      await unarchiveProductsOnMarketplaces(batch),
    );
    unarchiveActions.push(...batchUnarchiveActions);
    await reportProgress(
      10 + ((processedBefore + Math.floor((batch.length * 2) / 3)) / Math.max(1, targetProducts.length)) * 80,
      `Разархивирую карточки: ${processedBefore + Math.floor((batch.length * 2) / 3)} из ${targetProducts.length}.`,
    );
    const batchSecondStockActions = await restoreStocksOnMarketplaces(batch);
    secondStockActions.push(...batchSecondStockActions);
    localPatched += await applyArchivedStockRestoreLocalPatch(
      warehouse,
      batch,
      [...batchFirstStockActions, ...batchSecondStockActions],
      batchUnarchiveActions,
      stock,
      new Date().toISOString(),
    );
    await reportProgress(
      10 + (processedAfter / Math.max(1, targetProducts.length)) * 80,
      `Обработано ${processedAfter} из ${targetProducts.length}.`,
    );
    logger.info("archived stock restore batch complete", {
      batch: index + 1,
      batches: batches.length,
      processed: processedAfter,
      total: targetProducts.length,
      localPatched,
    });
  }
  const stockActions = [...firstStockActions, ...secondStockActions];
  const productStatuses = summarizeSupplierRecoveryProducts(targetProducts, stockActions, unarchiveActions);

  const stockOkIds = new Set(stockActions.filter((item) => item.ok).map((item) => String(item.id)));
  const unarchiveOkIds = new Set(unarchiveActions.filter((item) => item.ok).map((item) => String(item.id)));
  const sellableRecovered = targetProducts.filter((product) => stockOkIds.has(String(product.id)) && unarchiveOkIds.has(String(product.id))).length;
  const errors = [...stockActions, ...unarchiveActions]
    .filter((item) => !item.ok)
    .map((item) => ({ id: item.id, offerId: item.offerId, type: item.type, target: item.target, error: item.error }));
  const restoredStocks = stockActions.filter((item) => item.ok).length;
  const unarchived = unarchiveActions.filter((item) => item.ok).length;
  const unarchivePending = unarchiveActions.filter((item) => item.pending).length;
  const stockFailed = stockActions.filter((item) => !item.ok).length;
  const unarchiveFailed = unarchiveActions.filter((item) => !item.ok).length;
  const result = {
    ok: errors.length === 0,
    partial: Boolean(errors.length && (restoredStocks || unarchived)),
    scanned: Math.min(limit, (warehouse.products || []).length),
    candidates: candidates.length,
    marketplace,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors,
    productStatuses,
    summary: `Архивных товаров ${candidates.length}; остаток ${stock}; отправок остатка ${restoredStocks}; разархивировано ${unarchived}; готово к продаже ${sellableRecovered}; ошибки остатков ${stockFailed}; ошибки разархива ${unarchiveFailed}.`,
  };
  logger.info("archived stock restore complete", {
    candidates: result.candidates,
    stock,
    restoredStocks,
    unarchived,
    unarchivePending,
    sellableRecovered,
    stockFailed,
    unarchiveFailed,
    errors: errors.length,
  });
  return result;
}

async function runScanAndFixZeroStockOperation(payload = {}) {
  const dryRun = payload.dryRun !== false;
  const batchLimit = Math.max(50, Math.min(5000, Math.round(Number(payload.limit || 2000) || 2000)));
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { ok: false, error: "postgres_disabled" };

  // Find linked, non-archived products whose marketplace stock is below their target.
  const rows = await prisma.$queryRawUnsafe(`
    SELECT p.id, p.target_stock
    FROM warehouse_products p
    WHERE p.archived = false
      AND ${duplicateNameSqlExclusion("p")}
      AND (
        EXISTS (SELECT 1 FROM product_links l WHERE l.product_id = p.id)
        OR (p.marketplace = 'yandex' AND jsonb_array_length(COALESCE(p.raw->'links', '[]'::jsonb)) > 0)
      )
      AND p.target_stock > 0
      AND COALESCE(NULLIF(p.raw -> 'marketplaceState' ->> 'stock', '')::numeric, 0) < p.target_stock
    ORDER BY p.updated_at ASC
    LIMIT ${batchLimit * 3}
  `);

  if (!rows.length) {
    return { ok: true, scanned: 0, candidates: 0, sent: 0, dryRun, summary: "Все остатки в норме — нет товаров с нулевым остатком." };
  }

  const candidateIds = rows.slice(0, batchLimit).map((r) => String(r.id));
  const builtProducts = await buildFreshWarehouseProducts(candidateIds, { livePriceMaster: false }).catch((error) => {
    logger.warn("scan-and-fix-zero-stock build failed", { detail: error?.message || String(error) });
    return [];
  });

  const now = new Date().toISOString();
  const products = builtProducts
    .filter((p) => p?.selectedSupplier && p.hasLinks && !p.hasSnoozedLinks && Number(p.targetStock) > 0)
    .map((p) => ({ ...p, targetStock: Math.round(Number(p.targetStock)), updatedAt: now }));

  if (!products.length) {
    return { ok: true, scanned: rows.length, candidates: 0, sent: 0, dryRun, summary: "Нет подходящих товаров для отправки остатков." };
  }

  if (dryRun) {
    return {
      ok: true,
      scanned: rows.length,
      candidates: products.length,
      sent: 0,
      dryRun: true,
      items: products.slice(0, 50).map((p) => ({ id: p.id, offerId: p.offerId, marketplace: p.marketplace, target: p.target, targetStock: p.targetStock })),
      summary: `[dry run] Найдено ${products.length} товаров с нулевым остатком. Запустите без dryRun для исправления.`,
    };
  }

  const actions = await restoreStocksOnMarketplaces(products).catch((error) => {
    logger.warn("scan-and-fix-zero-stock send failed", { detail: error?.message || String(error) });
    return [];
  });
  const sent = actions.filter((a) => a.ok).length;
  const errors = actions.filter((a) => !a.ok).map((a) => `${a.id}: ${a.error || "failed"}`);

  logger.info("scan_and_fix_zero_stock_complete", { scanned: rows.length, candidates: products.length, sent, errors: errors.length });
  return {
    ok: errors.length === 0,
    scanned: rows.length,
    candidates: products.length,
    sent,
    errors,
    dryRun: false,
    summary: `Товаров с нулевым остатком: ${products.length}; отправлено остатков: ${sent}; ошибки: ${errors.length}.`,
  };
}

