// Автосинк цен и остатков Wildberries (worker, каждые 3 часа).
//
// Пересчитывает цены с актуальным коэффициентом наценки (Настройки → Цены) и
// приводит остатки к правилу «итоговая цена WB ≤ maxWbPriceRub»: вписался —
// цена + defaultStock, вылез (например, после смены коэффициента) — остаток 0,
// товар снимается с продажи. Ручные карточки кабинета (не наши vendorCode)
// не трогаются.

const wbSyncEnabled = process.env.WB_SYNC_ENABLED !== "false";
const wbSyncIntervalHours = Math.max(1, Math.min(48, Number(process.env.WB_SYNC_INTERVAL_HOURS || 3) || 3));
let wbSyncTimer = null;
let wbSyncRunning = false;
let wbSyncNextRunAt = null;
let wbSyncLastResult = null;

async function runWbMarketplaceSync({ source = "auto" } = {}) {
  if (wbSyncRunning) return { status: "already_running" };
  const account = getWbAccountByTarget("wb");
  if (!account) return { status: "no_wb_account" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  const warehouseId = Number(account.campaignId || process.env.WB_WAREHOUSE_ID || 0) || 1048198;
  wbSyncRunning = true;
  const startedAt = Date.now();
  try {
    const rules = await readWbImportRules();
    const pricing = await loadAvitoPricingContext();
    const cards = (await wbCardsList(account)).filter((card) => Number(card.nmID) > 0);
    const linked = await loadWbLinkedOzonProducts(cards.map((card) => card.vendorCode));

    const priceItems = [];
    const stocks = [];
    let inStock = 0;
    let zeroed = 0;
    let skippedManual = 0;
    for (const card of cards) {
      const product = linked.get(cleanText(card.vendorCode).toLowerCase());
      if (!product) {
        skippedManual += 1;
        continue;
      }
      const purchaseRub = wbSupplierPurchaseRub(product.supplier, pricing);
      const priceRub = purchaseRub > 0 ? wbSupplierPriceRub(product.supplier, pricing) : 0;
      const sellable = wbCardSellable({ product, purchaseRub, priceRub, rules });
      if (sellable) priceItems.push({ nmID: card.nmID, price: priceRub, discount: 0 });
      const skus = (Array.isArray(card.sizes) ? card.sizes : []).flatMap((size) => (Array.isArray(size.skus) ? size.skus : []));
      if (!skus.length) continue;
      const amount = sellable ? rules.defaultStock : 0;
      if (sellable) inStock += 1;
      else zeroed += 1;
      for (const sku of skus) stocks.push({ sku, amount });
    }

    const pricesResult = priceItems.length ? await wbSetPrices(account, priceItems) : { ok: true, sent: 0, tasks: [] };
    const stocksResult = stocks.length ? await wbUpdateStocks(account, warehouseId, stocks) : { ok: true, sent: 0 };

    // Дозабор описаний с Ozon: разовый enrich берёт максимум ~300 описаний
    // (лимит Ozon product/info), на ~10k карточек нужен не один прогон —
    // каждый тик синка добирает следующую порцию, пока не останется нечего.
    let enrichSummary = null;
    const enrichDescriptionsBudget = Math.max(0, Math.min(1000, Number(process.env.WB_SYNC_ENRICH_DESCRIPTIONS ?? 300) || 0));
    if (enrichDescriptionsBudget > 0) {
      try {
        const enrichResult = await enrichWbCards(account, { fetchDescriptions: enrichDescriptionsBudget });
        enrichSummary = {
          updated: enrichResult.updated,
          descriptionsFetched: enrichResult.descriptionsFetched,
          alreadyComplete: enrichResult.alreadyComplete,
        };
      } catch (error) {
        enrichSummary = { error: error?.message || String(error) };
        logger.warn("wb sync enrich failed", { detail: enrichSummary.error });
      }
    }

    const result = {
      status: "ok",
      source,
      warehouseId,
      cards: cards.length,
      pricesSent: priceItems.length,
      inStock,
      zeroed,
      skippedManual,
      maxWbPriceRub: rules.maxWbPriceRub,
      minSupplierPriceRub: rules.minSupplierPriceRub,
      tasks: pricesResult.tasks || [],
      stocksSent: stocksResult.sent,
      enrich: enrichSummary,
      elapsedMs: Date.now() - startedAt,
      at: new Date().toISOString(),
    };
    wbSyncLastResult = result;
    logger.info("wb_sync_complete", result);
    return result;
  } catch (error) {
    const result = { status: "error", source, error: error?.message || String(error), at: new Date().toISOString() };
    wbSyncLastResult = result;
    logger.warn("wb sync failed", { detail: result.error });
    return result;
  } finally {
    wbSyncRunning = false;
  }
}

function scheduleWbSync(delayMs = null) {
  if (!wbSyncEnabled) {
    wbSyncNextRunAt = null;
    return;
  }
  if (wbSyncTimer) clearTimeout(wbSyncTimer);
  const intervalMs = wbSyncIntervalHours * 60 * 60 * 1000;
  const normalizedDelay = Math.max(60_000, Number(delayMs ?? intervalMs) || intervalMs);
  wbSyncNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  wbSyncTimer = setTimeout(async () => {
    try {
      if (heavyBackgroundWorkShouldDefer("wb_sync")) {
        logger.info("wb sync deferred under load");
        scheduleWbSync(15 * 60 * 1000);
        return;
      }
      await runWbMarketplaceSync({ source: "schedule" });
    } catch (error) {
      logger.warn("wb sync tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleWbSync(intervalMs);
    }
  }, normalizedDelay);
  wbSyncTimer.unref?.();
}

// Статус и ручной запуск автосинка WB (выполняется в вызвавшем процессе).
app.get("/api/wb/sync/status", async (_request, response) => {
  response.json({
    enabled: wbSyncEnabled,
    intervalHours: wbSyncIntervalHours,
    nextRunAt: wbSyncNextRunAt,
    running: wbSyncRunning,
    lastResult: wbSyncLastResult,
  });
});

app.post("/api/wb/sync/run", requireAdmin, async (request, response, next) => {
  try {
    const result = await runWbMarketplaceSync({ source: "manual" });
    await appendAudit(request, "wb.sync.run", { newValue: { status: result.status, pricesSent: result.pricesSent, zeroed: result.zeroed } });
    response.json(result);
  } catch (error) {
    next(error);
  }
});
