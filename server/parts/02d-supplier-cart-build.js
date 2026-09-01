async function buildSupplierCartPreview(params = {}) {
  const appSettings = await readAppSettings();
  const settings = normalizeSupplierCartSettings(appSettings.supplierCart || {});
  const marketplace = cleanText(params.marketplace || "all").toLowerCase();
  const limit = Math.max(1, Math.min(9999, Number(params.limit || 500) || 500));
  const range = supplierCartRange(params, settings);
  const enabledMarketplaces = new Set(settings.marketplaces || ["ozon", "yandex"]);
  const lines = [];
  const warnings = [];
  if ((marketplace === "all" || marketplace === "ozon") && enabledMarketplaces.has("ozon")) {
    try {
      lines.push(...await fetchOzonSupplierCartLines({
        ...range,
        limit: Math.max(1, limit - lines.length),
        statuses: settings.includeOzonStatuses,
      }));
    } catch (error) {
      warnings.push({ marketplace: "ozon", error: error?.message || String(error) });
    }
  }
  if ((marketplace === "all" || marketplace === "yandex") && enabledMarketplaces.has("yandex")) {
    try {
      lines.push(...await fetchYandexSupplierCartLines({
        ...range,
        limit,
        statuses: settings.includeYandexStatuses,
        substatuses: settings.includeYandexSubstatuses,
      }));
    } catch (error) {
      warnings.push({ marketplace: "yandex", error: error?.message || String(error) });
    }
  }
  if ((marketplace === "all" || marketplace === "wb") && enabledMarketplaces.has("wb")) {
    try {
      lines.push(...await fetchWbSupplierCartLines({
        ...range,
        limit,
      }));
    } catch (error) {
      warnings.push({ marketplace: "wb", error: error?.message || String(error) });
    }
  }
  const uniqueLines = Array.from(new Map(lines.map((line) => [line.key, line])).values()).slice(0, limit);
  const warehouse = await hydrateSupplierCartWarehouse(
    await readWarehouse(),
    uniqueLines.map((line) => line.offerId),
  );
  const state = await readSupplierCartState();
  // Build a set of order keys that are already assembled (picking status="picked") so the
  // preview can mark them as committed — otherwise the cart keeps re-queuing assembled orders
  // that Ozon still shows as awaiting_packaging until the courier API call completes.
  const pickingStateForPreview = await readSupplierPickingState().catch(() => ({ rows: {} }));
  const pickingRows = Object.values(pickingStateForPreview.rows || {}).map(normalizeSupplierPickingRow);
  const pickedKeys = new Set(
    pickingRows
      .filter((pr) => pr.status === "picked")
      .map((pr) => pr.key),
  );
  // Build a set of offerIds already covered by active manual PM commits (marketplace="manual",
  // status="open" or "picked"). When a user manually orders an article before the autocart
  // runs, the resulting picking row has key "manual|pm|..." which never matches the
  // marketplace order key — so the cart would create a second PM order to a different
  // supplier. Index by offerId so resolveSupplierCartRow can detect this overlap.
  const manualActiveOfferIds = new Set(
    pickingRows
      .filter((pr) => pr.marketplace === "manual" && (pr.status === "open" || pr.status === "picked"))
      .map((pr) => cleanText(pr.offerId).toLowerCase())
      .filter(Boolean),
  );
  // Pre-fetch all PM matches in one batch to avoid N+1 MySQL queries per order row.
  const preloadedUsdRate = Number((await getUsdRate()).rate || process.env.DEFAULT_USD_RATE || 95);
  const allWarehouseLinks = (warehouse.products || []).flatMap((p) => p.links || []);
  const preloadedMatches = allWarehouseLinks.length
    ? await getBatchPriceMasterMatchesForLinks(allWarehouseLinks, warehouse.suppliers || [], preloadedUsdRate, { timeoutMs: 8000 }).catch(() => null)
    : null;
  const rows = [];
  const RESOLVE_BATCH = 20;
  for (let batchStart = 0; batchStart < uniqueLines.length; batchStart += RESOLVE_BATCH) {
    const batch = uniqueLines.slice(batchStart, batchStart + RESOLVE_BATCH);
    for (const line of batch) {
      try {
        rows.push(await resolveSupplierCartRow(warehouse, line, state, { preloadedMatches, preloadedUsdRate, pickedKeys, manualActiveOfferIds }));
      } catch (error) {
        rows.push(normalizeSupplierCartPreviewRow({
          ...line,
          ready: false,
          skipReason: `pricemaster_error: ${error?.message || String(error)}`,
          alreadyCommitted: Boolean(state.processed?.[line.key]) || pickedKeys.has(line.key),
          requestDocId: state.processed?.[line.key]?.requestDocId,
          requestRowId: state.processed?.[line.key]?.requestRowId,
        }));
      }
    }
    // Yield the event loop between batches so GC can reclaim PM query results
    if (batchStart + RESOLVE_BATCH < uniqueLines.length) {
      await new Promise((resolve) => setImmediate(resolve));
    }
  }
  const ready = rows.filter((row) => row.ready && !row.alreadyCommitted).length;
  const alreadyCommitted = rows.filter((row) => row.alreadyCommitted).length;
  const skipped = rows.length - ready - alreadyCommitted;
  return {
    ok: true,
    draftId: cleanText(params.draftId),
    mode: settings.mode,
    from: range.from.toISOString(),
    to: range.to.toISOString(),
    limit,
    rows,
    warnings,
    total: rows.length,
    ready,
    skipped,
    alreadyCommitted,
    summary: `Supplier cart preview: ${rows.length} rows; ready ${ready}; already ${alreadyCommitted}; skipped ${skipped}.`,
  };
}

async function generateSupplierCartDraft(params = {}, request = null) {
  const preview = await buildSupplierCartPreview(params);
  const state = await readSupplierCartState();
  const draft = {
    id: crypto.randomUUID(),
    generatedAt: new Date().toISOString(),
    generatedBy: requestUsername(request),
    params: {
      marketplace: cleanText(params.marketplace || "all"),
      from: params.from || preview.from,
      to: params.to || preview.to,
      limit: preview.limit,
    },
    rows: preview.rows || [],
    summary: {
      total: preview.total,
      ready: preview.ready,
      skipped: preview.skipped,
      alreadyCommitted: preview.alreadyCommitted,
      warnings: preview.warnings || [],
    },
  };
  state.draft = draft;
  await writeSupplierCartState(state);
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.draft_generate", {
    entityType: "supplier_cart",
    entityId: draft.id,
    newValue: draft.summary,
  }).catch((error) => logger.warn("supplier cart draft audit failed", { detail: error?.message || String(error) }));
  return { ...preview, draftId: draft.id, generatedAt: draft.generatedAt, generatedBy: draft.generatedBy };
}

function supplierCartAutomationPublic() {
  return {
    autoRunning: supplierCartAutoRunning,
    lastAutoRunAt: supplierCartAutoLastRunAt,
    nextAutoRunAt: supplierCartAutoNextRunAt,
    lastAutoResult: supplierCartAutoLastResult,
  };
}

function nextMoscowScheduleTimeIso(times = ["09:30", "12:00", "15:00"], now = new Date()) {
  const schedule = (Array.isArray(times) ? times : [])
    .map(normalizeSupplierOrderCutoff)
    .filter(Boolean)
    .sort();
  const source = schedule.length ? schedule : ["09:30", "12:00", "15:00"];
  const moscowNow = new Date(now.getTime() + 3 * 60 * 60 * 1000);
  for (let day = 0; day < 8; day += 1) {
    const base = new Date(Date.UTC(moscowNow.getUTCFullYear(), moscowNow.getUTCMonth(), moscowNow.getUTCDate() + day, 0, 0, 0, 0));
    for (const time of source) {
      const [hours, minutes] = time.split(":").map((item) => Number(item) || 0);
      const moscowCandidate = new Date(base);
      moscowCandidate.setUTCHours(hours, minutes, 0, 0);
      const utcCandidate = new Date(moscowCandidate.getTime() - 3 * 60 * 60 * 1000);
      if (utcCandidate.getTime() > now.getTime() + 30_000) return utcCandidate.toISOString();
    }
  }
  return new Date(now.getTime() + 30 * 60 * 1000).toISOString();
}

async function processSupplierCartAutoGenerate({ source = "scheduler" } = {}) {
  if (supplierCartAutoRunning) return { ok: true, skipped: true, reason: "already_running", ...supplierCartAutomationPublic() };
  supplierCartAutoRunning = true;
  supplierCartAutoLastRunAt = new Date().toISOString();
  try {
    const settings = normalizeSupplierCartSettings((await readAppSettings()).supplierCart || {});
    if (settings.enabled === false || settings.autoEnabled === false) {
      supplierCartAutoLastResult = { ok: true, skipped: true, reason: "disabled", at: new Date().toISOString() };
      return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
    }
    const systemRequest = { session: { username: "system", role: "admin" } };
    const result = await generateSupplierCartDraft({ marketplace: "all", limit: Number(process.env.SUPPLIER_CART_AUTO_LIMIT || 500) || 500 }, systemRequest);
    supplierCartAutoLastResult = {
      ok: true,
      source,
      mode: "draft",
      draftId: result.draftId,
      total: result.total,
      ready: result.ready,
      skipped: result.skipped,
      alreadyCommitted: result.alreadyCommitted,
      at: new Date().toISOString(),
    };
    logger.info("supplier cart draft generated (manual commit)", supplierCartAutoLastResult);
    return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
  } catch (error) {
    supplierCartAutoLastResult = { ok: false, source, error: error?.message || String(error), at: new Date().toISOString() };
    logger.warn("supplier cart auto draft failed", { detail: error?.message || String(error) });
    return { ...supplierCartAutoLastResult, ...supplierCartAutomationPublic() };
  } finally {
    supplierCartAutoRunning = false;
  }
}

async function scheduleSupplierCartAuto(delayMs = null) {
  if (supplierCartAutoTimer) clearTimeout(supplierCartAutoTimer);
  const settings = normalizeSupplierCartSettings((await readAppSettings().catch(() => defaultAppSettings())).supplierCart || {});
  if (settings.enabled === false || settings.autoEnabled === false) {
    supplierCartAutoNextRunAt = null;
    return;
  }
  const nextIso = delayMs === null
    ? nextMoscowScheduleTimeIso(settings.scheduleTimes)
    : new Date(Date.now() + Math.max(30_000, Number(delayMs) || 30_000)).toISOString();
  supplierCartAutoNextRunAt = nextIso;
  supplierCartAutoTimer = setTimeout(async () => {
    supplierCartAutoTimer = null;
    await processSupplierCartAutoGenerate({ source: "scheduler" });
    await scheduleSupplierCartAuto();
  }, Math.max(30_000, new Date(nextIso).getTime() - Date.now()));
}


