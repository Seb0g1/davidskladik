async function runSupplierCartPreviewOperation(payload = {}) {
  const result = await buildSupplierCartPreview(payload);
  return { ...result, ok: result.warnings.length === 0 || result.rows.length > 0 };
}

async function runSupplierCartCommitOperation(payload = {}, request = null) {
  const sourceRows = Array.isArray(payload.rows) && payload.rows.length
    ? payload.rows
    : (await buildSupplierCartPreview(payload)).rows;
  const keys = new Set(Array.isArray(payload.keys) ? payload.keys.map(cleanText).filter(Boolean) : []);
  const rows = keys.size ? sourceRows.filter((row) => keys.has(cleanText(row.key))) : sourceRows;
  const result = await insertSupplierCartRowsIntoPriceMaster(rows, request);
  return {
    ok: true,
    inserted: result.inserted.length,
    skipped: result.skipped,
    docIds: result.docIds,
    pickingCreated: result.pickingCreated?.length || 0,
    verifiedInPriceMaster: Boolean(result.verification?.ok),
    verifiedRows: Number(result.verification?.verifiedRows || 0),
    priceMasterDb: result.verification?.db || "",
    rows: result.inserted,
    summary: `Supplier cart committed ${result.inserted.length}; skipped ${result.skipped}.`,
  };
}

function operationTitle(type = "") {
  const titles = {
    "yandex-import-send": "Ozon -> Yandex import",
    "yandex-stock-sync": "Ozon -> Yandex stock sync",
    "yandex-price-push": "Send new Yandex prices",
    "linked-supplier-recovery": "Restore linked marketplace cards",
    "ozon-linked-unarchive": "Restore linked Ozon autoarchive",
    "restore-archived-stock": "Restore archived stock",
    "yandex-card-quality-ai-drafts": "Yandex card quality AI drafts",
    "repair-pricemaster-group-links": "Полный ремонт привязок Ozon/Yandex",
    "marketplace-supplier-cart-preview": "Supplier cart preview",
    "marketplace-supplier-cart-commit": "Supplier cart commit",
    "ozon-unarchive-queue-process": "Process Ozon autoarchive queue",
    "sales-automation-run": "Run sales automation",
    "problem-products-repair": "Repair problem products",
    "brand-index-rebuild": "Rebuild brand index",
    "health-deep": "Deep health check",
  };
  return titles[type] || type || "Operation";
}

