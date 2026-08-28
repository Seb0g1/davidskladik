"use strict";

function publicYandexCleanupAuditEntry(entry = {}) {
  const details = entry.details || {};
  return {
    at: entry.at || null,
    user: entry.user || "system",
    planned: Number(details.planned || 0),
    plannedNow: Number(details.plannedNow || 0),
    skippedByLimit: Number(details.skippedByLimit || 0),
    deleted: Number(details.deleted || 0),
    failed: Number(details.failed || 0),
    notDeleted: Number(details.notDeleted || 0),
    protectedBrands: Array.isArray(details.protectedBrands) ? details.protectedBrands : [],
    failedOfferIds: Array.isArray(details.failedOfferIds) ? details.failedOfferIds : [],
    summary: details.summary || {},
  };
}

function publicYandexImportAuditEntry(entry = {}) {
  const details = entry.details || {};
  return {
    at: entry.at || null,
    user: entry.user || "system",
    planned: Number(details.planned || 0),
    sent: Number(details.sent || 0),
    failed: Number(details.failed || 0),
    priceSent: Number(details.priceSent || 0),
    priceFailed: Number(details.priceFailed || 0),
    stockSent: Number(details.stockSent || 0),
    stockFailed: Number(details.stockFailed || 0),
    skippedExisting: Number(details.skippedExisting || 0),
    skippedBlocked: Number(details.skippedBlocked || 0),
    skippedMissing: Number(details.skippedMissing || 0),
    warnings: Array.isArray(details.warnings) ? details.warnings : [],
    failedOfferIds: Array.isArray(details.failedOfferIds) ? details.failedOfferIds : [],
    targets: Array.isArray(details.targets) ? details.targets : [],
  };
}
