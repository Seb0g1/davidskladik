#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

process.env.DISABLE_BACKGROUND_JOBS = process.env.DISABLE_BACKGROUND_JOBS || "true";

const originalStdoutWrite = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, encoding, callback) => {
  const text = Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk);
  if (text.startsWith("◇ injected env")) {
    if (typeof callback === "function") callback();
    return true;
  }
  return originalStdoutWrite(chunk, encoding, callback);
};

const {
  collectHealthDetails,
  readWarehouse,
  readPriceRetryQueue,
  readPriceHistory,
  readAuditFiltered,
  isWeakOzonWarehouseProduct,
  pickWeakOzonProductIds,
} = require("../server.js");
const { getPrisma } = require("../lib/postgres.js");

function parseArgs(argv) {
  const args = new Set(argv);
  const valueOf = (name, fallback) => {
    const prefix = `${name}=`;
    const match = argv.find((item) => item.startsWith(prefix));
    return match ? match.slice(prefix.length) : fallback;
  };
  return {
    json: args.has("--json"),
    pretty: args.has("--pretty"),
    deep: args.has("--deep"),
    weakLimit: Math.max(1, Math.min(200, Number(valueOf("--weak-limit", 20)) || 20)),
    logLines: Math.max(0, Math.min(500, Number(valueOf("--log-lines", 120)) || 120)),
  };
}

function countBy(items, keyFn) {
  const result = {};
  for (const item of items || []) {
    const key = String(keyFn(item) || "unknown");
    result[key] = (result[key] || 0) + 1;
  }
  return Object.fromEntries(Object.entries(result).sort(([a], [b]) => a.localeCompare(b)));
}

function formatNumber(value) {
  return new Intl.NumberFormat("ru-RU").format(Number(value) || 0);
}

function productLinks(product) {
  return Array.isArray(product?.links) ? product.links : [];
}

function productMarketplaces(product) {
  if (Array.isArray(product?.marketplaces) && product.marketplaces.length) {
    return product.marketplaces.map((item) => String(item?.marketplace || item?.target || "").toLowerCase()).filter(Boolean);
  }
  const marketplace = String(product?.marketplace || product?.target || "").toLowerCase();
  return marketplace ? [marketplace] : [];
}

function productName(product) {
  return product?.name || product?.title || product?.raw?.name || product?.raw?.title || null;
}

function hasProductName(product) {
  const name = productName(product);
  return Boolean(name && name !== "Товар Ozon");
}

function hasProductImage(product) {
  if (Array.isArray(product?.images) && product.images.length) return true;
  if (product?.image || product?.imageUrl || product?.primaryImage) return true;
  const raw = product?.raw || {};
  if (Array.isArray(raw.images) && raw.images.length) return true;
  if (raw.image || raw.imageUrl || raw.primaryImage || raw.primary_image) return true;
  return false;
}

function ozonState(product) {
  const state = product?.marketplaceState || product?.raw?.marketplaceState || product?.raw?.state || {};
  return String(state.code || state.state || state.status || product?.status || "unknown").toLowerCase() || "unknown";
}

function linkMatchType(link) {
  if (link?.matchType) return String(link.matchType);
  if (link?.sourceRowId || link?.rowId) return "selected_row";
  if (link?.exactName || link?.nativeName) return "name";
  if (link?.supplierArticle || link?.article) return "article";
  return "unknown";
}

function queueKey(item) {
  return `${item?.productId || item?.id || ""}:${item?.target || item?.marketplace || ""}:${item?.offerId || ""}`;
}

function summarizeQueue(items) {
  const queueItems = Array.isArray(items) ? items : [];
  const active = queueItems.filter((item) => !["success", "retried"].includes(String(item.status || "").toLowerCase()));
  const delayed = active
    .filter((item) => String(item.status || "").toLowerCase() === "delayed" && item.nextRetryAt)
    .sort((a, b) => new Date(a.nextRetryAt).getTime() - new Date(b.nextRetryAt).getTime());
  const errors = active
    .filter((item) => item.error || item.retryReason)
    .slice(0, 12)
    .map((item) => ({
      key: queueKey(item),
      status: item.status || "pending",
      attempts: item.attempts || 0,
      nextRetryAt: item.nextRetryAt || null,
      reason: item.retryReason || null,
      error: item.error || null,
    }));
  return {
    total: queueItems.length,
    active: active.length,
    byStatus: countBy(queueItems, (item) => item.status || "pending"),
    nextDelayedRetryAt: delayed[0]?.nextRetryAt || null,
    delayedDueCount: delayed.filter((item) => new Date(item.nextRetryAt).getTime() <= Date.now()).length,
    sampleErrors: errors,
  };
}

function summarizePriceHistory(history) {
  const items = Array.isArray(history?.items) ? history.items : [];
  const recentErrors = items
    .filter((item) => item.error || String(item.status || "").toLowerCase() === "failed")
    .slice(0, 12)
    .map((item) => ({
      productId: item.productId || null,
      offerId: item.offerId || null,
      status: item.status || null,
      oldPrice: item.oldPrice ?? null,
      newPrice: item.newPrice ?? null,
      error: item.error || null,
      at: item.at || item.createdAt || null,
    }));
  return {
    source: history?.source || null,
    total: history?.total ?? items.length,
    sampled: items.length,
    byStatus: countBy(items, (item) => item.status || "unknown"),
    lastSentAt: items.find((item) => String(item.status || "").toLowerCase() === "success")?.at || items[0]?.at || null,
    recentErrors,
  };
}

function summarizeAudit(entries) {
  const items = Array.isArray(entries) ? entries : [];
  return {
    sampled: items.length,
    byAction: countBy(items, (item) => item.action || "unknown"),
    recent: items.slice(0, 12).map((item) => ({
      at: item.createdAt || item.at || item.time || null,
      username: item.username || item.user || null,
      action: item.action || null,
      entityType: item.entityType || null,
      entityId: item.entityId || item.productId || null,
    })),
  };
}

function readTailLines(filePath, maxLines) {
  if (!maxLines || !filePath || !fs.existsSync(filePath)) return [];
  const text = fs.readFileSync(filePath, "utf8");
  return text.split(/\r?\n/).filter(Boolean).slice(-maxLines);
}

function parseRecentLogIssues(maxLines) {
  const candidates = [
    process.env.PM2_ERROR_LOG,
    path.join(os.homedir(), ".pm2", "logs", "davidsklad-error.log"),
    path.join(os.homedir(), ".pm2", "logs", "davidsklad-out.log"),
  ].filter(Boolean);
  const seen = new Set();
  const issues = [];
  for (const filePath of candidates) {
    if (seen.has(filePath)) continue;
    seen.add(filePath);
    for (const line of readTailLines(filePath, maxLines)) {
      if (!/(error|warn|failed|timeout|ECONNREFUSED|ResourceExhausted|Timed out|stalled)/i.test(line)) continue;
      let parsed = null;
      const jsonStart = line.indexOf("{");
      if (jsonStart >= 0) {
        try {
          parsed = JSON.parse(line.slice(jsonStart));
        } catch {
          parsed = null;
        }
      }
      issues.push(parsed ? {
        file: filePath,
        time: parsed.time || null,
        level: parsed.level || null,
        msg: parsed.msg || null,
        detail: parsed.detail || parsed.error || parsed.err || null,
      } : {
        file: filePath,
        line: line.slice(0, 500),
      });
    }
  }
  return issues.slice(-30);
}

function productSample(product) {
  return {
    id: product?.id || null,
    marketplace: product?.marketplace || null,
    target: product?.target || null,
    offerId: product?.offerId || null,
    productId: product?.productId || null,
    name: productName(product),
    links: productLinks(product).length,
    updatedAt: product?.updatedAt || null,
  };
}

function buildRecommendations(report) {
  const recommendations = [];
  if (report.health?.components?.storage?.ok === false || report.health?.components?.postgres?.ok === false) {
    recommendations.push("PostgreSQL is unhealthy or unavailable: check DATABASE_URL and Postgres service before syncing.");
  }
  if (report.health?.components?.redis?.ok === false && report.health?.components?.redis?.queueMode === "bullmq") {
    recommendations.push("BullMQ is enabled but Redis is unhealthy: start Redis or set BULLMQ_ENABLED=false.");
  }
  if (report.ozon.weakCards.total > 0) {
    recommendations.push(`Weak Ozon cards found: run npm run repair:ozon-details -- --limit=${Math.min(report.ozon.weakCards.total, 500)}.`);
  }
  if (report.priceRetryQueue.byStatus.failed > 0 || report.priceRetryQueue.byStatus.error > 0) {
    recommendations.push("Price retry queue has failed items: inspect errors, then retry selected items from UI or clear stale failures.");
  }
  if (report.priceRetryQueue.byStatus.delayed > 0) {
    recommendations.push("Some prices are delayed by Ozon limits; this is expected until nextRetryAt.");
  }
  if (report.links.productsWithoutLinks > report.links.productsWithLinks) {
    recommendations.push("Most products have no PriceMaster links; check data migration/snapshot before trusting auto prices.");
  }
  if (report.logs.recentIssues.length > 0) {
    recommendations.push("Recent warn/error log lines exist; see logs.recentIssues in this report.");
  }
  return recommendations;
}

async function buildReport(options) {
  const health = await collectHealthDetails({ deep: options.deep }).catch((error) => ({
    ok: false,
    error: error.message,
  }));
  const warehouse = await readWarehouse();
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  const links = products.flatMap((product) => productLinks(product).map((link) => ({ product, link })));
  const ozonProducts = products.filter((product) => productMarketplaces(product).includes("ozon"));
  const weakIds = pickWeakOzonProductIds(products, options.weakLimit);
  const productsById = new Map(products.map((product) => [product.id, product]));
  const queue = await readPriceRetryQueue().catch((error) => ({ items: [], error: error.message }));
  const history = await readPriceHistory({ limit: 100 }).catch((error) => ({ items: [], total: 0, error: error.message }));
  const audit = await readAuditFiltered({}, 50).catch(() => []);

  const report = {
    ok: health.ok !== false,
    generatedAt: new Date().toISOString(),
    env: {
      nodeEnv: process.env.NODE_ENV || null,
      dbMode: process.env.DB_MODE || null,
      jsonFallbackEnabled: process.env.JSON_FALLBACK_ENABLED || null,
      bullmqEnabled: process.env.BULLMQ_ENABLED || null,
    },
    health,
    warehouse: {
      source: warehouse.source || null,
      updatedAt: warehouse.updatedAt || null,
      products: products.length,
      suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
      byMarketplace: countBy(products.flatMap(productMarketplaces), (item) => item || "unknown"),
      withImages: products.filter(hasProductImage).length,
      withNames: products.filter(hasProductName).length,
      withoutNames: products.filter((product) => !hasProductName(product)).length,
      selectedSupplier: products.filter((product) => product.selectedSupplier || product.supplier).length,
    },
    links: {
      total: links.length,
      productsWithLinks: products.filter((product) => productLinks(product).length > 0).length,
      productsWithoutLinks: products.filter((product) => productLinks(product).length === 0).length,
      byMatchType: countBy(links, ({ link }) => linkMatchType(link)),
      recentProductsWithLinks: products
        .filter((product) => productLinks(product).length > 0)
        .sort((a, b) => new Date(b.updatedAt || 0).getTime() - new Date(a.updatedAt || 0).getTime())
        .slice(0, 10)
        .map(productSample),
    },
    ozon: {
      total: ozonProducts.length,
      byState: countBy(ozonProducts, ozonState),
      withProductId: ozonProducts.filter((product) => product.productId).length,
      withOfferId: ozonProducts.filter((product) => product.offerId).length,
      withImages: ozonProducts.filter(hasProductImage).length,
      withNames: ozonProducts.filter(hasProductName).length,
      weakCards: {
        total: products.filter(isWeakOzonWarehouseProduct).length,
        sample: weakIds.map((id) => productsById.get(id)).filter(Boolean).map(productSample),
      },
    },
    priceRetryQueue: summarizeQueue(queue.items || []),
    priceHistory: summarizePriceHistory(history),
    audit: summarizeAudit(audit),
    logs: {
      recentIssues: parseRecentLogIssues(options.logLines),
    },
  };
  report.recommendations = buildRecommendations(report);
  return report;
}

function renderText(report) {
  const lines = [];
  lines.push("Magic Vibes warehouse diagnostics");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Health: ${report.ok ? "OK" : "PROBLEM"} | DB mode: ${report.env.dbMode || "auto"} | BullMQ: ${report.env.bullmqEnabled || "false"}`);
  lines.push("");
  lines.push("Warehouse");
  lines.push(`  Products: ${formatNumber(report.warehouse.products)} | Suppliers: ${formatNumber(report.warehouse.suppliers)} | Updated: ${report.warehouse.updatedAt || "-"}`);
  lines.push(`  Names: ${formatNumber(report.warehouse.withNames)} ok, ${formatNumber(report.warehouse.withoutNames)} weak | Images: ${formatNumber(report.warehouse.withImages)} ok`);
  lines.push(`  Marketplaces: ${JSON.stringify(report.warehouse.byMarketplace)}`);
  lines.push("");
  lines.push("Links");
  lines.push(`  Links: ${formatNumber(report.links.total)} | Products with links: ${formatNumber(report.links.productsWithLinks)} | Without links: ${formatNumber(report.links.productsWithoutLinks)}`);
  lines.push(`  Match types: ${JSON.stringify(report.links.byMatchType)}`);
  lines.push("");
  lines.push("Ozon");
  lines.push(`  Products: ${formatNumber(report.ozon.total)} | With names: ${formatNumber(report.ozon.withNames)} | With images: ${formatNumber(report.ozon.withImages)} | Weak cards: ${formatNumber(report.ozon.weakCards.total)}`);
  lines.push(`  States: ${JSON.stringify(report.ozon.byState)}`);
  if (report.ozon.weakCards.sample.length) {
    lines.push("  Weak samples:");
    for (const product of report.ozon.weakCards.sample.slice(0, 10)) {
      lines.push(`    - ${product.offerId || product.id}: ${product.name || "-"} (${product.productId || "-"})`);
    }
  }
  lines.push("");
  lines.push("Prices");
  lines.push(`  Retry queue: ${formatNumber(report.priceRetryQueue.total)} total, ${formatNumber(report.priceRetryQueue.active)} active | statuses ${JSON.stringify(report.priceRetryQueue.byStatus)}`);
  lines.push(`  Next delayed retry: ${report.priceRetryQueue.nextDelayedRetryAt || "-"}`);
  lines.push(`  History: ${formatNumber(report.priceHistory.total)} total | sampled statuses ${JSON.stringify(report.priceHistory.byStatus)}`);
  lines.push("");
  lines.push("Audit");
  lines.push(`  Sampled: ${formatNumber(report.audit.sampled)} | actions ${JSON.stringify(report.audit.byAction)}`);
  lines.push("");
  lines.push("Recent Issues");
  if (!report.logs.recentIssues.length) {
    lines.push("  No recent warn/error lines found in known PM2 logs.");
  } else {
    for (const issue of report.logs.recentIssues.slice(-12)) {
      lines.push(`  - ${issue.time || ""} ${issue.level || ""} ${issue.msg || issue.line || ""}${issue.detail ? `: ${issue.detail}` : ""}`.trimEnd());
    }
  }
  lines.push("");
  lines.push("Recommendations");
  if (!report.recommendations.length) {
    lines.push("  No urgent recommendations.");
  } else {
    for (const item of report.recommendations) lines.push(`  - ${item}`);
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const report = await buildReport(options);
  if (options.json || options.pretty) {
    process.stdout.write(`${JSON.stringify(report, null, options.pretty ? 2 : 0)}\n`);
  } else {
    process.stdout.write(renderText(report));
  }
}

main()
  .catch((error) => {
    process.stderr.write(`${JSON.stringify({ ok: false, error: error.message, stack: error.stack }, null, 2)}\n`);
    process.exitCode = 1;
  })
  .finally(async () => {
    const prisma = getPrisma();
    if (prisma) await prisma.$disconnect().catch(() => {});
  });
