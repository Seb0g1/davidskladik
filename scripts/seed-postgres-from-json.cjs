#!/usr/bin/env node
"use strict";

require("dotenv").config();

const fs = require("node:fs/promises");
const fsSync = require("node:fs");
const path = require("node:path");
const readline = require("node:readline");
const crypto = require("node:crypto");

const rootDir = path.join(__dirname, "..");
const dataDir = path.join(rootDir, "data");

function argEnabled(name) {
  return process.argv.includes(name);
}

function argValue(name, fallback = null) {
  const prefix = `${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

const options = {
  dryRun: argEnabled("--dry-run"),
  skipWarehouse: argEnabled("--skip-warehouse"),
  skipSnapshot: argEnabled("--skip-snapshot"),
  replaceLinks: !argEnabled("--keep-existing-links"),
  limitProducts: Number(argValue("--limit-products", 0)) || 0,
  limitSnapshot: Number(argValue("--limit-snapshot", 0)) || 0,
};

function cleanText(value) {
  return String(value ?? "").trim();
}

function toDate(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function toInt(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? Math.round(number) : null;
}

function jsonOrNull(value) {
  return value === undefined ? null : value;
}

function marketplace(value) {
  return cleanText(value).toLowerCase() === "yandex" ? "yandex" : "ozon";
}

function currency(value) {
  return cleanText(value).toUpperCase() === "RUB" ? "RUB" : "USD";
}

function newId() {
  return crypto.randomUUID();
}

function stableId(prefix, value) {
  return `${prefix}_${crypto.createHash("sha1").update(String(value || "")).digest("hex")}`;
}

async function readJson(filename, fallback) {
  try {
    return JSON.parse(await fs.readFile(path.join(dataDir, filename), "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return fallback;
    throw error;
  }
}

async function createManyInBatches(model, rows, batchSize = 1000) {
  let count = 0;
  for (let index = 0; index < rows.length; index += batchSize) {
    const batch = rows.slice(index, index + batchSize);
    if (!batch.length) continue;
    const result = await model.createMany({ data: batch, skipDuplicates: true });
    count += result.count || 0;
  }
  return count;
}

function normalizeUserForPostgres(user = {}) {
  const username = cleanText(user.username);
  if (!username) return null;
  return {
    username,
    passwordHash: cleanText(user.passwordHash || user.password),
    role: cleanText(user.role).toLowerCase() === "admin" ? "admin" : "manager",
    active: !user.disabled && user.active !== false,
    source: cleanText(user.source) || "json",
    protected: Boolean(user.protected),
    createdAt: toDate(user.createdAt) || new Date(),
    updatedAt: toDate(user.updatedAt) || new Date(),
  };
}

function normalizeSupplierForPostgres(supplier = {}) {
  const name = cleanText(supplier.name || supplier.partnerName || supplier.supplierName);
  const partnerId = cleanText(supplier.partnerId || supplier.id);
  if (!name && !partnerId) return null;
  return {
    partnerId: partnerId || null,
    name: name || partnerId,
    active: supplier.active !== false && supplier.disabled !== true && supplier.stopped !== true,
    defaultCurrency: currency(supplier.defaultCurrency || supplier.priceCurrency || supplier.currency),
    stopReason: cleanText(supplier.stopReason || supplier.reason) || null,
    note: cleanText(supplier.note || supplier.comment) || null,
    raw: jsonOrNull(supplier),
    createdAt: toDate(supplier.createdAt) || new Date(),
    updatedAt: toDate(supplier.updatedAt) || new Date(),
  };
}

function normalizeProductForPostgres(product = {}) {
  const id = cleanText(product.id);
  if (!id) return null;
  const name = cleanText(product.name || product.title || product.productName || product.offerName || product.offerId);
  return {
    id,
    marketplace: marketplace(product.marketplace),
    target: cleanText(product.target || product.accountId || product.account) || null,
    offerId: cleanText(product.offerId || product.offer_id || product.sku || product.article),
    productId: cleanText(product.productId || product.product_id) || null,
    name: name || id,
    brand: cleanText(product.brand || product.brandName || product.ozon?.brand) || null,
    images: jsonOrNull(product.images || product.image ? { images: product.images || [], image: product.image || null } : null),
    marketplaceState: jsonOrNull(product.marketplaceState || product.state || null),
    currentPrice: toInt(product.currentPrice || product.marketplacePrice || product.price),
    targetPrice: toInt(product.nextPrice || product.targetPrice),
    targetStock: toInt(product.targetStock),
    status: cleanText(product.status || product.marketplaceStatus || product.state?.status) || null,
    archived: Boolean(product.archived || product.isArchived),
    raw: jsonOrNull(product),
    createdAt: toDate(product.createdAt) || new Date(),
    updatedAt: toDate(product.updatedAt) || new Date(),
  };
}

function normalizeLinkForPostgres(product, link = {}) {
  const productId = cleanText(product?.id);
  const supplierArticle = cleanText(link.article || link.supplierArticle || link.offerId);
  if (!productId || !supplierArticle) return null;
  return {
    id: cleanText(link.id) || stableId("link", [
      productId,
      supplierArticle,
      cleanText(link.partnerId || link.partnerID),
      cleanText(link.supplierName || link.partnerName || link.name),
      cleanText(link.keyword),
      currency(link.priceCurrency || link.currency),
    ].join("|")),
    productId,
    supplierArticle,
    supplierName: cleanText(link.supplierName || link.partnerName || link.name) || null,
    partnerId: cleanText(link.partnerId || link.partnerID) || null,
    priceCurrency: currency(link.priceCurrency || link.currency),
    keyword: cleanText(link.keyword) || null,
    raw: jsonOrNull(link),
    createdAt: toDate(link.createdAt) || new Date(),
    updatedAt: toDate(link.updatedAt) || new Date(),
  };
}

function normalizeRetryItemForPostgres(item = {}) {
  const offerId = cleanText(item.offerId || item.offer_id);
  if (!offerId) return null;
  return {
    queueKey: cleanText(item.queueKey) || `${cleanText(item.id || item.productId)}:${cleanText(item.target || item.marketplace || "ozon")}`,
    marketplace: marketplace(item.marketplace),
    target: cleanText(item.target) || null,
    productId: cleanText(item.id || item.productId) || null,
    offerId,
    price: toInt(item.price) || 0,
    oldPrice: toInt(item.oldPrice),
    status: item.nextRetryAt ? "delayed" : "pending",
    attempts: toInt(item.attempts) || 0,
    error: cleanText(item.error) || null,
    payload: jsonOrNull(item),
    nextRetryAt: toDate(item.nextRetryAt),
    lastAttemptAt: toDate(item.lastAttemptAt),
    createdAt: toDate(item.queuedAt || item.createdAt) || new Date(),
    updatedAt: new Date(),
  };
}

function normalizeSnapshotItemForPostgres(row = {}) {
  const article = cleanText(row.article || row.NativeID || row.nativeId);
  if (!article) return null;
  return {
    id: stableId("pm", [
      article,
      cleanText(row.partnerId || row.PartnerID),
      cleanText(row.rowId || row.RowID),
      cleanText(row.name || row.nativeName || row.NativeName),
      cleanText(row.docDate || row.DocDate),
    ].join("|")),
    rowId: cleanText(row.rowId || row.RowID) || null,
    article,
    partnerId: cleanText(row.partnerId || row.PartnerID) || null,
    partnerName: cleanText(row.partnerName || row.PartnerName) || null,
    nativeName: cleanText(row.name || row.nativeName || row.NativeName) || null,
    price: row.price === undefined && row.NativePrice === undefined ? null : String(row.price ?? row.NativePrice),
    currency: currency(row.currency || row.priceCurrency),
    docDate: toDate(row.docDate || row.DocDate),
    active: row.active !== false && row.Active !== false && row.Active !== 0,
    raw: jsonOrNull(row),
    updatedAt: new Date(),
  };
}

async function seedUsers(prisma) {
  const payload = await readJson("app-users.json", { users: [] });
  const users = (Array.isArray(payload.users) ? payload.users : [])
    .map(normalizeUserForPostgres)
    .filter(Boolean);
  for (const user of users) {
    await prisma.appUser.upsert({
      where: { username: user.username },
      create: user,
      update: {
        passwordHash: user.passwordHash,
        role: user.role,
        active: user.active,
        source: user.source,
        protected: user.protected,
        updatedAt: user.updatedAt,
      },
    });
  }
  return users.length;
}

async function seedSettings(prisma) {
  const settings = await readJson("app-settings.json", null);
  if (!settings) return 0;
  await prisma.appSetting.upsert({
    where: { key: "app" },
    create: { key: "app", value: settings },
    update: { value: settings },
  });
  return 1;
}

async function seedWarehouse(prisma) {
  if (options.skipWarehouse) return { products: 0, links: 0, suppliers: 0 };
  const warehouse = await readJson("personal-warehouse.json", { products: [], suppliers: [] });
  const sourceProducts = Array.isArray(warehouse.products) ? warehouse.products : [];
  const products = (options.limitProducts ? sourceProducts.slice(0, options.limitProducts) : sourceProducts)
    .map(normalizeProductForPostgres)
    .filter(Boolean);
  const suppliers = (Array.isArray(warehouse.suppliers) ? warehouse.suppliers : [])
    .map(normalizeSupplierForPostgres)
    .filter(Boolean);
  const links = [];
  const productById = new Map(sourceProducts.map((product) => [cleanText(product.id), product]));
  for (const product of products) {
    const rawProduct = productById.get(product.id) || {};
    for (const link of Array.isArray(rawProduct.links) ? rawProduct.links : []) {
      const normalized = normalizeLinkForPostgres(rawProduct, link);
      if (normalized) links.push(normalized);
    }
  }

  for (const supplier of suppliers) {
    if (supplier.partnerId) {
      await prisma.managedSupplier.upsert({
        where: { partnerId: supplier.partnerId },
        create: supplier,
        update: {
          name: supplier.name,
          active: supplier.active,
          defaultCurrency: supplier.defaultCurrency,
          stopReason: supplier.stopReason,
          note: supplier.note,
          raw: supplier.raw,
        },
      });
      continue;
    }
    const existing = await prisma.managedSupplier.findFirst({ where: { name: supplier.name, partnerId: null } });
    if (existing) await prisma.managedSupplier.update({ where: { id: existing.id }, data: supplier });
    else await prisma.managedSupplier.create({ data: supplier });
  }

  let productCount = 0;
  for (const product of products) {
    await prisma.warehouseProduct.upsert({
      where: { id: product.id },
      create: product,
      update: {
        marketplace: product.marketplace,
        target: product.target,
        offerId: product.offerId,
        productId: product.productId,
        name: product.name,
        brand: product.brand,
        images: product.images,
        marketplaceState: product.marketplaceState,
        currentPrice: product.currentPrice,
        targetPrice: product.targetPrice,
        targetStock: product.targetStock,
        status: product.status,
        archived: product.archived,
        raw: product.raw,
        updatedAt: product.updatedAt,
      },
    });
    productCount += 1;
  }

  if (options.replaceLinks && links.length) {
    await prisma.productLink.deleteMany({ where: { productId: { in: products.map((product) => product.id) } } });
  }
  const linkCount = await createManyInBatches(prisma.productLink, links, 1000);
  return { products: productCount, links: linkCount, suppliers: suppliers.length };
}

async function seedRetryQueue(prisma) {
  const payload = await readJson("price-retry-queue.json", { items: [] });
  const items = (Array.isArray(payload.items) ? payload.items : [])
    .map(normalizeRetryItemForPostgres)
    .filter(Boolean);
  for (const item of items) {
    await prisma.priceRetryQueueItem.upsert({
      where: { queueKey: item.queueKey },
      create: item,
      update: {
        marketplace: item.marketplace,
        target: item.target,
        productId: item.productId,
        offerId: item.offerId,
        price: item.price,
        oldPrice: item.oldPrice,
        status: item.status,
        attempts: item.attempts,
        error: item.error,
        payload: item.payload,
        nextRetryAt: item.nextRetryAt,
        lastAttemptAt: item.lastAttemptAt,
      },
    });
  }
  return items.length;
}

async function seedAudit(prisma) {
  const auditPath = path.join(dataDir, "audit-log.jsonl");
  if (!fsSync.existsSync(auditPath)) return 0;
  const stream = fsSync.createReadStream(auditPath, "utf8");
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const rows = [];
  let lineIndex = 0;
  for await (const line of reader) {
    if (!line.trim()) continue;
    lineIndex += 1;
    const entry = JSON.parse(line);
    rows.push({
      id: stableId("audit", `${lineIndex}:${line}`),
      username: cleanText(entry.user || entry.username) || "system",
      action: cleanText(entry.action) || "unknown",
      entityType: cleanText(entry.entityType || entry.details?.entityType) || null,
      entityId: cleanText(entry.entityId || entry.productId || entry.details?.productId) || null,
      oldValue: jsonOrNull(entry.oldValue ?? entry.before ?? null),
      newValue: jsonOrNull(entry.newValue ?? entry.after ?? null),
      details: jsonOrNull(entry.details || entry),
      createdAt: toDate(entry.at || entry.createdAt) || new Date(),
    });
  }
  return createManyInBatches(prisma.auditLog, rows, 1000);
}

async function seedSnapshot(prisma) {
  if (options.skipSnapshot) return 0;
  const snapshot = await readJson("snapshot.json", { items: {} });
  const rows = Object.values(snapshot.items || {});
  const sourceRows = options.limitSnapshot ? rows.slice(0, options.limitSnapshot) : rows;
  const normalized = sourceRows.map(normalizeSnapshotItemForPostgres).filter(Boolean);
  return createManyInBatches(prisma.priceMasterSnapshotItem, normalized, 2000);
}

async function run() {
  if (!process.env.DATABASE_URL) {
    console.log("DATABASE_URL is not set. JSON fallback remains active; nothing to seed.");
    return;
  }
  const { PrismaClient } = require("@prisma/client");
  const prisma = new PrismaClient();
  try {
    if (options.dryRun) {
      const users = await readJson("app-users.json", { users: [] });
      const warehouse = await readJson("personal-warehouse.json", { products: [], suppliers: [] });
      const retry = await readJson("price-retry-queue.json", { items: [] });
      console.log(JSON.stringify({
        dryRun: true,
        users: Array.isArray(users.users) ? users.users.length : 0,
        products: Array.isArray(warehouse.products) ? warehouse.products.length : 0,
        suppliers: Array.isArray(warehouse.suppliers) ? warehouse.suppliers.length : 0,
        retryItems: Array.isArray(retry.items) ? retry.items.length : 0,
      }, null, 2));
      return;
    }

    const result = {};
    result.users = await seedUsers(prisma);
    result.settings = await seedSettings(prisma);
    result.warehouse = await seedWarehouse(prisma);
    result.retryItems = await seedRetryQueue(prisma);
    result.auditLogs = await seedAudit(prisma);
    result.snapshotItems = await seedSnapshot(prisma);
    console.log(JSON.stringify({ ok: true, ...result }, null, 2));
  } finally {
    await prisma.$disconnect();
  }
}

if (require.main === module) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = {
  normalizeUserForPostgres,
  normalizeSupplierForPostgres,
  normalizeProductForPostgres,
  normalizeLinkForPostgres,
  normalizeRetryItemForPostgres,
  normalizeSnapshotItemForPostgres,
};
