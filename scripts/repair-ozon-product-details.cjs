#!/usr/bin/env node
"use strict";

require("dotenv").config();

const { PrismaClient } = require("@prisma/client");

const dryRun = process.argv.includes("--dry-run");
const includeAll = process.argv.includes("--all");
const limitArg = process.argv.find((arg) => arg.startsWith("--limit="));
const limit = limitArg ? Math.max(1, Number(limitArg.split("=")[1]) || 0) : 0;
const chunkSize = 100;

function cleanText(value) {
  return String(value ?? "").trim();
}

function firstImageUrl(value) {
  if (Array.isArray(value)) return cleanText(value[0]);
  const text = cleanText(value);
  if (!text) return "";
  return text.split(/\r?\n|,/).map(cleanText).find(Boolean) || "";
}

function splitList(value) {
  if (Array.isArray(value)) return value.map(cleanText).filter(Boolean);
  return cleanText(value).split(/\r?\n|,/).map(cleanText).filter(Boolean);
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) chunks.push(items.slice(index, index + size));
  return chunks;
}

function isWeakProductName(name, offerId) {
  const current = cleanText(name);
  const article = cleanText(offerId);
  if (!current) return true;
  if (/^товар\s+ozon$/i.test(current)) return true;
  if (/^ozon\s+\d+$/i.test(current)) return true;
  if (article && current.toLowerCase() === article.toLowerCase()) return true;
  return /^[A-ZА-Я0-9._-]{4,}$/i.test(current) && !/\s/.test(current);
}

function rowNeedsRepair(row) {
  if (includeAll) return true;
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const ozon = raw.ozon && typeof raw.ozon === "object" && !Array.isArray(raw.ozon) ? raw.ozon : {};
  const images = Array.isArray(row.images) ? row.images : splitList(raw.imageUrl || raw.image || ozon.primaryImage || ozon.images);
  return isWeakProductName(row.name || raw.name || ozon.name, row.offerId || raw.offerId || ozon.offerId)
    || !firstImageUrl(images)
    || isWeakProductName(ozon.name, row.offerId || raw.offerId || ozon.offerId);
}

function ozonHeaders() {
  const clientId = cleanText(process.env.OZON_CLIENT_ID);
  const apiKey = cleanText(process.env.OZON_API_KEY);
  if (!clientId || !apiKey) throw new Error("OZON_CLIENT_ID and OZON_API_KEY are required");
  return {
    "Client-Id": clientId,
    "Api-Key": apiKey,
    "Content-Type": "application/json",
  };
}

async function ozonRequest(pathname, body) {
  const response = await fetch(`https://api-seller.ozon.ru${pathname}`, {
    method: "POST",
    headers: ozonHeaders(),
    body: JSON.stringify(body || {}),
  });
  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_error) {
    data = { raw: text };
  }
  if (!response.ok) {
    const message = data.message || data.error || text || `Ozon API ${response.status}`;
    throw new Error(message);
  }
  return data;
}

function rememberInfo(map, item) {
  const offerId = cleanText(item.offer_id || item.offerId);
  const productId = cleanText(item.product_id || item.productId || item.id);
  if (offerId) map.byOffer.set(offerId.toLowerCase(), item);
  if (productId) map.byProduct.set(productId, item);
}

async function loadInfoByOffer(offerIds) {
  const map = { byOffer: new Map(), byProduct: new Map() };
  let failedChunks = 0;
  const chunks = chunkArray(offerIds, chunkSize);
  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    try {
      const data = await ozonRequest("/v3/product/info/list", { offer_id: chunk });
      for (const item of data.items || data.result?.items || []) rememberInfo(map, item);
    } catch (error) {
      failedChunks += 1;
      console.warn(JSON.stringify({ level: "warn", step: "offer_info", chunk: index + 1, totalChunks: chunks.length, detail: error.message }));
    }
    console.log(JSON.stringify({ step: "offer_info", processed: Math.min(offerIds.length, (index + 1) * chunkSize), total: offerIds.length, loaded: map.byOffer.size }));
  }
  return { ...map, failedChunks };
}

async function loadInfoByProductId(productIds) {
  const map = { byOffer: new Map(), byProduct: new Map() };
  let failedChunks = 0;
  const chunks = chunkArray(productIds, chunkSize);
  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index].map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0);
    if (!chunk.length) continue;
    try {
      const data = await ozonRequest("/v3/product/info/list", { product_id: chunk });
      for (const item of data.items || data.result?.items || []) rememberInfo(map, item);
    } catch (error) {
      failedChunks += 1;
      console.warn(JSON.stringify({ level: "warn", step: "product_info", chunk: index + 1, totalChunks: chunks.length, detail: error.message }));
    }
    console.log(JSON.stringify({ step: "product_info", processed: Math.min(productIds.length, (index + 1) * chunkSize), total: productIds.length, loaded: map.byProduct.size }));
  }
  return { ...map, failedChunks };
}

function productUrlFromInfo(info, currentUrl = "") {
  const sku = info.sources?.find((source) => source.sku)?.sku || info.sku || info.fbo_sku || info.fbs_sku;
  return cleanText(info.product_url || info.url || currentUrl || (sku ? `https://www.ozon.ru/product/${encodeURIComponent(String(sku))}/` : ""));
}

function updateDataForRow(row, info) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  const ozon = raw.ozon && typeof raw.ozon === "object" && !Array.isArray(raw.ozon) ? raw.ozon : {};
  const name = cleanText(info.name || row.name || raw.name || ozon.name || row.offerId);
  const primaryImage = firstImageUrl(info.primary_image || info.primaryImage || info.images || info.images360 || info.color_image || row.images || raw.imageUrl || ozon.primaryImage);
  const images = splitList(info.images || row.images || raw.images || ozon.images);
  const sourceSku = info.sources?.find((source) => source.sku)?.sku;
  const sku = cleanText(raw.sku || info.sku || sourceSku || info.fbo_sku || info.fbs_sku);
  const productUrl = productUrlFromInfo(info, raw.productUrl);
  const nextOzon = {
    ...ozon,
    offerId: cleanText(info.offer_id || info.offerId || row.offerId || ozon.offerId),
    productId: cleanText(info.product_id || info.productId || row.productId || ozon.productId),
    name,
    vendor: cleanText(info.brand || info.vendor || ozon.vendor),
    description: cleanText(info.description || ozon.description),
    categoryId: info.description_category_id || info.category_id || ozon.categoryId,
    typeId: info.type_id || info.description_type_id || ozon.typeId,
    barcode: (info.barcodes || ozon.barcodes || [])[0] || ozon.barcode || "",
    barcodes: info.barcodes || ozon.barcodes || [],
    primaryImage,
    images: images.length ? images : (primaryImage ? [primaryImage] : []),
    images360: splitList(info.images360 || ozon.images360),
    colorImage: firstImageUrl(info.color_image || ozon.colorImage),
  };
  const nextRaw = {
    ...raw,
    name,
    imageUrl: primaryImage || raw.imageUrl,
    images: nextOzon.images,
    sku: sku || raw.sku,
    productUrl,
    ozon: nextOzon,
  };
  return {
    name,
    brand: cleanText(info.brand || info.vendor || row.brand || raw.brand || ozon.vendor) || null,
    images: nextOzon.images,
    raw: nextRaw,
  };
}

async function main() {
  const prisma = new PrismaClient();
  try {
    const rows = await prisma.warehouseProduct.findMany({
      where: { marketplace: "ozon" },
      select: {
        id: true,
        offerId: true,
        productId: true,
        name: true,
        brand: true,
        images: true,
        raw: true,
      },
      orderBy: { updatedAt: "desc" },
    });
    let candidates = rows.filter(rowNeedsRepair);
    if (limit) candidates = candidates.slice(0, limit);
    console.log(JSON.stringify({ ok: true, dryRun, candidates: candidates.length, totalOzon: rows.length }));
    if (!candidates.length) return;

    const offerIds = [...new Set(candidates.map((row) => cleanText(row.offerId)).filter(Boolean))];
    const byOffer = await loadInfoByOffer(offerIds);
    const missingProductIds = candidates
      .filter((row) => !byOffer.byOffer.get(cleanText(row.offerId).toLowerCase()))
      .map((row) => cleanText(row.productId))
      .filter(Boolean);
    const byProduct = missingProductIds.length ? await loadInfoByProductId([...new Set(missingProductIds)]) : { byOffer: new Map(), byProduct: new Map(), failedChunks: 0 };

    const updates = [];
    for (const row of candidates) {
      const info = byOffer.byOffer.get(cleanText(row.offerId).toLowerCase()) || byProduct.byProduct.get(cleanText(row.productId));
      if (!info) continue;
      const data = updateDataForRow(row, info);
      if (isWeakProductName(data.name, row.offerId) && !firstImageUrl(data.images)) continue;
      updates.push({ id: row.id, data });
    }

    console.log(JSON.stringify({
      ok: true,
      dryRun,
      loadedByOffer: byOffer.byOffer.size,
      loadedByProduct: byProduct.byProduct.size,
      updates: updates.length,
      failedOfferChunks: byOffer.failedChunks,
      failedProductChunks: byProduct.failedChunks,
    }));

    if (dryRun || !updates.length) return;
    for (const chunk of chunkArray(updates, 50)) {
      await prisma.$transaction(chunk.map((item) =>
        prisma.warehouseProduct.update({
          where: { id: item.id },
          data: {
            name: item.data.name,
            brand: item.data.brand,
            images: item.data.images,
            raw: item.data.raw,
          },
        }),
      ), { timeout: 30_000 });
      console.log(JSON.stringify({ step: "write", processed: Math.min(updates.length, updates.indexOf(chunk[chunk.length - 1]) + 1), total: updates.length }));
    }
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ ok: false, error: error.message || String(error) }));
  process.exitCode = 1;
});
