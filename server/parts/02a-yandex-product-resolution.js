function extractBrandHintFromProductName(name = "") {
  const text = cleanText(name);
  if (!text) return "";
  const beforeDigits = text.split(/\d/)[0].trim();
  const tokens = beforeDigits.match(/[A-Za-zА-Яа-я][A-Za-zА-Яа-я'&.-]+/g) || [];
  const stop = new Set(["парфюмерная", "вода", "духи", "edp", "edt", "п/у", "туалетная", "парфюм", "spray", "tester"]);
  const brandTokens = [];
  for (const token of tokens) {
    const lower = token.toLowerCase();
    if (stop.has(lower)) break;
    brandTokens.push(token);
    if (brandTokens.length >= 3) break;
  }
  return brandTokens.join(" ").trim();
}

function resolveYandexVendorFromProduct(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const yandex = normalized.yandex || {};
  return cleanText(
    yandex.vendor
    || ozon.vendor
    || normalized.brand
    || resolveWarehouseBrand(normalized)
    || resolveWarehouseBrand({ ...normalized, ozon, yandex })
    || extractBrandHintFromProductName(yandex.name || normalized.name || ozon.name),
  );
}

function resolveYandexWeightDimensionsFromProduct(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  const ozon = normalized.ozon || {};
  const extra = parseJsonField(normalized.yandex?.extra, {});
  const cached = extra.weightDimensions && typeof extra.weightDimensions === "object" ? extra.weightDimensions : null;
  if (cached && Number(cached.length) > 0 && Number(cached.width) > 0 && Number(cached.height) > 0 && Number(cached.weight) > 0) {
    return {
      length: Number(cached.length),
      width: Number(cached.width),
      height: Number(cached.height),
      weight: Number(cached.weight),
    };
  }

  const dimsMm = [
    Number(ozon.depth || 0),
    Number(ozon.width || 0),
    Number(ozon.height || 0),
  ].filter((value) => Number.isFinite(value) && value > 0);
  const weightG = Number(ozon.weight || 0);
  if (dimsMm.length === 3 && weightG > 0) {
    const sorted = dimsMm.sort((a, b) => b - a);
    return {
      length: Number((sorted[0] / 10).toFixed(2)),
      width: Number((sorted[1] / 10).toFixed(2)),
      height: Number((sorted[2] / 10).toFixed(2)),
      weight: Number((weightG / 1000).toFixed(3)),
    };
  }

  const volumes = extractOzonYandexImportVolumesMl(
    [normalized.name, ozon.name, normalized.offerId].filter(Boolean).join(" "),
  );
  const volumeMl = volumes.length ? Math.max(...volumes) : 100;
  const heightCm = Math.max(6, Math.min(22, Number((volumeMl / 10).toFixed(1))));
  const weightKg = Math.max(0.12, Math.min(1.5, Number((volumeMl * 0.004 + 0.08).toFixed(3))));
  return {
    length: 12,
    width: 8,
    height: heightCm,
    weight: weightKg,
  };
}

function isWeakYandexWarehouseCard(product = {}) {
  const normalized = normalizeWarehouseProduct(product);
  if (cleanText(normalized.marketplace).toLowerCase() !== "yandex") return false;
  const built = buildYandexOfferMapping(normalized);
  const pictures = Array.isArray(built.offer?.pictures) ? built.offer.pictures : [];
  const vendor = resolveYandexVendorFromProduct(normalized);
  const dims = built.offer?.weightDimensions || {};
  const missingPhoto = !pictures.length;
  const missingBrand = !vendor || vendor.toLowerCase() === "без бренда";
  const missingDims = !(Number(dims.length) > 0 && Number(dims.width) > 0 && Number(dims.height) > 0 && Number(dims.weight) > 0);
  return missingPhoto || missingBrand || missingDims;
}

function yandexOfferRepairSignature(built = {}) {
  const offer = built.offer || {};
  return JSON.stringify({
    vendor: cleanText(offer.vendor),
    pictures: Array.isArray(offer.pictures) ? offer.pictures : [],
    weightDimensions: offer.weightDimensions || null,
    name: cleanText(offer.name),
    description: cleanText(offer.description),
  });
}

function patchYandexWarehouseProductFromOzonDonor(yandexProduct = {}, ozonDonor = {}) {
  const yandex = normalizeWarehouseProduct(yandexProduct);
  const ozon = normalizeWarehouseProduct(ozonDonor);
  const pictures = Array.from(new Set([
    ...splitList(yandex.yandex?.pictures),
    cleanText(yandex.imageUrl),
    cleanText(ozon.imageUrl),
    ...splitList(ozon.ozon?.primaryImage),
    ...splitList(ozon.ozon?.images),
  ].filter(Boolean)));
  const mergedForVendor = normalizeWarehouseProduct({
    ...yandex,
    ozon: { ...(yandex.ozon || {}), ...(ozon.ozon || {}) },
    name: yandex.name || ozon.name,
  });
  const vendor = resolveYandexVendorFromProduct(mergedForVendor);
  const description = cleanText(
    yandex.yandex?.description
    || ozon.ozon?.description
    || ozon.name
    || yandex.name,
  );
  const weightDimensions = resolveYandexWeightDimensionsFromProduct(
    normalizeWarehouseProduct({ ...yandex, ozon: ozon.ozon || ozon, name: yandex.name || ozon.name }),
  );
  const now = new Date().toISOString();
  return normalizeWarehouseProduct({
    ...yandex,
    brand: vendor || yandex.brand,
    imageUrl: pictures[0] || yandex.imageUrl,
    yandex: {
      ...(yandex.yandex || {}),
      vendor,
      pictures,
      description: description || yandex.yandex?.description,
      name: cleanText(yandex.yandex?.name || yandex.name || ozon.name || ozon.ozon?.name),
      extra: {
        ...parseJsonField(yandex.yandex?.extra, {}),
        weightDimensions,
        repairedFromOzonAt: now,
        repairedFromOzonId: ozon.id || "",
      },
    },
    updatedAt: now,
  });
}

function buildOzonDonorIndexes(ozonProducts = []) {
  const byId = new Map();
  const byOfferId = new Map();
  for (const product of Array.isArray(ozonProducts) ? ozonProducts : []) {
    const normalized = normalizeWarehouseProduct(product);
    if (cleanText(normalized.marketplace).toLowerCase() !== "ozon" || !normalized.id) continue;
    byId.set(String(normalized.id), normalized);
    const offerId = cleanText(normalized.offerId).toLowerCase();
    if (offerId && !byOfferId.has(offerId)) byOfferId.set(offerId, normalized);
  }
  return { byId, byOfferId };
}

function resolveOzonDonorForYandexProduct(yandexProduct = {}, indexes = {}) {
  const yandex = normalizeWarehouseProduct(yandexProduct);
  const pairOzonId = resolveWarehouseProductPairOzonId(yandex);
  if (pairOzonId && indexes.byId?.has(pairOzonId)) return indexes.byId.get(pairOzonId);
  const offerId = cleanText(yandex.offerId).toLowerCase();
  if (offerId && indexes.byOfferId?.has(offerId)) return indexes.byOfferId.get(offerId);
  return null;
}

async function repairWeakYandexCardsFromOzonPostgres(prisma, {
  dryRun = true,
  limit = 0,
  pushToYandex = false,
  batchSize = 50,
  offerIds = [],
  forcePush = false,
} = {}) {
  if (!prisma) throw new Error("Postgres prisma client is required");
  const maxItems = Math.max(1, Math.min(50000, Number(limit || 0) || 50000));
  const [yandexRows, ozonRows] = await Promise.all([
    prisma.warehouseProduct.findMany({
      where: { AND: [enabledWarehouseTargetWhere(), { marketplace: "yandex" }] },
      include: { links: true },
      take: maxItems,
    }),
    prisma.warehouseProduct.findMany({
      where: { AND: [enabledWarehouseTargetWhere(), { marketplace: "ozon" }] },
      include: { links: true },
      take: 50000,
    }),
  ]);
  const offerIdFilter = new Set(
    (Array.isArray(offerIds) ? offerIds : [])
      .map((value) => cleanText(value).toLowerCase())
      .filter(Boolean),
  );
  const yandexProducts = yandexRows
    .map(productFromPostgres)
    .filter((product) => !offerIdFilter.size || offerIdFilter.has(cleanText(product.offerId).toLowerCase()));
  const ozonProducts = ozonRows.map(productFromPostgres);
  const indexes = buildOzonDonorIndexes(ozonProducts);
  const candidates = yandexProducts.filter((product) => (
    forcePush || offerIdFilter.size || isWeakYandexWarehouseCard(product)
  ));
  const changedProducts = [];
  const pushOnlyProducts = [];
  const pushResults = [];
  const skipped = [];

  for (const yandexProduct of candidates) {
    const smallVolumeCheck = assessYandexSmallVolume(yandexProduct);
    if (smallVolumeCheck.blocked) {
      skipped.push({
        id: yandexProduct.id,
        offerId: yandexProduct.offerId,
        reason: "small_volume_blocked",
        minVolumeMl: smallVolumeCheck.minVolumeMl,
      });
      continue;
    }
    let donor = resolveOzonDonorForYandexProduct(yandexProduct, indexes);
    if (!donor) {
      skipped.push({ id: yandexProduct.id, offerId: yandexProduct.offerId, reason: "ozon_donor_not_found" });
      continue;
    }
    const donorSmallVolumeCheck = assessYandexSmallVolume(donor);
    if (donorSmallVolumeCheck.blocked) {
      skipped.push({
        id: yandexProduct.id,
        offerId: yandexProduct.offerId,
        reason: "donor_small_volume_blocked",
        minVolumeMl: donorSmallVolumeCheck.minVolumeMl,
      });
      continue;
    }
    if (ozonDonorNeedsApiRefresh(donor)) {
      try {
        donor = await refreshOzonWarehouseDonorFromApi(donor);
        if (!dryRun) {
          await upsertWarehouseProductPostgres(prisma, donor);
          if (Array.isArray(donor.links) && donor.links.length) {
            await replaceProductLinksInPostgres(prisma, donor.id, donor.links);
          }
        }
        indexes.byId.set(String(donor.id), donor);
        const offerKey = cleanText(donor.offerId).toLowerCase();
        if (offerKey) indexes.byOfferId.set(offerKey, donor);
      } catch (error) {
        logger.warn("yandex repair ozon donor refresh failed", {
          offerId: yandexProduct.offerId,
          donorId: donor.id,
          detail: error?.message || String(error),
        });
      }
    }
    const beforeBuilt = buildYandexOfferMapping(normalizeWarehouseProduct(yandexProduct));
    const patched = patchYandexWarehouseProductFromOzonDonor(yandexProduct, donor);
    const afterBuilt = buildYandexOfferMapping(patched);
    if (yandexOfferRepairSignature(beforeBuilt) === yandexOfferRepairSignature(afterBuilt)) {
      if (forcePush || offerIdFilter.size) {
        pushOnlyProducts.push(patched);
        continue;
      }
      skipped.push({ id: yandexProduct.id, offerId: yandexProduct.offerId, reason: "no_changes" });
      continue;
    }
    changedProducts.push(patched);
  }

  const productsToPersist = changedProducts;
  const productsToPush = [...changedProducts, ...pushOnlyProducts];
  if (!dryRun && productsToPersist.length) {
    for (const chunk of chunkArray(productsToPersist, Math.max(10, batchSize))) {
      await runWithLimitedConcurrency(chunk, 5, async (product) => {
        await upsertWarehouseProductPostgres(prisma, product);
        if (Array.isArray(product.links) && product.links.length) {
          await replaceProductLinksInPostgres(prisma, product.id, product.links);
        }
      });
    }
  }
  if (!dryRun && pushToYandex && productsToPush.length) {
    for (const product of productsToPush) {
      pushResults.push(await sendApprovedYandexProductCardRepair(product));
    }
  }

  return {
    ok: true,
    dryRun,
    pushToYandex,
    forcePush,
    offerIds: Array.from(offerIdFilter),
    scannedYandex: yandexProducts.length,
    weakCandidates: candidates.length,
    changed: changedProducts.length,
    pushOnly: pushOnlyProducts.length,
    skipped: skipped.length,
    pushed: pushResults.filter((item) => item?.ok).length,
    pushFailed: pushResults.filter((item) => item && !item.ok).length,
    sample: changedProducts.slice(0, 10).map((product) => ({
      id: product.id,
      offerId: product.offerId,
      vendor: product.yandex?.vendor || "",
      pictures: (product.yandex?.pictures || []).length,
      imageUrl: product.imageUrl || "",
    })),
    skippedSample: skipped.slice(0, 10),
    pushSample: pushResults.slice(0, 10),
  };
}

