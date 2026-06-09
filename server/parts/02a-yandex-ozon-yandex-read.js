async function getYandexPriceMap(shop, offerIds) {
  const map = new Map();

  for (const chunk of chunkArray(offerIds, 200)) {
    const data = await yandexRequest(
      shop,
      "POST",
      `/v2/businesses/${shop.businessId}/offer-prices`,
      { offerIds: chunk },
    );

    for (const offer of data.result?.offers || data.offers || []) {
      const value = offer.price?.value ?? offer.basicPrice?.value ?? offer.price;
      map.set(offer.offerId || offer.offer_id, Number(value || 0));
    }
  }

  return map;
}

async function getYandexOfferIdSet(shop, offerIds) {
  const set = new Set();

  const mappings = await getYandexOfferMappingsByOfferIds(shop, offerIds);
  for (const item of mappings) {
    const offerId = yandexOfferIdFromMapping(item);
    if (offerId) set.add(offerId);
  }

  return set;
}

async function getExistingYandexOfferIdSet(offerIds = []) {
  const normalizedOfferIds = Array.from(new Set((offerIds || []).map(cleanText).filter(Boolean)));
  const existing = new Set();
  const shops = getYandexShops().filter((shop) => shop.apiKey && shop.businessId);
  if (!normalizedOfferIds.length || !shops.length) return existing;

  for (const shop of shops) {
    const shopExisting = await getYandexOfferIdSet(shop, normalizedOfferIds);
    for (const offerId of shopExisting) {
      const normalized = cleanText(offerId).toLowerCase();
      if (normalized) existing.add(normalized);
    }
  }

  return existing;
}

function uniqueYandexShopsByBusiness(shops = null) {
  const seenBusinesses = new Set();
  const source = Array.isArray(shops) && shops.length ? shops : getYandexShops();
  return source.filter((shop) => {
    if (!shop.apiKey || !shop.businessId) return false;
    const key = String(shop.businessId);
    if (seenBusinesses.has(key)) return false;
    seenBusinesses.add(key);
    return true;
  });
}

async function sendYandexOfferMappings(shop, offers = []) {
  const results = [];
  const deduped = new Map();
  for (const offer of Array.isArray(offers) ? offers : []) {
    const normalizedOfferId = cleanText(offer.offerId).toLowerCase();
    if (!normalizedOfferId) continue;
    const volumeCheck = assessYandexSmallVolume([
      offer.name,
      offer.description,
      offer.vendor,
    ].map(cleanText).filter(Boolean).join(" "));
    if (volumeCheck.blocked) {
      results.push({
        offerId: cleanText(offer.offerId),
        ok: false,
        error: "yandex_small_volume_blocked",
        minVolumeMl: volumeCheck.minVolumeMl,
        reason: volumeCheck.reason,
      });
      continue;
    }
    deduped.set(normalizedOfferId, { ...offer, offerId: cleanText(offer.offerId) });
  }
  const prepared = Array.from(deduped.values());
  for (const chunk of chunkArray(prepared, 100)) {
    if (!chunk.length) continue;
    try {
      await yandexRequest(
        shop,
        "POST",
        `/v2/businesses/${shop.businessId}/offer-mappings/update`,
        { offerMappings: chunk.map((offer) => ({ offer })) },
      );
      results.push(...chunk.map((offer) => ({ offerId: offer.offerId, ok: true })));
    } catch (error) {
      const detail = error?.message || "yandex_import_failed";
      if (chunk.length === 1) {
        results.push(...chunk.map((offer) => ({ offerId: offer.offerId, ok: false, error: detail })));
        continue;
      }
      logger.warn("yandex offer mappings chunk failed, retrying one by one", {
        shop: shop.id,
        businessId: shop.businessId,
        items: chunk.length,
        detail,
      });
      for (const offer of chunk) {
        try {
          await yandexRequest(
            shop,
            "POST",
            `/v2/businesses/${shop.businessId}/offer-mappings/update`,
            { offerMappings: [{ offer }] },
          );
          results.push({ offerId: offer.offerId, ok: true, recoveredFromChunkError: true });
        } catch (singleError) {
          results.push({
            offerId: offer.offerId,
            ok: false,
            error: singleError?.message || detail,
            chunkError: detail,
          });
        }
      }
    }
  }
  return results;
}

