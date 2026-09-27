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

// POST offer-mappings/update. New cards go out whole; for existing ones (archived included)
// only changed fields are sent, and fields another system owns (GTIN, ТН ВЭД/ОКПД2,
// documents, shelf life) are never overwritten. Market rejects the whole request when any
// offer has results[].errors: those offers are dropped (logged per offerId) and the rest resent.
async function sendYandexOfferMappings(shop, offers = [], { onlyChanged = true } = {}) {
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
    let current = new Map();
    if (onlyChanged) {
      try {
        const mappings = await getYandexOfferMappingsByOfferIds(shop, chunk.map((offer) => offer.offerId));
        current = new Map(mappings.map((item) => [cleanText(item?.offer?.offerId).toLowerCase(), item]));
      } catch (error) {
        // Without the current cards we cannot tell what changed: do not risk wiping data.
        logger.warn("yandex offer mappings: current cards unavailable, chunk skipped", { shop: shop.id, detail: error?.message || String(error) });
        results.push(...chunk.map((offer) => ({ offerId: offer.offerId, ok: false, error: "yandex_current_cards_unavailable" })));
        continue;
      }
    }
    const toSend = [];
    for (const offer of chunk) {
      const existing = current.get(offer.offerId.toLowerCase());
      if (!existing) {
        if (!offer.marketCategoryId) {
          results.push({ offerId: offer.offerId, ok: false, error: "category_manual_review" });
          continue;
        }
        toSend.push(offer);
        continue;
      }
      const { basicPrice: _price, ...cardFields } = offer;
      const update = diffYandexOfferForUpdate(cardFields, existing.offer || {}, existing.mapping?.marketCategoryId);
      if (!update) {
        results.push({ offerId: offer.offerId, ok: true, unchanged: true, existing: true });
        continue;
      }
      toSend.push(update);
    }
    let pending = toSend;
    for (let attempt = 0; pending.length && attempt < 5; attempt += 1) {
      let apiResult;
      try {
        apiResult = await yandexRequest(
          shop,
          "POST",
          `/v2/businesses/${shop.businessId}/offer-mappings/update`,
          { offerMappings: pending.map((offer) => ({ offer })) },
        );
      } catch (error) {
        const parsed = parseYandexOfferMappingsResult(error?.yandex || error?.body || {});
        if (!parsed.errorsByOffer.size) {
          const detail = error?.message || "yandex_import_failed";
          logger.warn("yandex offer mappings request failed", { shop: shop.id, items: pending.length, detail });
          results.push(...pending.map((offer) => ({ offerId: offer.offerId, ok: false, error: detail })));
          pending = [];
          break;
        }
        apiResult = error?.yandex || error?.body;
      }
      const { errorsByOffer, warningsByOffer } = parseYandexOfferMappingsResult(apiResult);
      for (const [offerId, warnings] of warningsByOffer) {
        logger.info("yandex offer mapping warning", { shop: shop.id, offerId, warnings: warnings.slice(0, 5) });
      }
      if (!errorsByOffer.size) {
        results.push(...pending.map((offer) => ({ offerId: offer.offerId, ok: true, fields: Object.keys(offer).filter((key) => key !== "offerId") })));
        pending = [];
        break;
      }
      // One bad offer rejects the request: drop the offenders, resend the rest.
      for (const [offerId, errors] of errorsByOffer) {
        logger.warn("yandex offer mapping rejected", { shop: shop.id, offerId, errors: errors.slice(0, 5) });
        results.push({
          offerId,
          ok: false,
          error: errors.map((item) => `${item.type || "ERROR"}${item.parameterId ? `#${item.parameterId}` : ""}: ${item.message || ""}`).join("; "),
        });
      }
      const rejected = new Set(Array.from(errorsByOffer.keys()).map((id) => id.toLowerCase()));
      pending = pending.filter((offer) => !rejected.has(cleanText(offer.offerId).toLowerCase()));
    }
    if (pending.length) results.push(...pending.map((offer) => ({ offerId: offer.offerId, ok: false, error: "yandex_retry_limit" })));
  }
  return results;
}
