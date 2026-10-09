// "Where to buy" pages linked from Telegram posts (magicvibes.ru/buy/<product>).
// Only exact buyer prices are shown. The site price is ours. Marketplace buyer prices include the platforms'
// own discounts, which the APIs do not return (Ozon has customer_price only in the Premium method
// /v1/product/prices/details; Yandex has none), so for Ozon / Market the page shows availability and a link
// to OUR offer: Yandex offer-mappings showcaseUrls (raw.productUrl is the model card, where Market shows
// other sellers) and the per-store status (PUBLISHED / NO_STOCKS…). If Ozon Premium gets connected, its
// customer_price is picked up automatically.

const SHOP_BUY_COMPARE_TTL_MS = 10 * 60 * 1000;
const SHOP_BUY_LIVE_TIMEOUT_MS = 6000;
const _shopBuyCompareCache = new Map();

function _shopBuyTimeout(promise) {
  return Promise.race([promise, new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), SHOP_BUY_LIVE_TIMEOUT_MS))]);
}

function _shopBuyStock(state) {
  if (!state || typeof state !== "object") return null;
  if (state.archived === true) return false;
  const stock = Number(state.stock ?? state.present);
  return Number.isFinite(stock) ? stock > 0 : null;
}

function _shopBuyAccount(marketplace, target) {
  const list = marketplace === "ozon" ? getOzonAccounts({ includeSyncDisabled: true }) : getYandexShops({ includeSyncDisabled: true });
  return list.find((a) => a.id === target) || null;
}

let _shopBuyOzonPremiumDeniedAt = 0;
async function _shopBuyOzonLive(row) {
  const account = _shopBuyAccount("ozon", row.target);
  const sku = (String(row.url || "").match(/ozon\.ru\/product\/(\d+)/) || [])[1];
  if (!account || !sku || Date.now() - _shopBuyOzonPremiumDeniedAt < 6 * 3600 * 1000) return {};
  try {
    const data = await _shopBuyTimeout(ozonRequest("/v1/product/prices/details", { skus: [sku] }, account));
    const item = (data.prices || []).find((x) => String(x.sku) === sku);
    const price = Number(item?.customer_price?.amount) || 0;
    return price > 0 ? { exactPrice: price } : {};
  } catch (err) {
    if (/403|no access/i.test(err?.message || "")) _shopBuyOzonPremiumDeniedAt = Date.now(); // no Premium subscription
    return {};
  }
}

async function _shopBuyYandexLive(row, offerId) {
  const shop = _shopBuyAccount("yandex", row.target);
  if (!shop?.businessId) return {};
  const items = await _shopBuyTimeout(getYandexOfferMappingsByOfferIds(shop, [offerId]));
  const item = items.find((i) => String(i?.offer?.offerId || "").toLowerCase() === offerId.toLowerCase());
  if (!item) return { url: null, inStock: false };
  const offer = item.offer || {};
  const b2c = (item.showcaseUrls || []).find((u) => u.showcaseType === "B2C") || (item.showcaseUrls || [])[0];
  const statuses = (offer.campaigns || []).map((c) => c.status);
  const out = { url: b2c?.showcaseUrl || null };
  if (offer.archived) out.inStock = false;
  else if (statuses.length) out.inStock = statuses.includes("PUBLISHED");
  return out;
}

/** Best listing of one marketplace: in stock first, then the main cabinet ("ozon"). price = exact buyer price or null. */
function _shopBuyPickOffer(rows) {
  const usable = rows.filter((r) => /^https:\/\/(www\.)?(ozon\.ru|market\.yandex\.ru)\//.test(r.url || ""));
  usable.sort((a, b) =>
    (b.inStock === true) - (a.inStock === true)
    || (a.target === "ozon" ? -1 : 0) - (b.target === "ozon" ? -1 : 0));
  const r = usable[0];
  return r ? { price: r.exactPrice || null, url: r.url, inStock: r.inStock !== false } : null;
}

async function buildShopBuyCompare(offerId) {
  const product = await findShopProductByOfferId(offerId);
  if (!product) return null;
  const prisma = getPrisma();
  const rows = await prisma.$queryRaw`
    SELECT marketplace::text AS marketplace, target, marketplace_state AS state, raw->>'productUrl' AS url
    FROM warehouse_products
    WHERE lower(offer_id) = lower(${product.offerId}) AND archived = false`;

  const enriched = await Promise.all(rows.map(async (r) => {
    const base = { ...r, exactPrice: null, inStock: _shopBuyStock(r.state) };
    try {
      const live = r.marketplace === "ozon" ? await _shopBuyOzonLive(r)
        : r.marketplace === "yandex" ? await _shopBuyYandexLive(r, product.offerId) : {};
      // url: null (no showcase link) drops the model-card link on purpose.
      return { ...base, ...live };
    } catch (err) {
      logger.warn("shop buy compare live data failed", { marketplace: r.marketplace, target: r.target, offerId: product.offerId, detail: err?.message || String(err) });
      return base;
    }
  }));

  return {
    product: {
      offerId: product.offerId,
      name: product.name,
      brand: product.brand,
      image: product.images?.[0] || null,
      volume: product.volume || null,
      priceRub: product.priceRub,
      oldPriceRub: product.oldPriceRub || null,
      inStock: Boolean(product.inStock),
    },
    offers: {
      site: product.priceRub > 0 ? { price: product.priceRub, inStock: Boolean(product.inStock) } : null,
      ozon: _shopBuyPickOffer(enriched.filter((r) => r.marketplace === "ozon")),
      yandex: _shopBuyPickOffer(enriched.filter((r) => r.marketplace === "yandex")),
    },
  };
}

app.get("/api/shop/compare/:offerId", shopCors, async (request, response, next) => {
  try {
    const offerId = cleanText(request.params.offerId || "");
    if (!offerId) return response.status(400).json({ error: "offerId required" });
    const key = offerId.toLowerCase();
    const cached = _shopBuyCompareCache.get(key);
    let data = cached && Date.now() - cached.at < SHOP_BUY_COMPARE_TTL_MS ? cached.data : null;
    if (!data) {
      data = await buildShopBuyCompare(offerId);
      if (!data) return response.status(404).json({ error: "Товар не найден" });
      if (_shopBuyCompareCache.size > 2000) _shopBuyCompareCache.clear();
      _shopBuyCompareCache.set(key, { at: Date.now(), data });
    }
    response.set("Cache-Control", "public, max-age=120");
    response.json({ ok: true, ...data });
  } catch (error) { next(error); }
});
