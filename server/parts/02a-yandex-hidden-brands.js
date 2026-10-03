// Бренды, которые Маркет скрыл «из-за сомнений в подлинности» (ошибка карточки «Скрыт сотрудником Маркета …
// Мы скрыли товары бренда X, потому что сомневаемся в их подлинности»). Такие карточки удаляются с Маркета,
// а бренд больше не уходит туда ни одним путём (автоперенос Ozon→Маркет, ручной перенос, Фрагрантика) —
// пока его не уберут из списка (подлинность подтверждена). Список — data/yandex-hidden-brands.json.
//
// Плюс единый «жёсткий» фильтр Маркета (yandexHardBlockReasons): скрытый бренд, объём < 20 мл, тестер,
// пробник, отливант. Стоит в самой отправке exportOzonProductsToYandex — обойти его нельзя.

const yandexHiddenBrandsPath = path.join(dataDir, "yandex-hidden-brands.json");
let yandexHiddenBrandsCache = { at: 0, mtimeMs: 0, brands: {}, offers: {} };
const YANDEX_HIDDEN_RELOAD_MS = 60_000;

function yandexHiddenBrandKey(value) {
  return normalizedBrandIndexKey(cleanText(value));
}

/** «By Kilian» is written «Kilian» in many titles — both spellings are keys. */
function yandexHiddenBrandKeys(brand) {
  const key = yandexHiddenBrandKey(brand);
  if (!key) return [];
  const keys = new Set([key]);
  const withoutBy = key.replace(/^by\s+/, "");
  if (withoutBy && withoutBy !== key && withoutBy.length >= 4) keys.add(withoutBy);
  return [...keys];
}

/** Brand named in the Market comment: «Мы скрыли товары бренда By Kilian, потому что сомневаемся…». */
const YANDEX_AUTHENTICITY_RE = /сомнева\S*\s+в\s+(?:их|его|её|ее)?\s*подлинност/i;

function parseYandexHiddenBrandComment(text = "") {
  const value = String(text || "");
  if (!YANDEX_AUTHENTICITY_RE.test(value)) return null;
  const match = /товар\S*\s+бренда\s+(.+?)\s*(?:,|\s+потому\s+что|\.)/i.exec(value);
  return { brand: match ? cleanText(match[1]).replace(/^[«"']|[»"']$/g, "") : "" };
}

/** Pure: is this Market card error the «hidden for authenticity» one? Returns { brand } or null. */
function yandexHiddenForAuthenticity(error = {}) {
  const text = [error.message, error.comment, error.description, error.type].map((v) => String(v || "")).join(" \n ");
  // only the authenticity case — a card hidden by staff for another reason is left alone
  if (!YANDEX_AUTHENTICITY_RE.test(text)) return null;
  return parseYandexHiddenBrandComment(text) || { brand: "" };
}

/** Pure: all errors of a card → { brand } (a brand-level error wins over the product-level one) or null. */
function yandexCardHiddenForAuthenticity(errors = []) {
  let found = null;
  for (const error of Array.isArray(errors) ? errors : []) {
    const hit = yandexHiddenForAuthenticity(error);
    if (!hit) continue;
    if (hit.brand) return hit;
    found = found || hit;
  }
  return found;
}

function readYandexHiddenBrandsSync() {
  const now = Date.now();
  if (now - yandexHiddenBrandsCache.at < YANDEX_HIDDEN_RELOAD_MS) return yandexHiddenBrandsCache.brands;
  try {
    const stat = require("fs").statSync(yandexHiddenBrandsPath);
    if (stat.mtimeMs !== yandexHiddenBrandsCache.mtimeMs) {
      const parsed = JSON.parse(require("fs").readFileSync(yandexHiddenBrandsPath, "utf8") || "{}");
      yandexHiddenBrandsCache = {
        at: now,
        mtimeMs: stat.mtimeMs,
        brands: parsed.brands && typeof parsed.brands === "object" ? parsed.brands : {},
        offers: parsed.offers && typeof parsed.offers === "object" ? parsed.offers : {},
      };
    } else {
      yandexHiddenBrandsCache.at = now;
    }
  } catch {
    yandexHiddenBrandsCache = { at: now, mtimeMs: 0, brands: {}, offers: {} };
  }
  return yandexHiddenBrandsCache.brands;
}

async function writeYandexHiddenBrands(brands, offers = yandexHiddenBrandsCache.offers || {}) {
  await fs.mkdir(dataDir, { recursive: true });
  const tmp = `${yandexHiddenBrandsPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(tmp, JSON.stringify({ brands, offers }, null, 2), "utf8");
  await fs.rename(tmp, yandexHiddenBrandsPath);
  yandexHiddenBrandsCache = { at: 0, mtimeMs: 0, brands: {}, offers: {} };
}

/** Pure: which hidden brand (display name) this product belongs to, or "". brands: { key: { brand } } */
function matchYandexHiddenBrand(product = {}, brands = readYandexHiddenBrandsSync(), offers = yandexHiddenBrandsCache.offers || {}) {
  const offerKey = cleanText(product.offerId || product.offer_id).toLowerCase();
  if (offerKey && offers[offerKey]) return offers[offerKey].brand || "товар скрыт Маркетом";
  const entries = Object.values(brands || {});
  if (!entries.length) return "";
  const vendor = yandexHiddenBrandKey(product.ozon?.vendor || product.yandex?.vendor || product.vendor || product.brand || "");
  const name = ` ${yandexHiddenBrandKey(product.name || product.ozon?.name || product.yandexName || "")} `;
  for (const entry of entries) {
    for (const key of yandexHiddenBrandKeys(entry.brand)) {
      if (vendor && vendor === key) return entry.brand;
      if (name.includes(` ${key} `)) return entry.brand;
    }
  }
  return "";
}

/** Hard Market rules — no path may send such a product to Market (manual selection included). */
function yandexHardBlockReasons(product = {}) {
  const reasons = [];
  const hidden = matchYandexHiddenBrand(product);
  if (hidden) reasons.push(`Маркет скрыл бренд «${hidden}» (сомнения в подлинности)`);
  const name = cleanText(product.name || product.ozon?.name || product.yandexName || product.offerId);
  const lower = name.toLowerCase();
  const small = assessYandexSmallVolume(collectYandexVolumeSearchText(product));
  if (small.blocked) reasons.push(small.reason);
  if (/тестер|tester/iu.test(lower)) reasons.push("Тестер не продаётся на Маркете");
  if (isSingleSampleName(name)) reasons.push("Пробник не продаётся на Маркете");
  if (lower.includes("отливант")) reasons.push("Отливант не продаётся на Маркете");
  return reasons;
}

// ─── Сканер карточек Маркета ─────────────────────────────────────────────────

const yandexHiddenScanEnabled = process.env.YANDEX_HIDDEN_BRANDS_SCAN !== "false";
const yandexHiddenScanIntervalMs = Math.max(1, Number(process.env.YANDEX_HIDDEN_BRANDS_SCAN_HOURS || 4) || 4) * 3_600_000;
let yandexHiddenScanRunning = false;

/**
 * Reads the card errors of every Market offer we have, records hidden brands and (dryRun false) deletes the
 * hidden cards from Market and archives them on the warehouse.
 */
async function runYandexHiddenBrandScan({ dryRun = false } = {}) {
  if (yandexHiddenScanRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma) return { status: "postgres_disabled" };
  yandexHiddenScanRunning = true;
  const startedAt = Date.now();
  try {
    const brands = { ...readYandexHiddenBrandsSync() };
    const offers = { ...(yandexHiddenBrandsCache.offers || {}) };
    const result = { status: "ok", dryRun, shops: [], newBrands: [], hidden: 0, deleted: 0, failed: 0 };
    for (const shop of uniqueYandexShopsByBusiness()) {
      const rows = await prisma.warehouseProduct.findMany({
        where: { marketplace: "yandex", target: cleanText(shop.id), archived: false },
        select: { id: true, offerId: true, name: true },
      });
      const offerIds = [...new Set(rows.map((r) => cleanText(r.offerId)).filter(Boolean))];
      const cards = offerIds.length ? await getYandexOfferCardsContentStatus(shop, offerIds, { withRecommendations: false }).catch((error) => {
        logger.warn("yandex hidden brand scan: cards failed", { shop: shop.id, detail: error?.message });
        return [];
      }) : [];
      const hidden = [];
      for (const card of cards) {
        const found = yandexCardHiddenForAuthenticity(card.errors);
        if (!found) continue;
        const row = rows.find((r) => cleanText(r.offerId) === card.offerId);
        hidden.push({ offerId: card.offerId, brand: found.brand, id: row?.id, name: row?.name });
        if (found.brand) {
          const key = yandexHiddenBrandKey(found.brand);
          if (!brands[key]) {
            brands[key] = { brand: found.brand, firstSeenAt: new Date().toISOString(), shop: shop.id };
            result.newBrands.push(found.brand);
          }
          brands[key].lastSeenAt = new Date().toISOString();
        } else {
          // only this product is hidden — block just the offer, not the whole brand
          const offerKey = card.offerId.toLowerCase();
          if (!offers[offerKey]) result.newOffers = (result.newOffers || 0) + 1;
          offers[offerKey] = { name: row?.name || "", at: new Date().toISOString(), shop: shop.id };
        }
      }
      result.hidden += hidden.length;
      const shopResult = { shop: shop.id, offers: offerIds.length, hidden: hidden.length, sample: hidden.slice(0, 10).map((h) => `${h.offerId} (${h.brand || "?"})`) };
      if (!dryRun && hidden.length) {
        const deleted = await deleteYandexOfferIds(shop, hidden.map((h) => h.offerId));
        const okIds = new Set(deleted.filter((d) => d.ok).map((d) => d.offerId));
        result.deleted += okIds.size;
        result.failed += deleted.length - okIds.size;
        const archivedIds = hidden.filter((h) => h.id && okIds.has(h.offerId)).map((h) => h.id);
        if (archivedIds.length) {
          await prisma.$executeRawUnsafe(
            `UPDATE warehouse_products SET archived = true, updated_at = now(),
               raw = jsonb_set(COALESCE(raw, '{}'::jsonb), '{yandexQualityHidden}', jsonb_build_object('at', now()::text, 'reason', 'Скрыт сотрудником Маркета: сомнения в подлинности бренда'))
             WHERE id = ANY($1::text[])`,
            archivedIds,
          );
        }
        shopResult.deleted = okIds.size;
        shopResult.failedSample = deleted.filter((d) => !d.ok).slice(0, 5);
      }
      result.shops.push(shopResult);
    }
    if (!dryRun && (result.newBrands.length || result.newOffers)) await writeYandexHiddenBrands(brands, offers);
    result.brands = Object.values(brands).map((b) => b.brand);
    result.hiddenOffers = Object.keys(offers).length;
    result.ms = Date.now() - startedAt;
    logger.info("yandex hidden brand scan", result);
    if (!dryRun && result.newBrands.length && typeof sendHealthAlertTelegram === "function") {
      sendHealthAlertTelegram(`ℹ️ Маркет скрыл бренд(ы) из-за сомнений в подлинности: ${result.newBrands.join(", ")}. Карточки удалены с Маркета (${result.deleted}), бренд больше не переносится. Подтвердить подлинность: кабинет Маркета → Поддержка → Контроль качества.`).catch(() => {});
    }
    return result;
  } finally {
    yandexHiddenScanRunning = false;
  }
}

function scheduleYandexHiddenBrandScan(delayMs = yandexHiddenScanIntervalMs) {
  if (!yandexHiddenScanEnabled) return;
  setTimeout(async () => {
    try {
      await runYandexHiddenBrandScan();
    } catch (error) {
      logger.warn("yandex hidden brand scan failed", { detail: error?.message || String(error) });
    } finally {
      scheduleYandexHiddenBrandScan(yandexHiddenScanIntervalMs);
    }
  }, Math.max(60_000, Number(delayMs) || yandexHiddenScanIntervalMs)).unref?.();
}

// GET  /api/yandex/hidden-brands            — список скрытых брендов
// POST /api/yandex/hidden-brands/scan       — { dryRun } проверить карточки Маркета сейчас
// DELETE /api/yandex/hidden-brands/:brand   — снять запрет (Маркет подтвердил подлинность)
app.get("/api/yandex/hidden-brands", requireAdmin, (_request, response) => {
  response.json({ ok: true, brands: Object.values(readYandexHiddenBrandsSync()) });
});

app.post("/api/yandex/hidden-brands/scan", requireAdmin, async (request, response, next) => {
  try {
    response.json(await runYandexHiddenBrandScan({ dryRun: request.body?.dryRun === true }));
  } catch (error) {
    next(error);
  }
});

app.delete("/api/yandex/hidden-brands/:brand", requireAdmin, async (request, response, next) => {
  try {
    const brands = { ...readYandexHiddenBrandsSync() };
    const key = yandexHiddenBrandKey(decodeURIComponent(request.params.brand));
    if (!brands[key]) return response.status(404).json({ error: "Бренд не в списке" });
    delete brands[key];
    await writeYandexHiddenBrands(brands);
    await appendAudit(request, "yandex.hidden_brand.remove", { entityType: "yandex_hidden_brand", entityId: key });
    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
