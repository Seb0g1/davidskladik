// Фоновая досылка фото в карточки WB без фото (Ozon-картинки по vendorCode).
//
// Лимит WB media/save для кабинета — Token Bucket «1 запрос / ~15 минут»
// (X-Ratelimit-Limit: 1, Retry: ~900с; выяснено 2026-07-16 в бою): разовая
// цепочка не может загрузить сотни фото. Каждый тик шедулер шлёт фото по
// одной карточке, пока не упрётся в 429, — квота выбирается полностью, а
// если WB поднимет лимит, темп вырастет сам без изменений кода.

const wbMediaBackfillEnabled = process.env.WB_MEDIA_BACKFILL_ENABLED !== "false";
const wbMediaBackfillIntervalMinutes = Math.max(1, Math.min(120, Number(process.env.WB_MEDIA_BACKFILL_INTERVAL_MINUTES || 5) || 5));
const wbMediaBackfillPerRunLimit = Math.max(1, Math.min(500, Number(process.env.WB_MEDIA_BACKFILL_PER_RUN || 30) || 30));
let wbMediaBackfillTimer = null;
let wbMediaBackfillRunning = false;
let wbMediaBackfillNextRunAt = null;
const wbMediaBackfillStats = { totalSent: 0, lastSentAt: null, last429At: null, lastRetrySec: null, remaining: null, lastRunAt: null, lastStatus: "" };

// Шедулер тикает на worker, а HTTP обслуживает api-процесс — worker пишет
// снапшот статуса в data/, статус-роут читает его, когда локально пусто.
const wbMediaBackfillStatusPath = path.join(dataDir, "wb-media-backfill-status.json");

function wbMediaBackfillStatusSnapshot() {
  return {
    enabled: wbMediaBackfillEnabled,
    intervalMinutes: wbMediaBackfillIntervalMinutes,
    nextRunAt: wbMediaBackfillNextRunAt,
    ...wbMediaBackfillStats,
  };
}

function persistWbMediaBackfillStatus() {
  if (!wbMediaBackfillTimer) return;
  const snapshot = { ...wbMediaBackfillStatusSnapshot(), updatedAt: new Date().toISOString(), pid: process.pid };
  fs.writeFile(wbMediaBackfillStatusPath, JSON.stringify(snapshot, null, 2)).catch((error) => {
    logger.warn("wb media backfill status persist failed", { detail: error?.message || String(error) });
  });
}

// Картинки Ozon-товара по vendorCode карточки WB (vendorCode = наш offerId).
async function wbMediaBackfillImagesForVendorCode(prisma, vendorCode) {
  const code = cleanText(vendorCode).toLowerCase();
  if (!code) return [];
  const rows = await prisma.$queryRaw`
    SELECT raw FROM warehouse_products
    WHERE marketplace = 'ozon' AND LOWER(offer_id) = ${code}
    LIMIT 1
  `;
  const raw = rows?.[0]?.raw;
  const normalized = raw && typeof raw === "object" ? normalizeWarehouseProduct(raw) : null;
  return normalized ? wbExtractImageUrls(normalized) : [];
}

async function runWbMediaBackfill({ limit = wbMediaBackfillPerRunLimit, source = "auto" } = {}) {
  if (wbMediaBackfillRunning) return { status: "already_running" };
  const account = getWbAccountByTarget("wb");
  if (!account) return { status: "no_wb_account" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  wbMediaBackfillRunning = true;
  const startedAt = Date.now();
  try {
    const cards = await wbCardsList(account);
    const withoutPhoto = cards.filter((card) => Number(card.nmID) > 0 && !(Array.isArray(card.photos) && card.photos.length));
    wbMediaBackfillStats.remaining = withoutPhoto.length;
    wbMediaBackfillStats.lastRunAt = new Date().toISOString();
    if (!withoutPhoto.length) {
      wbMediaBackfillStats.lastStatus = "done";
      return { status: "ok", remaining: 0, sent: 0 };
    }

    // Квота ~1 фото/15 мин на ~10k карточек: фото в первую очередь товарам,
    // которые реально в продаже (sellable), остальные — когда очередь дойдёт.
    let ordered = withoutPhoto;
    try {
      const rules = await readWbImportRules();
      const pricing = await loadAvitoPricingContext();
      const linked = await loadWbLinkedOzonProducts(withoutPhoto.map((card) => card.vendorCode));
      const sellableNmIds = new Set();
      for (const card of withoutPhoto) {
        const product = linked.get(cleanText(card.vendorCode).toLowerCase());
        if (!product) continue;
        const purchaseRub = wbSupplierPurchaseRub(product.supplier, pricing);
        const priceRub = purchaseRub > 0 ? wbSupplierPriceRub(product.supplier, pricing) : 0;
        if (wbCardSellable({ product, purchaseRub, priceRub, rules })) sellableNmIds.add(card.nmID);
      }
      ordered = [
        ...withoutPhoto.filter((card) => sellableNmIds.has(card.nmID)),
        ...withoutPhoto.filter((card) => !sellableNmIds.has(card.nmID)),
      ];
    } catch (error) {
      logger.warn("wb media backfill prioritize failed", { detail: error?.message || String(error) });
    }

    let sent = 0;
    let skippedNoImages = 0;
    let throttled = false;
    const errors = [];
    for (const card of ordered) {
      if (sent >= limit || errors.length >= 5) break;
      let imageUrls = [];
      try {
        imageUrls = await wbMediaBackfillImagesForVendorCode(prisma, card.vendorCode);
      } catch (error) {
        errors.push({ vendorCode: card.vendorCode, error: error?.message || String(error) });
        continue;
      }
      if (!imageUrls.length) {
        skippedNoImages += 1;
        continue;
      }
      try {
        // attempts: 1 — при 429 не ретраим, а ждём следующего тика шедулера.
        const result = await wbMediaSave(account, card.nmID, imageUrls.slice(0, 10), { attempts: 1 });
        if (result.ok) {
          sent += 1;
          wbMediaBackfillStats.totalSent += 1;
          wbMediaBackfillStats.lastSentAt = new Date().toISOString();
          await new Promise((resolve) => setTimeout(resolve, 2000));
        } else {
          errors.push({ vendorCode: card.vendorCode, nmID: card.nmID, error: result.error || "media_save_failed" });
        }
      } catch (error) {
        if (Number(error?.statusCode) === 429) {
          throttled = true;
          wbMediaBackfillStats.last429At = new Date().toISOString();
          wbMediaBackfillStats.lastRetrySec = Number(error?.rateLimit?.retry || error?.retryAfterSec || 0) || null;
          break;
        }
        errors.push({ vendorCode: card.vendorCode, nmID: card.nmID, statusCode: error?.statusCode, error: error?.message || String(error) });
      }
    }

    wbMediaBackfillStats.lastStatus = throttled ? "throttled" : "ok";
    if (sent || errors.length) {
      logger.info("wb_media_backfill_tick", {
        source,
        sent,
        remaining: withoutPhoto.length - sent,
        throttled,
        retrySec: wbMediaBackfillStats.lastRetrySec,
        skippedNoImages,
        errors: errors.length,
        elapsedMs: Date.now() - startedAt,
      });
    }
    return { status: "ok", remaining: withoutPhoto.length - sent, sent, throttled, retrySec: wbMediaBackfillStats.lastRetrySec, skippedNoImages, errors: errors.slice(0, 5) };
  } catch (error) {
    wbMediaBackfillStats.lastStatus = "error";
    logger.warn("wb media backfill failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    wbMediaBackfillRunning = false;
    persistWbMediaBackfillStatus();
  }
}

function scheduleWbMediaBackfill(delayMs = null) {
  if (!wbMediaBackfillEnabled) {
    wbMediaBackfillNextRunAt = null;
    return;
  }
  if (wbMediaBackfillTimer) clearTimeout(wbMediaBackfillTimer);
  const intervalMs = wbMediaBackfillIntervalMinutes * 60 * 1000;
  const normalizedDelay = Math.max(30_000, Number(delayMs ?? intervalMs) || intervalMs);
  wbMediaBackfillNextRunAt = new Date(Date.now() + normalizedDelay).toISOString();
  wbMediaBackfillTimer = setTimeout(async () => {
    try {
      await runWbMediaBackfill({ source: "schedule" });
    } catch (error) {
      logger.warn("wb media backfill tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleWbMediaBackfill(intervalMs);
    }
  }, normalizedDelay);
  wbMediaBackfillTimer.unref?.();
  persistWbMediaBackfillStatus();
}

// Статус фоновой досылки фото WB (тикает на worker; на api отдаётся снапшот
// worker из data/wb-media-backfill-status.json).
app.get("/api/wb/media-backfill/status", async (_request, response) => {
  if (!wbMediaBackfillTimer && !wbMediaBackfillStats.lastRunAt) {
    try {
      const saved = JSON.parse(await fs.readFile(wbMediaBackfillStatusPath, "utf8"));
      return response.json({ ...saved, source: "worker" });
    } catch {
      // нет файла — worker ещё не тикал, отдаём локальное состояние
    }
  }
  response.json({ ...wbMediaBackfillStatusSnapshot(), source: "local" });
});

// Ручной тик (admin): одна порция до первого 429.
app.post("/api/wb/media-backfill/run", requireAdmin, async (request, response, next) => {
  try {
    response.json(await runWbMediaBackfill({ limit: Math.max(1, Math.min(100, Number(request.body?.limit || 0) || wbMediaBackfillPerRunLimit)), source: "manual" }));
  } catch (error) {
    next(error);
  }
});
