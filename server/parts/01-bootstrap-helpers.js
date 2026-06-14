}
function openAiImageSupportsInputFidelity(model = openaiImageModel) {
  const normalized = cleanText(model).toLowerCase();
  if (!normalized || normalized === "gpt-image-2") return false;
  return normalized.startsWith("gpt-image-1");
}
function normalizeOpenAiImageModelName(model) {
  const normalized = cleanText(model);
  if (normalized.toLowerCase() === "gpt-image-1.5-high-fidelity") return "gpt-image-1.5";
  return normalized;
}
function isOpenAiImageModelName(model) {
  const normalized = cleanText(model).toLowerCase();
  return normalized.startsWith("gpt-image") || normalized.startsWith("dall-e");
}
function effectiveOpenAiImageModel(model, aiSettings = {}) {
  if (isCodexSaleAiProvider(aiSettings)) return "gpt-image-2";
  const normalized = normalizeOpenAiImageModelName(model);
  return isOpenAiImageModelName(normalized) ? normalized : (openaiImageModel || "gpt-image-2");
}
const ozonAiImageDefaultPrompt = cleanText(process.env.OZON_AI_IMAGE_PROMPT)
  || 'Сгенерируй продающее изображение для карточки товара на Ozon. Используй название товара: "{productName}". Сохрани узнаваемость товара с исходного фото, улучшив фон, свет, композицию и визуальную привлекательность для маркетплейса. Не добавляй логотипы, водяные знаки, недостоверные характеристики или лишний текст.';

const aiImageStudioPresets = [
  {
    id: "white-packshot",
    label: "White background",
    prompt: "Centered ecommerce packshot on a clean white background. Single perfume bottle only, full bottle visible from cap to base, not cropped, centered with 10-15% empty margin on every side. Preserve the exact bottle shape, cap color and existing label from the source photo. Remove boxes and outer packaging. No added text outside the product label, no watermark.",
  },
  {
    id: "premium-shadow",
    label: "Premium shadow",
    prompt: "Premium perfume bottle photo with elegant soft shadow and subtle reflective surface. Full bottle visible, centered, not cut off by frame edges, product occupies about 75-85% of canvas height. Keep only the original bottle recognizable. No box, no packaging, no props, no text overlays.",
  },
  {
    id: "lifestyle",
    label: "Lifestyle",
    prompt: "Minimal marketplace lifestyle scene. Single perfume bottle only, centered, full bottle visible with safe margins, soft daylight, product in focus. Remove any box or packaging from the source. No hands, no faces, no typography or marketing text.",
  },
  {
    id: "bottle-only",
    label: "Bottle only",
    prompt: "Marketplace image showing only the perfume bottle, clean studio composition. Entire bottle must fit inside the square image, centered, no crop at top, bottom, left, or right. If the source photo includes a box, remove it. Do not create packaging. No extra text outside the original bottle label.",
  },
  {
    id: "close-up",
    label: "Close-up",
    prompt: "Clean hero image of the perfume bottle with crisp highlights, but keep the full bottle visible from cap to base. Do not crop the bottle. Keep proportions and label area consistent with the source photo. Remove packaging. No decorative text, no headline, no logo outside the original label.",
  },
];

let dailySyncTimer = null;
let dailySyncNextRunAt = null;
let dailySyncPromise = null;
let manualWarehouseSyncPromise = null;
let manualWarehouseSyncState = {
  status: "idle",
  trigger: null,
  startedAt: null,
  finishedAt: null,
  result: null,
  error: null,
  progress: {
    percent: 0,
    stage: "Ожидание",
    meta: "Синхронизация ещё не запускалась.",
    processed: 0,
    total: 0,
  },
};
let autoSyncTimer = null;
let autoSyncRunning = false;
let autoSyncNextRunAt = null;
let marketplaceMaintenanceTimer = null;
let marketplaceMaintenanceRunning = false;
let marketplaceMaintenanceNextRunAt = null;
let marketplaceMaintenancePromise = null;
let linkedReconcilerTimer = null;
let linkedReconcilerRunning = false;
let linkedReconcilerNextRunAt = null;
let yandexFastUnarchiveNextRunAt = null;
let ozonUnarchiveQueueAutoTimer = null;
let ozonUnarchiveQueueAutoRunning = false;
let yandexUnarchiveQueueAutoTimer = null;
let yandexUnarchiveQueueAutoRunning = false;
let yandexUnarchiveQueueProcessQueued = false;
let yandexUnarchiveQueueAutoNextRunAt = null;
let yandexUnarchiveQueueAutoLastRunAt = null;
let yandexUnarchiveQueueAutoLastResult = null;
let autoPricePushLastHeartbeatAt = 0;
let autoPricePushStallGuardTimer = null;
let ozonUnarchiveQueueProcessQueued = false;
let ozonUnarchiveQueueAutoNextRunAt = null;
let ozonUnarchiveQueueAutoLastRunAt = null;
let ozonUnarchiveQueueAutoLastResult = null;
let warehouseWritePromise = Promise.resolve();
let warehouseMemoryCache = null;
let warehouseMemoryLoadPromise = null;
let warehouseMemoryProductIndexCache = { products: null, byId: new Map() };
let warehousePostgresHashCache = new Map();
let warehousePostgresUpdatedAtCache = new Map();
let warehousePostgresWriteRunning = false;
let warehousePostgresWriteQueuedPayload = null;

function setManualWarehouseSyncProgress(patch = {}) {
  manualWarehouseSyncState = {
    ...manualWarehouseSyncState,
    progress: {
      ...(manualWarehouseSyncState.progress || {}),
      ...patch,
      updatedAt: new Date().toISOString(),
    },
  };
}
let warehousePostgresLinkBackfillPromise = null;
let warehousePostgresLinkBackfillDone = false;
let warehousePostgresBrandBackfillPromise = null;
let warehousePostgresBrandBackfillDone = false;
let priceMasterSnapshotMemoryCache = null;
let priceMasterArticleIndexCache = null;
let priceMasterSnapshotIndexCache = null;
const priceMasterLinkLookupCache = new Map();
const priceMasterLinkLookupCacheTtlMs = Math.max(1000, Number(process.env.LINK_SAVE_PM_CACHE_MS || 120000));
const priceMasterLinkLookupCacheMax = Math.max(100, Number(process.env.LINK_SAVE_PM_CACHE_MAX || 2000));
const priceMasterSearchCache = new Map();
const priceMasterSearchCacheTtlMs = Math.max(1000, Number(process.env.PRICEMASTER_SEARCH_CACHE_MS || 30000));
const priceMasterSearchCacheMax = Math.max(100, Number(process.env.PRICEMASTER_SEARCH_CACHE_MAX || 1000));
let warehousePostgresSummaryCache = null;
const warehousePostgresSummaryCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_PAGE_SUMMARY_CACHE_MS || 120000));
let warehousePostgresSuppliersCache = null;
let suppliersListCache = null;
const suppliersListCacheTtlMs = Math.max(1000, Number(process.env.SUPPLIERS_LIST_CACHE_MS || 30000) || 30000);
let warehouseBrandListCache = null;
const warehouseBrandListCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_BRAND_LIST_CACHE_MS || 120000));
let warehousePostgresDetailCache = new Map();
const warehousePostgresDetailInflight = new Map();
const warehousePostgresDetailCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_DETAIL_CACHE_MS || 45000));
const warehouseFastPageCache = new Map();
const warehouseFastPageInflight = new Map();
let warehousePageBuildActive = 0;
const warehousePageBuildWaiters = [];
const warehouseFastPageCacheTtlMs = Math.max(1000, Number(process.env.WAREHOUSE_PAGE_CACHE_TTL_MS || process.env.WAREHOUSE_FAST_PAGE_CACHE_MS || 45000));
const warehouseFastPageCacheMax = Math.max(10, Number(process.env.WAREHOUSE_FAST_PAGE_CACHE_MAX || 80));
const warehouseCatalogPairingContextCacheTtlMs = Math.max(5000, Number(process.env.WAREHOUSE_PAIRING_CONTEXT_CACHE_MS || 300000) || 300000);
let warehouseCatalogPairingContextCache = { at: 0, groupContext: null, pairingRows: null };
const warehouseGroupCountCache = new Map();
const warehouseGroupCountInflight = new Map();
const warehouseGroupCountCacheTtlMs = Math.max(5000, Number(process.env.WAREHOUSE_GROUP_COUNT_CACHE_MS || 120000) || 120000);
const warehouseGroupCountCacheMax = Math.max(5, Number(process.env.WAREHOUSE_GROUP_COUNT_CACHE_MAX || 20));
const warehouseGroupCountWaitMs = Math.max(0, Number(process.env.WAREHOUSE_GROUP_COUNT_WAIT_MS || 0) || 0);
const warehouseGroupCountMaxScanRows = Math.max(
  0,
  Number(process.env.WAREHOUSE_GROUP_COUNT_MAX_SCAN_ROWS || 12000) || 12000,
);
const serverHeapPressureRatioLimit = Math.max(
  0.5,
  Math.min(0.95, Number(process.env.SERVER_HEAP_PRESSURE_RATIO || 0.82) || 0.82),
);
const warehouseViewCache = new Map();
const warehouseViewBuilds = new Map();
let lastWarehouseViewSnapshot = null;
let ozonRequestChain = Promise.resolve();
let ozonLastRequestAt = 0;
const ozonWarehouseCache = new Map();
let immediateAutoPushTimer = null;
let immediateAutoPushAll = false;
const immediateAutoPushIds = new Set();
let immediateAutoPushRunning = false;
let immediateAutoPushForce = false;
const immediateAutoPushReasons = new Set();
let immediateAutoPushMarketplace = "all";
let immediateAutoPushMarketplaceLocked = false;
let immediateAutoPushOnlyChanged = true;
let immediateAutoPushRefreshMarketplacePrices = true;
let immediateAutoPushLivePriceMaster = true;
let immediateAutoPushLimit = undefined;
const changedPriceAutoPushAt = new Map();
let changedPriceAutoPushLastBatchAt = 0;
const detectedPriceAutoPushDefaultCooldownMs = 15 * 60_000;
const detectedPriceAutoPushDefaultBatchCooldownMs = 30_000;
let priceRetryTimer = null;
let priceRetryRunning = false;
const slowRequestThresholdMs = Math.max(500, Number(process.env.SLOW_REQUEST_LOG_THRESHOLD_MS || 3000) || 3000);
const recentSlowRequestsMax = Math.max(10, Math.min(200, Number(process.env.SLOW_REQUEST_RECENT_MAX || 50) || 50));
const recentSlowRequests = [];
let activeHttpRequests = 0;
const httpLoadSlowRequestWindowMs = Math.max(10_000, Number(process.env.HTTP_LOAD_SLOW_REQUEST_WINDOW_MS || 90_000) || 90_000);
const httpLoadSlowRequestThresholdMs = Math.max(1000, Number(process.env.HTTP_LOAD_SLOW_REQUEST_THRESHOLD_MS || 8000) || 8000);
const httpLoadActiveRequestThreshold = Math.max(1, Number(process.env.HTTP_LOAD_ACTIVE_REQUEST_THRESHOLD || 2) || 2);
const recentStateWarningsMax = Math.max(10, Math.min(200, Number(process.env.STATE_WARNING_RECENT_MAX || 50) || 50));
const recentStateWarnings = [];
const eventLoopHeartbeatIntervalMs = Math.max(1000, Number(process.env.EVENT_LOOP_HEARTBEAT_INTERVAL_MS || 15_000) || 15_000);
const eventLoopHeartbeatMaxAgeMs = Math.max(eventLoopHeartbeatIntervalMs, Number(process.env.EVENT_LOOP_HEARTBEAT_MAX_AGE_MS || 90_000) || 90_000);
let eventLoopHeartbeatAt = Date.now();
let supplierCartAutoTimer = null;
let supplierCartAutoRunning = false;
let supplierCartAutoNextRunAt = null;
let supplierCartAutoLastRunAt = null;
let supplierCartAutoLastResult = null;
let marketplaceQueue = null;
let marketplaceWorker = null;
const warehouseProductMutationLocks = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeFetchError(error) {
  const parts = [error?.message || String(error)];
  const cause = error?.cause;
  if (cause) {
    const causeParts = [
      cause.code,
      cause.errno,
      cause.syscall,
      cause.address,
      cause.port,
      cause.message,
    ].filter(Boolean);
    if (causeParts.length) parts.push(`cause=${causeParts.join(" ")}`);
  }
  return parts.join("; ");
}

function formatRuNumber(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) return "0";
  return new Intl.NumberFormat("ru-RU").format(number);
}

function summarizeRecentSlowRequests(limit = 20) {
  const items = recentSlowRequests.slice(-Math.max(1, limit));
  const byPath = new Map();
  for (const item of items) {
    const current = byPath.get(item.path) || {
      path: item.path,
      count: 0,
      maxMs: 0,
      lastMs: 0,
      lastAt: null,
      lastStatusCode: null,
      method: item.method,
    };
    current.count += 1;
    current.maxMs = Math.max(current.maxMs, Number(item.elapsedMs || 0));
    current.lastMs = Number(item.elapsedMs || 0);
    current.lastAt = item.at;
    current.lastStatusCode = item.statusCode;
    current.method = item.method || current.method;
    byPath.set(item.path, current);
  }
  return {
    thresholdMs: slowRequestThresholdMs,
    totalRecent: recentSlowRequests.length,
    recent: items,
    byPath: Array.from(byPath.values()).sort((a, b) => b.maxMs - a.maxMs).slice(0, 12),
  };
}

async function marketplaceQueueCounts() {
  if (!bullmqEnabled || !redisUrl) return { enabled: false, mode: "inline", ok: true };
  if (!marketplaceQueue) return { enabled: true, mode: "bullmq", ok: false, error: "queue_not_initialized" };
  try {
    const counts = await healthTimeout(marketplaceQueue.getJobCounts("waiting", "active", "delayed", "failed", "paused", "completed"));
    return { enabled: true, mode: "bullmq", ok: true, counts };
  } catch (error) {
    return { enabled: true, mode: "bullmq", ok: false, error: error?.message || String(error) };
  }
}

async function withWarehouseProductMutationLock(productIds = [], worker) {
  const ids = Array.from(new Set((Array.isArray(productIds) ? productIds : [productIds])
    .map((id) => cleanText(id))
    .filter(Boolean)))
    .sort();
  const lockIds = ids.length ? ids : ["__warehouse_products__"];
  const previousLocks = lockIds.map((id) => warehouseProductMutationLocks.get(id)).filter(Boolean);
  const run = (async () => {
    if (previousLocks.length) await Promise.allSettled(previousLocks);
    return worker();
  })();
  for (const id of lockIds) warehouseProductMutationLocks.set(id, run);
  try {
    return await run;
  } finally {
    for (const id of lockIds) {
      if (warehouseProductMutationLocks.get(id) === run) warehouseProductMutationLocks.delete(id);
    }
  }
}
function warehouseViewCacheKey({ sync = false, limit = Number.POSITIVE_INFINITY, usdRate, refreshPrices = false } = {}) {
  const limitKey = Number.isFinite(Number(limit)) ? Number(limit) : "all";
  const rateKey = Number.isFinite(Number(usdRate)) && Number(usdRate) > 0 ? Number(usdRate) : "default";
  return JSON.stringify({ sync: Boolean(sync), refreshPrices: Boolean(refreshPrices), limit: limitKey, usdRate: rateKey });
}

function invalidateWarehouseViewCache() {
  warehouseViewCache.clear();
  warehouseViewBuilds.clear();
  warehousePostgresSummaryCache = null;
  warehousePostgresSuppliersCache = null;
  suppliersListCache = null;
  warehouseBrandListCache = null;
  warehousePostgresDetailCache.clear();
  warehousePostgresDetailInflight.clear();
  warehouseFastPageCache.clear();
  warehouseGroupCountCache.clear();
  warehouseGroupCountInflight.clear();
  warehouseCatalogPairingContextCache = { at: 0, groupContext: null, pairingRows: null };
}

// PLAN-HARDENING.md 1.4: warehouse view caches must be invalidated AFTER a mutation
// commits — and after any follow-up work it queues (link activation, recovery, etc.) —
// not before. Invalidating early leaves a window where a concurrent catalog read
// repopulates warehouseFastPageCache with pre-mutation data and serves it stale for up
// to warehouseFastPageCacheTtlMs (the "save a link, page still shows it as not linked"
// bug). Any warehouse mutation (route handler or core write helper) should wrap its
// work in withWarehouseMutation instead of calling invalidateWarehouseViewCache
// directly — see scripts/check-warehouse-cache-invalidation.cjs.
async function withWarehouseMutation(fn) {
  try {
    return await fn();
  } finally {
    invalidateWarehouseViewCache();
  }
}

function rememberStateWarning(source, error, extra = {}) {
  const entry = {
    at: new Date().toISOString(),
    source: cleanText(source) || "unknown",
    detail: error?.message || String(error || ""),
    ...extra,
  };
  recentStateWarnings.push(entry);
  while (recentStateWarnings.length > recentStateWarningsMax) recentStateWarnings.shift();
  if (typeof recordErrorEvent === "function") {
    recordErrorEvent({ source: entry.source, message: entry.detail, code: extra.code || "" });
  }
  return entry;
}

// PLAN-HARDENING.md 2.1: a setInterval tick only fires if the event loop keeps turning,
// so a stale heartbeat means the process is wedged (e.g. a non-global regex driving a
// while-loop's .exec() call and spinning forever — see check-regex-exec-global-flag.cjs
// and the incident note in 02a-ozon-yandex-import-cleanup.js). /health exposes this so the
// external watchdog (scripts/health-watchdog.cjs) can `pm2 restart` a process that pm2
// itself still reports as "online".
function touchEventLoopHeartbeat() {
  eventLoopHeartbeatAt = Date.now();
}

function isEventLoopHeartbeatHealthy(ageMs, maxAgeMs = eventLoopHeartbeatMaxAgeMs) {
  return Number(ageMs) < maxAgeMs;
}

function eventLoopHeartbeatStatus() {
  const ageMs = Date.now() - eventLoopHeartbeatAt;
  return {
    lastAt: new Date(eventLoopHeartbeatAt).toISOString(),
    ageMs,
    maxAgeMs: eventLoopHeartbeatMaxAgeMs,
    healthy: isEventLoopHeartbeatHealthy(ageMs, eventLoopHeartbeatMaxAgeMs),
  };
}

function startEventLoopHeartbeat() {
  const timer = setInterval(touchEventLoopHeartbeat, eventLoopHeartbeatIntervalMs);
  if (typeof timer.unref === "function") timer.unref();
  return timer;
}

function warehouseProductIndexFor(warehouse = {}) {
  const products = Array.isArray(warehouse.products) ? warehouse.products : [];
  if (warehouseMemoryProductIndexCache.products === products) return warehouseMemoryProductIndexCache.byId;
  const byId = new Map();
  products.forEach((product, index) => {
    if (product?.id) byId.set(String(product.id), index);
  });
  warehouseMemoryProductIndexCache = { products, byId };
  return byId;
}

startEventLoopHeartbeat();
