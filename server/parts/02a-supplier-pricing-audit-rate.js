function cloneAuditValue(value) {
  return value == null ? null : JSON.parse(JSON.stringify(value));
}

function requestUsername(request) {
  return cleanText(request?.session?.username || "system") || "system";
}

// Returns null (no conflict) unless ANOTHER USER changed the card since the client loaded it.
// Background automation (price/stock sweeps, reconciler) bumps `updatedAt` constantly, which
// used to trip a false "изменено другим пользователем" 409. We now key the check on
// `userUpdatedAt` (bumped only by user edits): a conflict exists only when the product's
// userUpdatedAt is STRICTLY NEWER than the updatedAt the client loaded. Products with no
// userUpdatedAt yet (never user-edited) fall back to the old exact-updatedAt match so brand
// new cards still get basic protection, but only when the client also sent a links signature
// that no longer matches (i.e. a real divergence), otherwise background churn is ignored.
function warehouseUserEditIsNewer(product, expected) {
  const userUpdatedAt = cleanText(product?.userUpdatedAt || "");
  if (!userUpdatedAt) return null; // unknown — let caller decide via fallback
  const a = new Date(userUpdatedAt).getTime();
  const b = new Date(expected).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return cleanText(product?.updatedAt || "") !== expected;
  return a > b;
}

function productConflict(product, expectedUpdatedAt) {
  const expected = typeof expectedUpdatedAt === "object" && expectedUpdatedAt !== null
    ? cleanText(expectedUpdatedAt.expectedUpdatedAt || expectedUpdatedAt.updatedAt || "")
    : cleanText(expectedUpdatedAt || "");
  const expectedLinksSignature = typeof expectedUpdatedAt === "object" && expectedUpdatedAt !== null
    ? cleanText(expectedUpdatedAt.expectedLinksSignature || expectedUpdatedAt.linksSignature || "")
    : "";
  if (!expected) return null;
  const currentLinksSignature = warehouseProductLinksSignature(product);
  // Primary rule: only a newer USER edit is a real conflict (ignores background updatedAt churn).
  const userNewer = warehouseUserEditIsNewer(product, expected);
  if (userNewer === false) return null;
  if (userNewer === null) {
    // No userUpdatedAt recorded yet: ignore pure background churn — only flag a conflict when
    // the links actually diverged from what the client expected.
    if (cleanText(product?.updatedAt || "") === expected) return null;
    if (!expectedLinksSignature || expectedLinksSignature === currentLinksSignature) return null;
  } else if (expectedLinksSignature && expectedLinksSignature === currentLinksSignature) {
    return null;
  }
  return {
    id: product.id,
    offerId: product.offerId || product.ozon?.offerId || product.yandex?.offerId || "",
    expectedUpdatedAt: expected,
    expectedLinksSignature: expectedLinksSignature || undefined,
    currentLinksSignature: expectedLinksSignature ? currentLinksSignature : undefined,
    currentUpdatedAt: product.updatedAt || null,
    freshProduct: normalizeWarehouseProduct(product),
  };
}

function productLocksFromRequest(body = {}) {
  const locks = new Map();
  if (body.expectedUpdatedAt && body.productId) {
    locks.set(String(body.productId), {
      expectedUpdatedAt: cleanText(body.expectedUpdatedAt),
      expectedLinksSignature: cleanText(body.expectedLinksSignature || body.linksSignature || ""),
    });
  }
  if (body.expectedUpdatedAt && Array.isArray(body.productIds) && body.productIds.length === 1) {
    locks.set(String(body.productIds[0]), {
      expectedUpdatedAt: cleanText(body.expectedUpdatedAt),
      expectedLinksSignature: cleanText(body.expectedLinksSignature || body.linksSignature || ""),
    });
  }
  for (const item of Array.isArray(body.optimisticLocks) ? body.optimisticLocks : []) {
    const id = cleanText(item?.id);
    if (id) {
      locks.set(id, {
        expectedUpdatedAt: cleanText(item?.expectedUpdatedAt || ""),
        expectedLinksSignature: cleanText(item?.expectedLinksSignature || item?.linksSignature || ""),
      });
    }
  }
  return locks;
}

function collectProductConflicts(products = [], locks = new Map()) {
  return products
    .map((product) => productConflict(product, locks.get(String(product.id))))
    .filter(Boolean);
}

function collectProductConflictsExceptBackground(products = [], locks = new Map(), { mergeOnly = false } = {}) {
  const conflicts = collectProductConflicts(products, locks);
  return conflicts;
}

function canIgnoreStaleLinkSaveConflict(product = {}, links = [], lock = {}) {
  if (!lock?.expectedUpdatedAt) return false;
  const existingLinks = compactWarehouseLinks(product.links || []);
  if (!existingLinks.length) return true;
  return warehouseProductHasLinks(product, links);
}

function conflictResponse(response, conflicts) {
  return response.status(409).json({
    error: "Конфликт обновления: карточка уже изменена другим пользователем.",
    code: conflicts.length > 1 ? "warehouse_bulk_conflict" : "warehouse_product_conflict",
    conflicts,
  });
}

function normalizeSupplierName(value) {
  return String(value || "").trim().toLowerCase();
}

function isSupplierInactiveDateDue(isoDate, now = new Date()) {
  const value = cleanText(isoDate);
  if (!value) return false;
  const dateOnly = value.slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateOnly)) return false;
  const today = now.toISOString().slice(0, 10);
  return dateOnly <= today;
}

function applySupplierAutoReactivate(warehouse, now = new Date()) {
  const suppliers = Array.isArray(warehouse?.suppliers) ? warehouse.suppliers : [];
  const reactivated = [];
  for (const supplier of suppliers) {
    if (!supplier?.stopped) continue;
    if (!supplier.inactiveUntil) continue;
    if (!isSupplierInactiveDateDue(supplier.inactiveUntil, now)) continue;
    supplier.stopped = false;
    supplier.stopReason = "";
    supplier.inactiveComment = "";
    supplier.inactiveUntil = null;
    supplier.inactiveUntilUnknown = false;
    supplier.updatedAt = new Date().toISOString();
    reactivated.push({ id: supplier.id, name: supplier.name });
  }
  return reactivated;
}

async function readCachedExchangeRate() {
  try {
    return JSON.parse(await fs.readFile(exchangeRatePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

async function writeExchangeRate(rate) {
  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(exchangeRatePath, JSON.stringify(rate, null, 2), "utf8");
}

function parseApiResponse(text) {
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (_error) {
    return { raw: text.slice(0, 1000) };
  }
}

function summarizeApiErrorPayload(data = {}, fallback = "API error") {
  const parts = [];
  const push = (value) => {
    const text = cleanText(value);
    if (text && !parts.includes(text)) parts.push(text);
  };
  const visit = (value, depth = 0) => {
    if (!value || parts.length >= 12 || depth > 5) return;
    if (typeof value !== "object") {
      push(value);
      return;
    }
    push(value.message);
    push(value.errorText);
    // WB 429 кладёт человекочитаемое в title/detail («too many requests, Limited
    // by global limiter…»), а code — хеш запроса, бесполезный оператору.
    push(value.title);
    push(value.detail);
    // WB шлёт { error: true, errorText: "..." } — булев флаг в тексте бесполезен.
    if (typeof value.error !== "boolean") push(value.error);
    if (!/^[0-9a-f]{10,}$/i.test(cleanText(value.code))) push(value.code);
    push(value.field);
    push(value.sku || value.offerId || value.offer_id);
    for (const key of ["errors", "details", "result", "results", "warnings", "params", "raw"]) {
      const nested = value[key];
      if (Array.isArray(nested)) {
        for (const item of nested) visit(item, depth + 1);
      } else if (nested && typeof nested === "object") {
        visit(nested, depth + 1);
      } else {
        push(nested);
      }
    }
  };
  visit(data);
  return parts.slice(0, 6).join("; ").slice(0, 1000) || fallback;
}

function apiPayloadHasErrors(data = {}) {
  if (!data || typeof data !== "object") return false;
  const status = cleanText(data.status || data.result?.status).toUpperCase();
  if (status === "ERROR" || status === "FAILED") return true;
  if (Array.isArray(data.errors) && data.errors.length) return true;
  if (Array.isArray(data.result?.errors) && data.result.errors.length) return true;
  if (Array.isArray(data.results) && data.results.some(apiPayloadHasErrors)) return true;
  return false;
}

async function getUsdRate({ force = false } = {}) {
  const cached = await readCachedExchangeRate();
  if (!force && cached?.rate && Date.now() - new Date(cached.fetchedAt).getTime() < exchangeRateTtlMs) {
    return { ...cached, cached: true };
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(process.env.USD_RATE_TIMEOUT_MS || 3000) || 3000));
    const response = await fetch("https://www.cbr-xml-daily.ru/daily_json.js", { signal: controller.signal })
      .finally(() => clearTimeout(timeout));
    if (!response.ok) throw new Error(`CBR rate request failed: ${response.status}`);
    const data = await response.json();
    const rate = Number(data.Valute?.USD?.Value);
    if (!Number.isFinite(rate) || rate <= 0) throw new Error("USD rate was not found in CBR response");

    const payload = {
      rate: Number(rate.toFixed(4)),
      source: "CBR",
      fetchedAt: new Date().toISOString(),
      validForHours: 6,
    };
    await writeExchangeRate(payload);
    return { ...payload, cached: false };
  } catch (error) {
    if (cached?.rate) return { ...cached, cached: true, warning: error.message };
    return {
      rate: Number(process.env.DEFAULT_USD_RATE || 95),
      source: "fallback",
      fetchedAt: new Date().toISOString(),
      validForHours: 6,
      cached: false,
      warning: error.message,
    };
  }
}


