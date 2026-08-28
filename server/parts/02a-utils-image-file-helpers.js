function imageExtension(file) {
  const byMime = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
  };
  return byMime[String(file.mimetype || "").toLowerCase()] || path.extname(file.originalname || "").toLowerCase() || ".img";
}

function imageMimeFromPath(filePath) {
  const extension = path.extname(filePath || "").toLowerCase();
  const byExtension = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
  };
  return byExtension[extension] || "image/png";
}

function supportedOpenAiSourceMime(mimeType) {
  return /^image\/(png|jpe?g|webp)$/i.test(cleanText(mimeType));
}

function fileNameFromImageMime(mimeType, fallback = "source.png") {
  const mime = cleanText(mimeType).toLowerCase();
  if (mime.includes("jpeg") || mime.includes("jpg")) return "source.jpg";
  if (mime.includes("webp")) return "source.webp";
  if (mime.includes("png")) return "source.png";
  return fallback;
}

function aiImageExtension(format = openaiImageFormat) {
  const normalized = cleanText(format || "png").toLowerCase();
  if (normalized === "jpeg" || normalized === "jpg") return ".jpg";
  if (normalized === "webp") return ".webp";
  return ".png";
}

function chunkArray(items, size) {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function parseMoneyValue(value) {
  if (value == null || value === "") return null;
  const raw = typeof value === "string" ? value.replace(/\s/g, "").replace(",", ".") : value;
  const number = Number(raw);
  if (!Number.isFinite(number) || number <= 0) return null;
  return number;
}

function pickOzonCabinetListedPrice(details = {}) {
  if (!details || typeof details !== "object") return null;
  return (
    details.marketingSellerPrice ||
    details.currentPrice ||
    details.marketingPrice ||
    details.retailPrice ||
    null
  );
}

function roundPrice(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number) || number <= 0) return 0;
  return Math.round(number);
}

function shouldSkipWarehousePriceSend({
  currentPrice,
  nextPrice,
  minDiffRub = 0,
  minDiffPct = 0,
  force = false,
} = {}) {
  const current = roundPrice(currentPrice);
  const next = roundPrice(nextPrice);
  if (!next) return { skip: true, reason: "not_ready" };
  if (force) return { skip: false, reason: null };
  const diffRub = Math.abs(next - current);
  if (current > 0 && diffRub <= 0) return { skip: true, reason: "unchanged" };
  const minRub = Math.max(0, Number(minDiffRub || 0) || 0);
  if (current > 0 && minRub > 0 && diffRub < minRub) {
    return { skip: true, reason: "min_diff_rub", diffRub, minDiffRub: minRub };
  }
  const minPct = Math.max(0, Number(minDiffPct || 0) || 0);
  const diffPct = current > 0 ? (diffRub / current) * 100 : 100;
  if (current > 0 && minPct > 0 && diffPct < minPct) {
    return { skip: true, reason: "min_diff_pct", diffPct, minDiffPct: minPct };
  }
  return { skip: false, reason: null };
}

function priceHistoryDedupeWindowMs() {
  return Math.max(60_000, Number(process.env.PRICE_HISTORY_DEDUPE_WINDOW_MS || 15 * 60_000) || 15 * 60_000);
}

function normalizePriceHistoryComparable(entry = {}) {
  return {
    productId: cleanText(entry.productId || entry.id) || "",
    marketplace: cleanText(entry.marketplace || "ozon").toLowerCase() || "ozon",
    target: cleanText(entry.target || entry.marketplace) || "",
    offerId: cleanText(entry.offerId || entry.offer_id),
    oldPrice: entry.oldPrice === undefined || entry.oldPrice === null ? null : roundPrice(entry.oldPrice),
    newPrice: roundPrice(entry.newPrice ?? entry.price ?? 0),
    status: cleanText(entry.status || (entry.error ? "failed" : "success")).toLowerCase(),
    error: cleanText(entry.error || ""),
    at: toDateOrNull(entry.createdAt || entry.at) || null,
  };
}

function isDuplicatePriceHistoryEntry(previous = {}, next = {}, { now = new Date(), windowMs = priceHistoryDedupeWindowMs() } = {}) {
  const left = normalizePriceHistoryComparable(previous);
  const right = normalizePriceHistoryComparable(next);
  if (!right.offerId || !right.newPrice) return false;
  if (left.productId && right.productId && left.productId !== right.productId) return false;
  if (left.marketplace !== right.marketplace) return false;
  if (left.target !== right.target) return false;
  if (left.offerId !== right.offerId) return false;
  if (left.oldPrice !== right.oldPrice) return false;
  if (left.newPrice !== right.newPrice) return false;
  if (left.status !== right.status) return false;
  if (left.error !== right.error) return false;
  if (!left.at) return false;
  return Math.abs(now.getTime() - left.at.getTime()) <= windowMs;
}

function parseBooleanSetting(value, fallback = true) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  const text = String(value).trim().toLowerCase();
  if (["false", "0", "off", "no", "нет"].includes(text)) return false;
  if (["true", "1", "on", "yes", "да"].includes(text)) return true;
  return fallback;
}
