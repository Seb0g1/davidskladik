function isOzonRateLimitError(error) {
  const message = String(error?.message || "").toLowerCase();
  const status = Number(error?.statusCode || error?.status || 0);
  return status === 429
    || message.includes("rate limit")
    || message.includes("too many request")
    || message.includes("resourceexhausted")
    || message.includes("items limit")
    || message.includes("limit exceeded");
}

function ozonRetryDelayMs(attempt, response = null) {
  const retryAfter = Number(response?.headers?.get?.("retry-after") || 0);
  if (Number.isFinite(retryAfter) && retryAfter > 0) return Math.min(30_000, retryAfter * 1000);
  const base = Math.max(500, Number(process.env.OZON_RATE_LIMIT_RETRY_MS || 1200) || 1200);
  return Math.min(30_000, base * attempt * attempt);
}

function isOzonResourceExhaustedError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("resourceexhausted")
    || combined.includes("items limit")
    || combined.includes("limit exceeded")
    || combined.includes("acquire limit");
}

function isOzonPerItemPriceLimitError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("price-batch-set")
    && (
      combined.includes("per item")
      || combined.includes("items limit")
      || combined.includes("acquire limit per item")
      || combined.includes("10")
      || combined.includes("раз в час")
    );
}

function isOzonOldPriceLessError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("old price is less than price")
    || (combined.includes("old_price") && combined.includes("less") && combined.includes("price"))
    || (combined.includes("old price") && combined.includes("less") && combined.includes("price"));
}

function isOzonPriceDiscountQuarantineError(error) {
  const message = String(error?.message || error?.detail || "").toLowerCase();
  const ozonMessage = String(error?.ozon?.message || error?.ozon?.error || "").toLowerCase();
  const combined = `${message} ${ozonMessage}`;
  return combined.includes("скидк")
    || combined.includes("discount")
    || combined.includes("90%")
    || combined.includes("карантин")
    || combined.includes("quarantine");
}

function needsOzonOldPriceEscalation(error) {
  return isOzonOldPriceLessError(error) || isOzonPriceDiscountQuarantineError(error);
}

function getOzonPriceBatchSize() {
  // Ozon /v1/product/import/prices accepts up to 1000 prices per request.
  return Math.max(1, Math.min(1000, Number(process.env.OZON_PRICE_BATCH_SIZE || 100) || 100));
}

function getOzonPriceBatchDelayMs() {
  // Pause between batch requests (not per SKU). Kept short when batching many items per call.
  return Math.max(0, Number(process.env.OZON_PRICE_BATCH_DELAY_MS || 400) || 400);
}

function getOzonPriceBatchMaxAttempts() {
  return Math.max(1, Number(process.env.OZON_PRICE_BATCH_MAX_ATTEMPTS || 6) || 6);
}

function getOzonPriceBatchBackoffMs() {
  return Math.max(500, Number(process.env.OZON_PRICE_BATCH_BACKOFF_MS || 2500) || 2500);
}

// Limited-concurrency pool with paced start times. The previous strictly-serial promise
// chain meant every Ozon request waited for the previous response + interval, capping the
// whole app at ~1-2 requests/sec — imports, syncs and verification all shared that lane.
let ozonActiveRequests = 0;
const ozonRequestWaiters = [];
function enqueueOzonRequest(task) {
  const minIntervalMs = Math.max(0, Number(process.env.OZON_REQUEST_MIN_INTERVAL_MS || 450) || 450);
  const maxConcurrent = Math.max(1, Math.min(12, Number(process.env.OZON_REQUEST_CONCURRENCY || 4) || 4));
  return new Promise((resolve, reject) => {
    const attempt = async () => {
      if (ozonActiveRequests >= maxConcurrent) {
        ozonRequestWaiters.push(attempt);
        return;
      }
      ozonActiveRequests += 1;
      try {
        const waitMs = Math.max(0, ozonLastRequestAt + minIntervalMs - Date.now());
        if (waitMs > 0) await sleep(waitMs);
        ozonLastRequestAt = Date.now();
        resolve(await task());
      } catch (error) {
        reject(error);
      } finally {
        ozonActiveRequests -= 1;
        const next = ozonRequestWaiters.shift();
        if (next) next();
      }
    };
    attempt();
  });
}

