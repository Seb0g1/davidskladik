app.use(express.json({ limit: "1mb" }));
app.use(compression({ threshold: 1024 }));
// Базовые защитные заголовки: панель не должна открываться во фрейме чужого
// сайта (кликджекинг), а браузер не должен угадывать MIME-типы ответов.
const hstsEnabled = String(process.env.PUBLIC_BASE_URL || "").startsWith("https://");
app.use((_request, response, next) => {
  response.setHeader("X-Content-Type-Options", "nosniff");
  response.setHeader("X-Frame-Options", "SAMEORIGIN");
  response.setHeader("Referrer-Policy", "same-origin");
  if (hstsEnabled) response.setHeader("Strict-Transport-Security", "max-age=15552000");
  next();
});
app.use((request, response, next) => {
  activeHttpRequests += 1;
  let released = false;
  const releaseHttpSlot = () => {
    if (released) return;
    released = true;
    activeHttpRequests = Math.max(0, activeHttpRequests - 1);
  };
  const startedAt = process.hrtime.bigint();
  response.on("finish", () => {
    releaseHttpSlot();
    const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
    if (elapsedMs < slowRequestThresholdMs && response.statusCode < 500) return;
    const entry = {
      at: new Date().toISOString(),
      method: request.method,
      path: request.originalUrl || request.url,
      statusCode: response.statusCode,
      elapsedMs: Math.round(elapsedMs),
    };
    recentSlowRequests.push(entry);
    while (recentSlowRequests.length > recentSlowRequestsMax) recentSlowRequests.shift();
    if (elapsedMs >= slowRequestThresholdMs) {
      logger.warn("slow request", entry);
    }
  });
  response.on("close", releaseHttpSlot);
  next();
});

function isSafeMethod(method) {
  return method === "GET" || method === "HEAD" || method === "OPTIONS";
}

function allowedOriginHosts(request) {
  const hosts = new Set();
  const publicBase = String(process.env.PUBLIC_BASE_URL || "").trim();
  if (publicBase) {
    try { hosts.add(new URL(publicBase).host); } catch (_e) { /* ignore */ }
  }
  const requestHost = request.get("host");
  if (requestHost) hosts.add(requestHost);
  return hosts;
}

function csrfGuard(request, response, next) {
  if (process.env.CSRF_BYPASS_FOR_TESTS === "true") return next();
  if (isSafeMethod(request.method)) return next();
  if (!request.path.startsWith("/api/")) return next();
  if (request.path === "/api/login") return next();
  // Shop API uses Bearer JWT (not cookies) — CSRF guard does not apply here
  if (request.path.startsWith("/api/shop/")) return next();
  const origin = request.get("origin");
  const referer = request.get("referer");
  const allowed = allowedOriginHosts(request);
  const headerHost = (raw) => { if (!raw) return null; try { return new URL(raw).host; } catch (_e) { return null; } };
  const originHost = headerHost(origin);
  const refererHost = headerHost(referer);
  if (originHost) {
    if (!allowed.has(originHost)) return response.status(403).json({ error: "Запрос отклонён: чужой Origin" });
    return next();
  }
  if (refererHost) {
    if (!allowed.has(refererHost)) return response.status(403).json({ error: "Запрос отклонён: чужой Referer" });
    return next();
  }
  if (process.env.NODE_ENV === "production") {
    return response.status(403).json({ error: "Запрос отклонён: отсутствует Origin/Referer" });
  }
  return next();
}

app.use(csrfGuard);

const uploadImages = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024,
    files: 10,
  },
  fileFilter: (_request, file, callback) => {
    if (/^image\/(png|jpe?g|webp|gif)$/i.test(file.mimetype)) return callback(null, true);
    const error = new Error("Можно загружать только изображения PNG, JPG, WEBP или GIF.");
    error.statusCode = 400;
    return callback(error);
  },
});

const pool = mysql.createPool({
  host: process.env.PM_DB_HOST,
  port: Number(process.env.PM_DB_PORT || 3306),
  user: process.env.PM_DB_USER,
  password: process.env.PM_DB_PASSWORD,
  database: process.env.PM_DB_NAME,
  waitForConnections: true,
  connectionLimit: 0,
  maxIdle: Math.max(2, pmDbPoolSize),
  connectTimeout: pmDbConnectTimeoutMs,
  enableKeepAlive: true,
  keepAliveInitialDelay: 60000,
  decimalNumbers: true,
  dateStrings: true,
});

// READ UNCOMMITTED: our SELECTs are snapshot-based and do not need strict isolation.
// This prevents our heavy full-scan queries from acquiring shared locks that could
// delay Ginger PM's INSERT/UPDATE operations on the same tables.
pool.on("connection", (connection) => {
  connection.query("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", (err) => {
    if (err) logger.warn("PM pool: failed to set READ UNCOMMITTED", { detail: err?.message });
  });
});

// Retry once on stale-connection errors (ECONNRESET, Query inactivity timeout, etc.).
// mysql2 returns stale connections from the pool when keepAlive fails to prevent
// the server from closing the connection; a single retry gets a fresh one.
const pmConnectionRetryErrors = new Set(["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "PROTOCOL_CONNECTION_LOST"]);
const originalPoolQuery = pool.query.bind(pool);
pool.query = async function pmPoolQueryWithRetry(sqlOrOptions, values) {
  try {
    return values !== undefined ? await originalPoolQuery(sqlOrOptions, values) : await originalPoolQuery(sqlOrOptions);
  } catch (error) {
    const isStale = pmConnectionRetryErrors.has(error?.code) || /inactivity timeout|connection lost|gone away/i.test(error?.message || "");
    if (!isStale) throw error;
    return values !== undefined ? originalPoolQuery(sqlOrOptions, values) : originalPoolQuery(sqlOrOptions);
  }
};
