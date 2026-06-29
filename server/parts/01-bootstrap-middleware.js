app.use(express.json({ limit: "1mb" }));
app.use(compression({ threshold: 1024 }));
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
  connectionLimit: pmDbPoolSize,
  connectTimeout: pmDbConnectTimeoutMs,
  enableKeepAlive: true,
  keepAliveInitialDelay: 60000,
  decimalNumbers: true,
  dateStrings: true,
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
