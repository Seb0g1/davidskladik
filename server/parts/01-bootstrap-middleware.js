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
  decimalNumbers: true,
  dateStrings: true,
});
