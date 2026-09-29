// Shop media upload — allows authenticated shop customers to upload images for reviews,
// and unauthenticated users to upload images/videos for unboxing submissions.
// POST /api/shop/upload-media — public (shopCors), returns { ok, url }
//   - images: jpg/png/webp up to 8 MB → stored in public/uploads/shop/
//   - videos: mp4/webm up to 50 MB (unboxing only)
//   - Rate limited: 10 uploads per 10 minutes per IP

const _shopUploadMulier = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024, files: 1 },
  fileFilter: (_req, file, cb) => {
    const allowed = /^(image\/(jpe?g|png|webp)|video\/(mp4|webm|quicktime))$/i;
    if (allowed.test(file.mimetype)) return cb(null, true);
    const err = new Error("Разрешены форматы: JPG, PNG, WEBP, MP4, WEBM");
    err.statusCode = 400;
    cb(err);
  },
});

const _shopUploadRateMap = new Map();
function _shopUploadRateLimit(request) {
  const ip = request.ip || request.socket?.remoteAddress || "unknown";
  const now = Date.now();
  const window = 10 * 60 * 1000;
  const max = 10;
  let entry = _shopUploadRateMap.get(ip);
  if (!entry || entry.resetAt < now) entry = { count: 0, resetAt: now + window };
  if (entry.count >= max) throw Object.assign(new Error("Слишком много загрузок. Попробуйте через 10 минут."), { statusCode: 429 });
  entry.count++;
  _shopUploadRateMap.set(ip, entry);
}

// file type by magic bytes → safe extension, or null
function _shopUploadSniffExt(buf) {
  if (!buf || buf.length < 12) return null;
  if (buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return "jpg";
  if (buf.slice(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return "png";
  if (buf.toString("latin1", 0, 4) === "RIFF" && buf.toString("latin1", 8, 12) === "WEBP") return "webp";
  if (buf.toString("latin1", 4, 8) === "ftyp") return buf.toString("latin1", 8, 10) === "qt" ? "mov" : "mp4";
  if (buf[0] === 0x1a && buf[1] === 0x45 && buf[2] === 0xdf && buf[3] === 0xa3) return "webm";
  return null;
}

const _shopUploadDir = path.join(__dirname, "../../public/uploads/shop");
const _shopFsPromises = require("fs").promises;

app.post("/api/shop/upload-media", shopCors, _shopUploadMulier.single("file"), async (request, response, next) => {
  try {
    _shopUploadRateLimit(request);
    if (!request.file) return response.status(400).json({ error: "Файл не получен." });

    const { buffer, mimetype, originalname } = request.file;
    const isVideo = mimetype.startsWith("video/");

    if (!isVideo && buffer.length > 8 * 1024 * 1024) {
      return response.status(413).json({ error: "Изображение не должно превышать 8 МБ." });
    }

    // The extension comes from the real file bytes, never from the client's name or mimetype:
    // "x.html" sent as image/png would otherwise be served as a page from davidsklad.ru (stored XSS
    // on the admin panel's origin).
    const ext = _shopUploadSniffExt(buffer);
    if (!ext || isVideo !== ["mp4", "mov", "webm"].includes(ext)) {
      return response.status(400).json({ error: "Разрешены форматы: JPG, PNG, WEBP, MP4, WEBM" });
    }
    const safeName = `${Date.now()}-${require("crypto").randomBytes(8).toString("hex")}.${ext}`;

    await _shopFsPromises.mkdir(_shopUploadDir, { recursive: true });
    await _shopFsPromises.writeFile(path.join(_shopUploadDir, safeName), buffer);

    const baseUrl = process.env.UPLOAD_BASE_URL || `https://davidsklad.ru`;
    response.json({ ok: true, url: `${baseUrl}/uploads/shop/${safeName}`, isVideo });
  } catch (error) {
    if (error.statusCode) return response.status(error.statusCode).json({ error: error.message });
    next(error);
  }
});
