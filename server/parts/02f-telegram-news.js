// Telegram channel news import for magicvibes.ru
// Polls @magicvibes_ru via Bot API, imports posts with #новости hashtag.
// Photos are downloaded and served as /uploads/tg-news/.
// Env vars: TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID (default -1001234567890)

const _tgFs   = require("fs");
const _tgPath = require("path");

const TG_TOKEN   = process.env.TELEGRAM_BOT_TOKEN || "8691183442:AAFoWdQuuNHwYdFUAoJmv3ipvFOVnRkJ83c";
const TG_CHANNEL = process.env.TELEGRAM_CHANNEL   || "@magicvibes_ru";
const TG_API     = `https://api.telegram.org/bot${TG_TOKEN}`;
const TG_NEWS_TAG = "#новости";

const tgNewsPhotoDir = _tgPath.join(process.cwd(), "public", "uploads", "tg-news");
try { _tgFs.mkdirSync(tgNewsPhotoDir, { recursive: true }); } catch {}

let tgNewsLastOffset = 0;
let tgNewsImportRunning = false;

// ─── Download Telegram file ───────────────────────────────────────────────────
async function tgDownloadPhoto(fileId) {
  try {
    const infoRes = await fetch(`${TG_API}/getFile?file_id=${encodeURIComponent(fileId)}`);
    const info = await infoRes.json();
    if (!info.ok || !info.result?.file_path) return null;
    const tgUrl = `https://api.telegram.org/file/bot${TG_TOKEN}/${info.result.file_path}`;
    const ext = info.result.file_path.split(".").pop() || "jpg";
    const filename = `${fileId.slice(-20)}.${ext}`;
    const destPath = _tgPath.join(tgNewsPhotoDir, filename);
    // Download
    const fileRes = await fetch(tgUrl);
    if (!fileRes.ok) return null;
    const buffer = Buffer.from(await fileRes.arrayBuffer());
    _tgFs.writeFileSync(destPath, buffer);
    return `/uploads/tg-news/${filename}`;
  } catch (err) {
    logger.warn("tg photo download failed", { detail: err?.message || String(err) });
    return null;
  }
}

// ─── Import single channel_post update ───────────────────────────────────────
async function processTgChannelPost(post) {
  const text = post.text || post.caption || "";
  // Only import posts with #новости
  if (!text.toLowerCase().includes(tgNewsTag())) return;

  const prisma = getPrisma();
  const messageId = post.message_id;
  const existing = await prisma.telegramNewsPost.findUnique({ where: { messageId } });
  if (existing) return;

  let photoUrl = null;
  if (Array.isArray(post.photo) && post.photo.length > 0) {
    // Get largest photo
    const best = post.photo.reduce((a, b) => (b.file_size || 0) > (a.file_size || 0) ? b : a);
    photoUrl = await tgDownloadPhoto(best.file_id);
  }

  await prisma.telegramNewsPost.create({
    data: {
      messageId,
      text: text.slice(0, 10000),
      photoUrl,
      publishedAt: new Date((post.date || 0) * 1000),
      active: true,
    },
  });
  logger.info("tg news imported", { messageId, photoUrl: Boolean(photoUrl) });
}

function tgNewsTag() { return TG_NEWS_TAG.toLowerCase(); }

// ─── Poll Telegram getUpdates ─────────────────────────────────────────────────
async function pollTelegramNews() {
  if (tgNewsImportRunning) return;
  tgNewsImportRunning = true;
  try {
    const url = `${TG_API}/getUpdates?offset=${tgNewsLastOffset}&limit=100&allowed_updates=["channel_post"]&timeout=0`;
    const res = await fetch(url);
    if (!res.ok) return;
    const data = await res.json();
    if (!data.ok || !Array.isArray(data.result)) return;
    for (const update of data.result) {
      tgNewsLastOffset = Math.max(tgNewsLastOffset, update.update_id + 1);
      if (update.channel_post) {
        await processTgChannelPost(update.channel_post).catch((e) =>
          logger.warn("tg post process error", { detail: e?.message || String(e) })
        );
      }
    }
  } catch (err) {
    logger.warn("telegram poll failed", { detail: err?.message || String(err) });
  } finally {
    tgNewsImportRunning = false;
  }
}

// ─── Public API: get news ─────────────────────────────────────────────────────
app.get("/api/shop/news", async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const limit = Math.min(50, Number(request.query.limit || 12) || 12);
    const posts = await prisma.telegramNewsPost.findMany({
      where: { active: true },
      orderBy: { publishedAt: "desc" },
      take: limit,
    });
    response.json({ ok: true, posts: posts.map((p) => ({
      id: p.id,
      text: p.text,
      photoUrl: p.photoUrl,
      publishedAt: p.publishedAt,
    })) });
  } catch (error) { next(error); }
});

// ─── Admin: manual import trigger ────────────────────────────────────────────
app.post("/api/shop/admin/news/import", requireAdmin, async (request, response, next) => {
  try {
    void pollTelegramNews();
    response.json({ ok: true, message: "Import triggered" });
  } catch (error) { next(error); }
});

// ─── Admin: toggle post active ────────────────────────────────────────────────
app.patch("/api/shop/admin/news/:id", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const post = await prisma.telegramNewsPost.update({
      where: { id: request.params.id },
      data: { active: request.body.active !== false },
    });
    response.json({ ok: true, post });
  } catch (error) { next(error); }
});

// ─── Admin: list news ─────────────────────────────────────────────────────────
app.get("/api/shop/admin/news", requireAdmin, async (request, response, next) => {
  try {
    const prisma = getPrisma();
    const posts = await prisma.telegramNewsPost.findMany({
      orderBy: { publishedAt: "desc" },
      take: 100,
    });
    response.json({ ok: true, posts });
  } catch (error) { next(error); }
});

// ─── Scheduler: poll every 30 minutes ────────────────────────────────────────
if (backgroundJobsEnabled) {
  setInterval(() => void pollTelegramNews(), 30 * 60 * 1000);
  // Initial poll after 15s to let server fully start
  setTimeout(() => void pollTelegramNews(), 15_000);
}
