// Telegram channel news import for magicvibes.ru
// Primary source: Bot API. The bot (TELEGRAM_NEWS_BOT_TOKEN) is an admin of the channel and receives
// channel_post / edited_channel_post via getUpdates. Only this gives the video files and the premium
// (custom) emoji positions. The getUpdates offset is kept on disk so a restart does not replay or lose
// updates (Telegram keeps undelivered updates for 24 h). Nothing else may poll or set a webhook on this token.
// Fallback when no bot token is set: the public web preview https://t.me/s/<channel> (photos + text only;
// Telegram stopped serving it for @magicvibes_ru on 2026-09-30).
// Media is downloaded and served as /uploads/tg-news/, premium emoji as /uploads/tg-news/emoji/.
// Env vars: TELEGRAM_CHANNEL (default @magicvibes_ru),
//           TELEGRAM_NEWS_BOT_TOKEN (bot that is an admin of the channel),
//           TELEGRAM_NEWS_TAG (optional — import only posts containing this hashtag, e.g. "#новости").

const _tgFs   = require("fs");
const _tgPath = require("path");
const _tgZlib = require("zlib");
const { execFile: _tgExecFile } = require("child_process");

const TG_CHANNEL_NAME = String(process.env.TELEGRAM_CHANNEL || "@magicvibes_ru").replace(/^@/, "").trim();
const TG_NEWS_TAG = String(process.env.TELEGRAM_NEWS_TAG || "").trim().toLowerCase();
const TG_PREVIEW_URL = `https://t.me/s/${encodeURIComponent(TG_CHANNEL_NAME)}`;
const TG_USER_AGENT = "Mozilla/5.0 (compatible; MagicVibesNews/2.0; +https://magicvibes.ru)";
const TG_NEWS_BOT_TOKEN = String(process.env.TELEGRAM_NEWS_BOT_TOKEN || "").trim();
// getCustomEmojiStickers works for any bot, so emoji can be fetched even before the news bot is configured.
const TG_EMOJI_BOT_TOKEN = TG_NEWS_BOT_TOKEN || String(process.env.TELEGRAM_BOT_TOKEN || "").trim();
const TG_BOT_FILE_LIMIT = 20 * 1024 * 1024; // Bot API getFile refuses bigger files

// Premium emoji of the channel pack. Posts imported without entity data (t.me/s scraping) get these
// substituted by their base emoji character.
const TG_DEFAULT_EMOJI_IDS = [
  "5317038468700875694", "5316642348162132283", "5316620168951018731", "5316945564263298099",
  "5316982071485310129", "5316596632530232757", "5319202926059626894", "5319126192173920610",
  "5318950304673215407", "5317045731490571607", "5319073806457808375", "5316552909763161987",
  "5319192467814261318", "5316815065976975475", "5316894630246130997", "5316884107576258830",
];

const tgNewsPhotoDir = _tgPath.join(process.cwd(), "public", "uploads", "tg-news");
const tgNewsEmojiDir = _tgPath.join(tgNewsPhotoDir, "emoji");
const tgNewsEmojiIndexFile = _tgPath.join(tgNewsEmojiDir, "index.json");
const tgNewsOffsetFile = _tgPath.join(process.cwd(), "data", "tg-news-bot-offset.json");
try { _tgFs.mkdirSync(tgNewsEmojiDir, { recursive: true }); } catch {}

let tgNewsImportRunning = false;

// ─── HTTP to Telegram ─────────────────────────────────────────────────────────
// From the production server IPv4 routes to t.me and the Telegram CDN (cdn*.telesco.pe) are blocked.
// t.me answers over IPv6; the CDN has no IPv6, so it goes through TELEGRAM_PROXY_URL (HTTP CONNECT proxy).
// Order: IPv6 direct → proxy (if configured) → system default.
const _tgHttp = require("http");
const _tgHttps = require("https");
const _tgTls = require("tls");

function _tgReadResponse(url, attempt, resolve, reject) {
  return (res) => {
    if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
      res.resume();
      attempt(new URL(res.headers.location, url).toString()).then(resolve, reject);
      return;
    }
    const chunks = [];
    res.on("data", (c) => chunks.push(c));
    res.on("end", () => resolve({ status: res.statusCode, contentType: String(res.headers["content-type"] || ""), body: Buffer.concat(chunks) }));
    res.on("error", reject);
  };
}

function _tgGetDirect(url, family, timeoutMs) {
  const attempt = (u) => _tgGetDirect(u, family, timeoutMs);
  return new Promise((resolve, reject) => {
    const req = _tgHttps.get(url, { family, headers: { "User-Agent": TG_USER_AGENT }, timeout: timeoutMs }, _tgReadResponse(url, attempt, resolve, reject));
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
  });
}

function _tgGetViaProxy(url, proxyUrl, timeoutMs) {
  const attempt = (u) => _tgGetViaProxy(u, proxyUrl, timeoutMs);
  return new Promise((resolve, reject) => {
    const proxy = new URL(proxyUrl);
    const target = new URL(url);
    const headers = { Host: `${target.hostname}:443` };
    if (proxy.username) {
      headers["Proxy-Authorization"] = "Basic " + Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString("base64");
    }
    const connectReq = _tgHttp.request({ host: proxy.hostname, port: Number(proxy.port) || 3128, method: "CONNECT", path: `${target.hostname}:443`, headers, timeout: timeoutMs });
    connectReq.on("connect", (res, socket) => {
      if (res.statusCode !== 200) { socket.destroy(); reject(new Error(`proxy CONNECT ${res.statusCode}`)); return; }
      const req = _tgHttps.get(url, {
        headers: { "User-Agent": TG_USER_AGENT },
        timeout: timeoutMs,
        agent: false,
        createConnection: () => _tgTls.connect({ socket, servername: target.hostname }),
      }, _tgReadResponse(url, attempt, resolve, reject));
      req.on("timeout", () => req.destroy(new Error("timeout")));
      req.on("error", reject);
    });
    connectReq.on("timeout", () => connectReq.destroy(new Error("proxy timeout")));
    connectReq.on("error", reject);
    connectReq.end();
  });
}

// v6TimeoutMs is a socket idle timeout: short by default to fail over fast, long for getUpdates long polling.
async function tgHttpGet(url, timeoutMs = 20000, v6TimeoutMs = Math.min(timeoutMs, 8000)) {
  const proxyUrl = String(process.env.TELEGRAM_PROXY_URL || "").trim();
  const attempts = [
    () => _tgGetDirect(url, 6, v6TimeoutMs),
    ...(proxyUrl ? [() => _tgGetViaProxy(url, proxyUrl, timeoutMs)] : []),
    () => _tgGetDirect(url, 0, timeoutMs),
  ];
  let lastError = null;
  for (const attempt of attempts) {
    try {
      return await attempt();
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError;
}

// ─── HTML helpers ─────────────────────────────────────────────────────────────
const _TG_ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " ", "#39": "'" };
function tgDecodeEntities(s) {
  return s.replace(/&(#x[0-9a-f]+|#\d+|[a-z0-9]+);/gi, (m, e) => {
    const k = e.toLowerCase();
    if (k.startsWith("#x")) return String.fromCodePoint(parseInt(k.slice(2), 16));
    if (k.startsWith("#")) return String.fromCodePoint(parseInt(k.slice(1), 10));
    return _TG_ENTITIES[k] ?? m;
  });
}

function tgHtmlToText(fragment) {
  const text = fragment
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div)>/gi, "\n")
    .replace(/<[^>]+>/g, "");
  return tgDecodeEntities(text).replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

/** Parses the t.me/s/<channel> page into posts (newest last, as Telegram renders them). */
function tgParsePreview(html) {
  const posts = [];
  const blocks = html.split(/(?=<div class="tgme_widget_message_wrap)/);
  for (const block of blocks) {
    const idMatch = /data-post="[^"/]+\/(\d+)"/.exec(block);
    if (!idMatch) continue;
    if (block.includes("service_message")) continue; // "Channel created", pinned notices, etc.
    const textMatch = /<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/.exec(block);
    const photoMatch = /tgme_widget_message_photo_wrap[^>]*background-image:url\('([^']+)'\)/.exec(block);
    const timeMatch = /<time[^>]*datetime="([^"]+)"/.exec(block);
    const text = textMatch ? tgHtmlToText(textMatch[1]) : "";
    if (!text && !photoMatch) continue;
    posts.push({
      messageId: Number(idMatch[1]),
      text,
      photoSrc: photoMatch ? photoMatch[1] : null,
      publishedAt: timeMatch ? new Date(timeMatch[1]) : new Date(),
    });
  }
  return posts;
}

// ─── Bot API ──────────────────────────────────────────────────────────────────
async function tgBotApi(token, method, params = {}, timeoutMs = 20000) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) qs.set(k, typeof v === "string" ? v : JSON.stringify(v));
  const res = await tgHttpGet(`https://api.telegram.org/bot${token}/${method}?${qs}`, timeoutMs, method === "getUpdates" ? timeoutMs : undefined);
  let data;
  try { data = JSON.parse(res.body.toString("utf8")); } catch { throw new Error(`${method}: HTTP ${res.status}`); }
  if (!data.ok) throw new Error(`${method}: ${data.description || res.status}`);
  return data.result;
}

/** Downloads a Bot API file into tg-news/<relName> (skipped when already there). Returns the public path. */
async function tgBotDownload(token, fileId, relName) {
  const target = _tgPath.join(tgNewsPhotoDir, relName);
  if (!_tgFs.existsSync(target)) {
    const file = await tgBotApi(token, "getFile", { file_id: fileId });
    if (!file.file_path) throw new Error("getFile: no file_path");
    const res = await tgHttpGet(`https://api.telegram.org/file/bot${token}/${file.file_path}`, 120000);
    if (res.status !== 200 || !res.body.length) throw new Error(`file download HTTP ${res.status}`);
    _tgFs.writeFileSync(target, res.body);
  }
  return `/uploads/tg-news/${relName.split(_tgPath.sep).join("/")}`;
}

function _tgExt(mime, fallback) {
  const m = String(mime || "");
  if (m.includes("png")) return "png";
  if (m.includes("webp")) return "webp";
  if (m.includes("jpeg") || m.includes("jpg")) return "jpg";
  if (m.includes("quicktime")) return "mov";
  if (m.includes("webm")) return "webm";
  if (m.includes("mp4")) return "mp4";
  return fallback;
}

// ─── Premium (custom) emoji ───────────────────────────────────────────────────
// index.json: { "<id>": { emoji, kind: "lottie"|"video"|"image", file, thumb } } — shared by api and worker.
let _tgEmojiIndex = null;
let _tgEmojiIndexMtime = 0;
function tgLoadEmojiIndex() {
  try {
    const mtime = _tgFs.statSync(tgNewsEmojiIndexFile).mtimeMs;
    if (!_tgEmojiIndex || mtime !== _tgEmojiIndexMtime) {
      _tgEmojiIndex = JSON.parse(_tgFs.readFileSync(tgNewsEmojiIndexFile, "utf8"));
      _tgEmojiIndexMtime = mtime;
    }
  } catch {
    if (!_tgEmojiIndex) _tgEmojiIndex = {};
  }
  return _tgEmojiIndex;
}

let _tgEmojiFetching = null;
async function tgEnsureEmoji(ids) {
  const index = tgLoadEmojiIndex();
  const missing = [...new Set(ids.map(String))].filter((id) => /^\d+$/.test(id) && !index[id]);
  if (!missing.length || !TG_EMOJI_BOT_TOKEN) return index;
  if (_tgEmojiFetching) return _tgEmojiFetching;
  _tgEmojiFetching = (async () => {
    try {
      for (let i = 0; i < missing.length; i += 200) {
        const stickers = await tgBotApi(TG_EMOJI_BOT_TOKEN, "getCustomEmojiStickers", { custom_emoji_ids: missing.slice(i, i + 200) });
        for (const st of stickers) {
          const id = String(st.custom_emoji_id);
          try {
            const kind = st.is_animated ? "lottie" : st.is_video ? "video" : "image";
            const raw = _tgPath.join("emoji", `${id}.${kind === "lottie" ? "tgs" : kind === "video" ? "webm" : "webp"}`);
            await tgBotDownload(TG_EMOJI_BOT_TOKEN, st.file_id, raw);
            let file = raw;
            if (kind === "lottie") {
              // .tgs is gzipped Lottie JSON; the browser player needs the plain JSON.
              file = _tgPath.join("emoji", `${id}.json`);
              _tgFs.writeFileSync(_tgPath.join(tgNewsPhotoDir, file), _tgZlib.gunzipSync(_tgFs.readFileSync(_tgPath.join(tgNewsPhotoDir, raw))));
            }
            let thumb = null;
            if (st.thumbnail?.file_id) {
              thumb = await tgBotDownload(TG_EMOJI_BOT_TOKEN, st.thumbnail.file_id, _tgPath.join("emoji", `${id}.thumb.webp`)).catch(() => null);
            }
            index[id] = { emoji: st.emoji || "", kind, file: `/uploads/tg-news/${file.split(_tgPath.sep).join("/")}`, thumb };
          } catch (err) {
            logger.warn("tg emoji download failed", { id, detail: err?.message || String(err) });
          }
        }
      }
      _tgFs.writeFileSync(tgNewsEmojiIndexFile, JSON.stringify(index, null, 1));
    } catch (err) {
      logger.warn("tg emoji fetch failed", { detail: err?.message || String(err) });
    } finally {
      _tgEmojiFetching = null;
    }
    return tgLoadEmojiIndex();
  })();
  return _tgEmojiFetching;
}

const _TG_VS16 = "️";
/** Finds the channel-pack emoji in plain text (for posts that came without entity data). */
function tgGuessEmojiEntities(text, index) {
  if (!text) return [];
  const byChar = new Map();
  for (const id of TG_DEFAULT_EMOJI_IDS) {
    const base = String(index[id]?.emoji || "").replace(/️/g, "");
    if (base && !byChar.has(base)) byChar.set(base, id);
  }
  const found = [];
  for (const [base, id] of byChar) {
    let from = 0;
    for (;;) {
      const at = text.indexOf(base, from);
      if (at < 0) break;
      const length = base.length + (text[at + base.length] === _TG_VS16 ? 1 : 0);
      found.push({ offset: at, length, id });
      from = at + length;
    }
  }
  found.sort((a, b) => a.offset - b.offset);
  return found.filter((e, i) => i === 0 || e.offset >= found[i - 1].offset + found[i - 1].length);
}

// ─── Import from Bot API updates ──────────────────────────────────────────────
function tgReadOffset() {
  try { return Number(JSON.parse(_tgFs.readFileSync(tgNewsOffsetFile, "utf8")).offset) || 0; } catch { return 0; }
}
function tgWriteOffset(offset) {
  try { _tgFs.writeFileSync(tgNewsOffsetFile, JSON.stringify({ offset, updatedAt: new Date().toISOString() })); } catch {}
}

/** Photo / video / GIF of one message → media item with a local copy (null when there is none or it is too big). */
// Telegram video thumbnails are 320 px and often show an empty first frame, so the poster is a
// full-size frame taken with ffmpeg (a third into the video, at most 1.5 s in). Null when ffmpeg is missing.
function tgVideoFramePoster(videoUrl, durationSec) {
  const prefix = "/uploads/tg-news/";
  if (!String(videoUrl || "").startsWith(prefix)) return Promise.resolve(null);
  const rel = videoUrl.slice(prefix.length);
  const src = _tgPath.join(tgNewsPhotoDir, rel);
  const outRel = rel.replace(/\.[a-z0-9]+$/i, ".frame.jpg");
  const out = _tgPath.join(tgNewsPhotoDir, outRel);
  if (_tgFs.existsSync(out)) return Promise.resolve(`/uploads/tg-news/${outRel}`);
  const at = Math.min(1.5, Math.max(0, (Number(durationSec) || 3) / 3)).toFixed(2);
  return new Promise((resolve) => {
    _tgExecFile("ffmpeg", ["-y", "-loglevel", "error", "-ss", at, "-i", src, "-frames:v", "1", "-vf", "scale=720:-2", "-q:v", "3", out],
      { timeout: 60000 }, (err) => resolve(err || !_tgFs.existsSync(out) ? null : `/uploads/tg-news/${outRel}`));
  });
}

/** Posts imported before posters were made from frames get one on the next poll. */
async function tgFillVideoPosters(prisma) {
  const posts = await prisma.telegramNewsPost.findMany({ orderBy: { publishedAt: "desc" }, take: 200, select: { id: true, media: true } });
  for (const post of posts) {
    if (!Array.isArray(post.media)) continue;
    let changed = false;
    const media = [];
    for (const m of post.media) {
      if ((m.type === "video" || m.type === "animation") && m.url && !String(m.poster || "").endsWith(".frame.jpg")) {
        const poster = await tgVideoFramePoster(m.url, m.duration);
        if (poster) { media.push({ ...m, poster }); changed = true; continue; }
      }
      media.push(m);
    }
    if (!changed) continue;
    const first = media[0];
    await prisma.telegramNewsPost.update({
      where: { id: post.id },
      data: { media, photoUrl: first.type === "photo" ? first.url : first.poster },
    });
  }
}

async function tgMessageMedia(msg) {
  const mid = msg.message_id;
  if (Array.isArray(msg.photo) && msg.photo.length) {
    const sizes = msg.photo.filter((p) => !p.file_size || p.file_size <= TG_BOT_FILE_LIMIT);
    const best = sizes[sizes.length - 1];
    if (!best) return null;
    const url = await tgBotDownload(TG_NEWS_BOT_TOKEN, best.file_id, `${TG_CHANNEL_NAME}-${mid}-${best.file_unique_id}.jpg`);
    return { type: "photo", url, width: best.width, height: best.height, mid, uid: best.file_unique_id };
  }
  const video = msg.video || msg.animation;
  if (video) {
    const type = msg.video ? "video" : "animation";
    let poster = null;
    const thumb = video.thumbnail || video.thumb;
    if (thumb?.file_id) {
      poster = await tgBotDownload(TG_NEWS_BOT_TOKEN, thumb.file_id, `${TG_CHANNEL_NAME}-${mid}-${video.file_unique_id}.poster.jpg`).catch(() => null);
    }
    let url = null;
    if (!video.file_size || video.file_size <= TG_BOT_FILE_LIMIT) {
      url = await tgBotDownload(TG_NEWS_BOT_TOKEN, video.file_id, `${TG_CHANNEL_NAME}-${mid}-${video.file_unique_id}.${_tgExt(video.mime_type, "mp4")}`);
    } else {
      logger.warn("tg news video too big for Bot API", { messageId: mid, size: video.file_size });
    }
    if (url) poster = (await tgVideoFramePoster(url, video.duration)) || poster;
    if (!url && !poster) return null;
    return { type: url ? type : "photo", url: url || poster, poster, width: video.width, height: video.height, duration: video.duration, mid, uid: video.file_unique_id };
  }
  return null;
}

// Telegram entity → stored form: premium emoji { id }, links { url }, formatting { t }.
const _TG_FORMAT = { bold: "b", italic: "i", underline: "u", strikethrough: "s", spoiler: "spoiler", code: "code", pre: "code", blockquote: "quote", expandable_blockquote: "quote" };
function tgStoredEntities(msg, text) {
  return (msg.entities || msg.caption_entities || []).flatMap((e) => {
    if (e.offset + e.length > text.length) return [];
    const at = { offset: e.offset, length: e.length };
    if (e.type === "custom_emoji" && e.custom_emoji_id) return [{ ...at, id: String(e.custom_emoji_id) }];
    if (e.type === "text_link" && /^https?:\/\//i.test(e.url || "")) return [{ ...at, url: e.url }];
    if (e.type === "url") {
      const raw = text.slice(e.offset, e.offset + e.length);
      return [{ ...at, url: /^https?:\/\//i.test(raw) ? raw : `https://${raw}` }];
    }
    if (e.type === "mention") return [{ ...at, url: `https://t.me/${text.slice(e.offset + 1, e.offset + e.length)}` }];
    if (_TG_FORMAT[e.type]) return [{ ...at, t: _TG_FORMAT[e.type] }];
    return [];
  });
}

async function tgImportChannelMessage(prisma, msg, stats) {
  const text = String(msg.text ?? msg.caption ?? "").slice(0, 10000);
  if (TG_NEWS_TAG && text && !text.toLowerCase().includes(TG_NEWS_TAG)) return;
  const entities = tgStoredEntities(msg, text);
  const emojiIds = entities.filter((e) => e.id).map((e) => e.id);
  if (emojiIds.length) await tgEnsureEmoji(emojiIds);

  let item = null;
  try {
    item = await tgMessageMedia(msg);
  } catch (err) {
    logger.warn("tg news media download failed", { messageId: msg.message_id, detail: err?.message || String(err) });
  }
  if (!text && !item) return;

  const groupId = msg.media_group_id ? String(msg.media_group_id) : null;
  const existing = (groupId && await prisma.telegramNewsPost.findFirst({ where: { mediaGroupId: groupId } }))
    || await prisma.telegramNewsPost.findUnique({ where: { messageId: msg.message_id } });

  // Album items arrive as separate messages; one of them carries the caption.
  const media = Array.isArray(existing?.media) ? existing.media.filter((m) => m.mid !== msg.message_id) : [];
  if (item) media.push(item);
  media.sort((a, b) => a.mid - b.mid);
  const first = media[0];
  const photoUrl = first ? (first.type === "photo" ? first.url : first.poster) : existing?.photoUrl;
  const hasCaption = Boolean(text) || !existing;
  const data = {
    ...(media.length ? { media } : {}),
    photoUrl: photoUrl || null,
    ...(groupId ? { mediaGroupId: groupId } : {}),
    ...(hasCaption ? { text, entities } : {}),
  };

  if (!existing) {
    await prisma.telegramNewsPost.create({
      data: { messageId: msg.message_id, publishedAt: new Date(msg.date * 1000), active: true, ...data },
    });
    stats.imported++;
  } else {
    await prisma.telegramNewsPost.update({ where: { id: existing.id }, data });
    stats.updated++;
  }
}

// ─── Backfill of old posts ────────────────────────────────────────────────────
// The Bot API cannot read channel history. An admin of the channel presses /start in the bot's private
// chat once; the bot then forwards old posts there (silently), takes the full message (media, premium
// emoji, links) from the forwardMessage response and deletes the forwarded copy right away.
const tgNewsBackfillFile = _tgPath.join(process.cwd(), "data", "tg-news-backfill.json");
function tgReadBackfill() {
  try { return JSON.parse(_tgFs.readFileSync(tgNewsBackfillFile, "utf8")); } catch { return {}; }
}
function tgWriteBackfill(state) {
  try { _tgFs.writeFileSync(tgNewsBackfillFile, JSON.stringify(state, null, 1)); } catch {}
}

let _tgChannelId = null;
async function tgChannelId() {
  if (!_tgChannelId) _tgChannelId = (await tgBotApi(TG_NEWS_BOT_TOKEN, "getChat", { chat_id: `@${TG_CHANNEL_NAME}` })).id;
  return _tgChannelId;
}

// ─── Channel admin tools in the bot's private chat ────────────────────────────
// Only TELEGRAM_NEWS_ADMIN_IDS may use it (default: the channel owner).
//   /new    — compose a post: message → buttons → now or at a time (Moscow) → preview → confirm.
//   /queue  — scheduled posts, cancel.
//   forward a channel post → put a button under it ("Где купить", product link, own button) or remove it.
//   /pin    — publish and pin the "where to buy" post.   /backfill — re-read old posts for the site.
// Scheduled posts live in data/tg-news-scheduled.json; the source message stays in the admin chat and is
// copied to the channel (copyMessage keeps formatting and media) by the worker long-poll loop.
const TG_SITE = "https://magicvibes.ru";
const TG_BUY_URL = `${TG_SITE}/buy?utm_source=telegram&utm_medium=button`;
const TG_NEWS_ADMIN_IDS = new Set(String(process.env.TELEGRAM_NEWS_ADMIN_IDS || "962443492").split(/[\s,]+/).filter(Boolean));
const TG_PIN_TEXT = [
  "🛍 Где купить Magic Vibes",
  "",
  "Оригинальная парфюмерия — на нашем сайте, в Яндекс Маркете и на Ozon.",
  "Под постами с ароматами — кнопка «Где купить»: сразу видно, где аромат есть в наличии.",
].join("\n");
const tgNewsScheduleFile = _tgPath.join(process.cwd(), "data", "tg-news-scheduled.json");
const tgAdminSessions = new Map(); // chatId → session (see tgHandlePrivateMessage)

const tgIsNewsAdmin = (userId) => TG_NEWS_ADMIN_IDS.has(String(userId));

function tgReadSchedule() {
  try { return JSON.parse(_tgFs.readFileSync(tgNewsScheduleFile, "utf8")); } catch { return []; }
}
function tgWriteSchedule(list) {
  _tgFs.writeFileSync(tgNewsScheduleFile, JSON.stringify(list, null, 1));
}

function tgSend(chatId, text, keyboard) {
  return tgBotApi(TG_NEWS_BOT_TOKEN, "sendMessage", {
    chat_id: String(chatId), text, disable_web_page_preview: "true",
    ...(keyboard ? { reply_markup: { inline_keyboard: keyboard } } : {}),
  });
}

const tgButtonsMarkup = (buttons) => ({ inline_keyboard: (buttons || []).map((b) => [{ text: b.text, url: b.url }]) });

// Moscow time (UTC+3, no DST) ↔ Date.
const TG_MSK_MS = 3 * 3600 * 1000;
function tgFormatMsk(ms) {
  const d = new Date(ms + TG_MSK_MS);
  const p = (n) => String(n).padStart(2, "0");
  return `${p(d.getUTCDate())}.${p(d.getUTCMonth() + 1)} ${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}
/** "18:00", "завтра 10:30", "02.10 18:00", "02.10.2026 18:00" → epoch ms (Moscow time) or null. */
function tgParseMsk(text) {
  const s = String(text || "").trim().toLowerCase();
  const m = /^(?:(завтра)\s+|(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+)?(\d{1,2})[:.](\d{2})$/.exec(s);
  if (!m) return null;
  const nowMsk = new Date(Date.now() + TG_MSK_MS);
  let y = nowMsk.getUTCFullYear(), mo = nowMsk.getUTCMonth(), d = nowMsk.getUTCDate();
  if (m[2]) { d = Number(m[2]); mo = Number(m[3]) - 1; if (m[4]) y = Number(m[4].length === 2 ? `20${m[4]}` : m[4]); }
  const hh = Number(m[5]), mm = Number(m[6]);
  if (hh > 23 || mm > 59 || mo > 11 || d < 1 || d > 31) return null;
  let at = Date.UTC(y, mo, d, hh, mm) - TG_MSK_MS;
  if (m[1]) at += 24 * 3600 * 1000;
  // A bare time that has already passed today means tomorrow.
  if (!m[1] && !m[2] && at <= Date.now()) at += 24 * 3600 * 1000;
  return at;
}

const TG_ADMIN_HELP = [
  "Что я умею:",
  "",
  "/new — новый пост: текст или фото/видео с подписью → кнопки → сейчас или по времени.",
  "/queue — запланированные посты.",
  "Переслать мне пост из канала — поставлю под ним кнопку или уберу её.",
  "/pin — опубликую и закреплю пост «Где купить».",
  "/auto — вкл/выкл автокнопку «Где купить» под каждым новым постом.",
  "",
  "Ссылки на товары со сравнением цен — magicvibes.ru/studio → «Ссылки «Купить»».",
].join("\n");

const TG_BUTTON_MENU = (prefix) => [
  [{ text: "🛍 + «Где купить» (все магазины)", callback_data: `${prefix}:buy` }],
  [{ text: "🛍 + «Купить этот товар»", callback_data: `${prefix}:product` }],
  [{ text: "✏️ + Своя кнопка", callback_data: `${prefix}:custom` }],
];

async function tgSetPostButtons(postId, buttons) {
  await tgBotApi(TG_NEWS_BOT_TOKEN, "editMessageReplyMarkup", {
    chat_id: String(await tgChannelId()), message_id: String(postId), reply_markup: tgButtonsMarkup(buttons),
  });
}

/** Draft menu: current buttons + actions. */
async function tgDraftMenu(chatId, s) {
  const list = s.draft.buttons.length ? s.draft.buttons.map((b, i) => `${i + 1}. ${b.text} → ${b.url}`).join("\n") : "пока нет";
  await tgSend(chatId, `Кнопки под постом:\n${list}\n\nДобавьте кнопку или переходите к публикации.`, [
    ...TG_BUTTON_MENU("d"),
    ...(s.draft.buttons.length ? [[{ text: "↩️ Убрать последнюю кнопку", callback_data: "d:pop" }]] : []),
    [{ text: "▶️ Дальше — когда публиковать", callback_data: "d:when" }],
    [{ text: "Отмена", callback_data: "d:cancel" }],
  ]);
}

// Same slug as shop-next lib/slug.ts toProductSlug — /buy/<slug> resolves the offerId after "--".
const _TG_TRANSLIT = { а: "a", б: "b", в: "v", г: "g", д: "d", е: "e", ё: "yo", ж: "zh", з: "z", и: "i", й: "y", к: "k", л: "l", м: "m", н: "n", о: "o", п: "p", р: "r", с: "s", т: "t", у: "u", ф: "f", х: "kh", ц: "ts", ч: "ch", ш: "sh", щ: "shch", ъ: "", ы: "y", ь: "", э: "e", ю: "yu", я: "ya" };
function tgProductSlug(name, offerId) {
  const nameSlug = String(name || "").toLowerCase().split("").map((c) => _TG_TRANSLIT[c] ?? c).join("")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").substring(0, 80).replace(/-+$/, "");
  return `${nameSlug}--${encodeURIComponent(offerId)}`;
}
const tgProductButton = (p) => ({ text: "🛍 Купить этот товар", url: `${TG_SITE}/buy/${tgProductSlug(p.name, p.offerId)}?utm_source=telegram&utm_medium=button` });

/** Shop catalogue search through the API process (the worker serves no HTTP). */
async function tgSearchProducts(q) {
  const port = Number(process.env.PORT || 3000);
  const res = await fetch(`http://127.0.0.1:${port}/api/shop/catalog?pageSize=6&q=${encodeURIComponent(q)}`, { signal: AbortSignal.timeout(15000) });
  if (!res.ok) throw new Error(`catalog ${res.status}`);
  const data = await res.json();
  return (data.products || []).slice(0, 6).map((p) => ({ name: p.name, offerId: p.offerId, priceRub: p.priceRub, inStock: p.inStock }));
}

/** Reads a button from text: product link, or "Текст | https://…" for an own button. */
function tgButtonFromText(kind, text) {
  if (kind === "product") {
    const url = (text.match(/https?:\/\/(www\.)?magicvibes\.ru\/\S+/i) || [])[0];
    return url ? { text: "🛍 Купить этот товар", url } : null;
  }
  const m = /^(.{1,60}?)\s*[|—-]\s*(https?:\/\/\S+)$/s.exec(text.trim());
  return m ? { text: m[1].trim(), url: m[2] } : null;
}

const TG_ASK = {
  product: "Напишите название товара (например: kilian good girl) — найду в каталоге. Или пришлите ссылку magicvibes.ru из studio.",
  custom: "Пришлите кнопку в формате:\nТекст кнопки | https://ссылка",
};

/** Product button: an existing post gets [Купить этот товар, Где купить]; a draft gets it appended. */
async function tgApplyProductButton(chatId, s, button) {
  s.awaiting = null;
  s.results = null;
  if (s.step === "existing") {
    await tgSetPostButtons(s.postId, [button, TG_BUY_BUTTON()]);
    tgAdminSessions.delete(chatId);
    await tgSend(chatId, `Готово: под постом №${s.postId} кнопки «🛍 Купить этот товар» и «🛍 Где купить».`);
  } else if (s.draft) {
    s.draft.buttons.push(button);
    await tgSend(chatId, "Кнопка «Купить этот товар» добавлена.");
    await tgDraftMenu(chatId, s);
  }
}

async function tgHandlePrivateMessage(msg) {
  if (msg.chat?.type !== "private" || !msg.from?.id) return;
  if (!tgIsNewsAdmin(msg.from.id)) {
    await tgSend(msg.chat.id, `Это служебный бот канала @${TG_CHANNEL_NAME}.`).catch(() => {});
    return;
  }
  const chatId = msg.chat.id;
  const text = String(msg.text || "").trim();
  const s = tgAdminSessions.get(chatId) || {};
  const origin = msg.forward_origin;
  const channelId = await tgChannelId();

  const backfill = tgReadBackfill();
  if (/^\/backfill\b/.test(text) || (/^\/start\b/.test(text) && !backfill.chatId)) {
    tgWriteBackfill({ ...backfill, chatId, done: false });
    await tgSend(chatId, "Загружаю старые посты канала на сайт: они на секунду появятся здесь и сразу удалятся.");
    return;
  }
  if (/^\/(start|help)\b/.test(text)) { tgAdminSessions.delete(chatId); await tgSend(chatId, TG_ADMIN_HELP); return; }
  if (/^\/cancel\b/.test(text)) { tgAdminSessions.delete(chatId); await tgSend(chatId, "Отменено."); return; }

  if (/^\/new\b/.test(text)) {
    tgAdminSessions.set(chatId, { step: "post" });
    await tgSend(chatId, "Пришлите пост одним сообщением: текст или фото/видео с подписью. Форматирование и ссылки сохранятся.");
    return;
  }
  if (/^\/queue\b/.test(text)) {
    const list = tgReadSchedule().sort((a, b) => a.at - b.at);
    if (!list.length) { await tgSend(chatId, "Запланированных постов нет."); return; }
    for (const job of list) {
      await tgBotApi(TG_NEWS_BOT_TOKEN, "copyMessage", {
        chat_id: String(chatId), from_chat_id: String(job.fromChatId), message_id: String(job.messageId),
        reply_markup: tgButtonsMarkup(job.buttons),
      }).catch(() => {});
      await tgSend(chatId, `⏰ Выйдет ${tgFormatMsk(job.at)} (МСК)`, [[{ text: "✖️ Отменить", callback_data: `q:del:${job.id}` }]]);
    }
    return;
  }
  if (/^\/auto\b/.test(text)) {
    const on = !tgReadSettings().autoButton;
    tgWriteSettings({ autoButton: on });
    await tgSend(chatId, on ? "✅ Автокнопка включена: под каждым новым постом появится «🛍 Где купить»." : "Автокнопка выключена. Кнопки можно ставить вручную — перешлите пост мне.");
    return;
  }
  if (/^\/pin\b/.test(text)) {
    await tgSend(chatId, `Так будет выглядеть закреп:\n\n———\n${TG_PIN_TEXT}\n[ 🛍 Где купить ]\n———`, [
      [{ text: "📌 Опубликовать и закрепить", callback_data: "pin:go" }],
      [{ text: "Отмена", callback_data: "pin:cancel" }],
    ]);
    return;
  }

  // Forwarded channel post → buttons for it.
  if (origin?.type === "channel" && origin.chat?.id === channelId && origin.message_id) {
    tgAdminSessions.set(chatId, { step: "existing", postId: origin.message_id });
    await tgSend(chatId, `Пост №${origin.message_id} в канале. Какую кнопку поставить под ним? (прежние кнопки заменятся)`, [
      ...TG_BUTTON_MENU("e"),
      [{ text: "✖️ Убрать кнопки", callback_data: "e:clear" }],
    ]);
    return;
  }

  // Waiting for a button link (existing post or draft).
  if (s.awaiting) {
    const button = tgButtonFromText(s.awaiting, text);
    if (!button && s.awaiting === "product" && text.length >= 2 && !text.startsWith("/")) {
      const found = await tgSearchProducts(text).catch(() => []);
      if (!found.length) { await tgSend(chatId, "Ничего не нашёл. Попробуйте иначе: бренд + название, например «tom ford oud wood»."); return; }
      s.results = found;
      await tgSend(chatId, "Какой товар?", found.map((p, i) => [{
        // shortened so the volume at the end of the name stays visible on the button
        text: `${p.name.replace(/\s*(парфюмерная|туалетная)\s+вода\s*/i, " ").replace(/\s+/g, " ").trim().slice(0, 52)} · ${Math.round(p.priceRub).toLocaleString("ru-RU")} ₽${p.inStock ? "" : " (нет)"}`,
        callback_data: `pick:${i}`,
      }]));
      return;
    }
    if (!button) { await tgSend(chatId, TG_ASK[s.awaiting]); return; }
    if (s.awaiting === "product") { await tgApplyProductButton(chatId, s, button); return; }
    const kind = s.awaiting;
    s.awaiting = null;
    if (s.step === "existing") {
      await tgSetPostButtons(s.postId, [button]);
      tgAdminSessions.delete(chatId);
      await tgSend(chatId, `Готово: кнопка «${button.text}» стоит под постом №${s.postId}.`);
    } else if (s.draft) {
      s.draft.buttons.push(button);
      await tgSend(chatId, kind === "product" ? "Кнопка на товар добавлена." : `Кнопка «${button.text}» добавлена.`);
      await tgDraftMenu(chatId, s);
    }
    return;
  }

  if (s.step === "post") {
    if (msg.media_group_id) { await tgSend(chatId, "Альбомы пока не поддерживаются — пришлите одно фото или видео с подписью."); return; }
    s.step = "buttons";
    s.draft = { fromChatId: chatId, messageId: msg.message_id, buttons: [] };
    await tgDraftMenu(chatId, s);
    return;
  }
  if (s.step === "time") {
    const at = tgParseMsk(text);
    if (!at) { await tgSend(chatId, "Не понял время. Примеры: 18:00 · завтра 10:30 · 02.10 18:00"); return; }
    s.draft.at = at;
    await tgPreviewDraft(chatId, s);
    return;
  }

  await tgSend(chatId, TG_ADMIN_HELP);
}

async function tgPreviewDraft(chatId, s) {
  s.step = "confirm";
  await tgSend(chatId, "Так пост будет выглядеть в канале:");
  await tgBotApi(TG_NEWS_BOT_TOKEN, "copyMessage", {
    chat_id: String(chatId), from_chat_id: String(s.draft.fromChatId), message_id: String(s.draft.messageId),
    reply_markup: tgButtonsMarkup(s.draft.buttons),
  });
  await tgSend(chatId, s.draft.at ? `Публикация: ${tgFormatMsk(s.draft.at)} (МСК)` : "Публикация: сейчас", [
    [{ text: s.draft.at ? "✅ Запланировать" : "✅ Опубликовать", callback_data: "d:go" }],
    [{ text: "✏️ Изменить кнопки", callback_data: "d:buttons" }, { text: "Отмена", callback_data: "d:cancel" }],
  ]);
}

async function tgPublishCopy(fromChatId, messageId, buttons) {
  const channelId = await tgChannelId();
  return tgBotApi(TG_NEWS_BOT_TOKEN, "copyMessage", {
    chat_id: String(channelId), from_chat_id: String(fromChatId), message_id: String(messageId),
    reply_markup: tgButtonsMarkup(buttons),
  });
}

/** Called from the long-poll loop: publishes due scheduled posts. */
async function tgRunScheduled() {
  const list = tgReadSchedule();
  const due = list.filter((j) => j.at <= Date.now());
  if (!due.length) return;
  tgWriteSchedule(list.filter((j) => j.at > Date.now()));
  for (const job of due) {
    try {
      const sent = await tgPublishCopy(job.fromChatId, job.messageId, job.buttons);
      await tgSend(job.fromChatId, `✅ Запланированный пост опубликован (№${sent.message_id}).`).catch(() => {});
    } catch (err) {
      logger.warn("tg scheduled post failed", { id: job.id, detail: err?.message || String(err) });
      await tgSend(job.fromChatId, `Не удалось опубликовать запланированный пост: ${err?.message || err}`).catch(() => {});
    }
  }
}

async function tgHandleCallback(cq) {
  const chatId = cq.message?.chat?.id;
  const answer = (text) => tgBotApi(TG_NEWS_BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cq.id, ...(text ? { text } : {}) }).catch(() => {});
  if (!chatId || !tgIsNewsAdmin(cq.from?.id)) return answer("Только для админа канала");
  const [kind, action, arg] = String(cq.data || "").split(":");
  const s = tgAdminSessions.get(chatId) || {};
  try {
    if (kind === "e" && s.step === "existing") { // buttons for an existing channel post
      if (action === "buy") {
        await tgSetPostButtons(s.postId, [{ text: "🛍 Где купить", url: TG_BUY_URL }]);
        tgAdminSessions.delete(chatId);
        await answer("Кнопка добавлена");
        await tgSend(chatId, `Готово: кнопка «Где купить» стоит под постом №${s.postId}.`);
      } else if (action === "clear") {
        await tgSetPostButtons(s.postId, []);
        tgAdminSessions.delete(chatId);
        await answer("Кнопки убраны");
      } else if (action === "product" || action === "custom") {
        s.awaiting = action;
        await answer();
        await tgSend(chatId, TG_ASK[action]);
      }
    } else if (kind === "d" && s.draft) { // new post draft
      if (action === "buy") {
        s.draft.buttons.push({ text: "🛍 Где купить", url: TG_BUY_URL });
        await answer("Добавлено");
        await tgDraftMenu(chatId, s);
      } else if (action === "product" || action === "custom") {
        s.awaiting = action;
        await answer();
        await tgSend(chatId, TG_ASK[action]);
      } else if (action === "pop") {
        s.draft.buttons.pop();
        await answer("Убрано");
        await tgDraftMenu(chatId, s);
      } else if (action === "buttons") {
        s.step = "buttons";
        await answer();
        await tgDraftMenu(chatId, s);
      } else if (action === "when") {
        s.step = "when";
        await answer();
        await tgSend(chatId, "Когда публикуем?", [
          [{ text: "🚀 Сейчас", callback_data: "d:now" }],
          [{ text: "⏰ Выбрать время", callback_data: "d:time" }],
        ]);
      } else if (action === "now") {
        s.draft.at = null;
        await answer();
        await tgPreviewDraft(chatId, s);
      } else if (action === "time") {
        s.step = "time";
        await answer();
        await tgSend(chatId, `Напишите время по Москве. Примеры: 18:00 · завтра 10:30 · 02.10 18:00\nСейчас ${tgFormatMsk(Date.now())}.`);
      } else if (action === "go" && s.step === "confirm") {
        const d = s.draft;
        tgAdminSessions.delete(chatId);
        if (d.at && d.at > Date.now() + 30 * 1000) {
          const list = tgReadSchedule();
          list.push({ id: `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, fromChatId: d.fromChatId, messageId: d.messageId, buttons: d.buttons, at: d.at });
          tgWriteSchedule(list);
          await answer("Запланировано");
          await tgSend(chatId, `⏰ Запланировано на ${tgFormatMsk(d.at)} (МСК). Не удаляйте исходное сообщение из этого чата до публикации. Очередь — /queue`);
        } else {
          const sent = await tgPublishCopy(d.fromChatId, d.messageId, d.buttons);
          await answer("Опубликовано");
          await tgSend(chatId, `✅ Опубликовано в канале (№${sent.message_id}).`);
        }
      } else if (action === "cancel") {
        tgAdminSessions.delete(chatId);
        await answer("Отменено");
        await tgSend(chatId, "Черновик удалён.");
      }
    } else if (kind === "pick" && Array.isArray(s.results) && s.results[Number(action)]) {
      await answer();
      await tgApplyProductButton(chatId, s, tgProductButton(s.results[Number(action)]));
    } else if (kind === "q" && action === "del") {
      tgWriteSchedule(tgReadSchedule().filter((j) => j.id !== arg));
      await answer("Отменено");
      await tgBotApi(TG_NEWS_BOT_TOKEN, "editMessageText", { chat_id: String(chatId), message_id: String(cq.message.message_id), text: "✖️ Публикация отменена" }).catch(() => {});
    } else if (kind === "pin") {
      if (action === "go") {
        const channelId = await tgChannelId();
        const post = await tgSend(channelId, TG_PIN_TEXT, [[{ text: "🛍 Где купить", url: TG_BUY_URL }]]);
        await tgBotApi(TG_NEWS_BOT_TOKEN, "pinChatMessage", { chat_id: String(channelId), message_id: String(post.message_id), disable_notification: "true" });
        await answer("Опубликовано и закреплено");
        await tgSend(chatId, `Готово: пост №${post.message_id} опубликован и закреплён.`);
      } else {
        await answer("Отменено");
      }
    } else {
      await answer("Это меню устарело — начните заново");
    }
  } catch (err) {
    const detail = err?.message || String(err);
    await answer("Не получилось");
    await tgSend(chatId, /not modified/i.test(detail) ? "Под постом уже такие кнопки." : `Не получилось: ${detail}`).catch(() => {});
  }
}

/** Re-imports posts 1..(last known + 30) through forwarding. Returns the number of posts read. */
async function tgBackfillOldPosts(prisma, stats) {
  const state = tgReadBackfill();
  if (!state.chatId) return 0;
  const channelId = await tgChannelId();
  const last = await prisma.telegramNewsPost.findFirst({ orderBy: { messageId: "desc" }, select: { messageId: true } });
  const lastKnown = last?.messageId || 0;
  let read = 0;
  let missesInRow = 0;
  let group = null; // { startId, date } — album items share the same date and only one has a caption
  for (let id = 1; id <= lastKnown + 30; id++) {
    let fwd;
    try {
      fwd = await tgBotApi(TG_NEWS_BOT_TOKEN, "forwardMessage", {
        chat_id: String(state.chatId), from_chat_id: String(channelId), message_id: String(id), disable_notification: "true",
      });
    } catch (err) {
      const detail = err?.message || String(err);
      if (/bot was blocked|chat not found|user is deactivated/i.test(detail)) {
        tgWriteBackfill({ ...state, chatId: null, error: detail });
        throw err;
      }
      group = null;
      if (id > lastKnown && ++missesInRow >= 15) break;
      continue; // deleted post or service message
    }
    missesInRow = 0;
    await tgBotApi(TG_NEWS_BOT_TOKEN, "deleteMessage", { chat_id: String(state.chatId), message_id: String(fwd.message_id) }).catch(() => {});
    const date = fwd.forward_origin?.date || fwd.forward_date || fwd.date;
    const hasMedia = Boolean(fwd.photo || fwd.video || fwd.animation);
    if (!(hasMedia && group && group.date === date)) group = hasMedia ? { startId: id, date } : null;
    const msg = {
      ...fwd,
      message_id: id,
      date,
      chat: { id: channelId, username: TG_CHANNEL_NAME, type: "channel" },
      media_group_id: group ? `bf-${group.startId}` : undefined,
    };
    read++;
    await tgImportChannelMessage(prisma, msg, stats);
  }
  tgWriteBackfill({ ...state, done: true, doneAt: new Date().toISOString(), read });
  logger.info("telegram news backfill done", { read, ...stats });
  return read;
}

// ─── Auto "Где купить" button ─────────────────────────────────────────────────
// Every new channel post (published by hand or from Telegram's own scheduler, so premium emoji survive) gets
// the button right after it appears. Skipped: posts that already have buttons, albums (Telegram allows no
// buttons there) and service messages. /auto in the admin chat toggles it. On the first run the button is
// also put under all posts already known to the site.
const tgNewsSettingsFile = _tgPath.join(process.cwd(), "data", "tg-news-settings.json");
function tgReadSettings() {
  try { return { autoButton: true, ...JSON.parse(_tgFs.readFileSync(tgNewsSettingsFile, "utf8")) }; } catch { return { autoButton: true }; }
}
function tgWriteSettings(next) {
  _tgFs.writeFileSync(tgNewsSettingsFile, JSON.stringify({ ...tgReadSettings(), ...next }, null, 1));
}
const TG_BUY_BUTTON = () => ({ text: "🛍 Где купить", url: TG_BUY_URL });

function tgWantsAutoButton(msg) {
  if (!tgReadSettings().autoButton) return false;
  if (msg.reply_markup?.inline_keyboard?.length) return false;
  if (msg.media_group_id || msg.pinned_message || msg.new_chat_title || msg.new_chat_photo || msg.delete_chat_photo) return false;
  return Boolean(msg.text || msg.caption || msg.photo || msg.video || msg.animation || msg.document || msg.audio || msg.voice);
}

async function tgAutoButton(msg) {
  try {
    await tgSetPostButtons(msg.message_id, [TG_BUY_BUTTON()]);
  } catch (err) {
    logger.warn("tg auto button failed", { messageId: msg.message_id, detail: err?.message || String(err) });
  }
}

/** One-time: the button under every post the site knows (albums fail silently). */
async function tgButtonsForExistingPosts(prisma) {
  if (tgReadSettings().existingDone || !tgReadSettings().autoButton) return;
  const posts = await prisma.telegramNewsPost.findMany({ select: { messageId: true, media: true }, orderBy: { messageId: "asc" } });
  let done = 0;
  for (const p of posts) {
    if (Array.isArray(p.media) && p.media.length > 1) continue; // album
    try { await tgSetPostButtons(p.messageId, [TG_BUY_BUTTON()]); done++; } catch (err) {
      if (!/not modified/i.test(err?.message || "")) logger.warn("tg existing post button failed", { messageId: p.messageId, detail: err?.message || String(err) });
    }
  }
  tgWriteSettings({ existingDone: true, existingDoneAt: new Date().toISOString(), existingButtons: done });
  logger.info("tg buttons added to existing posts", { done, total: posts.length });
}

/** One getUpdates round (long polling up to longPollSec) — channel posts, admin chat, button presses. */
async function pollTelegramNewsBot(prisma, stats, longPollSec = 0) {
  let offset = tgReadOffset();
  const updates = await tgBotApi(TG_NEWS_BOT_TOKEN, "getUpdates", {
    offset: String(offset), limit: "100", timeout: String(longPollSec),
    allowed_updates: ["channel_post", "edited_channel_post", "message", "callback_query"],
  }, (longPollSec + 15) * 1000);
  for (const update of updates) {
    const msg = update.channel_post || update.edited_channel_post;
    try {
      if (msg && String(msg.chat?.username || "").toLowerCase() === TG_CHANNEL_NAME.toLowerCase()) {
        stats.found++;
        await tgImportChannelMessage(prisma, msg, stats);
        if (update.channel_post && tgWantsAutoButton(msg)) await tgAutoButton(msg);
      } else if (update.message) {
        await tgHandlePrivateMessage(update.message);
      } else if (update.callback_query) {
        await tgHandleCallback(update.callback_query);
      }
    } catch (err) {
      logger.warn("tg news bot update failed", { updateId: update.update_id, detail: err?.message || String(err) });
    }
    offset = update.update_id + 1;
    tgWriteOffset(offset);
  }
  const backfill = tgReadBackfill();
  if (backfill.chatId && !backfill.done) stats.backfilled = await tgBackfillOldPosts(prisma, stats);
  return updates.length;
}

/** Worker: keeps a long poll open so the bot answers admins right away and posts arrive within seconds. */
async function tgNewsBotLoop() {
  for (;;) {
    const stats = { found: 0, imported: 0, updated: 0 };
    try {
      const prisma = getPrisma();
      if (prisma) await tgButtonsForExistingPosts(prisma);
      if (prisma) await pollTelegramNewsBot(prisma, stats, 25);
      await tgRunScheduled();
      if (stats.imported || stats.updated) logger.info("telegram news synced", stats);
    } catch (err) {
      logger.warn("telegram news bot poll failed", { detail: err?.code || err?.message || String(err) });
      await new Promise((r) => setTimeout(r, 15000));
    }
  }
}

// ─── Import from the t.me/s web preview (fallback) ───────────────────────────
// The Telegram CDN is unreachable from the server (no IPv6, proxy dead). wsrv.nl — a public
// image proxy outside RU — fetches it for us; the file is then kept locally like before.
async function _tgGetViaImageProxy(src) {
  const r = await fetch(`https://wsrv.nl/?url=${encodeURIComponent(src)}&n=-1`, {
    headers: { "User-Agent": TG_USER_AGENT }, signal: AbortSignal.timeout(20000),
  });
  return { status: r.status, contentType: String(r.headers.get("content-type") || ""), body: Buffer.from(await r.arrayBuffer()) };
}

async function tgDownloadPhoto(src, messageId) {
  try {
    let res = await tgHttpGet(src, 10000).catch(() => null);
    if (!res || res.status !== 200 || !res.body.length || !/^image\//.test(res.contentType)) {
      res = await _tgGetViaImageProxy(src);
    }
    if (res.status !== 200 || !res.body.length || !/^image\//.test(res.contentType)) return null;
    const type = res.contentType;
    const ext = type.includes("png") ? "png" : type.includes("webp") ? "webp" : "jpg";
    const filename = `${TG_CHANNEL_NAME}-${messageId}.${ext}`;
    _tgFs.writeFileSync(_tgPath.join(tgNewsPhotoDir, filename), res.body);
    return `/uploads/tg-news/${filename}`;
  } catch (err) {
    logger.warn("tg photo download failed", { messageId, detail: err?.message || String(err) });
    return null;
  }
}

async function pollTelegramNewsPreview(prisma, stats) {
  const res = await tgHttpGet(TG_PREVIEW_URL, 30000);
  if (res.status !== 200) {
    logger.warn("telegram news fetch failed", { status: res.status, url: TG_PREVIEW_URL });
    return;
  }
  const posts = tgParsePreview(res.body.toString("utf8"))
    .filter((p) => !TG_NEWS_TAG || p.text.toLowerCase().includes(TG_NEWS_TAG));
  stats.found = posts.length;

  for (const post of posts) {
    const text = post.text.slice(0, 10000);
    const existing = await prisma.telegramNewsPost.findUnique({ where: { messageId: post.messageId } });
    // A local copy is preferred; when the Telegram CDN is unreachable from the server, the CDN link
    // itself is stored and replaced on later polls once a download succeeds.
    const hasLocalPhoto = Boolean(existing?.photoUrl && existing.photoUrl.startsWith("/uploads/"));
    let photoUrl = existing?.photoUrl || null;
    if (post.photoSrc && !hasLocalPhoto) {
      photoUrl = (await tgDownloadPhoto(post.photoSrc, post.messageId)) || post.photoSrc;
    }
    if (!existing) {
      await prisma.telegramNewsPost.create({
        data: { messageId: post.messageId, text, photoUrl, publishedAt: post.publishedAt, active: true },
      });
      stats.imported++;
    } else if (existing.entities == null && (existing.text !== text || existing.photoUrl !== photoUrl)) {
      // Posts that came from the Bot API (entities set) keep their exact text — emoji offsets depend on it.
      await prisma.telegramNewsPost.update({ where: { id: existing.id }, data: { text, photoUrl } });
      stats.updated++;
    }
  }
}

// ─── Import ───────────────────────────────────────────────────────────────────
async function pollTelegramNews() {
  if (tgNewsImportRunning) return { skipped: true };
  tgNewsImportRunning = true;
  const stats = { found: 0, imported: 0, updated: 0, source: TG_NEWS_BOT_TOKEN ? "bot" : "preview" };
  try {
    const prisma = getPrisma();
    if (!prisma) return stats;
    await tgEnsureEmoji(TG_DEFAULT_EMOJI_IDS);
    // With a bot, updates are read by the worker long poll (tgNewsBotLoop); a second getUpdates caller
    // would get "409 Conflict", so this run only does maintenance.
    if (TG_NEWS_BOT_TOKEN) await tgFillVideoPosters(prisma);
    else await pollTelegramNewsPreview(prisma, stats);
    if (stats.imported || stats.updated) logger.info("telegram news synced", stats);
    return stats;
  } catch (err) {
    logger.warn("telegram news poll failed", { detail: err?.code || err?.message || String(err) });
    return { ...stats, error: err?.message || String(err) };
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
    // The shop lives on another domain, so media needs an absolute URL on this API host.
    // Telegram CDN links are not exposed: the CDN is blocked for most visitors in Russia,
    // so such a post is shown as text until a local copy is downloaded.
    const apiBase = (process.env.SHOP_API_BASE_URL || "https://davidsklad.ru").replace(/\/+$/, "");
    const abs = (u) => (u && u.startsWith("/uploads/") ? apiBase + u : null);
    const index = tgLoadEmojiIndex();
    if (TG_DEFAULT_EMOJI_IDS.some((id) => !index[id])) void tgEnsureEmoji(TG_DEFAULT_EMOJI_IDS);

    const usedEmoji = new Set();
    const out = posts.map((p) => {
      const entities = (Array.isArray(p.entities) ? p.entities : tgGuessEmojiEntities(p.text || "", index))
        .filter((e) => e.url || e.t || index[e.id]);
      entities.forEach((e) => e.id && usedEmoji.add(e.id));
      const media = (Array.isArray(p.media) ? p.media : [])
        .map((m) => ({ type: m.type, url: abs(m.url), poster: abs(m.poster), width: m.width || null, height: m.height || null }))
        .filter((m) => m.url);
      return { id: p.id, text: p.text, photoUrl: abs(p.photoUrl), media, entities, publishedAt: p.publishedAt };
    });
    const emoji = {};
    for (const id of usedEmoji) {
      const e = index[id];
      emoji[id] = {
        kind: e.kind,
        // Lottie JSON is fetched by the browser, so it goes through the API route (CORS headers).
        url: e.kind === "lottie" ? `${apiBase}/api/shop/news/emoji/${id}.json` : abs(e.file),
        thumb: abs(e.thumb),
        alt: e.emoji,
      };
    }
    response.json({ ok: true, posts: out, emoji });
  } catch (error) { next(error); }
});

app.get("/api/shop/news/emoji/:id.json", (request, response) => {
  const id = String(request.params.id || "");
  const entry = /^\d+$/.test(id) ? tgLoadEmojiIndex()[id] : null;
  if (!entry || entry.kind !== "lottie") return response.status(404).json({ ok: false });
  response.setHeader("Cache-Control", "public, max-age=604800, immutable");
  response.type("application/json");
  response.sendFile(_tgPath.join(process.cwd(), "public", entry.file));
});

// ─── Admin: manual import trigger (waits for the result) ─────────────────────
app.post("/api/shop/admin/news/import", requireAdmin, async (request, response, next) => {
  try {
    const stats = await pollTelegramNews();
    response.json({ ok: true, ...stats });
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

// ─── Scheduler ────────────────────────────────────────────────────────────────
// Worker only: bot long poll (continuous) + maintenance / t.me/s fallback every 5 minutes.
if (backgroundJobsEnabled) {
  if (TG_NEWS_BOT_TOKEN) setTimeout(() => void tgNewsBotLoop(), 10_000);
  setInterval(() => void pollTelegramNews(), 5 * 60 * 1000);
  // Initial poll after 15s to let server fully start
  setTimeout(() => void pollTelegramNews(), 15_000);
}
