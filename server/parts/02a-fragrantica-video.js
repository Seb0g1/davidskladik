// Видеообложка Ozon для карточек со страницы «Фрагрантика» (complex_attributes 100002 / 21845).
//
// Из уже готовых фото карточки (флакон с премиум-фоном → «Пирамида аромата» стиля магазина →
// «Характеристики» → крупный план) ffmpeg собирает ролик 1080×1440 (3:4, как фото карточки), без звука,
// ~12 с: медленный наезд на каждое фото, мягкие переходы, в конце снова флакон — обложка крутится
// по кругу без рывка. Файл: uploads/fragrantica/cards/<id>-cover-<style>-v1.mp4 (публичный — Ozon
// скачивает по ссылке). Нет ffmpeg (FFMPEG_PATH) — обложки просто нет, карточка создаётся как раньше.

const fragVideoFs = require("fs");
const { execFile: fragVideoExecFile } = require("child_process");

const FRAG_VIDEO_W = 1080;
const FRAG_VIDEO_H = 1440;
const FRAG_VIDEO_FPS = 30;
const FRAG_VIDEO_SLIDE_S = 3.2;
const FRAG_VIDEO_FADE_S = 0.6;
const fragranticaFfmpegPath = process.env.FFMPEG_PATH || "ffmpeg";
let fragranticaFfmpegChecked = null;

function fragranticaVideoCoverFile(perfumeId, style) {
  return `${Number(perfumeId)}-cover-${String(style).replace(/[^a-z0-9]/gi, "")}-v1.mp4`;
}

/** ffmpeg filter graph: per slide a slow zoom (alternating in / out), then xfade between slides. */
function buildFragranticaCoverFilter(count, { slide = FRAG_VIDEO_SLIDE_S, fade = FRAG_VIDEO_FADE_S, fps = FRAG_VIDEO_FPS, w = FRAG_VIDEO_W, h = FRAG_VIDEO_H } = {}) {
  const frames = Math.round(slide * fps);
  const parts = [];
  for (let i = 0; i < count; i += 1) {
    // upscale first so zoompan moves smoothly (no 1-px jitter), then pad to exact 3:4 on white
    const zoom = i % 2 === 0
      ? `min(1+0.07*on/${frames},1.07)`
      : `max(1.07-0.07*on/${frames},1)`;
    parts.push(
      `[${i}:v]scale=${w * 2}:${h * 2}:force_original_aspect_ratio=decrease,pad=${w * 2}:${h * 2}:(ow-iw)/2:(oh-ih)/2:color=white,setsar=1,`
      + `zoompan=z='${zoom}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=${frames}:s=${w}x${h}:fps=${fps},format=yuv420p,setpts=PTS-STARTPTS[s${i}]`,
    );
  }
  let last = "s0";
  for (let i = 1; i < count; i += 1) {
    const offset = (slide - fade) * i;
    const out = i === count - 1 ? "vout" : `x${i}`;
    parts.push(`[${last}][s${i}]xfade=transition=${i === count - 1 ? "fade" : "smoothleft"}:duration=${fade}:offset=${offset.toFixed(3)}[${out}]`);
    last = out;
  }
  if (count === 1) parts.push("[s0]null[vout]");
  return parts.join(";");
}

function runFragranticaFfmpeg(args, timeoutMs = 180_000) {
  return new Promise((resolve, reject) => {
    const child = fragVideoExecFile(fragranticaFfmpegPath, args, { timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024 }, (error, _stdout, stderr) => {
      if (error) {
        error.message = `${error.message}\n${String(stderr || "").slice(-800)}`;
        reject(error);
      } else {
        resolve();
      }
    });
    // background work: the site and the shop get the CPU first
    try { if (child?.pid) require("os").setPriority(child.pid, 10); } catch { /* not allowed — fine */ }
  });
}

async function fragranticaFfmpegAvailable() {
  if (fragranticaFfmpegChecked !== null) return fragranticaFfmpegChecked;
  try {
    await runFragranticaFfmpeg(["-hide_banner", "-version"], 15_000);
    fragranticaFfmpegChecked = true;
  } catch {
    fragranticaFfmpegChecked = false;
  }
  return fragranticaFfmpegChecked;
}

/**
 * Make (or reuse) the video cover of one shop style. images — local file paths in slide order (2..5).
 * Returns the public URL or null.
 */
async function ensureFragranticaVideoCover(perfumeId, style, images = [], { refresh = false } = {}) {
  const file = fragranticaVideoCoverFile(perfumeId, style);
  const target = fragranticaMediaPath("cards", file);
  if (!refresh && fragVideoFs.existsSync(target)) return fragranticaMediaUrl("cards", file);
  const slides = images.filter((p) => p && fragVideoFs.existsSync(p)).slice(0, 5);
  if (slides.length < 2) return null;
  if (!(await fragranticaFfmpegAvailable())) return null;
  // back to the bottle at the end: the cover loops on Ozon without a jump
  const sequence = [...slides, slides[0]];
  const tmp = `${target}.tmp.mp4`;
  const args = ["-hide_banner", "-loglevel", "error", "-y"];
  for (const image of sequence) args.push("-i", image);
  args.push(
    "-filter_complex", buildFragranticaCoverFilter(sequence.length),
    "-map", "[vout]", "-an",
    // veryfast + 2 threads: a few times quicker than «medium» on all cores, same look at this bitrate
    "-c:v", "libx264", "-preset", "veryfast", "-threads", String(FRAG_VIDEO_THREADS), "-crf", "21", "-profile:v", "high", "-pix_fmt", "yuv420p",
    "-r", String(FRAG_VIDEO_FPS), "-movflags", "+faststart",
    tmp,
  );
  try {
    await runFragranticaFfmpeg(args);
    await fragVideoFs.promises.rename(tmp, target);
    return fragranticaMediaUrl("cards", file);
  } catch (error) {
    logger.warn("fragrantica video cover failed", { perfumeId, style, detail: String(error?.message || error).slice(0, 600) });
    await fragVideoFs.promises.unlink(tmp).catch(() => {});
    return null;
  }
}

/** Absolute URL of a made cover, or "" — used when the Ozon item is built. */
function fragranticaVideoCoverUrl(perfumeId, style) {
  const file = fragranticaVideoCoverFile(perfumeId, style);
  return fragVideoFs.existsSync(fragranticaMediaPath("cards", file)) ? fragranticaAbsoluteUrl(fragranticaMediaUrl("cards", file)) : "";
}

// ─── Очередь видеообложек ───────────────────────────────────────────────────
// Сборка черновика не ждёт ролик: он рендерится в фоне (FRAGRANTICA_VIDEO_PARALLEL, деф. 2), отправка
// карточки ждёт свой ролик (fragranticaVideoCoversReady).
const FRAG_VIDEO_THREADS = Math.max(1, Number(process.env.FRAGRANTICA_VIDEO_THREADS || 2) || 2);
const fragranticaVideoParallelPinned = Number(process.env.FRAGRANTICA_VIDEO_PARALLEL) || 0;
const fragranticaVideoParallelNow = () => fragranticaVideoParallelPinned || (isNightWorkWindow() ? 3 : 1);
const fragranticaVideoJobs = new Map();
const fragranticaVideoWaiting = [];
let fragranticaVideoActive = 0;

function fragranticaVideoPump() {
  while (fragranticaVideoActive < fragranticaVideoParallelNow() && fragranticaVideoWaiting.length) {
    const job = fragranticaVideoWaiting.shift();
    fragranticaVideoActive += 1;
    ensureFragranticaVideoCover(job.perfumeId, job.style, job.slides, { refresh: job.refresh })
      .then(job.resolve, () => job.resolve(null))
      .finally(() => {
        fragranticaVideoActive -= 1;
        fragranticaVideoJobs.delete(job.key);
        fragranticaVideoPump();
      });
  }
}

/** Queue the cover of one style (deduplicated); resolves to its URL or null. */
function queueFragranticaVideoCover(perfumeId, style, slides, { refresh = false } = {}) {
  const key = `${Number(perfumeId)}:${style}`;
  if (fragranticaVideoJobs.has(key)) return fragranticaVideoJobs.get(key);
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  fragranticaVideoJobs.set(key, promise);
  fragranticaVideoWaiting.push({ key, perfumeId: Number(perfumeId), style, slides, refresh, resolve });
  fragranticaVideoPump();
  return promise;
}

/** Waits (up to timeoutMs) for the queued covers of a perfume. */
async function fragranticaVideoCoversReady(perfumeId, styles = [], timeoutMs = 240_000) {
  const pending = styles.map((style) => fragranticaVideoJobs.get(`${Number(perfumeId)}:${style}`)).filter(Boolean);
  if (!pending.length) return;
  await Promise.race([Promise.all(pending), new Promise((r) => setTimeout(r, timeoutMs).unref?.())]);
}

setInterval(() => fragranticaVideoPump(), 60_000).unref?.();
