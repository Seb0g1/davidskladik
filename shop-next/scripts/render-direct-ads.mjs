// Yandex Direct creatives: brand shots + headline → JPGs in every Direct ratio.
// Usage: node scripts/render-direct-ads.mjs [outDir]   (uses installed Chrome via playwright channel)
import { chromium } from "playwright";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { pathToFileURL } from "node:url";
import os from "node:os";
import sharp from "sharp";

const ROOT = resolve(import.meta.dirname, "..");
const OUT = resolve(process.argv[2] || join(os.homedir(), "Desktop", "MagicVibes-Директ"));
mkdirSync(OUT, { recursive: true });

const pub = (p) => pathToFileURL(join(ROOT, "public", p)).href;
const fontsCss = readFileSync(join(ROOT, "app", "fonts.css"), "utf8").replace(/url\(\/fonts\//g, `url(${pub("fonts/")}`);

const ADS = [
  { id: "niche", shot: "niche.jpg", dark: true, title: "Нишевая парфюмерия", script: "1000+ брендов" },
  { id: "her", shot: "her.jpg", title: "Женские ароматы", script: "оригиналы с доставкой" },
  { id: "him", shot: "him.jpg", dark: true, title: "Мужские ароматы", script: "оригиналы с доставкой" },
  { id: "gift", shot: "gift.jpg", title: "Парфюм в подарок", script: "наборы и пробники" },
  { id: "oriental", shot: "oriental.jpg", title: "Арабская парфюмерия", script: "восточные ароматы", accent: "#4b3cff" },
  { id: "probniki", shot: "citrus.jpg", title: "Пробники и отливанты", script: "попробуй прежде чем купить", accent: "#4b3cff" },
];

// photo box per format: [left, top, height] of the 1200x1500 shot; text box [left, top, width]; font size
const FORMATS = [
  { id: "1x1", w: 1080, h: 1080, photo: [430, 0, 1080], text: [64, 72, 560], fs: 56 },
  { id: "16x9", w: 1920, h: 1080, photo: [1080, 0, 1080], text: [110, 110, 900], fs: 100 },
  { id: "4x3", w: 1440, h: 1080, photo: [680, 0, 1080], text: [72, 90, 640], fs: 64 },
  { id: "3x4", w: 1080, h: 1440, photo: [90, 470, 1120], text: [72, 72, 936], fs: 72, vertical: true },
  { id: "9x16", w: 1080, h: 1920, photo: [-60, 500, 1500], text: [72, 120, 936], fs: 84, vertical: true },
];

// background gradient from the shot's own left edge (top → bottom), so the faded photo blends in
const edge = {};
for (const ad of ADS) {
  const { data } = await sharp(join(ROOT, "public/brand/shots", ad.shot)).extract({ left: 0, top: 0, width: 24, height: 1500 }).resize(1, 6, { fit: "fill" }).raw().toBuffer({ resolveWithObject: true });
  edge[ad.id] = Array.from({ length: 6 }, (_, i) => `rgb(${data[i * 3]},${data[i * 3 + 1]},${data[i * 3 + 2]}) ${Math.round((i / 5) * 100)}%`).join(",");
}

function html(ad, f) {
  const ink = ad.dark ? "#f5f2ec" : "#121212";
  const [pl, pt, ph] = f.photo;
  const pw = Math.round(ph * 0.8);
  const [tl, tt, tw] = f.text;
  const s = f.fs;
  const mask = f.vertical
    ? "linear-gradient(to bottom, transparent 0, #000 22%)"
    : "linear-gradient(to right, transparent 0, #000 22%, #000 100%)";
  const logo = pub(`brand/logo/logo-full-${ad.dark ? "light" : "dark"}.png`);
  return `<!doctype html><html><head><meta charset="utf-8"><style>${fontsCss}
*{margin:0;box-sizing:border-box}
body{width:${f.w}px;height:${f.h}px;overflow:hidden;position:relative;font-family:Onest,sans-serif;color:${ink}}
.bg{position:absolute;left:0;right:0;top:${f.vertical ? 0 : pt}px;height:${f.vertical ? f.h : ph}px;background:linear-gradient(to bottom,${edge[ad.id]})}
.ph{position:absolute;left:${pl}px;top:${pt}px;height:${ph}px;width:${pw}px;background:url(${pub("brand/shots/" + ad.shot)}) center/cover;-webkit-mask-image:${mask}}
.tx{position:absolute;left:${tl}px;top:${tt}px;width:${tw}px}
.logo{height:${Math.round(s * 0.62)}px;display:block;margin-bottom:${Math.round(s * 0.7)}px}
h1{font-family:Unbounded,sans-serif;font-weight:800;font-size:${s}px;line-height:1.02;text-transform:uppercase;letter-spacing:-0.01em}
.sc{font-family:'Marck Script',cursive;font-size:${Math.round(s * 0.78)}px;color:${ad.accent || (ad.dark ? "#d9f84a" : "#ff3d7f")};margin-top:${Math.round(s * 0.12)}px;transform:rotate(-3deg);transform-origin:left}
.chips{display:flex;flex-wrap:wrap;gap:${Math.round(s * 0.16)}px;margin-top:${Math.round(s * 0.5)}px}
.chip{font-weight:600;font-size:${Math.round(s * 0.3)}px;padding:${Math.round(s * 0.13)}px ${Math.round(s * 0.26)}px;border-radius:999px;background:${ad.dark ? "rgba(245,242,236,.14)" : "rgba(255,255,255,.7)"};border:1px solid ${ad.dark ? "rgba(245,242,236,.25)" : "rgba(18,18,18,.1)"}}
.chip b{color:${ad.dark ? "#d9f84a" : "#4b3cff"}}
</style></head><body>
<div class="bg"></div><div class="ph"></div>
<div class="tx">
  <img class="logo" src="${logo}">
  <h1>${ad.title}</h1>
  <div class="sc">${ad.script}</div>
  <div class="chips"><span class="chip"><b>✓</b> Гарантия оригинала</span><span class="chip"><b>✓</b> Доставка по России</span></div>
</div>
</body></html>`;
}

const browser = await chromium.launch({ channel: "chrome" });
const page = await browser.newPage();
const tmp = join(os.tmpdir(), "mv-direct-ad.html");
for (const f of FORMATS) {
  await page.setViewportSize({ width: f.w, height: f.h });
  for (const ad of ADS) {
    writeFileSync(tmp, html(ad, f));
    await page.goto(pathToFileURL(tmp).href);
    await page.evaluate(() => document.fonts.ready);
    const file = join(OUT, `${ad.id}-${f.id}-${f.w}x${f.h}.jpg`);
    await page.screenshot({ path: file, type: "jpeg", quality: 90 });
    console.log(file);
  }
}
await browser.close();
