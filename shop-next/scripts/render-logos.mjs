// Renders the Magic Vibes logo (components/Logo.tsx, same SVG + fonts) to PNG files
// in public/brand/logo — transparent cut-outs, app icons and the e-mail header logo.
// Usage (from shop-next): node scripts/render-logos.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "public", "brand", "logo");
fs.mkdirSync(OUT, { recursive: true });
const require = createRequire(path.join(ROOT, "..", "package.json"));
const { chromium } = require("playwright");

const INK = "#121212", PAPER = "#f5f2ec", LIME = "#d9f84a", PINK = "#ff3d7f";

function mark(tone, size) {
  const orb = tone === "ink" ? INK : PAPER;
  const shine = tone === "ink" ? "rgba(255,255,255,0.35)" : "rgba(18,18,18,0.18)";
  return `<svg width="${size}" height="${size}" viewBox="0 0 40 40" style="display:block;flex-shrink:0">
    <circle cx="20" cy="20" r="19" fill="${orb}"/>
    <ellipse cx="13.5" cy="12" rx="5.5" ry="3.2" transform="rotate(-35 13.5 12)" fill="${shine}"/>
    <path d="M24 9.5c.9 5.3 3.2 7.6 8.5 8.5-5.3.9-7.6 3.2-8.5 8.5-.9-5.3-3.2-7.6-8.5-8.5 5.3-.9 7.6-3.2 8.5-8.5Z" fill="${LIME}"/>
    <circle cx="14" cy="27.5" r="2.4" fill="${PINK}"/></svg>`;
}

function full(tone, h) {
  const color = tone === "ink" ? INK : PAPER;
  return `<span style="display:inline-flex;align-items:center;gap:${h * 0.3}px;color:${color};line-height:1;padding:${h * 0.12}px ${h * 0.1}px">
    ${mark(tone, h)}
    <span style="font-family:Unbounded;font-weight:800;font-size:${h * 0.56}px;letter-spacing:-0.03em;text-transform:uppercase;white-space:nowrap">Magic<span style="font-family:'Marck Script';font-weight:400;text-transform:none;color:${PINK};font-size:${h * 0.78}px;letter-spacing:0;margin:0 0.08em 0 0.12em;position:relative;top:0.06em">vibes</span></span>
  </span>`;
}

// name, html, background (null = transparent)
const ITEMS = [
  ["logo-full-dark.png", full("ink", 200), null],          // for light backgrounds
  ["logo-full-light.png", full("paper", 200), null],       // for dark backgrounds
  ["logo-mark-dark.png", mark("ink", 1024), null],
  ["logo-mark-light.png", mark("paper", 1024), null],
  ["logo-email.png", full("ink", 72), null],               // e-mail header (rendered @2x → crisp at 36px)
  ["icon-1024-lime.png", `<div style="width:1024px;height:1024px;display:flex;align-items:center;justify-content:center;background:${LIME}">${mark("ink", 720)}</div>`, "x"],
  ["icon-1024-dark.png", `<div style="width:1024px;height:1024px;display:flex;align-items:center;justify-content:center;background:${INK}">${mark("paper", 720)}</div>`, "x"],
  ["icon-1024-paper.png", `<div style="width:1024px;height:1024px;display:flex;align-items:center;justify-content:center;background:${PAPER}">${mark("ink", 720)}</div>`, "x"],
  ["social-cover-1500x500.png", `<div style="width:1500px;height:500px;display:flex;align-items:center;justify-content:center;background:${LIME}">${full("ink", 150)}</div>`, "x"],
];

const browser = await chromium.launch({ channel: process.env.PW_CHANNEL || "chrome" });
try {
  const page = await browser.newPage({ deviceScaleFactor: 2, viewport: { width: 1600, height: 1200 } });
  for (const [name, html, bg] of ITEMS) {
    const scale = bg ? 1 : name === "logo-email.png" ? 2 : 1;
    await page.setViewportSize({ width: 2400, height: 1400 });
    await page.setContent(`<!doctype html><html><head><meta charset="utf-8">
      <link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@800&family=Marck+Script&display=block&subset=cyrillic,latin" rel="stylesheet">
      <style>html,body{margin:0;background:transparent}#x{display:inline-block}</style></head>
      <body><div id="x">${html}</div></body></html>`);
    await page.evaluate(async () => { await document.fonts.load("800 60px Unbounded"); await document.fonts.load("60px 'Marck Script'"); await document.fonts.ready; });
    const el = page.locator("#x");
    const file = path.join(OUT, name);
    // deviceScaleFactor 2 doubles pixels; plain cut-outs and icons are wanted at 1:1 of their CSS size
    const shotPage = scale === 2 ? page : await browser.newPage({ deviceScaleFactor: 1, viewport: { width: 2400, height: 1400 } });
    if (shotPage !== page) {
      await shotPage.setContent(await page.content());
      await shotPage.evaluate(async () => { await document.fonts.load("800 60px Unbounded"); await document.fonts.load("60px 'Marck Script'"); await document.fonts.ready; });
    }
    await shotPage.locator("#x").screenshot({ path: file, omitBackground: !bg });
    if (shotPage !== page) await shotPage.close();
    console.log("saved", path.relative(ROOT, file), Math.round(fs.statSync(file).size / 1024) + "KB");
    void el;
  }
} finally {
  await browser.close();
}
