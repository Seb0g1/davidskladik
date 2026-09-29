// 50 Yandex Direct creatives with fun phrases + magicvibes.ru link, 4 layouts, 1:1 and 16:9.
// Usage: node scripts/render-direct-fun.mjs [outDir]   (uses installed Chrome via playwright channel)
import { chromium } from "playwright";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { pathToFileURL } from "node:url";
import os from "node:os";
import sharp from "sharp";

const ROOT = resolve(import.meta.dirname, "..");
const OUT = resolve(process.argv[2] || join(os.homedir(), "Desktop", "MagicVibes-Директ", "50-фраз"));

const pub = (p) => pathToFileURL(join(ROOT, "public", p)).href;
const fontsCss = readFileSync(join(ROOT, "app", "fonts.css"), "utf8").replace(/url\(\/fonts\//g, `url(${pub("fonts/")}`);

// [title, script line, shot]; layout rotates photo → solid → stack → quote
const PHRASES = [
  ["Пахни так, чтобы оборачивались", "шлейф решает", "her"],
  ["Твой аромат уже в пути", "доставка по всей России", "fresh"],
  ["Характер во флаконе", "выбери свой", "him"],
  ["Сначала аромат — потом знакомство", "первое впечатление", "her"],
  ["Запах, который запомнят", "надолго", "niche"],
  ["Подари аромат, а не носки", "серьёзно", "gift"],
  ["Флакон хорошего настроения", "пшик — и всё отлично", "citrus"],
  ["Нос не обманешь", "только оригиналы", "niche"],
  ["Духи на каждый вайб", "от нежного до дерзкого", "fresh"],
  ["Шлейф решает", "проверено", "oriental"],
  ["Понедельник пахнет лучше", "с нишевым ароматом", "niche"],
  ["Один пшик — и ты главный герой", "дня", "him"],
  ["Аромат — лучший аксессуар", "к любому образу", "her"],
  ["Не знаешь, что подарить?", "мы знаем", "gift"],
  ["Попробуй, прежде чем влюбиться", "пробники и отливанты", "citrus"],
  ["Отливанты — чтобы не выбирать один", "бери несколько", "fresh"],
  ["Пахнешь дорого — чувствуешь уверенно", "проверь сам", "him"],
  ["Восток — дело ароматное", "арабская парфюмерия", "oriental"],
  ["Уд, амбра и немного магии", "восточные ароматы", "oriental"],
  ["Цветы, которые не завянут", "женские ароматы", "her"],
  ["Свежесть на весь день", "и даже вечер", "fresh"],
  ["Цитрус вместо кофе", "бодрит с утра", "citrus"],
  ["Аромат для первого свидания", "и второго тоже", "her"],
  ["Офисный вайб: свежо и по делу", "ароматы для работы", "fresh"],
  ["Мужской характер в каждой ноте", "мужские ароматы", "him"],
  ["Для неё: нежно и навсегда", "женские ароматы", "gift"],
  ["Нишевое — для своих", "1000+ брендов", "niche"],
  ["Не как у всех", "нишевая парфюмерия", "niche"],
  ["Твой новый любимый запах", "уже здесь", "citrus"],
  ["Найди свой аромат с AI", "подбор по настроению", "fresh"],
  ["AI знает, чем ты пахнешь", "ну почти", "niche"],
  ["Любимые духи закончились?", "не беда", "her"],
  ["Скажи это ароматом", "без лишних слов", "gift"],
  ["Пшик — и настроение в плюсе", "работает", "citrus"],
  ["Магия в каждой капле", "Magic Vibes", "niche"],
  ["Оригинал. Точка.", "гарантия оригинала", "him"],
  ["Собери свою коллекцию ароматов", "начни сегодня", "oriental"],
  ["Аромат, в который хочется завернуться", "как в плед", "oriental"],
  ["Пахни как отпуск", "даже в офисе", "citrus"],
  ["Лето во флаконе", "круглый год", "fresh"],
  ["Тёплый аромат для холодных дней", "уютно", "oriental"],
  ["Древесные ноты и тёплые вечера", "мужские ароматы", "him"],
  ["Подарок, который точно пригодится", "парфюм в подарок", "gift"],
  ["Дарим эмоции во флаконах", "подарочные наборы", "gift"],
  ["1000+ брендов — один сайт", "оригиналы", "niche"],
  ["— Чем ты пахнешь? — Секрет", "но мы подскажем", "her"],
  ["Твой вайб. Твой аромат.", "найди свой", "fresh"],
  ["Не просто духи — настроение", "на каждый день", "gift"],
  ["Ароматный апгрейд образа", "за один пшик", "him"],
  ["Пахнет магией", "Magic Vibes", "niche"],
];

const SHOTS = { her: "her.jpg", him: "him.jpg", niche: "niche.jpg", gift: "gift.jpg", oriental: "oriental.jpg", citrus: "citrus.jpg", fresh: "fresh.jpg" };
const DARK_SHOTS = new Set(["him", "niche"]);
const ACCENT = { citrus: "#4b3cff", oriental: "#4b3cff" };
// solid palettes: bg, text, script accent, pill bg, pill text, dark?
const SOLIDS = [
  { bg: "#121212", ink: "#f5f2ec", acc: "#d9f84a", pill: "#d9f84a", pillInk: "#121212", dark: true },
  { bg: "#d9f84a", ink: "#121212", acc: "#4b3cff", pill: "#121212", pillInk: "#d9f84a" },
  { bg: "#ff3d7f", ink: "#fff", acc: "#d9f84a", pill: "#121212", pillInk: "#fff", dark: true },
  { bg: "#4b3cff", ink: "#fff", acc: "#d9f84a", pill: "#d9f84a", pillInk: "#121212", dark: true },
  { bg: "#f5f2ec", ink: "#121212", acc: "#ff3d7f", pill: "#4b3cff", pillInk: "#fff" },
];
const LAYOUTS = ["photo", "solid", "stack", "quote"];

const edge = {};
for (const [k, f] of Object.entries(SHOTS)) {
  const { data } = await sharp(join(ROOT, "public/brand/shots", f)).extract({ left: 0, top: 0, width: 24, height: 1500 }).resize(1, 6, { fit: "fill" }).raw().toBuffer({ resolveWithObject: true });
  edge[k] = Array.from({ length: 6 }, (_, i) => `rgb(${data[i * 3]},${data[i * 3 + 1]},${data[i * 3 + 2]}) ${i * 20}%`).join(",");
}

const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;");

function html(i, W, H) {
  const [title, script, shot] = PHRASES[i];
  let layout = LAYOUTS[i % 4];
  const wide = W > H;
  if (wide && layout === "stack") layout = "photo";
  const solid = SOLIDS[Math.floor(i / 4) % SOLIDS.length];
  const photoTheme = { bg: `linear-gradient(to bottom,${edge[shot]})`, ink: DARK_SHOTS.has(shot) ? "#f5f2ec" : "#121212", acc: ACCENT[shot] || (DARK_SHOTS.has(shot) ? "#d9f84a" : "#ff3d7f"), dark: DARK_SHOTS.has(shot) };
  photoTheme.pill = photoTheme.dark ? "#d9f84a" : "#121212";
  photoTheme.pillInk = photoTheme.dark ? "#121212" : "#f5f2ec";
  const t = layout === "photo" || layout === "stack" ? photoTheme : layout === "quote" ? SOLIDS[(Math.floor(i / 4) + 4) % SOLIDS.length] : solid;
  const img = pub("brand/shots/" + SHOTS[shot]);
  const cut = pub("brand/shots/flacon-cutout.png");
  const logo = pub(`brand/logo/logo-full-${t.dark ? "light" : "dark"}.png`);
  const u = H / 1080; // scale unit

  let art = "", text = "";
  const textBlock = (align = "left") => `
    <img class="logo" src="${logo}" style="${align === "center" ? "margin-left:auto;margin-right:auto" : ""}">
    <h1 id="h">${esc(title)}</h1>
    <div class="sc">${esc(script)}</div>
    <div class="pill">magicvibes.ru <span>↗</span></div>`;

  if (layout === "photo") {
    const ph = H, pw = Math.round(ph * 0.8), pl = W - pw + (wide ? -80 * u : 170 * u);
    art = `<div class="bgf" style="background:${t.bg}"></div><div style="position:absolute;left:${pl}px;top:0;width:${pw}px;height:${ph}px;background:url(${img}) center/cover;-webkit-mask-image:linear-gradient(to right,transparent 0,#000 22%)"></div>`;
    text = `<div class="tx" style="left:${70 * u}px;top:${80 * u}px;width:${wide ? W * 0.5 : W * 0.54}px;height:${H - 160 * u}px">${textBlock()}</div>`;
  } else if (layout === "stack") {
    const ph = H * 0.7, pw = ph * 0.8;
    art = `<div class="bgf" style="background:${t.bg}"></div><div style="position:absolute;left:${(W - pw) / 2}px;top:${H - ph}px;width:${pw}px;height:${ph}px;background:url(${img}) center/cover;-webkit-mask-image:linear-gradient(to bottom,transparent 0,#000 30%)"></div>`;
    text = `<div class="tx center" style="left:${60 * u}px;top:${50 * u}px;width:${W - 120 * u}px;height:${H * 0.33}px">${textBlock("center").replace(/<div class="pill">.*<\/div>/, "")}</div>
      <div class="pill" style="position:absolute;left:50%;transform:translateX(-50%);bottom:${50 * u}px;margin:0">magicvibes.ru <span>↗</span></div>`;
  } else if (layout === "solid") {
    const bh = H * 0.72;
    art = `<div class="bgf" style="background:${t.bg}"></div>
      <div class="star" style="left:${W * 0.62}px;top:${H * 0.06}px;font-size:${220 * u}px;color:${t.acc};opacity:.9">✦</div>
      <div class="star" style="left:${W * 0.9}px;top:${H * 0.7}px;font-size:${90 * u}px;color:${t.acc};opacity:.7">✦</div>
      <div style="position:absolute;left:${W - bh * 0.83 + (wide ? -80 : 60) * u}px;top:${H * 0.2}px;width:${bh * 0.83}px;height:${bh}px;background:url(${cut}) center/contain no-repeat;transform:rotate(-8deg);filter:drop-shadow(0 ${30 * u}px ${40 * u}px rgba(0,0,0,.35))"></div>`;
    text = `<div class="tx" style="left:${70 * u}px;top:${80 * u}px;width:${wide ? W * 0.55 : W * 0.5}px;height:${H - 160 * u}px">${textBlock()}</div>`;
  } else {
    const bh = H * 0.52;
    art = `<div class="bgf" style="background:${t.bg}"></div>
      <div class="quote" style="color:${t.acc};font-size:${420 * u}px;left:${40 * u}px;top:${-90 * u}px">“</div>
      <div style="position:absolute;right:${-40 * u}px;bottom:${-40 * u}px;width:${bh * 1.05}px;height:${bh * 1.05}px;border-radius:50%;background:${t.acc};opacity:.9"></div>
      <div style="position:absolute;right:${40 * u}px;bottom:${10 * u}px;width:${bh * 0.83}px;height:${bh}px;background:url(${cut}) center/contain no-repeat;transform:rotate(10deg)"></div>`;
    text = `<div class="tx" style="left:${80 * u}px;top:${190 * u}px;width:${wide ? W * 0.6 : W - 160 * u}px;height:${wide ? H - 270 * u : H * 0.5}px">
      <h1 id="h">${esc(title)}</h1><div class="sc">${esc(script)}</div></div>
      <img class="logo" src="${logo}" style="position:absolute;left:${80 * u}px;bottom:${150 * u}px;margin:0">
      <div class="pill" style="position:absolute;left:${80 * u}px;bottom:${60 * u}px;margin:0">magicvibes.ru <span>↗</span></div>`;
  }

  return `<!doctype html><html><head><meta charset="utf-8"><style>${fontsCss}
*{margin:0;box-sizing:border-box}
body{width:${W}px;height:${H}px;overflow:hidden;position:relative;font-family:Onest,sans-serif;color:${t.ink}}
.bgf{position:absolute;inset:0}
.tx{position:absolute;display:flex;flex-direction:column;align-items:flex-start}
.tx.center{align-items:center;text-align:center}
.logo{height:${40 * u}px;display:block;margin-bottom:${40 * u}px}
h1{font-family:Unbounded,sans-serif;font-weight:800;font-size:100px;line-height:1.04;text-transform:uppercase;letter-spacing:-0.01em;width:100%;overflow-wrap:normal}
.sc{font-family:'Marck Script',cursive;font-size:${62 * u}px;color:${t.acc};margin-top:${14 * u}px;transform:rotate(-3deg);transform-origin:left;white-space:nowrap}
.tx.center .sc{transform-origin:center}
.pill{margin-top:auto;display:inline-flex;align-items:center;gap:${12 * u}px;font-weight:700;font-size:${34 * u}px;padding:${16 * u}px ${30 * u}px;border-radius:999px;background:${t.pill};color:${t.pillInk};box-shadow:0 ${8 * u}px ${24 * u}px rgba(0,0,0,.18)}
.star{position:absolute;line-height:1}
.quote{position:absolute;font-family:Unbounded,sans-serif;font-weight:800;line-height:1;opacity:.95}
</style></head><body>${art}${text}</body></html>`;
}

// shrink the headline until it fits its box (no overflowing words, leaves room for script + pill)
async function fit(page, H) {
  await page.evaluate((H) => {
    const h = document.getElementById("h"), box = h.parentElement;
    const reserve = box.querySelector(".pill") ? 0.62 : box.classList.contains("center") ? 0.62 : 0.72;
    let fs = Math.round(H * 0.12);
    const words = () => { const r = document.createRange(); r.selectNodeContents(h); return r.getBoundingClientRect().width <= h.clientWidth + 1; };
    for (; fs > 20; fs -= 2) {
      h.style.fontSize = fs + "px";
      if (h.scrollWidth <= h.clientWidth + 1 && words() && h.offsetHeight <= box.clientHeight * reserve) break;
    }
  }, H);
}

const browser = await chromium.launch({ channel: "chrome" });
const page = await browser.newPage();
const tmp = join(os.tmpdir(), "mv-direct-fun.html");
for (const [dir, W, H] of [["1x1", 1080, 1080], ["16x9", 1920, 1080]]) {
  mkdirSync(join(OUT, dir), { recursive: true });
  await page.setViewportSize({ width: W, height: H });
  for (let i = 0; i < PHRASES.length; i++) {
    writeFileSync(tmp, html(i, W, H));
    await page.goto(pathToFileURL(tmp).href);
    await page.evaluate(() => document.fonts.ready);
    await fit(page, H);
    await page.screenshot({ path: join(OUT, dir, `${String(i + 1).padStart(2, "0")}-${dir}.jpg`), type: "jpeg", quality: 90 });
  }
  console.log(dir, "done");
}
await browser.close();
