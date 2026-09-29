// Story video scenes (1080×1920): drawing is a pure function of time, so preview, scrubbing and
// recording show the same frames. Templates are lists of scenes; with music the scene lengths are
// snapped to the beat so every cut lands on a bar line.
import type { ShopProduct } from "@/lib/types";

export const W = 1080, H = 1920;
export const INK = "#121212", PAPER = "#f5f2ec", LIME = "#d9f84a", PINK = "#ff3d7f", VIOLET = "#4b3cff";

export const PALETTES: Record<string, { label: string; scenes: string[] }> = {
  brand: { label: "Фирменная", scenes: ["#ffc8dc", "#a9dcff", "#d9c7a4", "#ff9d4d", "#b7a9ff", "#d9f84a"] },
  lime:  { label: "Лайм",      scenes: ["#d9f84a", "#e8ff8a", "#c8ec2f"] },
  pink:  { label: "Розовая",   scenes: ["#ffc8dc", "#ffb0cc", "#ffd9e7"] },
  night: { label: "Ночь",      scenes: ["#1d1b22", "#26222e", "#1a1a1a"] },
};

export type TplId = "top" | "spotlight" | "sale" | "grid" | "brand";
export const TEMPLATES: { id: TplId; label: string; hint: string; min: number; max: number }[] = [
  { id: "top",       label: "Хиты",        hint: "интро → карточка на каждый товар → промокод", min: 1, max: 8 },
  { id: "spotlight", label: "Один аромат", hint: "крупно один товар: ноты, цена, призыв", min: 1, max: 1 },
  { id: "sale",      label: "Скидки",      hint: "SALE, зачёркнутые цены, дедлайн акции", min: 1, max: 8 },
  { id: "grid",      label: "Новинки",     hint: "коллаж 2×2 и крупно каждый товар", min: 2, max: 4 },
  { id: "brand",     label: "Бренд",       hint: "кинетическая типографика бренда + карусель", min: 1, max: 6 },
];

export interface Item { product?: ShopProduct; upload?: string; name?: string; brand?: string; price?: number; old?: number }
export interface Cfg {
  tpl: TplId; title: string; script: string; outro: string; promo: string; site: string;
  notes: string; deadline: string;
  per: number; palette: string; prices: boolean; items: Item[];
}
export interface Assets { imgs: (HTMLImageElement | null)[] }
export interface Scene { kind: string; start: number; dur: number; i: number; bg: string }

/* ───────── helpers ───────── */
export const clamp = (x: number, a = 0, b = 1) => Math.min(b, Math.max(a, x));
const seg = (t: number, a: number, b: number) => clamp((t - a) / (b - a));
const eOut = (x: number) => 1 - Math.pow(1 - x, 3);
const eInOut = (x: number) => (x < 0.5 ? 4 * x * x * x : 1 - Math.pow(-2 * x + 2, 3) / 2);
const eBack = (x: number) => { const c1 = 1.70158, c3 = c1 + 1; return 1 + c3 * Math.pow(x - 1, 3) + c1 * Math.pow(x - 1, 2); };
const lerp = (a: number, b: number, k: number) => a + (b - a) * k;
export const rub = (n: number) => `${Math.round(n).toLocaleString("ru-RU")} ₽`;
const isDark = (hex: string) => { const n = parseInt(hex.slice(1), 16); return ((n >> 16) * 0.299 + ((n >> 8) & 255) * 0.587 + (n & 255) * 0.114) < 110; };
const discount = (it: Item) => (it.old && it.price && it.old > it.price ? Math.round((1 - it.price / it.old) * 100) : 0);

function rr(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
  ctx.beginPath();
  ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + h, r); ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath();
}
function wrap(ctx: CanvasRenderingContext2D, text: string, maxW: number, maxLines: number) {
  const words = text.split(/\s+/).filter(Boolean);
  const lines: string[] = [];
  let cur = "";
  for (const w of words) {
    const next = cur ? `${cur} ${w}` : w;
    if (ctx.measureText(next).width > maxW && cur) { lines.push(cur); cur = w; } else cur = next;
    if (lines.length === maxLines) break;
  }
  if (lines.length < maxLines && cur) lines.push(cur);
  if (lines.length === maxLines && words.join(" ").length > lines.join(" ").length) {
    let last = lines[maxLines - 1];
    while (ctx.measureText(last + "…").width > maxW && last.length > 1) last = last.slice(0, -1);
    lines[maxLines - 1] = last.trimEnd() + "…";
  }
  return lines;
}
function spark(ctx: CanvasRenderingContext2D, x: number, y: number, r: number, color: string, rot = 0) {
  ctx.save(); ctx.translate(x, y); ctx.rotate(rot); ctx.fillStyle = color;
  ctx.beginPath(); ctx.moveTo(0, -r);
  ctx.quadraticCurveTo(r * 0.12, -r * 0.12, r, 0); ctx.quadraticCurveTo(r * 0.12, r * 0.12, 0, r);
  ctx.quadraticCurveTo(-r * 0.12, r * 0.12, -r, 0); ctx.quadraticCurveTo(-r * 0.12, -r * 0.12, 0, -r);
  ctx.fill(); ctx.restore();
}
function orb(ctx: CanvasRenderingContext2D, x: number, y: number, r: number) {
  ctx.save(); ctx.translate(x, y); const k = r / 19;
  ctx.fillStyle = INK; ctx.beginPath(); ctx.arc(0, 0, r, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = "rgba(255,255,255,0.35)"; ctx.beginPath(); ctx.ellipse(-6.5 * k, -8 * k, 5.5 * k, 3.2 * k, -0.61, 0, Math.PI * 2); ctx.fill();
  spark(ctx, 4 * k, -2 * k, 8.5 * k, LIME);
  ctx.fillStyle = PINK; ctx.beginPath(); ctx.arc(-6 * k, 7.5 * k, 2.4 * k, 0, Math.PI * 2); ctx.fill();
  ctx.restore();
}
function wordmark(ctx: CanvasRenderingContext2D, cx: number, y: number, size: number, fg: string) {
  ctx.save(); ctx.textBaseline = "middle"; ctx.textAlign = "left";
  ctx.font = `800 ${size}px Unbounded`; const a = ctx.measureText("MAGIC").width;
  ctx.font = `400 ${size * 1.35}px "Marck Script"`; const b = ctx.measureText("vibes").width;
  const orbR = size * 0.62, gap = size * 0.35, w = orbR * 2 + gap + a + size * 0.15 + b;
  let x = cx - w / 2;
  orb(ctx, x + orbR, y, orbR); x += orbR * 2 + gap;
  ctx.fillStyle = fg; ctx.font = `800 ${size}px Unbounded`; ctx.fillText("MAGIC", x, y); x += a + size * 0.15;
  ctx.fillStyle = PINK; ctx.font = `400 ${size * 1.35}px "Marck Script"`; ctx.fillText("vibes", x, y + size * 0.1);
  ctx.restore();
}
function sparkles(ctx: CanvasRenderingContext2D, t: number, seed: number, color: string, n = 9, alpha = 0.9) {
  for (let i = 0; i < n; i++) {
    const r1 = Math.sin(seed * 91.7 + i * 12.9898) * 43758.5453, fx = r1 - Math.floor(r1);
    const r2 = Math.sin(seed * 17.3 + i * 78.233) * 12345.678, fy = r2 - Math.floor(r2);
    const x = fx * W, y = ((fy * H - t * (40 + fx * 60)) % H + H) % H;
    ctx.globalAlpha = alpha * (0.35 + 0.65 * Math.abs(Math.sin(t * 1.4 + i)));
    spark(ctx, x, y, 14 + fx * 26, color, t * (0.6 + fy));
  }
  ctx.globalAlpha = 1;
}
function wipe(ctx: CanvasRenderingContext2D, k: number, color: string, ox = W * 0.8, oy = H * 0.2) {
  if (k <= 0) return;
  ctx.save(); ctx.fillStyle = color; ctx.beginPath(); ctx.arc(ox, oy, eInOut(k) * Math.hypot(W, H) * 1.05, 0, Math.PI * 2); ctx.fill(); ctx.restore();
}
function photo(ctx: CanvasRenderingContext2D, img: HTMLImageElement | null | undefined, w: number, h: number, zoom = 1, pad = 28, radius = 56) {
  ctx.save();
  ctx.shadowColor = "rgba(0,0,0,0.22)"; ctx.shadowBlur = 60; ctx.shadowOffsetY = 30;
  rr(ctx, -w / 2, -h / 2, w, h, radius); ctx.fillStyle = "#ffffff"; ctx.fill();
  ctx.shadowColor = "transparent";
  rr(ctx, -w / 2 + pad, -h / 2 + pad, w - pad * 2, h - pad * 2, Math.max(8, radius - 20)); ctx.clip();
  if (img && img.naturalWidth) {
    const s = Math.min((w - pad * 2) / img.naturalWidth, (h - pad * 2) / img.naturalHeight) * zoom;
    ctx.drawImage(img, -img.naturalWidth * s / 2, -img.naturalHeight * s / 2, img.naturalWidth * s, img.naturalHeight * s);
  } else { ctx.fillStyle = "#f1eee8"; ctx.fillRect(-w / 2, -h / 2, w, h); orb(ctx, 0, 0, Math.min(w, h) * 0.14); }
  ctx.restore();
}
function pill(ctx: CanvasRenderingContext2D, text: string, x: number, y: number, size: number, bg: string, fg: string, align: "left" | "center" = "left") {
  ctx.save(); ctx.font = `800 ${size}px Unbounded`; const w = ctx.measureText(text).width + size * 1.4, h = size * 1.75;
  const x0 = align === "center" ? x - w / 2 : x;
  rr(ctx, x0, y - h / 2, w, h, h / 2); ctx.fillStyle = bg; ctx.fill();
  ctx.fillStyle = fg; ctx.textBaseline = "middle"; ctx.textAlign = "left"; ctx.fillText(text, x0 + size * 0.7, y + size * 0.04); ctx.restore();
  return w;
}

/* ───────── timeline ───────── */
export function buildTimeline(c: Cfg, bpm: number | null): { scenes: Scene[]; duration: number } {
  const beat = bpm ? 60 / bpm : 0;
  // snap to whole beats (at least 4) so cuts sit on the grid
  const snap = (d: number) => (beat ? Math.max(4, Math.round(d / beat)) * beat : d);
  const pal = (PALETTES[c.palette] || PALETTES.brand).scenes;
  const n = Math.max(1, c.items.length);
  const list: [string, number, number, string][] = [];
  const bgOf = (i: number) => pal[i % pal.length];
  if (c.tpl === "top") {
    list.push(["intro", 2.6, 0, LIME]);
    for (let i = 0; i < n; i++) list.push(["product", c.per, i, bgOf(i)]);
    list.push(["outro", 3.4, 0, INK]);
  } else if (c.tpl === "spotlight") {
    list.push(["spot-intro", 2.4, 0, INK], ["spot-hero", 4.2, 0, bgOf(0)], ["spot-price", 2.8, 0, LIME], ["outro", 3.2, 0, INK]);
  } else if (c.tpl === "sale") {
    list.push(["sale-intro", 2.6, 0, PINK]);
    for (let i = 0; i < n; i++) list.push(["sale-item", Math.max(1.8, c.per * 0.8), i, i % 2 ? INK : LIME]);
    list.push(["sale-outro", 3.4, 0, PINK]);
  } else if (c.tpl === "grid") {
    list.push(["grid-intro", 2.2, 0, LIME], ["grid", 3.6, 0, PAPER]);
    for (let i = 0; i < Math.min(4, n); i++) list.push(["product", Math.max(2, c.per * 0.85), i, bgOf(i)]);
    list.push(["outro", 3.2, 0, INK]);
  } else {
    list.push(["brand-intro", 3.0, 0, INK]);
    for (let i = 0; i < n; i++) list.push(["carousel", c.per, i, bgOf(i)]);
    list.push(["outro", 3.2, 0, INK]);
  }
  let t = 0;
  const scenes = list.map(([kind, d, i, bg]) => { const s = { kind, start: t, dur: snap(d), i, bg }; t += s.dur; return s; });
  return { scenes, duration: t };
}

/* ───────── narration: one line per scene (editable in the studio) ───────── */
export function defaultLines(c: Cfg, scenes: Scene[]): string[] {
  const it = (i: number) => c.items[i] || {};
  const price = (i: number) => (c.prices && it(i).price ? `${Math.round(it(i).price!)} рублей` : "");
  const maxOff = Math.max(0, ...c.items.map(discount));
  return scenes.map((s) => {
    const x = it(s.i);
    switch (s.kind) {
      case "intro": return `${c.title}. ${c.script}.`;
      case "product": return [x.brand, x.name, price(s.i)].filter(Boolean).join(". ");
      case "outro": return `${c.outro}: ${c.site.replace(".ru", " точка ру")}.${c.promo ? ` Промокод ${c.promo} на первый заказ.` : ""}`;
      case "spot-intro": return `${x.brand || "Magic Vibes"}.`;
      case "spot-hero": return `${x.name || ""}.${c.notes ? ` Ноты: ${c.notes}.` : ""}`;
      case "spot-price": return c.prices && x.price ? `Всего ${price(s.i)}. Оригинал, доставка Ozon от одного дня.` : "Оригинал, доставка Ozon от одного дня.";
      case "sale-intro": return maxOff ? `Распродажа! Скидки до ${maxOff} процентов.` : "Распродажа в Magic Vibes!";
      case "sale-item": return [x.brand, x.name, price(s.i)].filter(Boolean).join(". ");
      case "sale-outro": return `${c.deadline ? `Акция ${c.deadline}. ` : ""}Успейте на ${c.site.replace(".ru", " точка ру")}.`;
      case "grid-intro": return `${c.title}.`;
      case "grid": return "Свежие поступления уже на сайте.";
      case "brand-intro": return `${c.title || x.brand}. ${c.script}.`;
      case "carousel": return [x.name, price(s.i)].filter(Boolean).join(". ");
      default: return "";
    }
  });
}

/* ───────── scenes ───────── */
type Draw = (ctx: CanvasRenderingContext2D, u: number, s: Scene, c: Cfg, a: Assets) => void;

const intro: Draw = (ctx, t, s, c) => {
  ctx.fillStyle = LIME; ctx.fillRect(0, 0, W, H);
  ctx.save(); ctx.globalAlpha = 0.08; ctx.fillStyle = INK; ctx.font = `400 420px "Marck Script"`;
  ctx.translate(-80 - t * 40, H - 160); ctx.rotate(-0.12); ctx.fillText("magic", 0, 0); ctx.restore();
  sparkles(ctx, t, 1, INK, 7, 0.5);
  const bk = eBack(seg(t, 0.05, 0.75));
  ctx.fillStyle = PINK; ctx.beginPath(); ctx.arc(W - 120, 330, 330 * bk, 0, Math.PI * 2); ctx.fill();
  const lk = eBack(seg(t, 0.15, 0.7));
  ctx.save(); ctx.globalAlpha = seg(t, 0.15, 0.4); ctx.translate(W / 2, 420); ctx.scale(lk, lk); wordmark(ctx, 0, 0, 58, INK); ctx.restore();
  ctx.font = `800 118px Unbounded`; ctx.textBaseline = "alphabetic"; ctx.textAlign = "left";
  const lines = wrap(ctx, c.title.toUpperCase(), W - 150, 3);
  let wi = 0;
  lines.forEach((line, li) => {
    let x = 75;
    for (const word of line.split(" ")) {
      const k = eOut(seg(t, 0.45 + wi * 0.09, 0.95 + wi * 0.09));
      ctx.save(); ctx.globalAlpha = k; ctx.fillStyle = INK; ctx.translate(0, (1 - k) * 90); ctx.fillText(word, x, 900 + li * 128); ctx.restore();
      x += ctx.measureText(word + " ").width; wi++;
    }
  });
  const sk = eOut(seg(t, 1.0, 1.6));
  ctx.save(); ctx.globalAlpha = sk; ctx.fillStyle = PINK; ctx.font = `400 104px "Marck Script"`;
  ctx.translate(75 + (1 - sk) * -60, 900 + lines.length * 128 + 40); ctx.rotate(-0.04); ctx.fillText(c.script, 0, 0); ctx.restore();
  const ck = eBack(seg(t, 1.35, 1.9));
  ctx.save(); ctx.translate(75, 1560); ctx.scale(ck, ck);
  ctx.font = `600 44px Onest`; const txt = "✦ 22 000+ оригинальных ароматов"; const tw = ctx.measureText(txt).width;
  rr(ctx, 0, -60, tw + 80, 96, 48); ctx.fillStyle = INK; ctx.fill();
  ctx.fillStyle = LIME; ctx.textBaseline = "middle"; ctx.fillText(txt, 40, -12); ctx.restore();
};

const product: Draw = (ctx, u, s, c, a) => {
  const i = s.i, bg = s.bg, fg = isDark(bg) ? PAPER : INK, accent = isDark(bg) ? LIME : PINK;
  wipe(ctx, seg(u, 0, 0.5), bg, i % 2 ? W * 0.15 : W * 0.85, i % 2 ? H * 0.85 : H * 0.15);
  if (u < 0.25) return;
  sparkles(ctx, u + i * 7, i + 3, isDark(bg) ? LIME : "#ffffff", 8, 0.8);
  const it = c.items[i] || {};
  const outK = seg(u, s.dur - 0.35, s.dur), inK = eBack(seg(u, 0.3, 0.9));
  ctx.save(); ctx.globalAlpha = 1 - outK; ctx.translate(0, -outK * 120);
  ctx.font = `800 200px Unbounded`; ctx.fillStyle = fg; ctx.globalAlpha = (1 - outK) * 0.12 * seg(u, 0.3, 0.6); ctx.textAlign = "left";
  ctx.fillText(String(i + 1).padStart(2, "0"), 60, 300); ctx.globalAlpha = 1 - outK;
  const cy = 820;
  ctx.save(); ctx.translate(W / 2, cy); ctx.rotate(lerp(-0.12, i % 2 ? 0.03 : -0.03, inK)); ctx.scale(lerp(0.6, 1, inK), lerp(0.6, 1, inK));
  photo(ctx, a.imgs[i], 860, 980, lerp(1, 1.06, seg(u, 0.4, s.dur))); ctx.restore();
  const off = discount(it);
  const stK = eBack(seg(u, 0.75, 1.15));
  ctx.save(); ctx.translate(W / 2 + 360, cy - 450); ctx.rotate(0.18); ctx.scale(stK, stK);
  ctx.fillStyle = accent; ctx.beginPath(); ctx.arc(0, 0, 96, 0, Math.PI * 2); ctx.fill();
  ctx.fillStyle = isDark(accent) ? PAPER : INK; ctx.textAlign = "center"; ctx.textBaseline = "middle";
  ctx.font = `800 34px Unbounded`; ctx.fillText(off ? `−${off}%` : "100%", 0, -14);
  ctx.font = `600 26px Onest`; ctx.fillText(off ? "скидка" : "оригинал", 0, 26); ctx.restore();
  const bK = eOut(seg(u, 0.6, 1.05));
  ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
  ctx.save(); ctx.globalAlpha = (1 - outK) * bK; ctx.translate((1 - bK) * -80, 0);
  ctx.fillStyle = fg; ctx.font = `800 70px Unbounded`; ctx.fillText(wrap(ctx, (it.brand || "").toUpperCase(), W - 180, 1)[0] || "", 90, 1420); ctx.restore();
  const nK = eOut(seg(u, 0.8, 1.25));
  ctx.save(); ctx.globalAlpha = (1 - outK) * nK; ctx.fillStyle = fg; ctx.font = `500 46px Onest`;
  wrap(ctx, it.name || "", W - 180, 2).forEach((l, li) => ctx.fillText(l, 90, 1496 + li * 58)); ctx.restore();
  if (c.prices && it.price) {
    const pK = eBack(seg(u, 1.0, 1.45));
    ctx.save(); ctx.translate(90, 1690); ctx.scale(pK, pK);
    const pw = pill(ctx, rub(it.price * eOut(seg(u, 1.0, 1.7))), 0, -12, 64, isDark(bg) ? LIME : INK, isDark(bg) ? INK : LIME);
    if (off) { ctx.font = `600 44px Onest`; ctx.fillStyle = fg; ctx.globalAlpha = 0.6; ctx.textBaseline = "middle"; const ow = ctx.measureText(rub(it.old!)).width; ctx.fillText(rub(it.old!), pw + 30, -12); ctx.fillRect(pw + 28, -14, ow + 4, 4); }
    ctx.restore();
  }
  ctx.restore();
};

const outro: Draw = (ctx, u, s, c) => {
  wipe(ctx, seg(u, 0, 0.55), INK, W / 2, H / 2);
  if (u < 0.3) return;
  sparkles(ctx, u, 42, LIME, 10, 0.7);
  const lk = eBack(seg(u, 0.35, 0.85));
  ctx.save(); ctx.translate(W / 2, 520); ctx.scale(lk, lk); wordmark(ctx, 0, 0, 64, PAPER); ctx.restore();
  ctx.textAlign = "center"; ctx.textBaseline = "alphabetic";
  const tK = eOut(seg(u, 0.55, 1.05));
  ctx.save(); ctx.globalAlpha = tK; ctx.translate(0, (1 - tK) * 60); ctx.fillStyle = PAPER; ctx.font = `800 86px Unbounded`;
  wrap(ctx, c.outro.toUpperCase(), W - 160, 3).forEach((l, li) => ctx.fillText(l, W / 2, 820 + li * 100)); ctx.restore();
  const sK = eBack(seg(u, 0.85, 1.35)) * (1 + 0.03 * Math.sin(u * 6));
  ctx.save(); ctx.translate(W / 2, 1180); ctx.scale(sK, sK); pill(ctx, c.site, 0, -14, 72, LIME, INK, "center"); ctx.restore();
  if (c.promo) {
    const pK = eBack(seg(u, 1.2, 1.7));
    ctx.save(); ctx.translate(W / 2, 1420); ctx.scale(pK, pK); ctx.rotate(-0.03);
    ctx.setLineDash([18, 14]); ctx.lineWidth = 5; ctx.strokeStyle = PAPER; rr(ctx, -380, -110, 760, 200, 36); ctx.stroke(); ctx.setLineDash([]);
    ctx.textAlign = "center"; ctx.fillStyle = PAPER; ctx.textBaseline = "middle"; ctx.font = `500 40px Onest`; ctx.fillText("промокод на первый заказ", 0, -52);
    ctx.fillStyle = PINK; ctx.font = `800 84px Unbounded`; ctx.fillText(c.promo, 0, 28); ctx.restore();
  }
  const aK = eOut(seg(u, 1.6, 2.1));
  ctx.save(); ctx.textAlign = "center"; ctx.globalAlpha = aK; ctx.fillStyle = LIME; ctx.font = `400 80px "Marck Script"`;
  ctx.fillText("доставка Ozon 1–5 дней", W / 2, 1700 + Math.sin(u * 3) * 6); ctx.restore();
  ctx.textAlign = "left";
};

/* spotlight: one hero product */
const spotIntro: Draw = (ctx, u, s, c) => {
  ctx.fillStyle = INK; ctx.fillRect(0, 0, W, H);
  const brand = ((c.items[0]?.brand || "MAGIC VIBES").toUpperCase());
  // letters rise one by one, then the word scales slowly
  ctx.font = `800 ${Math.min(210, 1700 / Math.max(4, brand.length))}px Unbounded`; ctx.textBaseline = "alphabetic"; ctx.textAlign = "left";
  const total = ctx.measureText(brand).width; let x = (W - total) / 2;
  const sc = lerp(1, 1.08, seg(u, 0, s.dur));
  ctx.save(); ctx.translate(W / 2, H / 2); ctx.scale(sc, sc); ctx.translate(-W / 2, -H / 2);
  [...brand].forEach((ch, i) => {
    const k = eOut(seg(u, 0.1 + i * 0.05, 0.6 + i * 0.05));
    ctx.save(); ctx.globalAlpha = k; ctx.fillStyle = i % 5 === 2 ? LIME : PAPER; ctx.translate(0, (1 - k) * 140); ctx.fillText(ch, x, H / 2 + 60); ctx.restore();
    x += ctx.measureText(ch).width;
  });
  ctx.restore();
  const k2 = eOut(seg(u, 0.9, 1.5));
  ctx.save(); ctx.globalAlpha = k2; ctx.fillStyle = PINK; ctx.font = `400 96px "Marck Script"`; ctx.textAlign = "center";
  ctx.fillText(c.script || "аромат недели", W / 2, H / 2 + 230); ctx.restore();
  sparkles(ctx, u, 7, LIME, 10, 0.6);
};
const spotHero: Draw = (ctx, u, s, c, a) => {
  const bg = s.bg, fg = isDark(bg) ? PAPER : INK;
  wipe(ctx, seg(u, 0, 0.5), bg, W / 2, H * 0.4);
  if (u < 0.25) return;
  // rotating light rays behind the bottle
  ctx.save(); ctx.translate(W / 2, 780); ctx.rotate(u * 0.25); ctx.globalAlpha = 0.18 * seg(u, 0.3, 0.9);
  for (let r = 0; r < 12; r++) { ctx.rotate(Math.PI / 6); ctx.fillStyle = "#ffffff"; ctx.beginPath(); ctx.moveTo(0, 0); ctx.lineTo(-60, -1100); ctx.lineTo(60, -1100); ctx.closePath(); ctx.fill(); }
  ctx.restore();
  const k = eBack(seg(u, 0.3, 1.0));
  ctx.save(); ctx.translate(W / 2, 780 + Math.sin(u * 1.6) * 12); ctx.scale(lerp(0.5, 1, k), lerp(0.5, 1, k));
  photo(ctx, a.imgs[0], 880, 960, lerp(1, 1.08, seg(u, 0.5, s.dur))); ctx.restore();
  const it = c.items[0] || {};
  ctx.textAlign = "center"; ctx.textBaseline = "alphabetic";
  const nK = eOut(seg(u, 0.7, 1.2));
  ctx.save(); ctx.globalAlpha = nK; ctx.fillStyle = fg; ctx.font = `800 56px Unbounded`;
  wrap(ctx, (it.name || "").toUpperCase(), W - 160, 2).forEach((l, li) => ctx.fillText(l, W / 2, 1390 + li * 66)); ctx.restore();
  // notes as chips, popping one by one
  const notes = c.notes.split(/[,;]/).map((x) => x.trim()).filter(Boolean).slice(0, 5);
  ctx.font = `700 42px Onest`;
  const widths = notes.map((n) => ctx.measureText(n).width + 64);
  const rows: number[][] = [[]]; let rowW = 0;
  widths.forEach((w, i) => { if (rowW + w > W - 160 && rows[rows.length - 1].length) { rows.push([]); rowW = 0; } rows[rows.length - 1].push(i); rowW += w + 16; });
  rows.forEach((row, ri) => {
    const rw = row.reduce((sum, i) => sum + widths[i] + 16, -16); let x = (W - rw) / 2;
    row.forEach((i) => {
      const pk = eBack(seg(u, 1.3 + i * 0.22, 1.7 + i * 0.22));
      ctx.save(); ctx.translate(x + widths[i] / 2, 1590 + ri * 96); ctx.scale(pk, pk);
      rr(ctx, -widths[i] / 2, -38, widths[i], 76, 38); ctx.fillStyle = isDark(bg) ? LIME : INK; ctx.fill();
      ctx.fillStyle = isDark(bg) ? INK : LIME; ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.fillText(notes[i], 0, 2); ctx.restore();
      x += widths[i] + 16;
    });
  });
};
const spotPrice: Draw = (ctx, u, s, c, a) => {
  wipe(ctx, seg(u, 0, 0.5), LIME, W * 0.2, H * 0.8);
  if (u < 0.25) return;
  const it = c.items[0] || {};
  const k = eBack(seg(u, 0.25, 0.8));
  ctx.save(); ctx.translate(W / 2, 640); ctx.rotate(-0.05); ctx.scale(k * 0.62, k * 0.62); photo(ctx, a.imgs[0], 880, 960); ctx.restore();
  ctx.textAlign = "center"; ctx.textBaseline = "alphabetic";
  if (c.prices && it.price) {
    const pk = eBack(seg(u, 0.5, 1.0));
    ctx.save(); ctx.translate(W / 2, 1230); ctx.scale(pk, pk);
    ctx.fillStyle = INK; ctx.font = `800 150px Unbounded`; ctx.fillText(rub(it.price * eOut(seg(u, 0.5, 1.3))), 0, 0);
    const off = discount(it);
    if (off) { ctx.font = `600 56px Onest`; ctx.globalAlpha = 0.55; const t = rub(it.old!); const w = ctx.measureText(t).width; ctx.fillText(t, 0, -170); ctx.fillRect(-w / 2, -188, w, 6); }
    ctx.restore();
  }
  const lines = ["✓ 100% оригинал", "✓ доставка Ozon 1–5 дней", "✓ оплата картой и СБП"];
  lines.forEach((l, i) => {
    const lk = eOut(seg(u, 1.0 + i * 0.2, 1.4 + i * 0.2));
    ctx.save(); ctx.globalAlpha = lk; ctx.translate((1 - lk) * 80, 0); ctx.fillStyle = INK; ctx.font = `600 50px Onest`; ctx.fillText(l, W / 2, 1420 + i * 80); ctx.restore();
  });
};

/* sale */
const saleIntro: Draw = (ctx, u, s, c) => {
  ctx.fillStyle = PINK; ctx.fillRect(0, 0, W, H);
  // diagonal stripes sliding
  ctx.save(); ctx.globalAlpha = 0.14; ctx.fillStyle = INK; ctx.rotate(-0.35);
  for (let x = -H; x < W + H; x += 160) ctx.fillRect(x + ((u * 220) % 160), -H, 70, H * 3); ctx.restore();
  const maxOff = Math.max(0, ...c.items.map(discount));
  const blink = u < 1.2 ? (Math.floor(u * 8) % 2 ? 1 : 0.4) : 1;
  const k = eBack(seg(u, 0.1, 0.7));
  ctx.save(); ctx.translate(W / 2, 760); ctx.rotate(-0.08); ctx.scale(k, k); ctx.globalAlpha = blink;
  ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.fillStyle = INK; ctx.font = `800 300px Unbounded`; ctx.fillText("SALE", 0, 0); ctx.restore();
  if (maxOff) {
    const pk = eBack(seg(u, 0.8, 1.3));
    ctx.save(); ctx.translate(W / 2, 1160); ctx.scale(pk, pk); ctx.rotate(0.05);
    ctx.fillStyle = LIME; ctx.beginPath(); ctx.arc(0, 0, 220, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = INK; ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.font = `600 44px Onest`; ctx.fillText("до", 0, -90);
    ctx.font = `800 130px Unbounded`; ctx.fillText(`−${maxOff}%`, 0, 20); ctx.restore();
  }
  const tk = eOut(seg(u, 1.2, 1.7));
  ctx.save(); ctx.globalAlpha = tk; ctx.textAlign = "center"; ctx.fillStyle = PAPER; ctx.font = `400 100px "Marck Script"`; ctx.fillText(c.script || "только на сайте", W / 2, 1540); ctx.restore();
};
const saleItem: Draw = (ctx, u, s, c, a) => {
  const bg = s.bg, dark = isDark(bg), fg = dark ? PAPER : INK;
  // hard cut with a quick flash instead of a wipe: faster rhythm for sale
  ctx.fillStyle = bg; ctx.fillRect(0, 0, W, H);
  if (u < 0.08) { ctx.fillStyle = "#ffffff"; ctx.globalAlpha = 1 - u / 0.08; ctx.fillRect(0, 0, W, H); ctx.globalAlpha = 1; }
  const it = c.items[s.i] || {};
  const k = eBack(seg(u, 0.02, 0.45));
  ctx.save(); ctx.translate(W / 2, 760); ctx.rotate(s.i % 2 ? 0.04 : -0.04); ctx.scale(lerp(1.3, 1, k), lerp(1.3, 1, k)); ctx.globalAlpha = seg(u, 0, 0.2);
  photo(ctx, a.imgs[s.i], 820, 900, lerp(1.06, 1, seg(u, 0, s.dur))); ctx.restore();
  ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
  ctx.fillStyle = fg; ctx.font = `800 64px Unbounded`; ctx.globalAlpha = eOut(seg(u, 0.2, 0.5));
  ctx.fillText(wrap(ctx, (it.brand || "").toUpperCase(), W - 180, 1)[0] || "", 90, 1330);
  ctx.font = `500 42px Onest`; wrap(ctx, it.name || "", W - 180, 2).forEach((l, li) => ctx.fillText(l, 90, 1400 + li * 54)); ctx.globalAlpha = 1;
  if (c.prices && it.price) {
    const off = discount(it);
    if (off) {
      // old price, then a strike line draws across it
      ctx.font = `700 64px Onest`; ctx.fillStyle = fg; ctx.globalAlpha = 0.7; const t = rub(it.old!); const w = ctx.measureText(t).width;
      ctx.fillText(t, 90, 1600); ctx.globalAlpha = 1; ctx.fillStyle = PINK; ctx.fillRect(84, 1578, (w + 12) * eOut(seg(u, 0.45, 0.7)), 10);
    }
    const pk = eBack(seg(u, 0.65, 1.0));
    ctx.save(); ctx.translate(90, 1720); ctx.scale(pk, pk); ctx.rotate(-0.03);
    pill(ctx, rub(it.price), 0, 0, 72, dark ? LIME : INK, dark ? INK : LIME); ctx.restore();
    if (off) {
      const sk = eBack(seg(u, 0.8, 1.1));
      ctx.save(); ctx.translate(W - 190, 1690); ctx.rotate(0.2); ctx.scale(sk, sk);
      ctx.fillStyle = PINK; ctx.beginPath(); ctx.arc(0, 0, 120, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = "#fff"; ctx.textAlign = "center"; ctx.textBaseline = "middle"; ctx.font = `800 58px Unbounded`; ctx.fillText(`−${off}%`, 0, 4); ctx.restore();
    }
  }
};
const saleOutro: Draw = (ctx, u, s, c) => {
  ctx.fillStyle = PINK; ctx.fillRect(0, 0, W, H);
  sparkles(ctx, u, 9, "#ffffff", 10, 0.8);
  ctx.textAlign = "center"; ctx.textBaseline = "alphabetic";
  const k = eBack(seg(u, 0.1, 0.6));
  ctx.save(); ctx.translate(W / 2, 560); ctx.scale(k, k); wordmark(ctx, 0, 0, 62, INK); ctx.restore();
  if (c.deadline) {
    const dk = eOut(seg(u, 0.4, 0.9));
    ctx.save(); ctx.globalAlpha = dk; ctx.fillStyle = INK; ctx.font = `800 80px Unbounded`;
    wrap(ctx, `АКЦИЯ ${c.deadline.toUpperCase()}`, W - 160, 2).forEach((l, li) => ctx.fillText(l, W / 2, 860 + li * 96)); ctx.restore();
  }
  const pk = eBack(seg(u, 0.8, 1.3)) * (1 + 0.04 * Math.sin(u * 7));
  ctx.save(); ctx.translate(W / 2, 1180); ctx.scale(pk, pk); pill(ctx, c.site, 0, 0, 72, INK, LIME, "center"); ctx.restore();
  if (c.promo) {
    const qk = eOut(seg(u, 1.2, 1.7));
    ctx.save(); ctx.globalAlpha = qk; ctx.fillStyle = "#fff"; ctx.font = `500 44px Onest`; ctx.fillText("промокод на первый заказ", W / 2, 1400);
    ctx.fillStyle = INK; ctx.font = `800 96px Unbounded`; ctx.fillText(c.promo, W / 2, 1510); ctx.restore();
  }
};

/* grid (new arrivals) */
const gridIntro: Draw = (ctx, u, s, c) => {
  ctx.fillStyle = LIME; ctx.fillRect(0, 0, W, H);
  const word = (c.title || "НОВИНКИ").toUpperCase();
  ctx.font = `800 170px Unbounded`; ctx.textBaseline = "middle"; ctx.textAlign = "left";
  const w = ctx.measureText(word + "  ✦  ").width;
  for (let r = 0; r < 9; r++) {
    const y = 140 + r * 210, dir = r % 2 ? 1 : -1, off = ((u * 260 * dir) % w + w) % w;
    ctx.fillStyle = r === 4 ? INK : "rgba(18,18,18,0.12)";
    for (let x = -w + off - (r * 137) % w; x < W; x += w) ctx.fillText(word + "  ✦  ", x, y);
  }
  const k = eBack(seg(u, 0.5, 1.1));
  ctx.save(); ctx.translate(W / 2, 1640); ctx.scale(k, k); ctx.fillStyle = PINK; ctx.font = `400 110px "Marck Script"`; ctx.textAlign = "center";
  ctx.fillText(c.script || "только что на сайте", 0, 0); ctx.restore();
};
const grid: Draw = (ctx, u, s, c, a) => {
  wipe(ctx, seg(u, 0, 0.45), PAPER, W / 2, H / 2);
  if (u < 0.2) return;
  const n = Math.min(4, Math.max(1, c.items.length)), cellW = 470, cellH = 600;
  const pos = [[-1, -1], [1, -1], [-1, 1], [1, 1]];
  for (let i = 0; i < n; i++) {
    const k = eBack(seg(u, 0.3 + i * 0.18, 0.8 + i * 0.18));
    const [px, py] = pos[i];
    ctx.save(); ctx.translate(W / 2 + px * 255, 900 + py * 330 + Math.sin(u * 1.5 + i) * 8); ctx.rotate((i % 2 ? 1 : -1) * 0.03 * (1 - k * 0.5)); ctx.scale(k, k);
    photo(ctx, a.imgs[i], cellW, cellH, 1, 18, 40);
    ctx.fillStyle = INK; ctx.font = `800 30px Unbounded`; ctx.textAlign = "center";
    ctx.fillText(wrap(ctx, (c.items[i]?.brand || "").toUpperCase(), cellW - 40, 1)[0] || "", 0, cellH / 2 + 50);
    ctx.restore();
  }
  const tk = eOut(seg(u, 1.2, 1.7));
  ctx.save(); ctx.globalAlpha = tk; ctx.textAlign = "center"; ctx.fillStyle = INK; ctx.font = `800 64px Unbounded`; ctx.fillText((c.title || "НОВИНКИ").toUpperCase(), W / 2, 260); ctx.restore();
};

/* brand: kinetic typography + carousel */
const brandIntro: Draw = (ctx, u, s, c) => {
  ctx.fillStyle = INK; ctx.fillRect(0, 0, W, H);
  const brand = (c.items[0]?.brand || c.title || "MAGIC VIBES").toUpperCase();
  ctx.textBaseline = "middle"; ctx.textAlign = "left";
  for (let r = 0; r < 7; r++) {
    const size = r === 3 ? 190 : 120;
    ctx.font = `800 ${size}px Unbounded`;
    const w = ctx.measureText(brand + " ").width, dir = r % 2 ? 1 : -1, off = ((u * (r === 3 ? 120 : 200) * dir) % w + w) % w;
    const k = eOut(seg(u, r * 0.08, 0.5 + r * 0.08));
    ctx.save(); ctx.globalAlpha = k; ctx.fillStyle = r === 3 ? LIME : r % 2 ? "rgba(245,242,236,0.16)" : "transparent";
    ctx.strokeStyle = "rgba(245,242,236,0.35)"; ctx.lineWidth = 2;
    for (let x = -w + off; x < W + w; x += w) { if (r === 3 || r % 2) ctx.fillText(brand, x, 330 + r * 210); else ctx.strokeText(brand, x, 330 + r * 210); }
    ctx.restore();
  }
  const k = eOut(seg(u, 1.0, 1.6));
  ctx.save(); ctx.globalAlpha = k; ctx.fillStyle = PINK; ctx.font = `400 104px "Marck Script"`; ctx.textAlign = "center"; ctx.fillText(c.script || "в Magic Vibes", W / 2, 1720); ctx.restore();
};
const carousel: Draw = (ctx, u, s, c, a) => {
  const bg = s.bg, fg = isDark(bg) ? PAPER : INK;
  wipe(ctx, seg(u, 0, 0.4), bg, W, H / 2);
  if (u < 0.2) return;
  const it = c.items[s.i] || {};
  // the card slides in from the right and out to the left
  const inK = eOut(seg(u, 0.15, 0.7)), outK = eInOut(seg(u, s.dur - 0.45, s.dur));
  const x = W / 2 + (1 - inK) * W * 0.9 - outK * W * 0.9;
  ctx.save(); ctx.translate(x, 820); ctx.rotate((1 - inK) * 0.12 - outK * 0.12); photo(ctx, a.imgs[s.i], 820, 940, lerp(1, 1.05, seg(u, 0.4, s.dur))); ctx.restore();
  // next card peeking on the right
  if (s.i + 1 < c.items.length) { ctx.save(); ctx.globalAlpha = 0.5 * inK * (1 - outK); ctx.translate(W + 330 - outK * 300, 860); ctx.scale(0.7, 0.7); photo(ctx, a.imgs[s.i + 1], 820, 940); ctx.restore(); }
  ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
  const tk = eOut(seg(u, 0.5, 0.9)) * (1 - outK);
  ctx.save(); ctx.globalAlpha = tk; ctx.fillStyle = fg; ctx.font = `500 48px Onest`;
  wrap(ctx, it.name || "", W - 180, 2).forEach((l, li) => ctx.fillText(l, 90, 1420 + li * 60));
  if (c.prices && it.price) pill(ctx, rub(it.price), 90, 1640, 60, isDark(bg) ? LIME : INK, isDark(bg) ? INK : LIME);
  ctx.fillStyle = fg; ctx.globalAlpha = tk * 0.5; ctx.font = `800 44px Unbounded`; ctx.textAlign = "right";
  ctx.fillText(`${s.i + 1}/${c.items.length}`, W - 90, 1640 + 20); ctx.restore();
};

const DRAW: Record<string, Draw> = {
  intro, product, outro, "spot-intro": spotIntro, "spot-hero": spotHero, "spot-price": spotPrice,
  "sale-intro": saleIntro, "sale-item": saleItem, "sale-outro": saleOutro, "grid-intro": gridIntro, grid,
  "brand-intro": brandIntro, carousel,
};

export function drawFrame(ctx: CanvasRenderingContext2D, t: number, c: Cfg, a: Assets, scenes: Scene[]) {
  ctx.clearRect(0, 0, W, H);
  ctx.globalAlpha = 1; ctx.textAlign = "left"; ctx.textBaseline = "alphabetic";
  let idx = scenes.findIndex((s) => t < s.start + s.dur);
  if (idx < 0) idx = scenes.length - 1;
  const s = scenes[idx];
  // background of the previous scene stays under the next one's wipe
  ctx.fillStyle = idx > 0 ? scenes[idx - 1].bg : s.bg; ctx.fillRect(0, 0, W, H);
  (DRAW[s.kind] || intro)(ctx, Math.min(t - s.start, s.dur), s, c, a);
}
