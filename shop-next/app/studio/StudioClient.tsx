"use client";
import { useEffect, useMemo, useRef, useState } from "react";
import { Download, Copy, Search, Upload, Check, Loader2, X } from "lucide-react";
import type { ShopProduct } from "@/lib/types";
import { productImg } from "@/lib/img";
import { toProductSlug } from "@/lib/slug";
import { brandHref } from "@/lib/landings";
import StoryVideo from "./StoryVideo";

/* ─────────────────────────── config ─────────────────────────── */
const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";
const SITE = "https://magicvibes.ru";

const FORMATS = { "1:1": 1080, "4:5": 1350, "9:16": 1920 } as const;
type Fmt = keyof typeof FORMATS;

const THEMES = {
  lime:   { label: "Лайм",     bg: "#d9f84a", fg: "#121212", accent: "#ff3d7f", chip: "#121212", chipFg: "#d9f84a" },
  pink:   { label: "Цветы",    bg: "#ffc8dc", fg: "#121212", accent: "#4b3cff", chip: "#121212", chipFg: "#ffc8dc" },
  blue:   { label: "Свежесть", bg: "#a9dcff", fg: "#121212", accent: "#ff3d7f", chip: "#121212", chipFg: "#a9dcff" },
  sand:   { label: "Дерево",   bg: "#d9c7a4", fg: "#121212", accent: "#ff3d7f", chip: "#121212", chipFg: "#d9f84a" },
  orange: { label: "Восток",   bg: "#ff9d4d", fg: "#121212", accent: "#4b3cff", chip: "#121212", chipFg: "#ff9d4d" },
  violet: { label: "Ниша",     bg: "#4b3cff", fg: "#f5f2ec", accent: "#d9f84a", chip: "#d9f84a", chipFg: "#121212" },
  ink:    { label: "Ночь",     bg: "#121212", fg: "#f5f2ec", accent: "#d9f84a", chip: "#d9f84a", chipFg: "#121212" },
  paper:  { label: "Бумага",   bg: "#f5f2ec", fg: "#121212", accent: "#ff3d7f", chip: "#121212", chipFg: "#d9f84a" },
} as const;
type ThemeKey = keyof typeof THEMES;
type Theme = (typeof THEMES)[ThemeKey];

type Tpl = "new" | "sale" | "top" | "quote" | "brand" | "review" | "tips";
const TEMPLATES: { id: Tpl; label: string; theme: ThemeKey; slots: number; fields: (keyof Fields)[] }[] = [
  { id: "new",    label: "Новинка",  theme: "pink",   slots: 1, fields: ["kicker", "brand", "title", "script", "text", "price"] },
  { id: "sale",   label: "Акция",    theme: "lime",   slots: 1, fields: ["kicker", "discount", "title", "oldPrice", "price", "promo", "text"] },
  { id: "top",    label: "Подборка", theme: "paper",  slots: 3, fields: ["kicker", "title", "script"] },
  { id: "quote",  label: "Цитата",   theme: "ink",    slots: 0, fields: ["kicker", "text", "author"] },
  { id: "brand",  label: "Бренд",    theme: "violet", slots: 1, fields: ["kicker", "brand", "script", "text", "stat1", "stat2"] },
  { id: "review", label: "Отзыв",    theme: "pink",   slots: 1, fields: ["kicker", "text", "author", "script", "price"] },
  { id: "tips",   label: "Советы",   theme: "blue",   slots: 0, fields: ["kicker", "title", "script", "list"] },
];

interface Fields {
  kicker: string; brand: string; title: string; script: string; text: string;
  price: string; oldPrice: string; discount: string; promo: string;
  author: string; stat1: string; stat2: string; list: string;
}
const FIELD_LABELS: Record<keyof Fields, string> = {
  kicker: "Плашка", brand: "Бренд", title: "Заголовок", script: "Подпись (рукописная)", text: "Текст",
  price: "Цена", oldPrice: "Старая цена", discount: "Скидка", promo: "Промокод",
  author: "Автор", stat1: "Цифра 1", stat2: "Цифра 2", list: "Пункты (каждый с новой строки)",
};
const DEFAULTS: Record<Tpl, Partial<Fields>> = {
  new:    { kicker: "Новинка", brand: "Tom Ford", title: "Oud Wood", script: "дымный уд и сандал", text: "Парфюмерная вода · 100 мл · в наличии", price: "33 048 ₽" },
  sale:   { kicker: "Акция недели", discount: "−20%", title: "Montale Roses Musk", oldPrice: "12 900 ₽", price: "10 320 ₽", promo: "VIBES20", text: "только до 5 октября" },
  top:    { kicker: "Подборка", title: "Топ-3 на осень", script: "тёплые и уютные" },
  quote:  { kicker: "Мысль дня", text: "Аромат — это невидимый аксессуар, который оставляет самое сильное воспоминание", author: "Коко Шанель" },
  brand:  { kicker: "Бренд недели", brand: "Byredo", script: "шведская ниша", text: "Минимализм, редкие ноты и безупречные флаконы.", stat1: "48 ароматов", stat2: "от 9 900 ₽" },
  review: { kicker: "Отзыв покупателя", text: "Пришёл за 2 дня, упаковка идеальная, аромат оригинал — сравнила с тестером в ЦУМе. Буду заказывать ещё!", author: "Анна", script: "Краснодар", price: "18 490 ₽" },
  tips:   { kicker: "Гид", title: "5 правил стойкости", script: "аромат на весь день", list: "Наносите на увлажнённую кожу\nТочки пульса: запястья и шея\nНе растирайте запястья\nХраните флакон вдали от солнца\nОдежда держит аромат дольше" },
};
const EMPTY: Fields = { kicker: "", brand: "", title: "", script: "", text: "", price: "", oldPrice: "", discount: "", promo: "", author: "", stat1: "", stat2: "", list: "" };

interface Slot { product?: ShopProduct; upload?: string }

/** brand + short title from a catalogue name (many items have an empty brand field) */
function splitName(p: ShopProduct) {
  const esc = (x: string) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const brand = (p.brand || "").trim() || (p.name.match(/^([^-–—(]{2,40}?)\s+[-–—]\s+/)?.[1] ?? "");
  let title = brand ? p.name.replace(new RegExp(`^${esc(brand)}\\s*[-–—]?\\s*`, "i"), "") : p.name;
  title = title.replace(/\s*(\(|парфюм|туалетн|вода\b|духи|одеколон|набор|тестер|отливант|пробник|\d+\s*мл).*$/i, "").trim();
  return { brand, title: title || p.name };
}

const rub = (n: number) => `${Math.round(n).toLocaleString("ru-RU")} ₽`;
/** shrink long strings so they never overflow the frame */
const fit = (text: string, base: number, chars: number) => Math.round(base * Math.min(1, Math.pow(chars / Math.max(chars, text.length || 1), 0.75)));

/* ─────────────────────────── canvas pieces ─────────────────────────── */
function Mark({ size = 64, dark = true }: { size?: number; dark?: boolean }) {
  return (
    <svg width={size} height={size} viewBox="0 0 40 40" style={{ display: "block", flexShrink: 0 }}>
      <circle cx="20" cy="20" r="19" fill={dark ? "#121212" : "#f5f2ec"} />
      <ellipse cx="13.5" cy="12" rx="5.5" ry="3.2" transform="rotate(-35 13.5 12)" fill={dark ? "rgba(255,255,255,0.35)" : "rgba(18,18,18,0.18)"} />
      <path d="M24 9.5c.9 5.3 3.2 7.6 8.5 8.5-5.3.9-7.6 3.2-8.5 8.5-.9-5.3-3.2-7.6-8.5-8.5 5.3-.9 7.6-3.2 8.5-8.5Z" fill="#d9f84a" />
      <circle cx="14" cy="27.5" r="2.4" fill="#ff3d7f" />
    </svg>
  );
}

function Brandline({ t }: { t: Theme }) {
  const dark = t.fg === "#121212";
  return (
    <div className="st-logo" style={{ color: t.fg }}>
      <Mark size={60} dark={dark} />
      <span className="st-logo-w">MAGIC<em>vibes</em></span>
    </div>
  );
}

function Frame({ t, h, kicker, children, footer = true }: { t: Theme; h: number; kicker?: string; children: React.ReactNode; footer?: boolean }) {
  return (
    <div className="st-canvas" style={{ width: 1080, height: h, background: t.bg, color: t.fg }}>
      <div className="st-top">
        <Brandline t={t} />
        {kicker && <span className="st-kicker" style={{ background: t.chip, color: t.chipFg }}>✦ {kicker}</span>}
      </div>
      <div className="st-body">{children}</div>
      {footer && (
        <div className="st-foot">
          <span>magicvibes.ru</span>
          <span className="st-foot-r"><i style={{ color: t.accent }}>✦</i> оригинальная парфюмерия · доставка 1–5 дней</span>
        </div>
      )}
    </div>
  );
}

function Photo({ slot, className = "", style }: { slot?: Slot; className?: string; style?: React.CSSProperties }) {
  const src = slot?.upload || (slot?.product?.images?.[0] ? productImg(slot.product.images[0], 960) : "");
  return (
    <div className={`st-photo ${className}`} style={style}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      {src ? <img src={src} alt="" crossOrigin="anonymous" /> : <span className="st-photo-empty">Фото товара</span>}
    </div>
  );
}

/* ─────────────────────────── templates ─────────────────────────── */
function Render({ tpl, fmt, t, f, slots }: { tpl: Tpl; fmt: Fmt; t: Theme; f: Fields; slots: Slot[] }) {
  const h = FORMATS[fmt];
  const tall = fmt !== "1:1";

  if (tpl === "new") return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-bgword" style={{ color: t.fg }}>{(f.brand || "vibes").toLowerCase()}</div>
      <div className="st-new-photo-wrap">
        <Photo slot={slots[0]} className="st-new-photo" />
        <span className="st-sticker" style={{ background: t.accent, color: t.accent === "#d9f84a" ? "#121212" : "#fff" }}>NEW</span>
      </div>
      <div className="st-new-copy">
        {f.brand && <div className="st-brand">{f.brand}</div>}
        <div className="st-title" style={{ fontSize: fit(f.title, tall ? 112 : 92, 14) }}>{f.title}</div>
        {f.script && <div className="st-script" style={{ color: t.accent }}>{f.script}</div>}
        <div className="st-row">
          {f.price && <span className="st-price" style={{ background: "#121212", color: "#d9f84a" }}>{f.price}</span>}
          {f.text && <span className="st-meta">{f.text}</span>}
        </div>
      </div>
    </Frame>
  );

  if (tpl === "sale") return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-sale-big" style={{ fontSize: fit(f.discount, 215, 4) }}>{f.discount}</div>
      <div className="st-sale-grid" style={{ flexDirection: tall ? "column" : "row" }}>
        <Photo slot={slots[0]} className="st-sale-photo" style={tall ? { width: "100%", flex: 1 } : undefined} />
        <div className="st-sale-info">
          <div className="st-title" style={{ fontSize: fit(f.title, 62, 18) }}>{f.title}</div>
          <div className="st-row" style={{ marginTop: 22, alignItems: "baseline" }}>
            {f.oldPrice && <s className="st-old">{f.oldPrice}</s>}
            {f.price && <span className="st-newprice">{f.price}</span>}
          </div>
          {f.promo && (
            <div className="st-promo" style={{ borderColor: t.fg }}>
              <span>промокод</span><b>{f.promo}</b>
            </div>
          )}
          {f.text && <div className="st-script" style={{ color: t.accent, marginTop: 18 }}>{f.text}</div>}
        </div>
      </div>
    </Frame>
  );

  if (tpl === "top") return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-title" style={{ fontSize: fit(f.title, tall ? 104 : 88, 16) }}>{f.title}</div>
      {f.script && <div className="st-script" style={{ color: t.accent }}>{f.script}</div>}
      <div className={`st-top3 ${tall ? "col" : "row"}`}>
        {[0, 1, 2].map((i) => {
          const p = slots[i]?.product;
          return (
            <div key={i} className="st-top3-card">
              <span className="st-num" style={{ background: t.chip, color: t.chipFg }}>0{i + 1}</span>
              <Photo slot={slots[i]} className="st-top3-photo" />
              <div className="st-top3-txt">
                {(!p || splitName(p).brand) && <div className="st-brand" style={{ fontSize: 24 }}>{p ? splitName(p).brand : "Бренд"}</div>}
                <div className="st-top3-name">{p ? (splitName(p).brand ? splitName(p).title : p.name) : "Название аромата"}</div>
                <div className="st-top3-price">{p ? rub(p.priceRub) : "0 ₽"}</div>
              </div>
            </div>
          );
        })}
      </div>
    </Frame>
  );

  if (tpl === "quote") return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-quote-mark" style={{ color: t.accent }}>“</div>
      <div className="st-quote" style={{ fontSize: fit(f.text, tall ? 76 : 64, 70) }}>{f.text}</div>
      {f.author && <div className="st-script" style={{ color: t.accent, fontSize: 64, marginTop: 30 }}>— {f.author}</div>}
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img className="st-quote-flacon" src="/brand/shots/flacon-cutout.png?v=3" alt="" />
    </Frame>
  );

  if (tpl === "brand") return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-brandname" style={{ fontSize: fit(f.brand, tall ? 190 : 160, 7) }}>{f.brand}</div>
      {f.script && <div className="st-script" style={{ color: t.accent, fontSize: 64 }}>{f.script}</div>}
      <div className="st-brand-grid" style={{ flexDirection: tall ? "column" : "row" }}>
        <Photo slot={slots[0]} className="st-brand-photo" style={tall ? { width: "100%", flex: 1 } : undefined} />
        <div className="st-brand-info">
          {f.text && <div className="st-text">{f.text}</div>}
          <div className="st-stats">
            {f.stat1 && <div className="st-stat" style={{ background: t.chip, color: t.chipFg }}>{f.stat1}</div>}
            {f.stat2 && <div className="st-stat ghost" style={{ borderColor: t.fg }}>{f.stat2}</div>}
          </div>
        </div>
      </div>
    </Frame>
  );

  if (tpl === "review") {
    const p = slots[0]?.product;
    return (
      <Frame t={t} h={h} kicker={f.kicker}>
        <div className="st-stars" style={{ color: t.accent }}>★★★★★</div>
        <div className="st-review" style={{ fontSize: fit(f.text, tall ? 60 : 50, 110) }}>«{f.text}»</div>
        <div className="st-row" style={{ marginTop: 26, alignItems: "baseline", gap: 18 }}>
          {f.author && <span className="st-author">{f.author}</span>}
          {f.script && <span className="st-script" style={{ color: t.accent, fontSize: 52 }}>{f.script}</span>}
        </div>
        <div className="st-mini">
          <Photo slot={slots[0]} className="st-mini-photo" />
          <div style={{ minWidth: 0, flex: 1 }}>
            {(!p || splitName(p).brand) && <div className="st-brand" style={{ fontSize: 24 }}>{p ? splitName(p).brand : "Бренд"}</div>}
            <div className="st-mini-name">{p?.name || "Название аромата"}</div>
          </div>
          {(f.price || p) && <span className="st-price" style={{ background: "#121212", color: "#d9f84a", fontSize: 38 }}>{f.price || (p ? rub(p.priceRub) : "")}</span>}
        </div>
      </Frame>
    );
  }

  // tips
  const items = f.list.split("\n").map((s) => s.trim()).filter(Boolean).slice(0, tall ? 7 : 5);
  return (
    <Frame t={t} h={h} kicker={f.kicker}>
      <div className="st-title" style={{ fontSize: fit(f.title, tall ? 104 : 90, 16) }}>{f.title}</div>
      {f.script && <div className="st-script" style={{ color: t.accent }}>{f.script}</div>}
      <ol className="st-tips">
        {items.map((it, i) => (
          <li key={i}><span className="st-num" style={{ background: t.chip, color: t.chipFg }}>{i + 1}</span><span style={{ fontSize: fit(it, tall ? 44 : 40, 34) }}>{it}</span></li>
        ))}
      </ol>
    </Frame>
  );
}

/* ─────────────────────────── caption for the TG post ─────────────────────────── */
function caption(tpl: Tpl, f: Fields, slots: Slot[]) {
  const link = (p?: ShopProduct) => (p ? `${SITE}/product/${toProductSlug(p.name, p.offerId)}` : `${SITE}/catalog`);
  const p = slots[0]?.product;
  switch (tpl) {
    case "new": return `✨ ${f.kicker || "Новинка"}: ${f.brand} ${f.title}\n\n${f.script ? f.script[0].toUpperCase() + f.script.slice(1) + ". " : ""}${f.text}\n\n💸 ${f.price}\n🛒 ${link(p)}`;
    case "sale": return `🔥 ${f.discount} на ${f.title}\n\n${f.oldPrice ? `Было ${f.oldPrice} → ` : ""}${f.price}${f.promo ? `\n🎟 Промокод: ${f.promo}` : ""}${f.text ? `\n⏳ ${f.text}` : ""}\n\n🛒 ${link(p)}`;
    case "top": return `🏆 ${f.title}${f.script ? ` — ${f.script}` : ""}\n\n${slots.slice(0, 3).map((s, i) => s.product ? `${i + 1}. ${s.product.name} — ${rub(s.product.priceRub)}\n${link(s.product)}` : "").filter(Boolean).join("\n\n")}`;
    case "quote": return `💬 «${f.text}»\n— ${f.author}\n\n✦ Magic Vibes · ${SITE}`;
    case "brand": return `🌟 ${f.kicker}: ${f.brand}\n\n${f.text}\n${[f.stat1, f.stat2].filter(Boolean).join(" · ")}\n\n🛒 ${SITE}${brandHref(f.brand)}`;
    case "review": return `⭐️⭐️⭐️⭐️⭐️ ${f.author}${f.script ? `, ${f.script}` : ""}\n\n«${f.text}»${p ? `\n\n${p.name}\n🛒 ${link(p)}` : ""}`;
    case "tips": return `📌 ${f.title}\n\n${f.list.split("\n").filter(Boolean).map((s, i) => `${i + 1}. ${s}`).join("\n")}\n\n✦ Больше гайдов: ${SITE}/blog`;
  }
}

/* ─────────────────────────── export ─────────────────────────── */
declare global { interface Window { htmlToImage?: { toPng: (n: HTMLElement, o?: object) => Promise<string> } } }
async function loadExporter() {
  if (window.htmlToImage) return window.htmlToImage;
  await new Promise<void>((res, rej) => {
    const s = document.createElement("script");
    s.src = "https://cdn.jsdelivr.net/npm/html-to-image@1.11.11/dist/html-to-image.js";
    s.onload = () => res(); s.onerror = () => rej(new Error("exporter"));
    document.head.appendChild(s);
  });
  return window.htmlToImage!;
}

// html-to-image can't read cross-origin Google Fonts rules, so the CSS is fetched here
// and every font file is inlined as a data: URL (once per session).
const FONTS_CSS = "https://fonts.googleapis.com/css2?family=Unbounded:wght@600;700;800&family=Onest:wght@400;500;600;700&family=Marck+Script&display=block&subset=cyrillic,latin";
let fontCssPromise: Promise<string> | null = null;
function embeddedFontCss() {
  fontCssPromise ||= (async () => {
    let css = await (await fetch(FONTS_CSS)).text();
    const urls = [...new Set([...css.matchAll(/url\((https:[^)]+)\)/g)].map((m) => m[1]))];
    await Promise.all(urls.map(async (u) => {
      const blob = await (await fetch(u)).blob();
      const data = await new Promise<string>((res) => { const r = new FileReader(); r.onload = () => res(String(r.result)); r.readAsDataURL(blob); });
      css = css.split(u).join(data);
    }));
    return css;
  })();
  return fontCssPromise;
}

/* ─────────────────────────── editor ─────────────────────────── */
export default function StudioClient() {
  const [mode, setMode] = useState<"post" | "video">("post");
  const modeSwitch = (
    <div className="st-group">
      <div className="st-chips sv-mode">
        <button className={mode === "post" ? "on" : ""} onClick={() => setMode("post")}>Посты (PNG)</button>
        <button className={mode === "video" ? "on" : ""} onClick={() => setMode("video")}>Видео-сторис</button>
      </div>
    </div>
  );
  return mode === "video" ? <StoryVideo modeSwitch={modeSwitch} /> : <PostStudio modeSwitch={modeSwitch} />;
}

function PostStudio({ modeSwitch }: { modeSwitch: React.ReactNode }) {
  const [tpl, setTpl] = useState<Tpl>("new");
  const [fmt, setFmt] = useState<Fmt>("4:5");
  const [themeKey, setThemeKey] = useState<ThemeKey>("pink");
  const [fields, setFields] = useState<Record<Tpl, Fields>>(() => Object.fromEntries(TEMPLATES.map((x) => [x.id, { ...EMPTY, ...DEFAULTS[x.id] }])) as Record<Tpl, Fields>);
  const [slots, setSlots] = useState<Record<Tpl, Slot[]>>(() => Object.fromEntries(TEMPLATES.map((x) => [x.id, []])) as unknown as Record<Tpl, Slot[]>);
  const [activeSlot, setActiveSlot] = useState(0);
  const [q, setQ] = useState("");
  const [results, setResults] = useState<ShopProduct[]>([]);
  const [searching, setSearching] = useState(false);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);
  const [scale, setScale] = useState(0.4);
  const stageRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLDivElement>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  const def = TEMPLATES.find((x) => x.id === tpl)!;
  const f = fields[tpl];
  const s = slots[tpl];
  const t = THEMES[themeKey];
  const h = FORMATS[fmt];

  // fit preview into the stage
  useEffect(() => {
    const el = stageRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      const w = el.clientWidth - 8, hh = Math.max(360, window.innerHeight - 140);
      setScale(Math.min(w / 1080, hh / h, 1));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [h]);

  // catalog search (debounced)
  useEffect(() => {
    const term = q.trim();
    if (term.length < 2) { setResults([]); return; }
    const id = setTimeout(async () => {
      setSearching(true);
      try {
        const r = await fetch(`${API}/catalog?pageSize=12&inStock=true&q=${encodeURIComponent(term)}`);
        const d = await r.json();
        setResults(d.products || []);
      } catch { setResults([]); }
      setSearching(false);
    }, 300);
    return () => clearTimeout(id);
  }, [q]);

  const setField = (k: keyof Fields, v: string) => setFields((all) => ({ ...all, [tpl]: { ...all[tpl], [k]: v } }));
  const setSlot = (i: number, slot: Slot) => setSlots((all) => { const arr = [...all[tpl]]; arr[i] = slot; return { ...all, [tpl]: arr }; });

  function pickTemplate(id: Tpl) {
    setTpl(id);
    setThemeKey(TEMPLATES.find((x) => x.id === id)!.theme);
    setActiveSlot(0);
  }

  function useProduct(p: ShopProduct) {
    setSlot(activeSlot, { product: p });
    // fill text fields from the product for single-product templates
    const nm = splitName(p);
    if (tpl === "new") setFields((all) => ({ ...all, new: { ...all.new, brand: nm.brand, title: nm.title.slice(0, 42), price: rub(p.priceRub), text: [p.categoryLabel, p.volume, "в наличии"].filter(Boolean).join(" · ") } }));
    if (tpl === "sale") setFields((all) => ({ ...all, sale: { ...all.sale, title: `${nm.brand} ${nm.title}`.trim().slice(0, 48), price: rub(p.priceRub) } }));
    if (tpl === "review") setFields((all) => ({ ...all, review: { ...all.review, price: rub(p.priceRub) } }));
    if (tpl === "brand" && nm.brand) setFields((all) => ({ ...all, brand: { ...all.brand, brand: nm.brand } }));
    if (def.slots > 1) setActiveSlot((i) => Math.min(def.slots - 1, i + 1));
  }

  function onUpload(file?: File) {
    if (!file) return;
    const r = new FileReader();
    r.onload = () => setSlot(activeSlot, { ...s[activeSlot], upload: String(r.result) });
    r.readAsDataURL(file);
  }

  async function exportPng() {
    if (!canvasRef.current) return;
    setBusy(true);
    try {
      await document.fonts.ready;
      const lib = await loadExporter();
      const node = canvasRef.current.firstElementChild as HTMLElement;
      const fontEmbedCSS = await embeddedFontCss();
      const url = await lib.toPng(node, { width: 1080, height: h, pixelRatio: 1, cacheBust: true, fontEmbedCSS, includeQueryParams: true }); // /img?u=… differ only by query
      const a = document.createElement("a");
      a.href = url;
      a.download = `magicvibes-${tpl}-${fmt.replace(":", "x")}-${Date.now()}.png`;
      a.click();
    } catch (e) {
      alert("Не удалось сохранить PNG: " + (e instanceof Error ? e.message : e));
    }
    setBusy(false);
  }

  const text = useMemo(() => caption(tpl, f, s), [tpl, f, s]);

  return (
    <div className="st-root">
      <aside className="st-panel">
        <div className="st-panel-h">
          <b>Студия постов</b><span>Telegram · Magic Vibes</span>
        </div>
        {modeSwitch}

        <div className="st-group">
          <div className="st-label">Шаблон</div>
          <div className="st-chips">
            {TEMPLATES.map((x) => <button key={x.id} className={tpl === x.id ? "on" : ""} onClick={() => pickTemplate(x.id)}>{x.label}</button>)}
          </div>
        </div>

        <div className="st-group">
          <div className="st-label">Формат</div>
          <div className="st-chips">
            {(Object.keys(FORMATS) as Fmt[]).map((k) => <button key={k} className={fmt === k ? "on" : ""} onClick={() => setFmt(k)}>{k === "1:1" ? "1:1 пост" : k === "4:5" ? "4:5 пост" : "9:16 сторис"}</button>)}
          </div>
        </div>

        <div className="st-group">
          <div className="st-label">Цвет</div>
          <div className="st-swatches">
            {(Object.keys(THEMES) as ThemeKey[]).map((k) => (
              <button key={k} title={THEMES[k].label} className={themeKey === k ? "on" : ""} style={{ background: THEMES[k].bg }} onClick={() => setThemeKey(k)} aria-label={THEMES[k].label} />
            ))}
          </div>
        </div>

        {def.slots > 0 && (
          <div className="st-group">
            <div className="st-label">Товар{def.slots > 1 ? "ы" : ""} — найдите в каталоге или загрузите фото</div>
            {def.slots > 1 && (
              <div className="st-chips">
                {Array.from({ length: def.slots }).map((_, i) => (
                  <button key={i} className={activeSlot === i ? "on" : ""} onClick={() => setActiveSlot(i)}>
                    {i + 1}. {s[i]?.product?.brand || (s[i]?.upload ? "фото" : "пусто")}
                  </button>
                ))}
              </div>
            )}
            <div className="st-search">
              <Search size={16} />
              <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Бренд или название, напр. Kilian" />
              {searching ? <Loader2 size={16} className="mv-spin" /> : q && <button onClick={() => setQ("")} aria-label="Очистить"><X size={15} /></button>}
            </div>
            {results.length > 0 && (
              <div className="st-results">
                {results.map((p) => (
                  <button key={p.id} onClick={() => useProduct(p)}>
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img src={productImg(p.images?.[0], 160)} alt="" />
                    <span><b>{p.brand}</b>{p.name}</span>
                    <em>{rub(p.priceRub)}</em>
                  </button>
                ))}
              </div>
            )}
            <button className="st-btn ghost" onClick={() => fileRef.current?.click()}><Upload size={15} /> Загрузить своё фото</button>
            <input ref={fileRef} type="file" accept="image/*" hidden onChange={(e) => onUpload(e.target.files?.[0])} />
          </div>
        )}

        <div className="st-group">
          <div className="st-label">Тексты</div>
          {def.fields.map((k) => (
            <label key={k} className="st-field">
              <span>{FIELD_LABELS[k]}</span>
              {k === "text" || k === "list"
                ? <textarea rows={k === "list" ? 6 : 3} value={f[k]} onChange={(e) => setField(k, e.target.value)} />
                : <input value={f[k]} onChange={(e) => setField(k, e.target.value)} />}
            </label>
          ))}
        </div>

        <div className="st-group">
          <div className="st-label">Текст поста</div>
          <textarea className="st-caption" readOnly value={text} rows={7} />
          <div className="st-actions">
            <button className="st-btn" onClick={exportPng} disabled={busy}>{busy ? <Loader2 size={16} className="mv-spin" /> : <Download size={16} />} Скачать PNG</button>
            <button className="st-btn ghost" onClick={async () => { await navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1500); }}>
              {copied ? <Check size={16} /> : <Copy size={16} />} {copied ? "Скопировано" : "Копировать текст"}
            </button>
          </div>
        </div>
      </aside>

      <main className="st-stage" ref={stageRef}>
        <div className="st-scaler" style={{ width: 1080 * scale, height: h * scale }}>
          <div ref={canvasRef} style={{ transform: `scale(${scale})`, transformOrigin: "0 0", width: 1080, height: h }}>
            <Render tpl={tpl} fmt={fmt} t={t} f={f} slots={s} />
          </div>
        </div>
        <div className="st-size">{1080}×{h}px · PNG</div>
      </main>
    </div>
  );
}
