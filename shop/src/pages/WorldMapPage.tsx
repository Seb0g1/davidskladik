import { useState } from "react";
import { Link } from "react-router-dom";
import { X } from "lucide-react";
import { ComposableMap, Geographies, Geography, Marker } from "react-simple-maps";

const S = {
  bg:         "#09090b",
  surface:    "#111113",
  border:     "rgba(255,255,255,0.07)",
  borderGold: "rgba(201,162,94,0.3)",
  text:       "#f2ede6",
  muted:      "rgba(242,237,230,0.5)",
  accent:     "#c9a25e",
};

// Natural Earth 110m GeoJSON via CDN (public domain, ~130KB)
const GEO_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

interface Country {
  name: string;
  flag: string;
  lat: number;
  lon: number;
  brands: { name: string; note: string }[];
  tagline: string;
}

const COUNTRIES: Country[] = [
  {
    name: "Франция", flag: "🇫🇷", lat: 48, lon: 2,
    tagline: "Родина haute parfumerie",
    brands: [
      { name: "Chanel",   note: "No. 5 · Coco Mademoiselle · Bleu" },
      { name: "Dior",     note: "Sauvage · Miss Dior · J'adore" },
      { name: "Hermès",   note: "Terre d'Hermès · Un Jardin" },
      { name: "Guerlain", note: "Mon Guerlain · Shalimar · Aqua Allegoria" },
      { name: "YSL",      note: "Black Opium · L'Homme · Libre" },
      { name: "Givenchy", note: "Gentleman · L'Interdit · Dahlia Divin" },
      { name: "Kilian",   note: "Angels' Share · Good Girl Gone Bad" },
    ],
  },
  {
    name: "Великобритания", flag: "🇬🇧", lat: 54, lon: -2,
    tagline: "Традиция и изысканность",
    brands: [
      { name: "Jo Malone",    note: "Wood Sage & Sea Salt · Peony & Blush Suede" },
      { name: "Creed",        note: "Aventus · Silver Mountain Water · Viking" },
      { name: "Burberry",     note: "Hero · Her · Mr. Burberry" },
      { name: "Penhaligon's", note: "Halfeti · Quercus · Endymion" },
    ],
  },
  {
    name: "Италия", flag: "🇮🇹", lat: 42, lon: 12,
    tagline: "Dolce vita в каждом флаконе",
    brands: [
      { name: "Valentino",      note: "Voce Viva · Born in Roma · Donna" },
      { name: "Bvlgari",        note: "Aqva · Omnia · Goldea" },
      { name: "Acqua di Parma", note: "Colonia · Peonia Nobile" },
      { name: "Versace",        note: "Eros · Crystal Noir · Dylan Blue" },
      { name: "Prada",          note: "L'Homme · Candy · Infusion d'Iris" },
      { name: "Armani",         note: "Sì · Acqua di Giò · Code" },
    ],
  },
  {
    name: "ОАЭ", flag: "🇦🇪", lat: 24, lon: 54,
    tagline: "Роскошь Востока",
    brands: [
      { name: "Amouage", note: "Interlude · Reflection · Gold" },
      { name: "Montale", note: "Intense Cafe · Dark Purple · Roses Musk" },
      { name: "Lattafa", note: "Raghba · Khamrah · Oud for Glory" },
    ],
  },
  {
    name: "США", flag: "🇺🇸", lat: 38, lon: -97,
    tagline: "Ароматный авангард",
    brands: [
      { name: "Tom Ford",        note: "Black Orchid · Tobacco Vanille · Oud Wood" },
      { name: "Maison Margiela", note: "Replica — Jazz Club · Flower Market" },
      { name: "Marc Jacobs",     note: "Daisy · Dot · Honey" },
      { name: "Calvin Klein",    note: "Eternity · CK One · Obsession" },
    ],
  },
  {
    name: "Германия", flag: "🇩🇪", lat: 51, lon: 10,
    tagline: "Точность и чистота",
    brands: [
      { name: "Hugo Boss",  note: "BOSS Bottled · Hugo Man · The Scent" },
      { name: "Jil Sander", note: "Sun · Simply · Pure" },
      { name: "4711",       note: "Original Eau de Cologne — с 1792 года" },
    ],
  },
  {
    name: "Швейцария", flag: "🇨🇭", lat: 47, lon: 8,
    tagline: "Нишевое совершенство",
    brands: [
      { name: "Byredo",   note: "Gypsy Water · Mojave Ghost · Bal d'Afrique" },
      { name: "Bvlgari",  note: "Швейцарский дизайн, итальянская роскошь" },
      { name: "Zimmerli", note: "Нишевые ароматы ручной работы" },
    ],
  },
  {
    name: "Япония", flag: "🇯🇵", lat: 36, lon: 138,
    tagline: "Минимализм и чистота",
    brands: [
      { name: "Issey Miyake",      note: "L'Eau d'Issey · A Scent · Nuit d'Issey" },
      { name: "Comme des Garçons", note: "CDG2 · Odeur 53 · Play" },
      { name: "Shiseido",          note: "Zen · Waso · Benefiance" },
    ],
  },
  {
    name: "Испания", flag: "🇪🇸", lat: 40, lon: -4,
    tagline: "Страсть и яркость",
    brands: [
      { name: "Zara",  note: "Woman · Femme · Find Me" },
      { name: "Loewe", note: "Solo · Aire · Agua de Loewe" },
    ],
  },
];

export default function WorldMapPage() {
  const [active, setActive] = useState<Country | null>(null);

  return (
    <div style={{ background: S.bg, minHeight: "100vh", paddingBottom: 64 }}>
      {/* Hero */}
      <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(48px,8vw,88px) clamp(20px,5vw,40px) 0", textAlign: "center" }}>
        <p style={{ fontSize: 10, letterSpacing: "0.28em", textTransform: "uppercase", color: S.accent, marginBottom: 16 }}>
          Ароматная карта мира
        </p>
        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic", fontWeight: 300,
          fontSize: "clamp(36px,6vw,72px)",
          color: S.text, lineHeight: 1.0, marginBottom: 18,
        }}>
          Откуда родом<br />ваш аромат?
        </h1>
        <p style={{ fontSize: "clamp(13px,1.5vw,15px)", color: S.muted, lineHeight: 1.75, maxWidth: "44ch", margin: "0 auto 40px" }}>
          Нажмите на страну, чтобы узнать, какие ароматы родились именно там — и какие из них можно найти в нашем каталоге.
        </p>
      </div>

      {/* Map */}
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 clamp(16px,4vw,32px)" }}>
        <div style={{
          background: "#070b14",
          borderRadius: 8,
          border: "1px solid rgba(201,162,94,0.18)",
          overflow: "hidden",
        }}>
          <ComposableMap
            projectionConfig={{ scale: 155, center: [10, 10] }}
            style={{ width: "100%", height: "auto", display: "block" }}
          >
            {/* Ocean background */}
            <rect width="800" height="400" fill="#070b14" />

            <Geographies geography={GEO_URL}>
              {({ geographies }) =>
                geographies.map((geo) => (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    style={{
                      default: {
                        fill: "rgba(201,162,94,0.10)",
                        stroke: "rgba(201,162,94,0.22)",
                        strokeWidth: 0.5,
                        outline: "none",
                      },
                      hover: {
                        fill: "rgba(201,162,94,0.26)",
                        stroke: "rgba(201,162,94,0.5)",
                        strokeWidth: 0.5,
                        outline: "none",
                      },
                      pressed: {
                        fill: "rgba(201,162,94,0.35)",
                        outline: "none",
                      },
                    }}
                  />
                ))
              }
            </Geographies>

            {/* Country markers */}
            {COUNTRIES.map((c) => {
              const isActive = active?.name === c.name;
              return (
                <Marker key={c.name} coordinates={[c.lon, c.lat]}>
                  <g
                    onClick={() => setActive(isActive ? null : c)}
                    style={{ cursor: "pointer" }}
                    transform="translate(-12, -12)"
                  >
                    {/* Pulse ring */}
                    {!isActive && (
                      <circle
                        cx={12} cy={12} r={14}
                        fill="rgba(201,162,94,0.06)"
                        stroke="rgba(201,162,94,0.3)"
                        strokeWidth={1}
                      >
                        <animate
                          attributeName="r"
                          values="10;16;10"
                          dur="2.5s"
                          repeatCount="indefinite"
                        />
                        <animate
                          attributeName="opacity"
                          values="0.4;0;0.4"
                          dur="2.5s"
                          repeatCount="indefinite"
                        />
                      </circle>
                    )}
                    {/* Marker circle */}
                    <circle
                      cx={12} cy={12} r={10}
                      fill={isActive ? "rgba(201,162,94,0.35)" : "rgba(17,17,19,0.85)"}
                      stroke={isActive ? "rgba(201,162,94,0.95)" : "rgba(201,162,94,0.6)"}
                      strokeWidth={isActive ? 2 : 1.5}
                      style={{
                        filter: isActive ? "drop-shadow(0 0 6px rgba(201,162,94,0.6))" : undefined,
                      }}
                    />
                    {/* Flag emoji */}
                    <text
                      x={12} y={16}
                      textAnchor="middle"
                      fontSize={12}
                      style={{ userSelect: "none", pointerEvents: "none" }}
                    >
                      {c.flag}
                    </text>
                    {/* Country label */}
                    <text
                      x={12} y={30}
                      textAnchor="middle"
                      fontSize={6}
                      fontWeight={700}
                      letterSpacing={0.5}
                      fill={isActive ? "#c9a25e" : "rgba(201,162,94,0.85)"}
                      style={{
                        userSelect: "none", pointerEvents: "none",
                        textShadow: "0 1px 4px rgba(0,0,0,1)",
                      }}
                    >
                      {c.name}
                    </text>
                  </g>
                </Marker>
              );
            })}
          </ComposableMap>
        </div>

        {/* Instruction */}
        {!active && (
          <p style={{ textAlign: "center", marginTop: 12, fontSize: 12, color: "rgba(201,162,94,0.45)", letterSpacing: "0.06em" }}>
            Нажмите на маркер страны
          </p>
        )}

        {/* Active country panel */}
        {active && (
          <div style={{
            marginTop: 20,
            background: "linear-gradient(135deg, rgba(20,16,8,0.85) 0%, rgba(14,13,11,0.75) 100%)",
            border: "1px solid rgba(201,162,94,0.25)",
            borderRadius: 8,
            padding: "clamp(20px,3vw,36px)",
            position: "relative",
            backdropFilter: "blur(8px)",
            animation: "slide-down 0.35s cubic-bezier(0.16,1,0.3,1)",
          }}>
            <style>{`
              @keyframes slide-down {
                from { opacity: 0; transform: translateY(-10px); }
                to   { opacity: 1; transform: none; }
              }
            `}</style>
            <button
              onClick={() => setActive(null)}
              style={{ position: "absolute", top: 16, right: 16, background: "rgba(255,255,255,0.05)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 6, cursor: "pointer", color: S.muted, padding: 6, display: "flex" }}
              aria-label="Закрыть"
            >
              <X size={14} />
            </button>

            <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
              <span style={{ fontSize: 28 }}>{active.flag}</span>
              <h2 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(24px,3vw,38px)", color: S.text, lineHeight: 1 }}>
                {active.name}
              </h2>
            </div>
            <p style={{ fontSize: 13, color: S.accent, letterSpacing: "0.06em", marginBottom: 24 }}>{active.tagline}</p>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 10 }}>
              {active.brands.map((b) => (
                <Link
                  key={b.name}
                  to={`/catalog?brand=${encodeURIComponent(b.name)}`}
                  style={{ textDecoration: "none" }}
                >
                  <div
                    style={{ padding: "14px 16px", background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 6, transition: "border-color 0.2s, background 0.2s", cursor: "pointer" }}
                    onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.35)"; (e.currentTarget as HTMLElement).style.background = "rgba(201,162,94,0.04)"; }}
                    onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.07)"; (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.03)"; }}
                  >
                    <div style={{ fontSize: 14, fontWeight: 600, color: S.text, marginBottom: 4 }}>{b.name}</div>
                    <div style={{ fontSize: 11, color: S.muted, lineHeight: 1.5 }}>{b.note}</div>
                    <div style={{ marginTop: 8, fontSize: 10, color: S.accent, letterSpacing: "0.08em" }}>Смотреть в каталоге →</div>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Footer CTA */}
      <div style={{ textAlign: "center", marginTop: 56, padding: "0 24px" }}>
        <p style={{ fontSize: 14, color: S.muted, marginBottom: 16 }}>Не знаете, с чего начать?</p>
        <div style={{ display: "flex", justifyContent: "center", gap: 12, flexWrap: "wrap" }}>
          <Link to="/find" style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "11px 24px", borderRadius: 4, background: "rgba(201,162,94,0.1)", border: "1px solid rgba(201,162,94,0.3)", color: S.accent, fontSize: 13, fontWeight: 600, textDecoration: "none", letterSpacing: "0.04em" }}>
            ✦ AI-подбор аромата
          </Link>
          <Link to="/catalog" style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "11px 24px", borderRadius: 4, background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)", color: S.text, fontSize: 13, fontWeight: 600, textDecoration: "none" }}>
            Весь каталог
          </Link>
        </div>
      </div>
    </div>
  );
}
