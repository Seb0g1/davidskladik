"use client";
import { useState } from "react";
import Link from "next/link";
import { ComposableMap, Geographies, Geography, Marker } from "react-simple-maps";
import { X } from "lucide-react";

const GEO_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json";

const S = {
  bg:         "#09090b",
  surface:    "#111113",
  border:     "rgba(255,255,255,0.07)",
  text:       "#f2ede6",
  muted:      "rgba(242,237,230,0.5)",
  accent:     "#c9a25e",
};

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
      { name: "Byredo",  note: "Gypsy Water · Mojave Ghost · Bal d'Afrique" },
      { name: "Bvlgari", note: "Швейцарский дизайн, итальянская роскошь" },
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

export default function WorldMap() {
  const [active, setActive] = useState<Country | null>(null);
  const [hoveredGeo, setHoveredGeo] = useState<string | null>(null);

  return (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 clamp(16px,4vw,32px)" }}>
      <style>{`
        @keyframes rsm-pulse {
          0%   { r: 10; opacity: 0.4; }
          50%  { r: 16; opacity: 0; }
          100% { r: 10; opacity: 0.4; }
        }
        .rsm-pulse { animation: rsm-pulse 2.5s ease-in-out infinite; }
        @keyframes world-slide-down {
          from { opacity: 0; transform: translateY(-10px); }
          to   { opacity: 1; transform: none; }
        }
      `}</style>

      {/* Map */}
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
          <rect width="800" height="400" fill="#070b14" />

          <Geographies geography={GEO_URL}>
            {({ geographies }) =>
              geographies.map((geo) => (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={hoveredGeo === geo.rsmKey ? "rgba(201,162,94,0.26)" : "rgba(201,162,94,0.10)"}
                  stroke="rgba(201,162,94,0.22)"
                  strokeWidth={0.5}
                  style={{ outline: "none" }}
                  onMouseEnter={() => setHoveredGeo(geo.rsmKey)}
                  onMouseLeave={() => setHoveredGeo(null)}
                />
              ))
            }
          </Geographies>

          {COUNTRIES.map((c) => {
            const isActive = active?.name === c.name;
            return (
              <Marker key={c.name} coordinates={[c.lon, c.lat]}>
                <g
                  onClick={() => setActive(isActive ? null : c)}
                  style={{ cursor: "pointer" }}
                  transform="translate(-12, -12)"
                >
                  {!isActive && (
                    <circle
                      cx={12} cy={12} r={10}
                      className="rsm-pulse"
                      fill="rgba(201,162,94,0.06)"
                      stroke="rgba(201,162,94,0.3)"
                      strokeWidth={1}
                    />
                  )}
                  <circle
                    cx={12} cy={12} r={10}
                    fill={isActive ? "rgba(201,162,94,0.35)" : "rgba(17,17,19,0.85)"}
                    stroke={isActive ? "rgba(201,162,94,0.95)" : "rgba(201,162,94,0.6)"}
                    strokeWidth={isActive ? 2 : 1.5}
                    style={{ filter: isActive ? "drop-shadow(0 0 6px rgba(201,162,94,0.6))" : undefined }}
                  />
                  <text
                    x={12} y={16}
                    textAnchor="middle"
                    fontSize={12}
                    style={{ userSelect: "none", pointerEvents: "none" }}
                  >
                    {c.flag}
                  </text>
                  <text
                    x={12} y={30}
                    textAnchor="middle"
                    fontSize={6}
                    fontWeight={700}
                    letterSpacing={0.5}
                    fill={isActive ? "#c9a25e" : "rgba(201,162,94,0.85)"}
                    style={{ userSelect: "none", pointerEvents: "none" }}
                  >
                    {c.name}
                  </text>
                </g>
              </Marker>
            );
          })}
        </ComposableMap>
      </div>

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
          animation: "world-slide-down 0.35s cubic-bezier(0.16,1,0.3,1)",
        }}>
          <button
            onClick={() => setActive(null)}
            style={{ position: "absolute", top: 16, right: 16, background: "rgba(255,255,255,0.05)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 6, cursor: "pointer", color: S.muted, padding: 6, display: "flex" }}
            aria-label="Закрыть"
          >
            <X size={14} />
          </button>

          <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
            <span style={{ fontSize: 28 }}>{active.flag}</span>
            <h2 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(24px,3vw,38px)", color: S.text, lineHeight: 1, margin: 0 }}>
              {active.name}
            </h2>
          </div>
          <p style={{ fontSize: 13, color: S.accent, letterSpacing: "0.06em", marginBottom: 24 }}>{active.tagline}</p>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 10 }}>
            {active.brands.map((b) => (
              <Link
                key={b.name}
                href={`/catalog?brand=${encodeURIComponent(b.name)}`}
                style={{ textDecoration: "none" }}
              >
                <div
                  style={{ padding: "14px 16px", background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 6, cursor: "pointer", transition: "border-color 0.2s, background 0.2s" }}
                  onMouseEnter={e => { (e.currentTarget as HTMLDivElement).style.borderColor = "rgba(201,162,94,0.35)"; (e.currentTarget as HTMLDivElement).style.background = "rgba(201,162,94,0.04)"; }}
                  onMouseLeave={e => { (e.currentTarget as HTMLDivElement).style.borderColor = "rgba(255,255,255,0.07)"; (e.currentTarget as HTMLDivElement).style.background = "rgba(255,255,255,0.03)"; }}
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
  );
}
