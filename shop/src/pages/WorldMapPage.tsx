import { useState } from "react";
import { Link } from "react-router-dom";
import { X } from "lucide-react";

const S = {
  bg:         "#09090b",
  surface:    "#111113",
  border:     "rgba(255,255,255,0.07)",
  borderGold: "rgba(201,162,94,0.3)",
  text:       "#f2ede6",
  muted:      "rgba(242,237,230,0.5)",
  accent:     "#c9a25e",
};

// Equirectangular: lon → x%, lat → y%
function lonToX(lon: number) { return (lon + 180) / 360 * 100; }
function latToY(lat: number) { return (90 - lat) / 180 * 100; }

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
    name: "Великобритания", flag: "🇬🇧", lat: 52, lon: -1,
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
    name: "ОАЭ", flag: "🇦🇪", lat: 25, lon: 55,
    tagline: "Роскошь Востока",
    brands: [
      { name: "Amouage", note: "Interlude · Reflection · Gold" },
      { name: "Montale", note: "Intense Cafe · Dark Purple · Roses Musk" },
      { name: "Lattafa", note: "Raghba · Khamrah · Oud for Glory" },
    ],
  },
  {
    name: "США", flag: "🇺🇸", lat: 38, lon: -95,
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
      { name: "Issey Miyake",     note: "L'Eau d'Issey · A Scent · Nuit d'Issey" },
      { name: "Comme des Garçons", note: "CDG2 · Odeur 53 · Play" },
      { name: "Shiseido",         note: "Zen · Waso · Benefiance" },
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

// Graticule lines for the SVG (equirectangular, viewBox 0 0 1000 500)
const LAT_LINES = [-60, -30, 0, 30, 60];
const LON_LINES = [-120, -60, 0, 60, 120];

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
          position: "relative",
          width: "100%",
          paddingBottom: "50%",
          background: "#090d14",
          borderRadius: 8,
          border: "1px solid rgba(201,162,94,0.15)",
          overflow: "hidden",
        }}>
          {/* SVG world map (equirectangular, viewBox 0 0 1000 500) */}
          <svg
            viewBox="0 0 1000 500"
            style={{ position: "absolute", inset: 0, width: "100%", height: "100%", display: "block" }}
            aria-hidden
          >
            {/* Ocean background */}
            <rect width="1000" height="500" fill="#0a0e18" />

            {/* Graticule — latitude lines */}
            {LAT_LINES.map(lat => {
              const y = (90 - lat) / 180 * 500;
              const isEquator = lat === 0;
              return (
                <line
                  key={`lat${lat}`}
                  x1={0} y1={y} x2={1000} y2={y}
                  stroke="rgba(201,162,94,0.10)"
                  strokeWidth={isEquator ? 0.8 : 0.4}
                  strokeDasharray={isEquator ? "none" : "3 9"}
                />
              );
            })}

            {/* Graticule — longitude lines */}
            {LON_LINES.map(lon => {
              const x = (lon + 180) / 360 * 1000;
              return (
                <line
                  key={`lon${lon}`}
                  x1={x} y1={0} x2={x} y2={500}
                  stroke="rgba(201,162,94,0.07)"
                  strokeWidth={0.4}
                  strokeDasharray="3 9"
                />
              );
            })}

            {/* ── Continent paths ── */}
            <g fill="rgba(201,162,94,0.18)" stroke="rgba(201,162,94,0.38)" strokeWidth="0.6" strokeLinejoin="round">
              {/* North America */}
              <path d="M 33,50 L 111,83 L 139,97 L 153,117 L 164,144 L 175,161 L 181,172 L 189,186 L 236,200 L 264,222 L 281,225 L 289,225 L 300,217 L 317,217 L 328,222 L 328,181 L 278,161 L 292,144 L 306,128 L 319,125 L 328,119 L 317,83 L 300,56 L 278,50 L 233,39 L 194,28 L 42,28 L 28,50 Z" />
              {/* Greenland */}
              <path d="M 306,39 L 319,19 L 361,19 L 444,33 L 444,56 L 375,83 L 333,72 Z" />
              {/* South America */}
              <path d="M 278,225 L 319,217 L 361,236 L 394,264 L 400,278 L 394,300 L 381,314 L 367,328 L 353,342 L 342,356 L 319,369 L 306,389 L 311,406 L 328,406 L 333,389 L 328,361 L 306,333 L 289,292 L 278,256 Z" />
              {/* Europe mainland */}
              <path d="M 472,150 L 478,144 L 478,128 L 494,128 L 500,122 L 506,117 L 522,117 L 528,117 L 533,117 L 542,117 L 550,111 L 561,103 L 567,92 L 561,92 L 550,89 L 544,89 L 528,97 L 522,89 L 519,78 L 533,61 L 550,56 L 578,50 L 578,56 L 567,72 L 583,89 L 583,106 L 572,117 L 561,117 L 556,122 L 561,133 L 578,133 L 589,144 L 578,150 L 561,150 L 550,147 L 539,144 L 533,144 L 528,144 L 522,128 L 517,125 L 506,128 L 500,128 L 494,133 L 489,139 L 478,144 L 475,147 Z" />
              {/* UK */}
              <path d="M 483,89 L 494,83 L 500,83 L 506,89 L 506,106 L 500,111 L 486,111 L 483,106 Z" />
              {/* Ireland */}
              <path d="M 472,106 L 478,100 L 483,100 L 486,106 L 483,111 L 475,111 Z" />
              {/* Africa */}
              <path d="M 456,150 L 478,150 L 500,150 L 528,147 L 556,147 L 589,167 L 611,189 L 622,217 L 633,228 L 622,236 L 617,244 L 617,256 L 611,272 L 600,300 L 594,328 L 578,347 L 550,347 L 533,328 L 522,300 L 522,278 L 511,256 L 506,244 L 494,236 L 486,236 L 478,239 L 467,239 L 458,222 L 456,211 L 453,189 Z" />
              {/* Madagascar */}
              <path d="M 622,289 L 639,289 L 639,300 L 633,322 L 622,322 L 622,311 Z" />
              {/* Arabian Peninsula */}
              <path d="M 622,189 L 639,189 L 653,189 L 661,189 L 667,189 L 681,200 L 667,217 L 644,217 L 622,206 Z" />
              {/* India */}
              <path d="M 689,150 L 722,150 L 750,189 L 717,228 L 689,228 L 672,217 L 672,189 Z" />
              {/* Asia mainland */}
              <path d="M 572,133 L 600,133 L 617,144 L 644,144 L 661,144 L 672,144 L 689,167 L 700,189 L 700,228 L 722,228 L 750,189 L 778,217 L 789,244 L 806,256 L 819,244 L 828,228 L 839,217 L 850,200 L 839,183 L 839,161 L 839,144 L 850,133 L 861,144 L 867,156 L 889,150 L 903,133 L 911,117 L 889,97 L 861,117 L 833,97 L 806,83 L 778,69 L 750,56 L 722,47 L 694,50 L 672,61 L 667,83 L 653,97 L 639,111 L 625,119 L 606,119 L 583,125 L 578,111 L 561,97 L 561,117 L 572,117 Z" />
              {/* SE Asia / Indochina */}
              <path d="M 772,189 L 791,189 L 800,200 L 806,208 L 791,228 L 778,236 L 772,222 L 778,206 Z" />
              {/* Japan Honshu */}
              <path d="M 861,164 L 866,156 L 872,150 L 875,153 L 883,153 L 889,144 L 891,139 L 889,133 L 883,139 L 875,144 L 863,156 Z" />
              {/* Japan Hokkaido */}
              <path d="M 891,133 L 897,128 L 902,128 L 902,133 L 897,133 Z" />
              {/* Australia */}
              <path d="M 817,311 L 833,306 L 861,294 L 889,292 L 917,311 L 928,328 L 922,339 L 911,356 L 889,353 L 861,342 L 833,344 L 817,328 Z" />
            </g>
          </svg>

          {/* Country marker hotspots — positioned by lat/lon */}
          {COUNTRIES.map((c) => {
            const left = lonToX(c.lon);
            const top  = latToY(c.lat);
            const isActive = active?.name === c.name;
            return (
              <button
                key={c.name}
                onClick={() => setActive(isActive ? null : c)}
                style={{
                  position: "absolute",
                  left: `${left}%`,
                  top: `${top}%`,
                  transform: "translate(-50%, -50%)",
                  background: "none", border: "none", cursor: "pointer",
                  display: "flex", flexDirection: "column", alignItems: "center", gap: 3,
                  zIndex: 10,
                  padding: 0,
                }}
                title={c.name}
              >
                {/* Pulse ring */}
                <div style={{ position: "relative", width: 28, height: 28 }}>
                  <div style={{
                    position: "absolute", inset: 0,
                    borderRadius: "50%",
                    background: isActive ? "rgba(201,162,94,0.35)" : "rgba(201,162,94,0.12)",
                    border: `1.5px solid ${isActive ? "rgba(201,162,94,0.9)" : "rgba(201,162,94,0.5)"}`,
                    boxShadow: isActive
                      ? "0 0 16px rgba(201,162,94,0.55), 0 0 32px rgba(201,162,94,0.2)"
                      : "0 0 8px rgba(201,162,94,0.2)",
                    transition: "all 0.3s ease",
                    animation: isActive ? undefined : "hotspot-pulse 2.5s ease-in-out infinite",
                  }} />
                  <span style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>
                    {c.flag}
                  </span>
                </div>
                <span style={{
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.05em",
                  color: isActive ? S.accent : "rgba(201,162,94,0.75)",
                  textShadow: "0 1px 4px rgba(0,0,0,1), 0 0 8px rgba(0,0,0,0.9)",
                  whiteSpace: "nowrap",
                  transition: "color 0.2s",
                }}>
                  {c.name}
                </span>
              </button>
            );
          })}
        </div>

        {/* Animations */}
        <style>{`
          @keyframes hotspot-pulse {
            0%, 100% { box-shadow: 0 0 6px rgba(201,162,94,0.18); }
            50% { box-shadow: 0 0 18px rgba(201,162,94,0.5), 0 0 36px rgba(201,162,94,0.15); }
          }
          @keyframes slide-down {
            from { opacity: 0; transform: translateY(-10px); }
            to   { opacity: 1; transform: none; }
          }
        `}</style>

        {/* Instruction */}
        {!active && (
          <p style={{ textAlign: "center", marginTop: 16, fontSize: 12, color: "rgba(201,162,94,0.45)", letterSpacing: "0.06em" }}>
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
