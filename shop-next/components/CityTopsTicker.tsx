"use client";

const CITY_FALLBACK = [
  { city: "Москве",           brand: "Tom Ford",  name: "Tobacco Vanille" },
  { city: "Санкт-Петербурге", brand: "Chanel",    name: "Chance Eau Tendre" },
  { city: "Краснодаре",       brand: "Dior",      name: "Sauvage" },
  { city: "Екатеринбурге",    brand: "Jo Malone", name: "Peony & Blush Suede" },
  { city: "Казани",           brand: "Byredo",    name: "Gypsy Water" },
  { city: "Новосибирске",     brand: "YSL",       name: "Black Opium" },
];

interface Entry { city: string; brand: string; name: string; offerId?: string }

export default function CityTopsTicker({ entries }: { entries?: Entry[] }) {
  const items = (entries && entries.length >= 4) ? entries : CITY_FALLBACK;
  const doubled = [...items, ...items];

  return (
    <div style={{ overflow: "hidden", borderTop: "1px solid rgba(255,255,255,0.05)", borderBottom: "1px solid rgba(255,255,255,0.05)", padding: "12px 0", background: "rgba(201,162,94,0.02)" }}>
      <style>{`
        @keyframes ticker-scroll { from { transform: translateX(0); } to { transform: translateX(-50%); } }
        .city-ticker-track { display: flex; animation: ticker-scroll 32s linear infinite; width: max-content; }
        .city-ticker-track:hover { animation-play-state: paused; }
      `}</style>
      <div className="city-ticker-track">
        {doubled.map((e, i) => (
          <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "0 32px", flexShrink: 0, fontSize: 12, color: "rgba(242,237,230,0.55)", letterSpacing: "0.04em", whiteSpace: "nowrap" }}>
            <span style={{ color: "rgba(201,162,94,0.7)", fontSize: 10, fontWeight: 700, letterSpacing: "0.16em", textTransform: "uppercase" }}>в {e.city}</span>
            <span style={{ color: "rgba(255,255,255,0.12)" }}>·</span>
            {e.brand && <span style={{ color: "rgba(201,162,94,0.55)" }}>{e.brand}</span>}
            <span>{e.name}</span>
            <span style={{ color: "rgba(255,255,255,0.08)", margin: "0 8px" }}>✦</span>
          </span>
        ))}
      </div>
    </div>
  );
}
