"use client";
import { useState } from "react";
import Link from "next/link";
import { Search, ArrowRight, X } from "lucide-react";

const S = {
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent3: "#EDD9B0",
};

const AVATAR_COLORS: [string, string][] = [
  ["rgba(201,169,110,0.14)", "#EDD9B0"],
  ["rgba(201,169,110,0.10)", "#D9BF8F"],
  ["rgba(201,169,110,0.18)", "#EDD9B0"],
  ["rgba(201,169,110,0.12)", "#D9BF8F"],
  ["rgba(201,169,110,0.08)", "#C9A96E"],
];

function avatarColor(name: string): [string, string] {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return AVATAR_COLORS[h % AVATAR_COLORS.length];
}

function ruPlural(n: number, one: string, few: string, many: string) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return few;
  return many;
}

function BrandCard({ name, count }: { name: string; count: number }) {
  const [bg, color] = avatarColor(name);
  return (
    <Link href={`/catalog?brand=${encodeURIComponent(name)}`} style={{
      display: "flex", alignItems: "center", gap: 12, padding: "12px 14px",
      background: S.surface, borderRadius: 16, border: `1px solid ${S.border}`,
      textDecoration: "none", transition: "border-color 0.18s, background 0.18s",
    }}
      onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,169,110,0.25)"; el.style.background = S.surface2; }}
      onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = S.border; el.style.background = S.surface; }}
    >
      <div style={{ width: 36, height: 36, borderRadius: 12, flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "center", background: bg, color, fontSize: 15, fontWeight: 800 }}>
        {name[0]?.toUpperCase() ?? "?"}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: S.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{name}</div>
        <div style={{ fontSize: 11, color: S.muted, marginTop: 1 }}>{count} {ruPlural(count, "товар", "товара", "товаров")}</div>
      </div>
      <ArrowRight size={13} style={{ color: S.subtle, flexShrink: 0 }} />
    </Link>
  );
}

interface Props {
  brands: { name: string; count: number }[];
  total: number;
  totalItems: number;
}

export default function BrandsSearch({ brands, total, totalItems }: Props) {
  const [q, setQ] = useState("");

  const filtered = q.trim()
    ? brands.filter(b => b.name.toLowerCase().includes(q.toLowerCase()))
    : brands;

  const grouped: Record<string, { name: string; count: number }[]> = {};
  for (const b of filtered) {
    const key = b.name[0]?.toUpperCase() ?? "#";
    const letter = /[А-ЯЁA-Z]/.test(key) ? key : "#";
    (grouped[letter] ??= []).push(b);
  }

  const letters = Object.keys(grouped).sort((a, b) => {
    const isLatA = /[A-Z]/.test(a), isLatB = /[A-Z]/.test(b);
    const isRuA = /[А-ЯЁ]/.test(a), isRuB = /[А-ЯЁ]/.test(b);
    if (isLatA && isRuB) return -1;
    if (isRuA && isLatB) return 1;
    return a.localeCompare(b, "ru");
  });

  return (
    <>
      {/* Search */}
      <div style={{ background: S.surface, borderBottom: `1px solid ${S.border}` }}>
        <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(20px,3vw,40px) clamp(16px,4vw,32px)" }}>
          <p style={{ fontSize: 13, color: S.muted, marginBottom: 20 }}>
            {total} {ruPlural(total, "бренд", "бренда", "брендов")} · {totalItems.toLocaleString("ru-RU")} {ruPlural(totalItems, "товар", "товара", "товаров")}
          </p>
          <div style={{ position: "relative", maxWidth: 380 }}>
            <Search size={14} style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", color: S.subtle, pointerEvents: "none" }} />
            <input
              type="text"
              value={q}
              onChange={e => setQ(e.target.value)}
              placeholder="Найти бренд..."
              style={{
                width: "100%", paddingLeft: 40, paddingRight: q ? 40 : 16, paddingTop: 11, paddingBottom: 11,
                background: S.surface2, border: `1.5px solid ${S.border}`, borderRadius: 12,
                fontSize: 13, color: S.text, outline: "none", fontFamily: "inherit", boxSizing: "border-box",
              }}
              onFocus={e => (e.target.style.borderColor = "rgba(201,169,110,0.4)")}
              onBlur={e => (e.target.style.borderColor = S.border)}
            />
            {q && (
              <button onClick={() => setQ("")} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", cursor: "pointer", color: S.subtle, display: "flex", padding: 4 }}>
                <X size={14} />
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Letter anchors (only when not searching) */}
      {!q && letters.length > 0 && (
        <div style={{ maxWidth: 900, margin: "0 auto", padding: "20px clamp(16px,4vw,32px)", display: "flex", flexWrap: "wrap", gap: 6 }}>
          {letters.map(l => (
            <a key={l} href={`#letter-${l}`} style={{ padding: "4px 10px", fontSize: 12, color: "rgba(201,169,110,0.7)", textDecoration: "none", border: "1px solid rgba(201,169,110,0.2)", borderRadius: 4 }}>{l}</a>
          ))}
        </div>
      )}

      {/* Grid */}
      <div style={{ maxWidth: 900, margin: "0 auto", padding: "16px clamp(16px,4vw,32px) 80px" }}>
        {filtered.length === 0 ? (
          <div style={{ textAlign: "center", padding: "80px 20px", color: S.muted, fontSize: 14 }}>Бренды не найдены</div>
        ) : q ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 10 }}>
            {filtered.map(b => <BrandCard key={b.name} name={b.name} count={b.count} />)}
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>
            {letters.map(letter => (
              <section key={letter} id={`letter-${letter}`}>
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: S.accent3, textTransform: "uppercase", letterSpacing: "0.12em" }}>{letter}</span>
                  <div style={{ flex: 1, height: 1, background: S.border }} />
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 10 }}>
                  {grouped[letter].map(b => <BrandCard key={b.name} name={b.name} count={b.count} />)}
                </div>
              </section>
            ))}
          </div>
        )}
      </div>
    </>
  );
}
