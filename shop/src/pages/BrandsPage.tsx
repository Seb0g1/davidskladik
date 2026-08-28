import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Search, ArrowRight, X } from "lucide-react";
import { api } from "../api";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent3: "#EDD9B0",
};

const AVATAR_COLORS: [string, string][] = [
  ["rgba(201,169,110,0.14)","#EDD9B0"],
  ["rgba(201,169,110,0.10)","#D9BF8F"],
  ["rgba(201,169,110,0.18)","#EDD9B0"],
  ["rgba(201,169,110,0.12)","#D9BF8F"],
  ["rgba(201,169,110,0.08)","#C9A96E"],
];
function avatarColor(name: string): [string, string] {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return AVATAR_COLORS[h % AVATAR_COLORS.length];
}

function BrandCard({ name, count }: { name: string; count: number }) {
  const [bg, color] = avatarColor(name);
  return (
    <Link
      to={`/catalog?brand=${encodeURIComponent(name)}`}
      style={{
        display: "flex", alignItems: "center", gap: 12, padding: "12px 14px",
        background: S.surface, borderRadius: 16, border: `1px solid ${S.border}`,
        textDecoration: "none", transition: "border-color 0.18s, background 0.18s, box-shadow 0.18s",
      }}
      onMouseEnter={e => {
        (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,169,110,0.25)";
        (e.currentTarget as HTMLElement).style.background = S.surface2;
        (e.currentTarget as HTMLElement).style.boxShadow = "0 4px 20px rgba(201,169,110,0.12)";
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLElement).style.borderColor = S.border;
        (e.currentTarget as HTMLElement).style.background = S.surface;
        (e.currentTarget as HTMLElement).style.boxShadow = "none";
      }}
    >
      <div style={{
        width: 36, height: 36, borderRadius: 12, flexShrink: 0,
        display: "flex", alignItems: "center", justifyContent: "center",
        background: bg, color,
        fontSize: 15, fontWeight: 800,
      }}>
        {name[0]?.toUpperCase() ?? "?"}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: S.text, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{name}</div>
        <div style={{ fontSize: 11, color: S.muted, marginTop: 1 }}>{count} товаров</div>
      </div>
      <ArrowRight size={13} style={{ color: S.subtle, flexShrink: 0, transition: "color 0.15s, transform 0.15s" }} />
    </Link>
  );
}

export default function BrandsPage() {
  const [q, setQ] = useState("");

  const { data: brands = [], isLoading } = useQuery({
    queryKey: ["shop-brands"],
    queryFn: () => api.brands(),
    staleTime: 5 * 60_000,
  });

  const filtered = q.trim()
    ? brands.filter((b) => b.name.toLowerCase().includes(q.toLowerCase()))
    : brands;

  const grouped: Record<string, typeof brands> = {};
  for (const b of filtered) {
    const letter = b.name[0]?.toUpperCase() ?? "#";
    if (!grouped[letter]) grouped[letter] = [];
    grouped[letter].push(b);
  }
  const letters = Object.keys(grouped).sort();

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      {/* Header */}
      <div style={{ borderBottom: `1px solid ${S.border}`, background: S.surface }}>
        <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
          <h1 style={{ fontSize: "clamp(24px,3.5vw,40px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em", marginBottom: 6 }}>
            Бренды
          </h1>
          <p style={{ fontSize: 13, color: S.muted, marginBottom: 24 }}>
            {brands.length} брендов · {brands.reduce((s, b) => s + b.count, 0).toLocaleString("ru-RU")} товаров
          </p>
          <div style={{ position: "relative", maxWidth: 380 }}>
            <Search size={14} style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", color: S.subtle, pointerEvents: "none" }} />
            <input
              type="text"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Найти бренд..."
              style={{
                width: "100%", paddingLeft: 40, paddingRight: q ? 40 : 16, paddingTop: 11, paddingBottom: 11,
                background: S.surface2, border: `1.5px solid ${S.border}`, borderRadius: 12, fontSize: 13,
                color: S.text, outline: "none", fontFamily: "inherit", boxSizing: "border-box",
                transition: "border-color 0.15s",
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

      {/* Content */}
      <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
        {isLoading ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 10 }}>
            {Array.from({ length: 24 }).map((_, i) => (
              <div key={i} className="skeleton" style={{ height: 62, borderRadius: 16 }} />
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div style={{ textAlign: "center", padding: "80px 20px", color: S.muted, fontSize: 14 }}>
            Бренды не найдены
          </div>
        ) : q ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 10 }}>
            {filtered.map((b) => <BrandCard key={b.name} name={b.name} count={b.count} />)}
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>
            {letters.map((letter) => (
              <section key={letter}>
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
                  <span style={{ fontSize: 12, fontWeight: 700, color: S.accent3, textTransform: "uppercase", letterSpacing: "0.12em" }}>{letter}</span>
                  <div style={{ flex: 1, height: 1, background: S.border }} />
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 10 }}>
                  {grouped[letter].map((b) => <BrandCard key={b.name} name={b.name} count={b.count} />)}
                </div>
              </section>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
