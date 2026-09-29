"use client";
import { useRef, useState } from "react";
import Link from "next/link";
import { brandHref } from "@/lib/landings";

const BRANDS = ["Chanel","Dior","Tom Ford","Hermès","Byredo","Jo Malone","Creed","Guerlain","Givenchy","Prada","Valentino","Burberry","Versace","Montale","Kilian","YSL","Bvlgari","Lancôme","Amouage","Xerjoff","Maison Margiela","Acqua di Parma"];

const BRAND_NOTES: Record<string, string> = {
  "Chanel": "Chanel No. 5 — альдегиды, роза, жасмин",
  "Dior": "Sauvage — бергамот, амброксан",
  "Tom Ford": "Black Orchid — трюфель, чёрная орхидея",
  "Hermès": "Terre d'Hermès — грейпфрут, кедр, кремний",
  "Byredo": "Gypsy Water — сосна, ваниль, янтарь",
  "Jo Malone": "Wood Sage & Sea Salt — морская соль, шалфей",
  "Creed": "Aventus — ананас, берёза, мускус",
  "Guerlain": "Shalimar — ваниль, ирис, бергамот",
  "Givenchy": "L'Interdit — белые цветы, пачули",
  "Prada": "La Femme — иланг, ирис, ладан",
  "Valentino": "Valentina — белая трюфель, апельсин",
  "Burberry": "Her — ягоды, пион, амброксан",
  "Versace": "Eros — мята, зелёное яблоко, тонка",
  "Montale": "Black Aoud — уд, роза, пачули",
  "Kilian": "Angels' Share — коньяк, корица, миндаль",
  "YSL": "Black Opium — кофе, ваниль, белые цветы",
  "Bvlgari": "Man in Black — ром, ирис, гваяк",
  "Lancôme": "La Vie est Belle — ирис, пачули, ваниль",
  "Amouage": "Interlude — ладан, сосна, орхидея",
  "Xerjoff": "Naxos — лаванда, мёд, табак",
  "Maison Margiela": "Replica — в зависимости от аромата",
  "Acqua di Parma": "Colonia — цитрус, лаванда, сандал",
};

export default function BrandGallery() {
  const trackRef = useRef<HTMLDivElement>(null);
  const [hovered, setHovered] = useState<string | null>(null);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const dragging = useRef(false);
  const startX = useRef(0);
  const scrollLeft = useRef(0);

  function onMouseDown(e: React.MouseEvent) {
    if (!trackRef.current) return;
    dragging.current = true;
    startX.current = e.pageX - trackRef.current.offsetLeft;
    scrollLeft.current = trackRef.current.scrollLeft;
    trackRef.current.style.cursor = "grabbing";
  }
  function onMouseMove(e: React.MouseEvent) {
    if (!dragging.current || !trackRef.current) return;
    e.preventDefault();
    trackRef.current.scrollLeft = scrollLeft.current - (e.pageX - trackRef.current.offsetLeft - startX.current) * 1.4;
  }
  function onMouseUp() {
    dragging.current = false;
    if (trackRef.current) trackRef.current.style.cursor = "grab";
  }

  return (
    <div style={{ position: "relative" }}>
      <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to right, var(--paper), transparent)", zIndex: 10, pointerEvents: "none" }} />
      <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to left, var(--paper), transparent)", zIndex: 10, pointerEvents: "none" }} />
      <div
        ref={trackRef}
        onMouseDown={onMouseDown} onMouseMove={onMouseMove} onMouseUp={onMouseUp}
        onMouseLeave={() => { onMouseUp(); setHovered(null); }}
        style={{ display: "flex", gap: 6, overflowX: "auto", cursor: "grab", scrollbarWidth: "none", padding: "8px clamp(80px,8vw,120px)", userSelect: "none" }}
      >
        {BRANDS.map((brand) => (
          <Link
            key={brand}
            href={brandHref(brand)}
            draggable={false}
            onMouseEnter={e => { setHovered(brand); setPos({ x: e.clientX, y: e.clientY }); }}
            onMouseLeave={() => setHovered(null)}
            onMouseMove={e => setPos({ x: e.clientX, y: e.clientY })}
            style={{
              flexShrink: 0, padding: "12px 28px", border: "1px solid rgba(var(--ink-rgb),0.056)", borderRadius: 10,
              background: hovered === brand ? "rgba(var(--accent-rgb),0.08)" : "transparent",
              borderColor: hovered === brand ? "rgba(var(--accent-rgb),0.4)" : "rgba(var(--ink-rgb),0.056)",
              fontFamily: "var(--font-display)", fontSize: 18,
              color: hovered === brand ? "var(--accent2)" : "rgba(var(--ink-rgb),0.45)", whiteSpace: "nowrap", textDecoration: "none",
              transition: "background 0.3s, border-color 0.3s, color 0.3s",
            }}
          >
            {brand}
          </Link>
        ))}
      </div>
      {hovered && BRAND_NOTES[hovered] && (
        <div style={{ position: "fixed", left: pos.x + 14, top: pos.y - 48, pointerEvents: "none", zIndex: 1000, background: "var(--surface)", border: "1px solid rgba(var(--accent-rgb),0.3)", borderRadius: 14, padding: "10px 16px", maxWidth: 260, boxShadow: "0 8px 24px rgba(var(--ink-rgb),0.21)" }}>
          <p style={{ margin: "0 0 4px", fontFamily: "var(--font-display)", fontSize: 15, color: "var(--ink)" }}>{hovered}</p>
          <p style={{ margin: 0, fontSize: 11.5, color: "rgba(var(--ink-rgb),0.66)", lineHeight: 1.5 }}>{BRAND_NOTES[hovered]}</p>
        </div>
      )}
    </div>
  );
}
