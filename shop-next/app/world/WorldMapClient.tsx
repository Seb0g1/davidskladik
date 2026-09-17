"use client";
import dynamic from "next/dynamic";

const WorldMap = dynamic(() => import("./WorldMap"), {
  ssr: false,
  loading: () => (
    <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 clamp(16px,4vw,32px)" }}>
      <div style={{ background: "#070b14", borderRadius: 8, border: "1px solid rgba(201,162,94,0.18)", height: 380, display: "flex", alignItems: "center", justifyContent: "center" }}>
        <span style={{ fontSize: 13, color: "rgba(201,162,94,0.4)", letterSpacing: "0.08em" }}>Загрузка карты…</span>
      </div>
    </div>
  ),
});

export default function WorldMapClient() {
  return <WorldMap />;
}
