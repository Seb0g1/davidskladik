import { ImageResponse } from "next/og";

export const runtime = "edge";
export const alt = "Magic Vibes — Оригинальный парфюм";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function OGImage() {
  return new ImageResponse(
    (
      <div
        style={{
          background: "#09090b",
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          fontFamily: "Georgia, serif",
          position: "relative",
        }}
      >
        {/* Corner decorations */}
        <div style={{ position: "absolute", top: 48, left: 64, width: 32, height: 32, borderTop: "1px solid rgba(201,162,94,0.5)", borderLeft: "1px solid rgba(201,162,94,0.5)", display: "flex" }} />
        <div style={{ position: "absolute", top: 48, right: 64, width: 32, height: 32, borderTop: "1px solid rgba(201,162,94,0.5)", borderRight: "1px solid rgba(201,162,94,0.5)", display: "flex" }} />
        <div style={{ position: "absolute", bottom: 48, left: 64, width: 32, height: 32, borderBottom: "1px solid rgba(201,162,94,0.5)", borderLeft: "1px solid rgba(201,162,94,0.5)", display: "flex" }} />
        <div style={{ position: "absolute", bottom: 48, right: 64, width: 32, height: 32, borderBottom: "1px solid rgba(201,162,94,0.5)", borderRight: "1px solid rgba(201,162,94,0.5)", display: "flex" }} />

        {/* Eyebrow */}
        <p style={{ fontSize: 14, letterSpacing: "0.32em", textTransform: "uppercase", color: "rgba(201,162,94,0.7)", margin: "0 0 28px", fontFamily: "Georgia, serif" }}>
          ОРИГИНАЛЬНАЯ ПАРФЮМЕРИЯ
        </p>

        {/* Brand name */}
        <p style={{ fontSize: 96, fontStyle: "italic", fontWeight: 300, color: "#f2ede6", margin: 0, lineHeight: 1, fontFamily: "Georgia, serif" }}>
          Magic Vibes
        </p>

        {/* Divider */}
        <div style={{ width: 120, height: 1, background: "rgba(201,162,94,0.5)", margin: "32px 0" }} />

        {/* Tagline */}
        <p style={{ fontSize: 22, color: "rgba(242,237,230,0.55)", margin: 0, letterSpacing: "0.04em", fontFamily: "Georgia, serif" }}>
          22 000+ ароматов · Гарантия оригинала · Доставка по России
        </p>
      </div>
    ),
    { width: 1200, height: 630 },
  );
}
