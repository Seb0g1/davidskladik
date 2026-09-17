import type { Metadata } from "next";
import Link from "next/link";
import WorldMapClient from "./WorldMapClient";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";

export const metadata: Metadata = {
  title: `Ароматная карта мира | ${SITE_NAME}`,
  description: "Узнайте, откуда родом ваш аромат. Нажмите на страну и откройте парфюмерные традиции Франции, Италии, Аравии, Японии и других стран.",
  alternates: { canonical: "/world" },
  openGraph: { title: "Ароматная карта мира — Magic Vibes", url: `${SITE_URL}/world` },
};

const S = {
  bg:     "#09090b",
  text:   "#f2ede6",
  muted:  "rgba(242,237,230,0.5)",
  accent: "#c9a25e",
};

export default function WorldPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "Карта ароматов", url: "/world" }]);
  return (
    <>
    <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
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
        <p style={{ fontSize: "clamp(13px,1.5vw,15px)", color: S.muted, lineHeight: 1.75, maxWidth: "44ch", margin: "0 auto 48px" }}>
          Нажмите на страну, чтобы узнать, какие ароматы родились именно там — и какие из них можно найти в нашем каталоге.
        </p>
      </div>

      <WorldMapClient />

      {/* Footer CTA */}
      <div style={{ textAlign: "center", marginTop: 56, padding: "0 24px" }}>
        <p style={{ fontSize: 14, color: S.muted, marginBottom: 16 }}>Не знаете, с чего начать?</p>
        <div style={{ display: "flex", justifyContent: "center", gap: 12, flexWrap: "wrap" }}>
          <Link href="/find" style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "11px 24px", borderRadius: 4, background: "rgba(201,162,94,0.1)", border: "1px solid rgba(201,162,94,0.3)", color: S.accent, fontSize: 13, fontWeight: 600, textDecoration: "none" }}>
            ✦ AI-подбор аромата
          </Link>
          <Link href="/catalog" style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "11px 24px", borderRadius: 4, background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)", color: S.text, fontSize: 13, fontWeight: 600, textDecoration: "none" }}>
            Весь каталог
          </Link>
        </div>
      </div>
    </div>
    </>
  );
}
