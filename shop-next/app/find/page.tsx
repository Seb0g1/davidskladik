import type { Metadata } from "next";
import Link from "next/link";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import AiFinderClient from "./AiFinderClient";

export const metadata: Metadata = {
  title: `AI-подбор аромата | ${SITE_NAME}`,
  description: "Наш AI поможет подобрать идеальный парфюм именно для вас — по характеру, настроению, сезону и предпочтениям.",
  alternates: { canonical: "/find" },
  openGraph: { title: "AI-подбор аромата — Magic Vibes", url: `${SITE_URL}/find` },
};

export default function FindPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "AI-подбор", url: "/find" }]);
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <div style={{ maxWidth: 800, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>AI-подбор</span>
        </nav>
        <div style={{ textAlign: "center", marginBottom: 40 }}>
          <p className="eyebrow" style={{ marginBottom: 12 }}>✦ Искусственный интеллект</p>
          <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(32px,5vw,56px)", color: "#f5f4f0", margin: "0 0 16px" }}>Подбор аромата</h1>
          <p style={{ fontSize: 14, color: "rgba(245,244,240,0.5)", maxWidth: 500, margin: "0 auto", lineHeight: 1.7 }}>Расскажите о своих предпочтениях — AI Magic Vibes найдёт идеальный аромат</p>
        </div>
        <AiFinderClient />
      </div>
    </>
  );
}
