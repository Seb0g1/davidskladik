import type { Metadata } from "next";
import Link from "next/link";
import { Sparkles } from "lucide-react";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import { Suspense } from "react";
import AiFinderClient from "./AiFinderClient";

export const metadata: Metadata = {
  title: `AI-подбор аромата`,
  description: "AI-подбор аромата: опишите желаемый парфюм своими словами — ноты, настроение, повод, бюджет или «похожий на…» — и получите подборку оригиналов из 22 000+ ароматов.",
  alternates: { canonical: "/find" },
  openGraph: { title: "AI-подбор аромата — Magic Vibes", url: `${SITE_URL}/find` },
};

const S = {
  bg:     "var(--paper)",
  text:   "var(--ink)",
  muted:  "rgba(var(--ink-rgb),0.57)",
  accent: "var(--accent)",
};

export default function FindPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "AI-подбор", url: "/find" }]);
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <div style={{ background: S.bg, minHeight: "100vh", paddingBottom: 64 }}>
        {/* Hero */}
        <div style={{ maxWidth: 780, margin: "0 auto", padding: "clamp(48px,8vw,96px) clamp(20px,5vw,40px) clamp(32px,5vw,56px)", textAlign: "center" }}>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "rgba(var(--accent-rgb),0.08)", border: "1px solid rgba(var(--accent-rgb),0.25)", borderRadius: 20, padding: "5px 14px", marginBottom: 24 }}>
            <Sparkles size={12} style={{ color: S.accent }} />
            <span style={{ fontSize: 11, letterSpacing: "0.15em", color: S.accent, textTransform: "uppercase" }}>AI-подбор</span>
          </div>
          <h1 style={{ fontFamily: "var(--font-display)", fontWeight: 300, fontSize: "clamp(40px,7vw,80px)", color: S.text, lineHeight: 0.95, marginBottom: 20 }}>
            Откройте свой<br />аромат
          </h1>
          <p style={{ fontSize: "clamp(13px,1.5vw,15px)", color: S.muted, lineHeight: 1.75, maxWidth: "40ch", margin: "0 auto 12px" }}>
            Опишите аромат своими словами — ноты, настроение, для кого и бюджет, или напишите «похожий на…». Подберём из тысяч ароматов с разобранными нотами и объясним, почему каждый подходит
          </p>
          <nav style={{ fontSize: 12, color: "rgba(var(--ink-rgb),0.45)", marginBottom: 0 }}>
            <Link href="/" style={{ color: "rgba(var(--ink-rgb),0.45)", textDecoration: "none" }}>Главная</Link>
            {" / "}<span style={{ color: "rgba(var(--ink-rgb),0.57)" }}>AI-подбор</span>
          </nav>
        </div>

        {/* Interactive finder */}
        <div style={{ maxWidth: 800, margin: "0 auto", padding: "0 clamp(20px,5vw,40px)" }}>
          <Suspense fallback={null}><AiFinderClient /></Suspense>
        </div>
      </div>
    </>
  );
}
