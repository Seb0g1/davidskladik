import Link from "next/link";
import { BookOpen, ChevronRight } from "lucide-react";
import type { Metadata } from "next";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";

export const metadata: Metadata = {
  title: `Как выбрать парфюм в подарок — гид | ${SITE_NAME}`,
  description: "Как выбрать парфюм в подарок: универсальные ароматы, подарки по бюджету от 3 000 до 10 000+ рублей.",
  alternates: { canonical: "/guide/gift" },
  openGraph: { title: `Парфюм в подарок — гид — ${SITE_NAME}`, url: `${SITE_URL}/guide/gift` },
};

const S = {
  bg:         "#0E0D0B",
  surface:    "#161512",
  border:     "rgba(255,252,245,0.07)",
  borderMd:   "rgba(255,252,245,0.13)",
  borderGold: "rgba(201,169,110,0.25)",
  text:       "#F4EFE6",
  muted:      "rgba(244,239,230,0.48)",
  subtle:     "rgba(244,239,230,0.22)",
  accent:     "#C9A96E",
  accent2:    "#D9BF8F",
};

const config = {
  headline: "Парфюм в подарок",
  intro: "Аромат — один из самых личных и запоминающихся подарков. Чтобы не ошибиться с выбором, мы составили гид по правилам выбора парфюма в подарок для мужчин и женщин в разных ценовых диапазонах.\n\nВажно: если вы не уверены в предпочтениях человека — выбирайте нейтральные, универсальные ароматы или дополните подарок подарочной картой.",
  catalogLink: "/catalog",
  catalogLabel: "Выбрать аромат в подарок",
  tips: [
    { title: "Универсальные ароматы для подарка", body: "Выбирайте ароматы с нотами ванили, розы, сандала или мускуса — они воспринимаются нейтрально большинством людей." },
    { title: "Подарок до 3 000 рублей", body: "В этом диапазоне — Montale, Zara Emotions, парфюм Antonio Banderas. Отличное соотношение цены и качества." },
    { title: "Подарок 3 000–10 000 рублей", body: "Territoire благородных брендов: Paco Rabanne, Armani, Versace. Оригинальная флакон-упаковка — уже часть подарка." },
    { title: "Подарок премиум 10 000+", body: "Creed, Parfums de Marly, Maison Margiela — ароматы, которые помнят. Роскошная презентация и нишевый характер." },
  ],
  relatedGuides: [
    { label: "Женские ароматы", to: "/guide/women" },
    { label: "Мужские ароматы", to: "/guide/men" },
    { label: "Ароматы для офиса", to: "/guide/office" },
  ],
};

export default function GiftGuidePage() {
  const paragraphs = config.intro.split("\n\n").filter(Boolean);
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: config.headline, url: "/guide/gift" }]);
  const articleLd = { "@context": "https://schema.org", "@type": "Article", headline: config.headline, description: "Как выбрать парфюм в подарок: универсальные ароматы, подарки по бюджету от 3 000 до 10 000+ рублей.", url: `${SITE_URL}/guide/gift`, publisher: { "@type": "Organization", name: SITE_NAME, logo: `${SITE_URL}/favicon.svg` } };
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(articleLd) }} />
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 900, margin: "0 auto", padding: "72px clamp(18px,4vw,56px) 56px", textAlign: "center" }}>
        <div style={{ display: "inline-flex", alignItems: "center", gap: 9, marginBottom: 24, padding: "6px 16px", borderRadius: 999, border: `1px solid ${S.borderMd}`, background: "rgba(201,169,110,0.06)" }}>
          <BookOpen size={12} style={{ color: S.accent }} />
          <span style={{ fontSize: 11, letterSpacing: "0.2em", textTransform: "uppercase", color: S.accent }}>Гид по ароматам</span>
        </div>
        <h1 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontSize: "clamp(32px, 6vw, 56px)", fontStyle: "italic", fontWeight: 600, color: S.text, margin: "0 0 20px", lineHeight: 1.15 }}>
          {config.headline}
        </h1>
        <div style={{ width: 52, height: 1, background: S.accent, margin: "0 auto 24px" }} />
        <p style={{ fontSize: "clamp(14px,2vw,16px)", color: S.muted, lineHeight: 1.85, maxWidth: 620, margin: "0 auto 36px" }}>{paragraphs[0]}</p>
        <Link href={config.catalogLink} style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "14px 32px", borderRadius: 3, background: S.accent, color: "#0E0D0B", textDecoration: "none", fontSize: 13, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase" }}>
          {config.catalogLabel} <ChevronRight size={14} />
        </Link>
      </div>

      {paragraphs.length > 1 && (
        <div style={{ maxWidth: 1060, margin: "0 auto", padding: "0 clamp(18px,4vw,56px) 64px" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))", gap: "clamp(20px,3vw,48px)" }}>
            {paragraphs.slice(1).map((p, i) => <p key={i} style={{ fontSize: 15, color: S.muted, lineHeight: 1.9, margin: 0 }}>{p}</p>)}
          </div>
        </div>
      )}

      <div style={{ maxWidth: 1060, margin: "0 auto", padding: "0 clamp(18px,4vw,56px) 72px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 32 }}>
          <div style={{ flex: 1, height: 1, background: S.border }} />
          <span style={{ fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: S.subtle, whiteSpace: "nowrap" }}>Что важно знать</span>
          <div style={{ flex: 1, height: 1, background: S.border }} />
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 16 }}>
          {config.tips.map((tip, i) => (
            <div key={i} style={{ background: S.surface, border: `1px solid ${S.border}`, borderLeft: `3px solid ${S.accent}`, borderRadius: "0 12px 12px 0", padding: "22px 22px 20px" }}>
              <h3 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontSize: 18, fontWeight: 600, fontStyle: "italic", color: S.accent2, margin: "0 0 10px", lineHeight: 1.3 }}>{tip.title}</h3>
              <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.8, margin: 0 }}>{tip.body}</p>
            </div>
          ))}
        </div>
      </div>

      <div style={{ maxWidth: 1060, margin: "0 auto", padding: "0 clamp(18px,4vw,56px) 72px" }}>
        <div style={{ background: S.surface, border: `1px solid ${S.borderGold}`, borderRadius: 16, padding: "40px clamp(24px,4vw,56px)", display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: 24 }}>
          <div>
            <p style={{ fontSize: 11, letterSpacing: "0.2em", textTransform: "uppercase", color: S.accent, margin: "0 0 8px" }}>Магазин Magic Vibes</p>
            <p style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontSize: "clamp(18px,3vw,24px)", fontStyle: "italic", fontWeight: 500, color: S.text, margin: 0, lineHeight: 1.3 }}>Весь каталог оригинальной парфюмерии</p>
          </div>
          <Link href={config.catalogLink} style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "13px 28px", borderRadius: 3, border: `1px solid ${S.accent}`, background: "transparent", color: S.accent, textDecoration: "none", fontSize: 12, fontWeight: 600, letterSpacing: "0.1em", textTransform: "uppercase", whiteSpace: "nowrap" }}>
            Смотреть ароматы <ChevronRight size={13} />
          </Link>
        </div>
      </div>

      <div style={{ maxWidth: 1060, margin: "0 auto", padding: "0 clamp(18px,4vw,56px) 96px" }}>
        <h2 style={{ fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: S.subtle, margin: "0 0 20px", fontWeight: 400 }}>Другие гиды</h2>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
          {config.relatedGuides.map(g => (
            <Link key={g.to} href={g.to} style={{ display: "inline-flex", alignItems: "center", gap: 7, padding: "10px 20px", borderRadius: 3, border: `1px solid ${S.borderMd}`, background: S.surface, color: S.muted, textDecoration: "none", fontSize: 13 }}>
              {g.label} <ChevronRight size={12} style={{ opacity: 0.5 }} />
            </Link>
          ))}
        </div>
      </div>
    </div>
    </>
  );
}
