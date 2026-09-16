import Link from "next/link";
import { BookOpen, ChevronRight } from "lucide-react";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Лучшие женские ароматы — гид",
  description: "Гид по женской парфюмерии: как выбрать аромат, EDT vs EDP, сезонность и топ нот 2026.",
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
  headline: "Лучшие женские ароматы",
  intro: "Женский парфюм — это не просто запах, это характер. Цветочные, восточные, древесные и шипровые — каждая семья ароматов рассказывает свою историю. Мы собрали самые популярные и любимые женские ароматы по версии наших покупателей.\n\nВ подборку вошли как классика — Chanel, Dior, Lancôme — так и нишевые открытия: Byredo, Maison Margiela, Montale. Все ароматы оригинальные, с доставкой по всей России.",
  catalogLink: "/catalog/women",
  catalogLabel: "Смотреть женские ароматы",
  tips: [
    { title: "Как выбрать аромат для себя", body: "Начните с нот, которые вам нравятся — цветочные, фруктовые, пряные. Тестируйте на коже: аромат меняется в течение 20–30 минут." },
    { title: "EDT vs EDP: что выбрать", body: "EDP (Eau de Parfum) стойче и богаче — 6–8 часов. EDT (Eau de Toilette) легче и свежее — идеальна для тёплого сезона." },
    { title: "Сезонность ароматов", body: "Весна/лето — лёгкие цветочные и цитрусовые. Осень/зима — тёплые восточные и древесные с амброй и мускусом." },
    { title: "Топ нот женской парфюмерии 2026", body: "Ирис, жасмин, роза — вечная классика. Уд и пачули — тренд востока. Ваниль и карамель — сладкий уют осенних коллекций." },
  ],
  relatedGuides: [
    { label: "Мужские ароматы", to: "/guide/men" },
    { label: "Парфюм в подарок", to: "/guide/gift" },
    { label: "Ароматы для офиса", to: "/guide/office" },
  ],
};

export default function WomenGuidePage() {
  const paragraphs = config.intro.split("\n\n").filter(Boolean);
  return (
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
  );
}
