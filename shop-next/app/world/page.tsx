"use client";
import { useState } from "react";
import Link from "next/link";
import { X } from "lucide-react";
import type { Metadata } from "next";

const S = {
  bg:         "#09090b",
  surface:    "#111113",
  border:     "rgba(255,255,255,0.07)",
  borderGold: "rgba(201,162,94,0.3)",
  text:       "#f2ede6",
  muted:      "rgba(242,237,230,0.5)",
  accent:     "#c9a25e",
};

interface Country {
  name: string;
  flag: string;
  brands: { name: string; note: string }[];
  tagline: string;
}

const COUNTRIES: Country[] = [
  {
    name: "Франция", flag: "🇫🇷",
    tagline: "Родина haute parfumerie",
    brands: [
      { name: "Chanel",   note: "No. 5 · Coco Mademoiselle · Bleu" },
      { name: "Dior",     note: "Sauvage · Miss Dior · J'adore" },
      { name: "Hermès",   note: "Terre d'Hermès · Un Jardin" },
      { name: "Guerlain", note: "Mon Guerlain · Shalimar · Aqua Allegoria" },
      { name: "YSL",      note: "Black Opium · L'Homme · Libre" },
      { name: "Givenchy", note: "Gentleman · L'Interdit · Dahlia Divin" },
      { name: "Kilian",   note: "Angels' Share · Good Girl Gone Bad" },
    ],
  },
  {
    name: "Великобритания", flag: "🇬🇧",
    tagline: "Традиция и изысканность",
    brands: [
      { name: "Jo Malone",    note: "Wood Sage & Sea Salt · Peony & Blush Suede" },
      { name: "Creed",        note: "Aventus · Silver Mountain Water · Viking" },
      { name: "Burberry",     note: "Hero · Her · Mr. Burberry" },
      { name: "Penhaligon's", note: "Halfeti · Quercus · Endymion" },
    ],
  },
  {
    name: "Италия", flag: "🇮🇹",
    tagline: "Dolce vita в каждом флаконе",
    brands: [
      { name: "Valentino",      note: "Voce Viva · Born in Roma · Donna" },
      { name: "Bvlgari",        note: "Aqva · Omnia · Goldea" },
      { name: "Acqua di Parma", note: "Colonia · Peonia Nobile" },
      { name: "Versace",        note: "Eros · Crystal Noir · Dylan Blue" },
      { name: "Prada",          note: "L'Homme · Candy · Infusion d'Iris" },
      { name: "Armani",         note: "Sì · Acqua di Giò · Code" },
    ],
  },
  {
    name: "ОАЭ", flag: "🇦🇪",
    tagline: "Роскошь Востока",
    brands: [
      { name: "Amouage", note: "Interlude · Reflection · Gold" },
      { name: "Montale", note: "Intense Cafe · Dark Purple · Roses Musk" },
      { name: "Lattafa", note: "Raghba · Khamrah · Oud for Glory" },
    ],
  },
  {
    name: "США", flag: "🇺🇸",
    tagline: "Ароматный авангард",
    brands: [
      { name: "Tom Ford",        note: "Black Orchid · Tobacco Vanille · Oud Wood" },
      { name: "Maison Margiela", note: "Replica — Jazz Club · Flower Market" },
      { name: "Marc Jacobs",     note: "Daisy · Dot · Honey" },
      { name: "Calvin Klein",    note: "Eternity · CK One · Obsession" },
    ],
  },
  {
    name: "Германия", flag: "🇩🇪",
    tagline: "Точность и чистота",
    brands: [
      { name: "Hugo Boss",  note: "BOSS Bottled · Hugo Man · The Scent" },
      { name: "Jil Sander", note: "Sun · Simply · Pure" },
      { name: "4711",       note: "Original Eau de Cologne — с 1792 года" },
    ],
  },
  {
    name: "Швейцария", flag: "🇨🇭",
    tagline: "Нишевое совершенство",
    brands: [
      { name: "Byredo",  note: "Gypsy Water · Mojave Ghost · Bal d'Afrique" },
      { name: "Bvlgari", note: "Швейцарский дизайн, итальянская роскошь" },
    ],
  },
  {
    name: "Япония", flag: "🇯🇵",
    tagline: "Минимализм и чистота",
    brands: [
      { name: "Issey Miyake",      note: "L'Eau d'Issey · A Scent · Nuit d'Issey" },
      { name: "Comme des Garçons", note: "CDG2 · Odeur 53 · Play" },
      { name: "Shiseido",          note: "Zen · Waso · Benefiance" },
    ],
  },
  {
    name: "Испания", flag: "🇪🇸",
    tagline: "Страсть и яркость",
    brands: [
      { name: "Zara",  note: "Woman · Femme · Find Me" },
      { name: "Loewe", note: "Solo · Aire · Agua de Loewe" },
    ],
  },
];

export default function WorldMapPage() {
  const [active, setActive] = useState<Country | null>(null);

  return (
    <div style={{ background: S.bg, minHeight: "100vh", paddingBottom: 64 }}>
      <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(48px,8vw,88px) clamp(20px,5vw,40px) 0", textAlign: "center" }}>
        <p style={{ fontSize: 10, letterSpacing: "0.28em", textTransform: "uppercase", color: S.accent, marginBottom: 16 }}>
          Ароматная карта мира
        </p>
        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300,
          fontSize: "clamp(36px,6vw,72px)", color: S.text, lineHeight: 1.0, marginBottom: 18,
        }}>
          Откуда родом<br />ваш аромат?
        </h1>
        <p style={{ fontSize: "clamp(13px,1.5vw,15px)", color: S.muted, lineHeight: 1.75, maxWidth: "44ch", margin: "0 auto 48px" }}>
          Нажмите на страну, чтобы узнать, какие ароматы родились именно там — и какие из них можно найти в нашем каталоге.
        </p>
      </div>

      {/* Country grid */}
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 clamp(16px,4vw,32px)" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 12 }}>
          {COUNTRIES.map((c) => {
            const isActive = active?.name === c.name;
            return (
              <button
                key={c.name}
                onClick={() => setActive(isActive ? null : c)}
                style={{
                  background: isActive ? "rgba(201,162,94,0.12)" : S.surface,
                  border: `1px solid ${isActive ? "rgba(201,162,94,0.5)" : S.border}`,
                  borderRadius: 8, padding: "18px 16px", cursor: "pointer",
                  display: "flex", flexDirection: "column", alignItems: "center", gap: 8,
                  transition: "border-color 0.2s, background 0.2s", textAlign: "center",
                  color: S.text,
                }}
              >
                <span style={{ fontSize: 32 }}>{c.flag}</span>
                <span style={{ fontSize: 13, fontWeight: 600, color: isActive ? S.accent : S.text }}>{c.name}</span>
                <span style={{ fontSize: 11, color: S.muted, lineHeight: 1.4 }}>{c.tagline}</span>
              </button>
            );
          })}
        </div>

        {/* Active country panel */}
        {active && (
          <div style={{
            marginTop: 24,
            background: "linear-gradient(135deg, rgba(20,16,8,0.9) 0%, rgba(14,13,11,0.8) 100%)",
            border: "1px solid rgba(201,162,94,0.25)", borderRadius: 12,
            padding: "clamp(20px,3vw,36px)", position: "relative",
          }}>
            <button
              onClick={() => setActive(null)}
              style={{ position: "absolute", top: 16, right: 16, background: "rgba(255,255,255,0.05)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 6, cursor: "pointer", color: S.muted, padding: 6, display: "flex" }}
              aria-label="Закрыть"
            >
              <X size={14} />
            </button>
            <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
              <span style={{ fontSize: 28 }}>{active.flag}</span>
              <h2 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(24px,3vw,38px)", color: S.text, lineHeight: 1, margin: 0 }}>
                {active.name}
              </h2>
            </div>
            <p style={{ fontSize: 13, color: S.accent, letterSpacing: "0.06em", marginBottom: 24 }}>{active.tagline}</p>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 10 }}>
              {active.brands.map((b) => (
                <Link key={b.name} href={`/catalog?brand=${encodeURIComponent(b.name)}`} style={{ textDecoration: "none" }}>
                  <div style={{ padding: "14px 16px", background: "rgba(255,255,255,0.03)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 6, cursor: "pointer" }}>
                    <div style={{ fontSize: 14, fontWeight: 600, color: S.text, marginBottom: 4 }}>{b.name}</div>
                    <div style={{ fontSize: 11, color: S.muted, lineHeight: 1.5 }}>{b.note}</div>
                    <div style={{ marginTop: 8, fontSize: 10, color: S.accent, letterSpacing: "0.08em" }}>Смотреть в каталоге →</div>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        )}
      </div>

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
  );
}
