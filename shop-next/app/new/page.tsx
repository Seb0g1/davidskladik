import type { Metadata } from "next";
import Link from "next/link";
import { Sparkles } from "lucide-react";
import { fetchNewProducts } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import ProductCard from "@/components/ProductCard";

export const revalidate = 300;

export const metadata: Metadata = {
  title: `Новинки парфюмерии | ${SITE_NAME}`,
  description: "Новые поступления оригинальной парфюмерии в Magic Vibes. Свежие ароматы с доставкой по всей России.",
  alternates: { canonical: "/new" },
  openGraph: { title: "Новинки — Magic Vibes", url: `${SITE_URL}/new` },
};

const C = {
  text:   "#f5f4f0",
  muted:  "rgba(245,244,240,0.45)",
  subtle: "rgba(245,244,240,0.15)",
  accent: "#C9A96E",
  border: "rgba(245,244,240,0.08)",
};

function ruPlural(n: number, one: string, few: string, many: string) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return one;
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return few;
  return many;
}

export default async function NewPage() {
  let products: Awaited<ReturnType<typeof fetchNewProducts>>["products"] = [];
  try { const d = await fetchNewProducts(30); products = d.products; } catch {}

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Новинки", url: "/new" },
  ]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(28px,4vw,48px) clamp(18px,4vw,56px) 80px" }}>
        <nav style={{ fontSize: 12, color: C.muted, marginBottom: 32 }}>
          <Link href="/" style={{ color: C.muted, textDecoration: "none" }}>Главная</Link>
          {" / "}<span style={{ color: C.text }}>Новинки</span>
        </nav>

        {/* Header */}
        <div style={{ marginBottom: 40 }}>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 7, marginBottom: 12, padding: "5px 14px", borderRadius: 20, background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.25)" }}>
            <Sparkles size={12} style={{ color: C.accent }} />
            <span style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase", color: C.accent }}>Свежие поступления</span>
          </div>
          <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: "clamp(36px,5vw,64px)", letterSpacing: "-0.02em", lineHeight: 1, color: C.text, margin: "0 0 10px" }}>
            Новинки
          </h1>
          {products.length > 0 && (
            <p style={{ fontSize: 13, color: C.muted }}>
              {products.length} {ruPlural(products.length, "товар", "товара", "товаров")} за последние 30 дней
            </p>
          )}
        </div>

        <div style={{ height: 1, background: C.border, marginBottom: 36 }} />

        {products.length === 0 ? (
          <p style={{ color: C.subtle, textAlign: "center", padding: 60 }}>Пока нет новинок</p>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 16 }}>
            {products.map(p => <ProductCard key={p.id} product={p} />)}
          </div>
        )}
      </div>
    </>
  );
}
