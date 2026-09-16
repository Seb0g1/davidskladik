import type { Metadata } from "next";
import Link from "next/link";
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

      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>Новинки</span>
        </nav>
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(32px,5vw,56px)", color: "#f5f4f0", margin: "0 0 8px" }}>Новинки</h1>
        <p style={{ fontSize: 13, color: "rgba(245,244,240,0.4)", marginBottom: 36 }}>Поступления за последние 30 дней</p>
        {products.length === 0 ? (
          <p style={{ color: "rgba(245,244,240,0.3)", textAlign: "center", padding: 60 }}>Пока нет новинок</p>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 16 }}>
            {products.map(p => <ProductCard key={p.id} product={p} />)}
          </div>
        )}
      </div>
    </>
  );
}
