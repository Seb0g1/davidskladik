import type { Metadata } from "next";
import Link from "next/link";
import { fetchBrands } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import BrandsSearch from "./BrandsSearch";

export const revalidate = 3600;

export const metadata: Metadata = {
  title: `Все бренды парфюмерии | ${SITE_NAME}`,
  description: "Полный список брендов оригинальной парфюмерии в Magic Vibes: Chanel, Dior, Tom Ford, Byredo, Creed, Montale, Kilian и сотни других. Оригинальные ароматы с доставкой по России.",
  alternates: { canonical: "/brands" },
  openGraph: { title: "Все бренды парфюмерии", url: `${SITE_URL}/brands` },
  keywords: "бренды парфюмерии, марки духов, Chanel парфюм, Dior ароматы, Tom Ford духи, нишевые бренды парфюмерии",
};

const S = {
  bg:     "#0E0D0B",
  surface:"#161512",
  border: "rgba(255,252,245,0.07)",
  text:   "#F4EFE6",
  muted:  "rgba(244,239,230,0.48)",
};

export default async function BrandsPage() {
  let brands: { name: string; count: number }[] = [];
  try { brands = await fetchBrands(); } catch {}

  const total = brands.length;
  const totalItems = brands.reduce((s, b) => s + b.count, 0);

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Бренды", url: "/brands" },
  ]);

  const itemListLd = brands.length > 0 ? {
    "@context": "https://schema.org",
    "@type": "ItemList",
    name: "Бренды парфюмерии в Magic Vibes",
    numberOfItems: brands.length,
    itemListElement: brands.slice(0, 100).map((b, i) => ({
      "@type": "ListItem", position: i + 1, name: b.name,
      url: `${SITE_URL}/catalog?brand=${encodeURIComponent(b.name)}`,
    })),
  } : null;

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      {itemListLd && <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(itemListLd) }} />}

      <div style={{ background: S.bg, minHeight: "100vh" }}>
        {/* Header — server rendered for SEO */}
        <div style={{ background: S.surface, borderBottom: `1px solid ${S.border}` }}>
          <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px) 0" }}>
            <nav style={{ fontSize: 12, color: S.muted, marginBottom: 20 }}>
              <Link href="/" style={{ color: S.muted, textDecoration: "none" }}>Главная</Link>
              {" / "}<span style={{ color: S.text }}>Бренды</span>
            </nav>
            <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(28px,4vw,44px)", color: S.text, margin: "0 0 6px" }}>
              Бренды парфюмерии
            </h1>
          </div>
        </div>

        {/* Client search + grid */}
        <BrandsSearch brands={brands} total={total} totalItems={totalItems} />
      </div>
    </>
  );
}
