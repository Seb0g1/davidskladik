import type { Metadata } from "next";
import Link from "next/link";
import { fetchBrands } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL } from "@/lib/seo";
import BrandsSearch, { type BrandRow } from "./BrandsSearch";
import { brandHref } from "@/lib/landings";

export const revalidate = 600;

export const metadata: Metadata = {
  title: `Все бренды парфюмерии`,
  description: "Полный список брендов оригинальной парфюмерии в Magic Vibes: Chanel, Dior, Tom Ford, Byredo, Creed, Montale, Kilian и сотни других. Оригинальные ароматы с доставкой по России.",
  alternates: { canonical: "/brands" },
  openGraph: { title: "Все бренды парфюмерии", url: `${SITE_URL}/brands` },
  keywords: "бренды парфюмерии, марки духов, Chanel парфюм, Dior ароматы, Tom Ford духи, нишевые бренды парфюмерии",
};

export default async function BrandsPage() {
  let brands: BrandRow[] = [];
  try { brands = await fetchBrands(); } catch {}
  // real brand names only: the brand field sometimes holds a shade code or a whole product title
  brands = brands.filter((b) => b.name && b.name.length <= 40 && /[a-zа-яё]{2}/i.test(b.name));

  const totalItems = brands.reduce((s, b) => s + b.count, 0);
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "Бренды", url: "/brands" }]);
  const itemListLd = brands.length > 0 ? {
    "@context": "https://schema.org",
    "@type": "ItemList",
    name: "Бренды парфюмерии в Magic Vibes",
    numberOfItems: brands.length,
    itemListElement: [...brands].sort((a, b) => b.count - a.count).slice(0, 100).map((b, i) => ({
      "@type": "ListItem", position: i + 1, name: b.name, url: `${SITE_URL}${brandHref(b.name)}`,
    })),
  } : null;

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      {itemListLd && <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(itemListLd) }} />}
      <div className="mv-land">
        <div className="mv-land-wrap">
          <nav className="mv-legal-crumbs" aria-label="Навигация">
            <span><Link href="/">Главная</Link> / </span><span>Бренды</span>
          </nav>
          <header className="mv-land-hero mv-brands-hero">
            <span className="mv-land-kicker">✦ Каталог</span>
            <h1 className="mv-land-title">Бренды</h1>
            <div className="mv-legal-script">от Chanel до нишевых домов</div>
            <div className="mv-land-stats">
              <span><b>{brands.length.toLocaleString("ru-RU")}</b> брендов</span>
              <span><b>{totalItems.toLocaleString("ru-RU")}</b> ароматов</span>
              <span><b>100%</b> оригинал</span>
            </div>
          </header>
          <BrandsSearch brands={brands} total={brands.length} totalItems={totalItems} />
        </div>
      </div>
    </>
  );
}
