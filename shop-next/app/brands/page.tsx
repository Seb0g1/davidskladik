import type { Metadata } from "next";
import Link from "next/link";
import { fetchBrands } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

export const revalidate = 3600;

export const metadata: Metadata = {
  title: `Все бренды парфюмерии | ${SITE_NAME}`,
  description: "Полный список брендов оригинальной парфюмерии в Magic Vibes: Chanel, Dior, Tom Ford, Byredo, Creed, Montale, Kilian и сотни других.",
  alternates: { canonical: "/brands" },
  openGraph: { title: "Все бренды парфюмерии", url: `${SITE_URL}/brands` },
};

export default async function BrandsPage() {
  let brands: { name: string; count: number }[] = [];
  try { brands = await fetchBrands(); } catch {}

  const grouped: Record<string, { name: string; count: number }[]> = {};
  for (const b of brands) {
    const key = b.name[0]?.toUpperCase() ?? "#";
    const letter = /[А-ЯЁA-Z]/.test(key) ? key : "#";
    (grouped[letter] ??= []).push(b);
  }

  const sorted = Object.entries(grouped).sort(([a], [b]) => {
    const isRuA = /[А-ЯЁ]/.test(a), isRuB = /[А-ЯЁ]/.test(b);
    const isLatA = /[A-Z]/.test(a), isLatB = /[A-Z]/.test(b);
    if (isLatA && isRuB) return -1;
    if (isRuA && isLatB) return 1;
    return a.localeCompare(b, "ru");
  });

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

      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link>
          {" / "}<span style={{ color: "#f5f4f0" }}>Бренды</span>
        </nav>

        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(32px,5vw,56px)", color: "#f5f4f0", margin: "0 0 8px" }}>Все бренды</h1>
        <p style={{ fontSize: 13, color: "rgba(245,244,240,0.4)", marginBottom: 40 }}>{brands.length} брендов в каталоге</p>

        {/* Letter navigation */}
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 40, borderBottom: "1px solid rgba(255,255,255,0.07)", paddingBottom: 20 }}>
          {sorted.map(([letter]) => (
            <a key={letter} href={`#letter-${letter}`} style={{ padding: "4px 10px", fontSize: 13, color: "rgba(201,162,94,0.7)", textDecoration: "none", border: "1px solid rgba(201,162,94,0.2)", borderRadius: 4 }}>{letter}</a>
          ))}
        </div>

        {/* Brands by letter */}
        {sorted.map(([letter, list]) => (
          <div key={letter} id={`letter-${letter}`} style={{ marginBottom: 40 }}>
            <div style={{ fontSize: 11, letterSpacing: "0.24em", color: "rgba(201,162,94,0.6)", marginBottom: 16, fontWeight: 500 }}>{letter}</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(180px,1fr))", gap: 8 }}>
              {list.map(b => (
                <Link key={b.name} href={`/catalog?brand=${encodeURIComponent(b.name)}`} style={{ padding: "10px 14px", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 6, textDecoration: "none", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontSize: 13, color: "#f5f4f0" }}>{b.name}</span>
                  {b.count > 0 && <span style={{ fontSize: 10, color: "rgba(245,244,240,0.3)" }}>{b.count}</span>}
                </Link>
              ))}
            </div>
          </div>
        ))}
      </div>
    </>
  );
}
