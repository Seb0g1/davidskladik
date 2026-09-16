import type { Metadata } from "next";
import Link from "next/link";
import { fetchCatalog, fetchBrands, fetchCategories } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import ProductCard from "@/components/ProductCard";
import CatalogFilters from "./CatalogFilters";

interface Props {
  searchParams: Promise<{ brand?: string; category?: string; q?: string; page?: string; sort?: string; inStock?: string }>;
}

export const revalidate = 120;

export async function generateMetadata({ searchParams }: Props): Promise<Metadata> {
  const sp = await searchParams;
  const brand = sp.brand;
  const category = sp.category;
  const q = sp.q;

  let title: string;
  let description: string;
  let canonical: string;
  let noindex = false;

  if (brand) {
    title = `${brand} — купить парфюм ${brand} | ${SITE_NAME}`;
    description = `Оригинальная парфюмерия ${brand} в Magic Vibes. Широкий выбор ароматов ${brand} с доставкой по России. Гарантия подлинности.`;
    canonical = `/catalog?brand=${encodeURIComponent(brand)}`;
  } else if (category) {
    title = `${category} — парфюм | ${SITE_NAME}`;
    description = `Купить ${category.toLowerCase()} в Magic Vibes. Оригинальная парфюмерия с доставкой по всей России.`;
    canonical = `/catalog?category=${encodeURIComponent(category)}`;
  } else if (q) {
    title = `Поиск: ${q} | ${SITE_NAME}`;
    description = `Результаты поиска парфюмерии по запросу «${q}» в Magic Vibes.`;
    canonical = "/catalog";
    noindex = true;
  } else {
    title = `Каталог парфюмерии — купить духи с доставкой | ${SITE_NAME}`;
    description = "Каталог оригинальной парфюмерии: 22 000+ ароматов. Chanel, Dior, Tom Ford, Byredo, Creed, Montale и другие. Доставка по России.";
    canonical = "/catalog";
  }

  return {
    title,
    description,
    alternates: { canonical },
    robots: noindex ? { index: false, follow: true } : undefined,
    openGraph: { title, description, url: `${SITE_URL}${canonical}` },
  };
}

const PAGE_SIZE = 24;

export default async function CatalogPage({ searchParams }: Props) {
  const sp = await searchParams;
  const brand = sp.brand;
  const category = sp.category;
  const q = sp.q;
  const page = Number(sp.page ?? 1);
  const sort = (sp.sort as "price_asc" | "price_desc" | "name") || undefined;
  const inStock = sp.inStock === "true" ? true : undefined;

  const [catalogRes, brandsRes, catsRes] = await Promise.allSettled([
    fetchCatalog({ brand, category, q, page, pageSize: PAGE_SIZE, sort, inStock }),
    fetchBrands(),
    fetchCategories(),
  ]);

  const catalog = catalogRes.status === "fulfilled" ? catalogRes.value : { products: [], total: 0, page: 1, pageSize: PAGE_SIZE, brands: [] };
  const brands = brandsRes.status === "fulfilled" ? brandsRes.value : [];
  const categories = catsRes.status === "fulfilled" ? catsRes.value : [];
  const totalPages = Math.ceil(catalog.total / PAGE_SIZE);

  const heading = brand ? `Парфюм ${brand}` : category ?? "Каталог";

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: heading, url: brand ? `/catalog?brand=${encodeURIComponent(brand)}` : "/catalog" },
  ]);

  const SORT_OPTIONS = [
    { value: "", label: "По умолчанию" },
    { value: "price_asc", label: "Цена ↑" },
    { value: "price_desc", label: "Цена ↓" },
    { value: "name", label: "По названию" },
  ];

  function pageUrl(p: number) {
    const u = new URLSearchParams();
    if (brand) u.set("brand", brand);
    if (category) u.set("category", category);
    if (q) u.set("q", q);
    if (sort) u.set("sort", sort);
    if (inStock) u.set("inStock", "true");
    if (p > 1) u.set("page", String(p));
    const s = u.toString();
    return `/catalog${s ? "?" + s : ""}`;
  }

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(20px,3vw,40px) clamp(18px,4vw,56px)" }}>
        {/* Breadcrumb */}
        <nav aria-label="breadcrumb" style={{ display: "flex", gap: 6, fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24, flexWrap: "wrap" }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link>
          <span>/</span>
          <span style={{ color: "#f5f4f0" }}>{heading}</span>
        </nav>

        {/* Heading */}
        <div style={{ marginBottom: 28, display: "flex", alignItems: "baseline", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
          <div>
            <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,4vw,46px)", color: "#f5f4f0", margin: 0 }}>{heading}</h1>
            {catalog.total > 0 && <p style={{ fontSize: 12, color: "rgba(245,244,240,0.4)", marginTop: 6 }}>{catalog.total.toLocaleString("ru-RU")} товаров</p>}
          </div>
          {/* Sort */}
          <form method="GET" action="/catalog" style={{ display: "flex", gap: 8, alignItems: "center" }}>
            {brand && <input type="hidden" name="brand" value={brand} />}
            {category && <input type="hidden" name="category" value={category} />}
            {q && <input type="hidden" name="q" value={q} />}
            {inStock && <input type="hidden" name="inStock" value="true" />}
            <select name="sort" defaultValue={sort ?? ""} onChange={(e) => (e.currentTarget.form as HTMLFormElement).submit()} style={{ background: "#1D1C18", color: "#f5f4f0", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 6, padding: "6px 10px", fontSize: 12, cursor: "pointer" }}>
              {SORT_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </form>
        </div>

        {/* Layout: sidebar + grid */}
        <div style={{ display: "grid", gridTemplateColumns: "220px 1fr", gap: 32 }} className="catalog-layout">
          <style>{`@media(max-width:767px){.catalog-layout{grid-template-columns:1fr!important;}}`}</style>

          {/* Sidebar */}
          <aside>
            <CatalogFilters brands={brands} categories={categories} activeBrand={brand} activeCategory={category} activeInStock={inStock} activeQ={q} />
          </aside>

          {/* Main */}
          <div>
            {catalog.products.length === 0 ? (
              <div style={{ textAlign: "center", padding: "80px 20px", color: "rgba(245,244,240,0.35)" }}>
                <div style={{ fontSize: 48, marginBottom: 16 }}>✦</div>
                <p style={{ fontSize: 16 }}>Ничего не найдено</p>
                <Link href="/catalog" style={{ fontSize: 13, color: "rgba(201,162,94,0.7)", textDecoration: "none", display: "inline-block", marginTop: 12 }}>Показать все товары</Link>
              </div>
            ) : (
              <>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(180px,1fr))", gap: 16 }}>
                  {catalog.products.map(p => <ProductCard key={p.id} product={p} />)}
                </div>

                {/* Pagination */}
                {totalPages > 1 && (
                  <div style={{ display: "flex", justifyContent: "center", gap: 8, marginTop: 40, flexWrap: "wrap" }}>
                    {page > 1 && <Link href={pageUrl(page - 1)} style={{ padding: "8px 14px", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 6, color: "rgba(245,244,240,0.6)", textDecoration: "none", fontSize: 13 }}>← Назад</Link>}
                    {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
                      const p = Math.max(1, page - 3) + i;
                      if (p > totalPages) return null;
                      return (
                        <Link key={p} href={pageUrl(p)} style={{ padding: "8px 14px", border: `1px solid ${p === page ? "rgba(201,162,94,0.5)" : "rgba(255,255,255,0.1)"}`, borderRadius: 6, color: p === page ? "#C9A96E" : "rgba(245,244,240,0.6)", textDecoration: "none", fontSize: 13 }}>{p}</Link>
                      );
                    })}
                    {page < totalPages && <Link href={pageUrl(page + 1)} style={{ padding: "8px 14px", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 6, color: "rgba(245,244,240,0.6)", textDecoration: "none", fontSize: 13 }}>Вперёд →</Link>}
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </>
  );
}
