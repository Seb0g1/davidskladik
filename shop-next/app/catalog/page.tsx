import type { Metadata } from "next";
import { fetchCatalog, fetchAutoCategories } from "@/lib/api";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import CatalogClient from "./CatalogClient";

interface Props {
  searchParams: Promise<{ brand?: string; category?: string; q?: string; page?: string; sort?: string; inStock?: string }>;
}

export const revalidate = 120;

const CAT_LABELS: Record<string, string> = {
  testers: "Тестеры и отливанты",
  parfum:  "Духи",
  edp:     "Парфюмерная вода",
  edt:     "Туалетная вода",
  edc:     "Одеколон",
  deo:     "Дезодоранты",
  home:    "Ароматы для дома",
  sets:    "Подарочные наборы",
  body:    "Уход за телом",
};

export async function generateMetadata({ searchParams }: Props): Promise<Metadata> {
  const sp = await searchParams;
  const brand    = sp.brand ?? "";
  const category = sp.category ?? "";
  const q        = sp.q ?? "";

  const catLabel = category ? (CAT_LABELS[category] ?? category) : "";

  const title = brand
    ? (catLabel ? `${brand} — ${catLabel} купить | ${SITE_NAME}` : `${brand} — парфюмерия купить | ${SITE_NAME}`)
    : q
    ? `${q.charAt(0).toUpperCase() + q.slice(1)} парфюмерия — каталог | ${SITE_NAME}`
    : catLabel
    ? `${catLabel} купить онлайн | ${SITE_NAME}`
    : `Каталог парфюмерии — купить духи | ${SITE_NAME}`;

  const description = brand
    ? `Купить парфюмерию ${brand} в Magic Vibes. Оригинальные ароматы${catLabel ? ` — ${catLabel.toLowerCase()}` : ""}. Быстрая доставка по России. Гарантия подлинности.`
    : `Каталог оригинальной парфюмерии в Magic Vibes${q ? ` — ${q}` : ""}${catLabel ? `, ${catLabel.toLowerCase()}` : ""}. Chanel, Dior, Tom Ford, Montale и тысячи других брендов. Доставка по России.`;

  const canonical = brand
    ? `/catalog?brand=${encodeURIComponent(brand)}`
    : category
    ? `/catalog?category=${encodeURIComponent(category)}`
    : "/catalog";

  const noindex = Boolean(q && !brand && !category);

  return {
    title,
    description,
    keywords: [brand, catLabel, q, "купить парфюм", "духи онлайн"].filter(Boolean).join(", "),
    alternates: { canonical },
    robots: noindex ? { index: false, follow: true } : undefined,
    openGraph: { title, description, url: `${SITE_URL}${canonical}` },
  };
}

const PAGE_SIZE = 24;

export default async function CatalogPage({ searchParams }: Props) {
  const sp = await searchParams;
  const brand    = sp.brand ?? "";
  const category = sp.category ?? "";
  const q        = sp.q ?? "";
  const page     = Number(sp.page ?? 1);
  const sort     = (sp.sort ?? "name") as "name" | "price_asc" | "price_desc";
  const inStock  = sp.inStock === "true";

  const showGrid = !!(brand || category || q || inStock || sort !== "name");

  const emptyResult = { products: [], total: 0, page: 1, pageSize: PAGE_SIZE, brands: [] as string[] };

  const [catalogRes, autoCatsRes] = await Promise.allSettled([
    showGrid
      ? fetchCatalog({ brand: brand || undefined, category: category || undefined, q: q || undefined, page, pageSize: PAGE_SIZE, sort, inStock: inStock || undefined })
      : Promise.resolve(emptyResult),
    fetchAutoCategories(),
  ]);

  const catalog       = catalogRes.status === "fulfilled" ? catalogRes.value : emptyResult;
  const autoCategories = autoCatsRes.status === "fulfilled" ? autoCatsRes.value : [];

  const crumbLabel = brand ? brand : category ? (CAT_LABELS[category] ?? category) : "Каталог";
  const crumbUrl   = brand ? `/catalog?brand=${encodeURIComponent(brand)}` : category ? `/catalog?category=${encodeURIComponent(category)}` : "/catalog";
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: crumbLabel, url: crumbUrl }]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <CatalogClient
        initialProducts={catalog.products}
        initialTotal={catalog.total}
        initialBrands={catalog.brands ?? []}
        autoCategories={autoCategories}
      />
    </>
  );
}
