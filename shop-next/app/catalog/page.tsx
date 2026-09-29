import type { Metadata } from "next";
import { fetchCatalog, fetchAutoCategories } from "@/lib/api";
import { SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import CatalogClient from "./CatalogClient";
import { brandHref, collectionForCategory, collectionForQuery, collectionHref } from "@/lib/landings";

interface Props {
  searchParams: Promise<{ brand?: string; category?: string; q?: string; page?: string; sort?: string; inStock?: string; view?: string; gender?: string; line?: string; group?: string; priceMin?: string; priceMax?: string; volume?: string }>;
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

// Landing collections (backend SHOP_QUERY_COLLECTIONS): indexable pages with their own titles.
const COLLECTIONS: Record<string, { title: string; description: string }> = {
  "женская":    { title: "Женская парфюмерия — купить оригинальные духи", description: "Женские духи, парфюмерная и туалетная вода: Chanel, Dior, Lancôme, Kilian, Byredo и сотни брендов. Только оригиналы, доставка по России 1–5 дней." },
  "мужская":    { title: "Мужская парфюмерия — купить оригинальные духи", description: "Мужские ароматы: Dior Sauvage, Tom Ford, Creed, Armani и нишевые бренды. Оригинальная парфюмерия с доставкой по России через Ozon." },
  "унисекс":    { title: "Парфюмерия унисекс — ароматы для него и для неё", description: "Унисекс-ароматы от нишевых и люксовых домов: Montale, Byredo, Le Labo, Kilian. Оригиналы с гарантией, доставка 1–5 дней." },
  "нишевая":    { title: "Нишевая парфюмерия — купить оригинал", description: "Нишевые ароматы: Amouage, Creed, Initio, Xerjoff, Byredo, Parfums de Marly, Nishane. Редкие ноты, только оригиналы, доставка по России." },
  "элитная":    { title: "Элитная парфюмерия — купить оригинальные духи люкс", description: "Элитные духи Chanel, Dior, Tom Ford, Guerlain, YSL, Hermès, Creed и Kilian. Оригинальная люксовая парфюмерия с доставкой по России." },
  "арабская":   { title: "Арабская парфюмерия — купить оригинальные духи", description: "Арабские ароматы Lattafa, Rasasi, Ajmal, Armaf, Afnan, Al Haramain: уд, амбра, шафран и пряности. Оригиналы, доставка 1–5 дней." },
  "миниатюр":   { title: "Миниатюры духов — купить мини-парфюм", description: "Миниатюры и travel-версии популярных ароматов: попробуйте парфюм перед покупкой большого флакона. Оригиналы, доставка по России." },
  "цветочный":  { title: "Цветочные ароматы — купить духи с нотами розы и жасмина", description: "Цветочная парфюмерия: роза, пион, жасмин, тубероза, ирис. Женские и унисекс ароматы мировых брендов, только оригиналы." },
  "древесный":  { title: "Древесные ароматы — купить духи с нотами сандала и кедра", description: "Древесная парфюмерия: сандал, кедр, ветивер, уд, пачули. Тёплые ароматы для мужчин и женщин, оригиналы с доставкой." },
  "цитрусовый": { title: "Цитрусовые ароматы — купить свежие духи", description: "Цитрусовая парфюмерия: бергамот, лимон, мандарин, грейпфрут, нероли. Лёгкие ароматы на лето и каждый день." },
  "мускусный":  { title: "Мускусные ароматы — купить духи с мускусом", description: "Мускусная парфюмерия: чистые, кожаные и пудровые ароматы с мускусом. Оригиналы мировых брендов с доставкой по России." },
  "восточный":  { title: "Восточные ароматы — купить духи с амброй и удом", description: "Восточная парфюмерия: амбра, уд, ваниль, ладан, пряности. Шлейфовые ароматы на вечер и холодный сезон." },
  "свежий":     { title: "Свежие ароматы — купить морские и зелёные духи", description: "Свежая парфюмерия: морские, акватические, зелёные и спортивные ароматы для офиса, лета и каждого дня." },
  "фужерный":   { title: "Фужерные ароматы — купить духи с лавандой", description: "Фужерная парфюмерия: лаванда, герань, мох, бобы тонка. Классика мужской парфюмерии и современные прочтения." },
  "шипровый":   { title: "Шипровые ароматы — купить духи с дубовым мхом", description: "Шипровая парфюмерия: бергамот, пачули, дубовый мох, лабданум. Элегантные ароматы с характером." },
  "сладкий":    { title: "Сладкие ароматы — купить гурманские духи", description: "Гурманская парфюмерия: ваниль, карамель, шоколад, пралине, кофе. Тёплые сладкие ароматы мировых брендов." },
};

export async function generateMetadata({ searchParams }: Props): Promise<Metadata> {
  const sp = await searchParams;
  const brand    = sp.brand ?? "";
  const category = sp.category ?? "";
  const q        = sp.q ?? "";

  const catLabel = category ? (CAT_LABELS[category] ?? category) : "";
  const page = Math.max(1, Number(sp.page ?? 1) || 1);
  const coll = !brand && !category ? COLLECTIONS[q.trim().toLowerCase()] : undefined;
  const pageSuffix = page > 1 ? ` — страница ${page}` : "";

  const title = brand
    ? (catLabel ? `${brand} — ${catLabel} купить` : `${brand} — парфюмерия купить`)
    : q
    ? `${q.charAt(0).toUpperCase() + q.slice(1)} парфюмерия — каталог`
    : catLabel
    ? `${catLabel} купить онлайн`
    : `Каталог парфюмерии — купить духи`;

  const description = brand
    ? `Купить парфюмерию ${brand} в Magic Vibes. Оригинальные ароматы${catLabel ? ` — ${catLabel.toLowerCase()}` : ""}. Быстрая доставка по России. Гарантия подлинности.`
    : `Каталог оригинальной парфюмерии в Magic Vibes${q ? ` — ${q}` : ""}${catLabel ? `, ${catLabel.toLowerCase()}` : ""}. Chanel, Dior, Tom Ford, Montale и тысячи других брендов. Доставка по России.`;

  // plain brand / collection / category listings are duplicated by the SEO landings — canonical goes there
  const landingColl = !brand && !category && q ? collectionForQuery(q) : !brand && !q && category ? collectionForCategory(category) : undefined;
  const landing = brand && !category && !q ? brandHref(brand) : landingColl ? collectionHref(landingColl) : "";
  const base = landing
    ? landing
    : brand
    ? `/catalog?brand=${encodeURIComponent(brand)}`
    : category
    ? `/catalog?category=${encodeURIComponent(category)}`
    : coll
    ? `/catalog?q=${encodeURIComponent(q.trim().toLowerCase())}`
    : "/catalog";
  // each page of a listing is its own canonical (page 2+ are not duplicates of page 1)
  const canonical = page > 1 ? `${base}${base.includes("?") ? "&" : "?"}page=${page}` : base;

  // free-text searches stay out of the index; landing collections are indexable
  const filtered = ["gender", "line", "group", "priceMin", "priceMax", "volume", "inStock", "sort"].some((k) => (sp as Record<string, string | undefined>)[k]);
  const noindex = Boolean(q && !brand && !category && !coll) || filtered;
  const finalTitle = (coll ? coll.title : title) + pageSuffix;
  const finalDescription = coll ? coll.description : description;

  return {
    title: finalTitle,
    description: finalDescription,
    keywords: [brand, catLabel, q, "купить парфюм", "духи онлайн"].filter(Boolean).join(", "),
    alternates: { canonical },
    robots: noindex ? { index: false, follow: true } : undefined,
    openGraph: { title: finalTitle, description: finalDescription, url: `${SITE_URL}${canonical}` },
  };
}

const PAGE_SIZE = 24;

export default async function CatalogPage({ searchParams }: Props) {
  const sp = await searchParams;
  const brand    = sp.brand ?? "";
  const category = sp.category ?? "";
  const q        = sp.q ?? "";
  const page     = Number(sp.page ?? 1);
  const sortRaw  = sp.sort ?? "";
  const sort     = (["name", "price_asc", "price_desc", "new", "popular"].includes(sortRaw) ? sortRaw : "popular") as "name" | "price_asc" | "price_desc" | "new" | "popular";
  const extra    = { gender: sp.gender || undefined, line: sp.line || undefined, group: sp.group || undefined,
    priceMin: Number(sp.priceMin) || undefined, priceMax: Number(sp.priceMax) || undefined, volume: Number(sp.volume) || undefined };
  const inStock  = sp.inStock === "true";
  const viewGrid = sp.view === "grid";

  const showGrid = !!(brand || category || q || inStock || sortRaw || Object.values(extra).some(Boolean)) || viewGrid;

  const emptyResult = { products: [], total: 0, page: 1, pageSize: PAGE_SIZE, brands: [] as string[] };

  const [catalogRes, autoCatsRes] = await Promise.allSettled([
    showGrid
      ? fetchCatalog({ brand: brand || undefined, category: category || undefined, q: q || undefined, page, pageSize: PAGE_SIZE, sort, inStock: inStock || undefined, ...extra })
      : Promise.resolve(emptyResult),
    fetchAutoCategories(),
  ]);

  const catalog       = catalogRes.status === "fulfilled" ? catalogRes.value : emptyResult;
  const autoCategories = autoCatsRes.status === "fulfilled" ? autoCatsRes.value : [];

  const crumbLabel = brand ? brand : category ? (CAT_LABELS[category] ?? category) : "Каталог";
  const crumbColl  = !brand && category ? collectionForCategory(category) : undefined;
  const crumbUrl   = brand ? brandHref(brand) : crumbColl ? collectionHref(crumbColl) : category ? `/catalog?category=${encodeURIComponent(category)}` : "/catalog";
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: crumbLabel, url: crumbUrl }]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <CatalogClient
        initialProducts={catalog.products}
        initialTotal={catalog.total}
        initialBrands={catalog.brands ?? []}
        initialFacets={"facets" in catalog ? catalog.facets : undefined}
        autoCategories={autoCategories}
      />
    </>
  );
}
