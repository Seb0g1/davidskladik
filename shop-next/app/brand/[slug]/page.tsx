import type { Metadata } from "next";
import { notFound, permanentRedirect } from "next/navigation";
import LandingPage from "@/components/LandingPage";
import { fetchBrands, fetchCatalog } from "@/lib/api";
import { SITE_URL } from "@/lib/seo";
import { brandHref, slugify, COLLECTION_DEFS, collectionHref, brandSubHref } from "@/lib/landings";
import { brandSubCounts, resolveBrand } from "@/lib/brand-subs";

export const revalidate = 600;
const PAGE_SIZE = 48;

interface Props { params: Promise<{ slug: string }>; searchParams: Promise<{ page?: string }> }


// old long names («12-parfumeurs-francais») → the brand they resolve to now («12-parfumeurs»)
// houses that used to have several spellings in the catalogue
const SLUG_ALIASES: Record<string, string> = {
  "christian-dior": "dior", "c-dior": "dior", "ysl": "yves-saint-laurent", "dolce": "dolce-and-gabbana", "dolce-gabbana": "dolce-and-gabbana",
  "lancome": "lancome", "hermes": "hermes", "armani": "giorgio-armani", "by-kilian": "kilian", "mfk": "maison-francis-kurkdjian",
  "francis-kurkdjian": "maison-francis-kurkdjian", "mont-blanc": "montblanc",
};

async function legacyBrandHref(slug: string) {
  const alias = Object.entries(SLUG_ALIASES).find(([from]) => slug === from || slug.startsWith(from + "-"));
  if (alias && alias[1] !== slug) return `/brand/${alias[1]}`;
  const brands = await fetchBrands().catch(() => [] as { name: string; count: number }[]);
  const hit = brands.map((b) => ({ b, s: slugify(b.name) })).filter(({ s }) => s && slug.startsWith(s + "-")).sort((a, b) => b.s.length - a.s.length)[0];
  return hit ? brandHref(hit.b.name) : null;
}

const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const b = await resolveBrand((await params).slug);
  if (!b) return { title: "Бренд не найден", robots: { index: false } };
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const path = brandHref(b.name) + (page > 1 ? `?page=${page}` : "");
  const title = `${b.name} — купить оригинальные духи и парфюмерию${page > 1 ? ` — страница ${page}` : ""}`;
  const description = `Духи ${b.name} в интернет-магазине Magic Vibes: ${b.count} ${plural(b.count)} — парфюмерная и туалетная вода, женские и мужские ароматы. Оригинал, доставка по России 1–5 дней, оплата картой или СБП.`;
  return {
    title,
    description,
    alternates: { canonical: path },
    // tiny brands (1–2 products) stay reachable but out of the index
    robots: b.count < 3 ? { index: false, follow: true } : undefined,
    openGraph: { title, description, url: `${SITE_URL}${path}` },
  };
}

export default async function BrandPage({ params, searchParams }: Props) {
  const slug = (await params).slug;
  const b = await resolveBrand(slug);
  if (!b) {
    const to = await legacyBrandHref(slug);
    if (to) permanentRedirect(to);
    notFound();
  }
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const [data, subCounts] = await Promise.all([
    fetchCatalog({ brand: b.name, page, pageSize: PAGE_SIZE, sort: "price_desc" }).catch(() => ({ products: [], total: 0, page, pageSize: PAGE_SIZE, brands: [] })),
    brandSubCounts(b.name),
  ]);
  const prices = data.products.map((p) => p.priceRub).filter((x) => x > 0);
  const min = prices.length ? Math.min(...prices) : 0;
  const max = prices.length ? Math.max(...prices) : 0;

  const intro = `${b.name} в Magic Vibes — ${data.total || b.count} ${plural(data.total || b.count)} бренда в каталоге${min ? `, цены от ${rub(min)}${max > min ? ` до ${rub(max)}` : ""}` : ""}. Только оригинальная продукция в заводской упаковке, отправка в течение 1 рабочего дня и доставка по России через Ozon за 1–5 дней.`;
  const faq: [string, string][] = [
    [`Духи ${b.name} оригинальные?`, `Да. Мы продаём только оригинальную продукцию ${b.name} в заводской упаковке и по запросу предоставляем документы о происхождении товара.`],
    [`Сколько стоит парфюмерия ${b.name}?`, min ? `Цены на ароматы ${b.name} в нашем каталоге — от ${rub(min)}${max > min ? ` до ${rub(max)}` : ""} в зависимости от объёма и концентрации.` : `Актуальные цены указаны в карточках товаров.`],
    ["Как быстро доставят заказ?", "Передаём заказ в доставку в течение 1 рабочего дня после оплаты. Ozon Доставка привозит его в пункт выдачи или курьером за 1–5 рабочих дней."],
    ["Можно ли вернуть аромат?", "Да, в течение 7 дней после получения, если флакон не вскрывался и сохранена заводская упаковка."],
  ];

  return (
    <LandingPage
      path={brandHref(b.name)}
      kicker="Бренд"
      h1={b.name}
      script="оригинальная парфюмерия"
      intro={intro}
      products={data.products}
      total={data.total}
      page={page}
      pageSize={PAGE_SIZE}
      catalogHref={`/catalog?brand=${encodeURIComponent(b.name)}`}
      faq={faq}
      subs={subCounts.map(({ sub, count }) => ({ label: `${sub.label.charAt(0).toUpperCase() + sub.label.slice(1)} · ${count}`, href: brandSubHref(b.name, sub) }))}
      subsTitle={`Парфюмерия ${b.name} по разделам`}
      seo={[
        `В интернет-магазине Magic Vibes можно купить оригинальную парфюмерию ${b.name}: ${(data.total || b.count).toLocaleString("ru-RU")} ${plural(data.total || b.count)}${subCounts.length ? ` — ${subCounts.map(({ sub }) => sub.label).join(", ")}` : ""}. Все ароматы поставляются в заводской упаковке, подлинность подтверждается документами.`,
        `Чтобы выбрать аромат ${b.name}, ориентируйтесь на семейство (цветочное, древесное, восточное, свежее), концентрацию и сезон: парфюмерная вода и духи звучат насыщеннее и стойче, туалетная вода — легче и подходит для жары и офиса. Если сомневаетесь, начните с пробника или миниатюры.`,
        `Заказ передаём в доставку в течение 1 рабочего дня, доставляем курьером или в пункт выдачи Ozon, СДЭК и Яндекс Доставки по всей России. Оплата онлайн картой, через СБП или в рассрочку.`,
      ]}
      seoTitle={`Купить духи ${b.name} в Magic Vibes`}
      related={[{ label: "Все бренды", href: "/brands" }, ...COLLECTION_DEFS.slice(0, 8).map((c) => ({ label: c.h1, href: collectionHref(c) }))]}
      crumbs={[{ name: "Главная", url: "/" }, { name: "Бренды", url: "/brands" }, { name: b.name, url: brandHref(b.name) }]}
    />
  );
}

function plural(n: number) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return "аромат";
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return "аромата";
  return "ароматов";
}
