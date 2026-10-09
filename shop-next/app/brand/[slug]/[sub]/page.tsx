// /brand/<brand>/<sub> — «Chanel для женщин», «Парфюмерная вода Dior»: narrow listings for queries like
// «женские духи шанель купить» (randewoo-style listings).
import type { Metadata } from "next";
import { notFound } from "next/navigation";
import LandingPage from "@/components/LandingPage";
import { fetchCatalog } from "@/lib/api";
import { SITE_URL } from "@/lib/seo";
import { BRAND_SUB_MIN, brandHref, brandSubBySlug, brandSubHref, collectionHref, COLLECTION_DEFS } from "@/lib/landings";
import { brandSubCounts, resolveBrand } from "@/lib/brand-subs";

export const revalidate = 600;
const PAGE_SIZE = 48;

interface Props { params: Promise<{ slug: string; sub: string }>; searchParams: Promise<{ page?: string }> }

const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

async function load(slug: string, subSlug: string, page: number) {
  const sub = brandSubBySlug(subSlug);
  if (!sub) return null;
  const b = await resolveBrand(slug);
  if (!b) return null;
  const data = await fetchCatalog({ brand: b.name, gender: sub.gender, category: sub.category, page, pageSize: PAGE_SIZE, sort: "popular" })
    .catch(() => ({ products: [], total: 0, page, pageSize: PAGE_SIZE, brands: [] as string[], facets: undefined }));
  return { b, sub, data };
}

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const { slug, sub: subSlug } = await params;
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const r = await load(slug, subSlug, page);
  if (!r) return { title: "Раздел не найден", robots: { index: false } };
  const { b, sub, data } = r;
  const path = brandSubHref(b.name, sub) + (page > 1 ? `?page=${page}` : "");
  const title = sub.title(b.name) + (page > 1 ? ` — страница ${page}` : "");
  const min = data.facets?.price?.min || 0;
  const description = `${sub.h1(b.name)} в интернет-магазине Magic Vibes: ${data.total} ${plural(data.total)}${min ? ` от ${rub(min)}` : ""}. Только оригинал, доставка по России 1–5 дней, оплата картой, СБП или в рассрочку.`;
  return {
    title,
    description,
    alternates: { canonical: path },
    robots: data.total < BRAND_SUB_MIN ? { index: false, follow: true } : undefined,
    openGraph: { title, description, url: `${SITE_URL}${path}` },
  };
}

export default async function BrandSubPage({ params, searchParams }: Props) {
  const { slug, sub: subSlug } = await params;
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const r = await load(slug, subSlug, page);
  if (!r) notFound();
  const { b, sub, data } = r;
  if (!data.total && page === 1) notFound();
  const siblings = (await brandSubCounts(b.name)).filter((x) => x.sub.slug !== sub.slug);
  const min = data.facets?.price?.min || 0;
  const max = data.facets?.price?.max || 0;
  const h1 = sub.h1(b.name);
  const coll = COLLECTION_DEFS.find((c) => c.slug === sub.slug);

  const intro = `${h1} — ${data.total} ${plural(data.total)} в каталоге Magic Vibes${min ? `, цены от ${rub(min)}${max > min ? ` до ${rub(max)}` : ""}` : ""}. Только оригинальная продукция в заводской упаковке, отправка в течение 1 рабочего дня и доставка по всей России за 1–5 дней.`;
  const faq: [string, string][] = [
    [`Это оригинальная продукция ${b.name}?`, `Да. Мы продаём только оригинальную парфюмерию ${b.name} в заводской упаковке и по запросу предоставляем документы о происхождении товара.`],
    [`Сколько стоит ${sub.label} ${b.name}?`, min ? `Цены в нашем каталоге — от ${rub(min)}${max > min ? ` до ${rub(max)}` : ""} в зависимости от аромата, объёма и концентрации.` : "Актуальные цены указаны в карточках товаров."],
    ["Как быстро доставят заказ?", "Передаём заказ в доставку в течение 1 рабочего дня после оплаты. Доставка курьером или в пункт выдачи Ozon, СДЭК и Яндекс Доставки занимает 1–5 рабочих дней."],
    ["Можно ли вернуть аромат?", "Да, в течение 7 дней после получения, если флакон не вскрывался и сохранена заводская упаковка."],
  ];

  return (
    <LandingPage
      path={brandSubHref(b.name, sub)}
      kicker={b.name}
      h1={h1}
      script={sub.label}
      intro={intro}
      products={data.products}
      total={data.total}
      page={page}
      pageSize={PAGE_SIZE}
      catalogHref={`/catalog?brand=${encodeURIComponent(b.name)}${sub.category ? `&category=${sub.category}` : `&gender=${encodeURIComponent(sub.gender || "")}`}`}
      faq={faq}
      subs={[{ label: `Вся парфюмерия ${b.name}`, href: brandHref(b.name) }, ...siblings.map(({ sub: s, count }) => ({ label: `${s.label.charAt(0).toUpperCase() + s.label.slice(1)} · ${count}`, href: brandSubHref(b.name, s) }))]}
      subsTitle={`Парфюмерия ${b.name}`}
      seo={[
        `В Magic Vibes собрана ${sub.label} ${b.name} — ${data.total} ${plural(data.total)} в наличии и под заказ. Мы работаем только с оригинальной продукцией и проверяем каждый флакон перед отправкой.`,
        ...(coll?.seo?.slice(0, 1) ?? []),
      ]}
      seoTitle={`Купить ${sub.label} ${b.name} — оригинал с доставкой`}
      related={[{ label: "Все бренды", href: "/brands" }, ...(coll ? [{ label: coll.h1, href: collectionHref(coll) }] : []),
        ...COLLECTION_DEFS.filter((c) => c !== coll).slice(0, 6).map((c) => ({ label: c.h1, href: collectionHref(c) }))]}
      crumbs={[{ name: "Главная", url: "/" }, { name: "Бренды", url: "/brands" }, { name: b.name, url: brandHref(b.name) }, { name: h1, url: brandSubHref(b.name, sub) }]}
    />
  );
}

function plural(n: number) {
  const m10 = n % 10, m100 = n % 100;
  if (m10 === 1 && m100 !== 11) return "аромат";
  if (m10 >= 2 && m10 <= 4 && (m100 < 10 || m100 >= 20)) return "аромата";
  return "ароматов";
}
