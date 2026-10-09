import type { Metadata } from "next";
import { notFound } from "next/navigation";
import LandingPage from "@/components/LandingPage";
import { fetchCatalog } from "@/lib/api";
import type { CatalogResponse } from "@/lib/types";
import { SITE_URL } from "@/lib/seo";
import { COLLECTION_DEFS, collectionBySlug, collectionHref, brandHref, brandSubBySlug, brandSubHref, BRAND_SUB_MIN } from "@/lib/landings";

export const revalidate = 600;
const PAGE_SIZE = 48;

interface Props { params: Promise<{ slug: string }>; searchParams: Promise<{ page?: string }> }

export function generateStaticParams() {
  return COLLECTION_DEFS.map((c) => ({ slug: c.slug }));
}

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const c = collectionBySlug((await params).slug);
  if (!c) return { title: "Коллекция не найдена" };
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const path = collectionHref(c) + (page > 1 ? `?page=${page}` : "");
  const title = c.title + (page > 1 ? ` — страница ${page}` : "");
  return {
    title,
    description: c.description,
    alternates: { canonical: path },
    openGraph: { title, description: c.description, url: `${SITE_URL}${path}` },
  };
}

export default async function CollectionPage({ params, searchParams }: Props) {
  const c = collectionBySlug((await params).slug);
  if (!c) notFound();
  const page = Math.max(1, Number((await searchParams).page) || 1);
  const data: CatalogResponse = await fetchCatalog({ q: c.q, category: c.category, page, pageSize: PAGE_SIZE, inStock: true }).catch(() => ({ products: [], total: 0, page, pageSize: PAGE_SIZE, brands: [] }));
  const catalogHref = c.category ? `/catalog?category=${c.category}` : `/catalog?q=${encodeURIComponent(c.q || "")}`;
  // a few neighbouring collections for internal linking
  const idx = COLLECTION_DEFS.indexOf(c);
  const related = [...COLLECTION_DEFS.slice(idx + 1), ...COLLECTION_DEFS.slice(0, idx)].slice(0, 10).map((r) => ({ label: r.h1, href: collectionHref(r) }));
  // top brands of this collection → «Chanel для женщин» listing when the collection has a brand sub-listing
  const sub = brandSubBySlug(c.slug);
  const facetBrands = data.facets?.brands ?? [];
  const subs = facetBrands
    .filter((b) => b.count >= BRAND_SUB_MIN && b.name.length <= 30 && brandHref(b.name) !== "/brand/")
    .slice(0, 18)
    .map((b) => ({ label: b.name, href: sub ? brandSubHref(b.name, sub) : brandHref(b.name) }));

  return (
    <LandingPage
      path={collectionHref(c)}
      kicker="Коллекция"
      h1={c.h1}
      script={c.script}
      intro={c.intro}
      products={data.products}
      total={data.total}
      page={page}
      pageSize={PAGE_SIZE}
      catalogHref={catalogHref}
      faq={c.faq}
      related={related}
      subs={subs}
      subsTitle="Популярные бренды"
      seo={c.seo}
      seoTitle={`${c.h1} — купить в интернет-магазине Magic Vibes`}
      crumbs={[{ name: "Главная", url: "/" }, { name: "Каталог", url: "/catalog" }, { name: c.h1, url: collectionHref(c) }]}
    />
  );
}
