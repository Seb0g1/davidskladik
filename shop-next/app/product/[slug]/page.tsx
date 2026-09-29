import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { fetchProduct, fetchSettings, fetchMarketplaceReviews, fetchFragranceNotes, fetchCatalog, fetchReviews, fetchProductQA, fetchVariants } from "@/lib/api";
import { productJsonLd, breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import { parseSlugForOfferId, toProductSlug } from "@/lib/slug";
import ProductClient from "./ProductClient";
import { brandHref } from "@/lib/landings";

interface Props {
  params: Promise<{ slug: string }>;
}

export const revalidate = 300;

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  try {
    const product = await fetchProduct(parseSlugForOfferId(slug));
    const title = product.brand && !product.name.toLowerCase().includes(product.brand.toLowerCase())
      ? `${product.name} — ${product.brand}, купить`
      : `${product.name} — купить`;
    const description = `Купить ${product.name} (${product.brand}) в Magic Vibes за ${product.priceRub.toLocaleString("ru-RU")} ₽. ${product.inStock ? "В наличии. " : ""}Оригинал, быстрая доставка по России.`;
    const url = `/product/${toProductSlug(product.name, product.offerId)}`;
    return {
      title,
      description,
      keywords: `${product.name}, ${product.brand}, купить ${product.brand}, ${product.name} цена, оригинальный парфюм ${product.brand}`,
      alternates: { canonical: url },
      openGraph: {
        title,
        description,
        url: `${SITE_URL}${url}`,
        type: "website",
        images: product.images[0] ? [{ url: product.images[0], alt: `${product.name} ${product.brand}` }] : [],
      },
      twitter: { title, description, images: product.images[0] ? [product.images[0]] : [] },
    };
  } catch {
    return { title: "Товар не найден" };
  }
}

export default async function ProductPage({ params }: Props) {
  const { slug } = await params;

  let product;
  try {
    product = await fetchProduct(parseSlugForOfferId(slug));
  } catch {
    notFound();
  }

  const [settings, mpReviews, notes, related, siteReviews, qa, vars] = await Promise.allSettled([
    fetchSettings(),
    fetchMarketplaceReviews(product.offerId),
    fetchFragranceNotes(product.brand, product.name, product.offerId),
    fetchCatalog({ brand: product.brand, pageSize: 8, inStock: true }),
    fetchReviews(20, product.offerId),
    fetchProductQA(product.offerId),
    fetchVariants(product.offerId),
  ]);

  const s = settings.status === "fulfilled" ? settings.value : null;
  const reviews = mpReviews.status === "fulfilled" ? mpReviews.value : null;
  const fragNotes = notes.status === "fulfilled" ? notes.value.data : null;
  const relatedProducts = related.status === "fulfilled"
    ? related.value.products.filter(p => p.offerId !== product.offerId).slice(0, 6)
    : [];
  const initialSiteReviews = siteReviews.status === "fulfilled" ? siteReviews.value.reviews : [];
  const qaItems = qa.status === "fulfilled" ? qa.value.items : [];
  const variants = vars.status === "fulfilled" ? vars.value : [];

  const productUrl = `/product/${toProductSlug(product.name, product.offerId)}`;
  const schema = productJsonLd(product, s);
  const breadcrumbItems = [
    { name: "Главная", url: "/" },
    ...(product.brand ? [{ name: product.brand, url: brandHref(product.brand) }] : []),
    { name: product.name, url: productUrl },
  ];
  const breadcrumb = breadcrumbJsonLd(breadcrumbItems);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(schema) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <ProductClient
        product={product}
        settings={s}
        initialMpReviews={reviews?.reviews ?? []}
        initialAvgRating={reviews?.avgRating}
        initialReviewCount={reviews?.reviewCount}
        initialSiteReviews={initialSiteReviews}
        fragranceNotes={fragNotes}
        relatedProducts={relatedProducts}
        qaItems={qaItems}
        variants={variants}
      />
    </>
  );
}
