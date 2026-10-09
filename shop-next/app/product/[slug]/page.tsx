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
    // product names already start with the brand; the brand field is sometimes wrong («Burberry MR» → BDK), so it stays out
    const title = `${product.name} — купить оригинал`;
    const price = product.priceRub > 0 ? ` по цене ${product.priceRub.toLocaleString("ru-RU")} ₽` : "";
    const description = `${product.name}: купить в интернет-магазине Magic Vibes${price}. ${product.inStock ? "В наличии. " : ""}100% оригинал, доставка по России 1–5 дней, оплата картой, СБП или в рассрочку.`;
    const url = `/product/${toProductSlug(product.name, product.offerId)}`;
    return {
      title,
      description,
      keywords: `${product.name}, купить ${product.name}, ${product.name} цена, ${product.name} оригинал${product.brand ? `, духи ${product.brand}` : ""}`,
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
