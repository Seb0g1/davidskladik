import type { Metadata } from "next";
import { notFound } from "next/navigation";
import Link from "next/link";
import { fetchProduct, fetchSettings, fetchMarketplaceReviews, fetchFragranceNotes, fetchCatalog, fetchReviews, fetchProductQA } from "@/lib/api";
import { productJsonLd, breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import ProductClient from "./ProductClient";

interface Props {
  params: Promise<{ offerId: string }>;
}

export const revalidate = 300;

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { offerId } = await params;
  try {
    const product = await fetchProduct(decodeURIComponent(offerId));
    const title = `${product.name} — ${product.brand} купить в ${SITE_NAME}`;
    const description = `Купить ${product.name} (${product.brand}) в Magic Vibes за ${product.priceRub.toLocaleString("ru-RU")} ₽. ${product.inStock ? "В наличии. " : ""}Оригинал, быстрая доставка по России.`;
    const url = `/product/${encodeURIComponent(product.offerId)}`;
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
  const { offerId } = await params;
  const decoded = decodeURIComponent(offerId);

  let product;
  try {
    product = await fetchProduct(decoded);
  } catch {
    notFound();
  }

  const [settings, mpReviews, notes, related, siteReviews, qa] = await Promise.allSettled([
    fetchSettings(),
    fetchMarketplaceReviews(product.offerId),
    fetchFragranceNotes(product.brand, product.name, product.offerId),
    fetchCatalog({ brand: product.brand, pageSize: 8, inStock: true }),
    fetchReviews(20, product.offerId),
    fetchProductQA(product.offerId),
  ]);

  const s = settings.status === "fulfilled" ? settings.value : null;
  const reviews = mpReviews.status === "fulfilled" ? mpReviews.value : null;
  const fragNotes = notes.status === "fulfilled" ? notes.value.data : null;
  const relatedProducts = related.status === "fulfilled"
    ? related.value.products.filter(p => p.offerId !== product.offerId).slice(0, 6)
    : [];
  const initialSiteReviews = siteReviews.status === "fulfilled" ? siteReviews.value.reviews : [];
  const qaItems = qa.status === "fulfilled" ? qa.value.items : [];

  const productUrl = `/product/${encodeURIComponent(product.offerId)}`;
  const schema = productJsonLd(product, s);
  const breadcrumbItems = [
    { name: "Главная", url: "/" },
    ...(product.brand ? [{ name: product.brand, url: `/catalog?brand=${encodeURIComponent(product.brand)}` }] : []),
    { name: product.name, url: productUrl },
  ];
  const breadcrumb = breadcrumbJsonLd(breadcrumbItems);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(schema) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      {/* SSR breadcrumb for crawlers */}
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "16px clamp(18px,4vw,56px) 0" }}>
        <nav aria-label="breadcrumb" style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "rgba(245,244,240,0.48)", flexWrap: "wrap" }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.48)", textDecoration: "none" }}>Главная</Link>
          {product.brand && (<><span>/</span><Link href={`/catalog?brand=${encodeURIComponent(product.brand)}`} style={{ color: "rgba(245,244,240,0.48)", textDecoration: "none" }}>{product.brand}</Link></>)}
          <span>/</span>
          <span style={{ color: "#f5f4f0" }}>{product.name}</span>
        </nav>
      </div>

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
      />
    </>
  );
}
