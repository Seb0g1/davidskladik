import { Helmet } from "react-helmet-async";

const SITE_NAME = "Magic Vibes";
const BASE_URL = "https://magicvibes.ru";
const DEFAULT_IMG = `${BASE_URL}/og-image.jpg`;
const DEFAULT_DESC =
  "Оригинальная парфюмерия мировых брендов с доставкой по России. Chanel, Dior, Tom Ford, Montale, Creed, Byredo и тысячи других ароматов. Гарантия подлинности. Рейтинг 4.9 на Ozon.";

interface SeoProps {
  title: string;
  description?: string;
  canonical?: string;
  image?: string;
  type?: "website" | "product" | "article";
  noindex?: boolean;
  jsonLd?: object | object[];
  /** Extra keywords appended after defaults */
  keywords?: string;
}

const BASE_KEYWORDS =
  "парфюм купить, духи купить, оригинальный парфюм, интернет-магазин парфюмерии, парфюм с доставкой по России, туалетная вода, парфюмерная вода, нишевая парфюмерия, Magic Vibes";

export function PageSeo({
  title,
  description = DEFAULT_DESC,
  canonical,
  image = DEFAULT_IMG,
  type = "website",
  noindex = false,
  jsonLd,
  keywords,
}: SeoProps) {
  const fullTitle = title.includes(SITE_NAME) ? title : `${title} | ${SITE_NAME}`;
  const url = canonical ? `${BASE_URL}${canonical}` : BASE_URL;
  const kw = keywords ? `${keywords}, ${BASE_KEYWORDS}` : BASE_KEYWORDS;
  const schemas = jsonLd ? (Array.isArray(jsonLd) ? jsonLd : [jsonLd]) : [];

  return (
    <Helmet>
      <title>{fullTitle}</title>
      <meta name="description" content={description} />
      <meta name="keywords" content={kw} />
      {noindex ? (
        <meta name="robots" content="noindex, nofollow" />
      ) : (
        <meta name="robots" content="index, follow, max-snippet:-1, max-image-preview:large, max-video-preview:-1" />
      )}
      <link rel="canonical" href={url} />

      <meta property="og:type" content={type} />
      <meta property="og:url" content={url} />
      <meta property="og:site_name" content={SITE_NAME} />
      <meta property="og:title" content={fullTitle} />
      <meta property="og:description" content={description} />
      <meta property="og:image" content={image} />
      <meta property="og:image:width" content="1200" />
      <meta property="og:image:height" content="630" />
      <meta property="og:locale" content="ru_RU" />

      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content={fullTitle} />
      <meta name="twitter:description" content={description} />
      <meta name="twitter:image" content={image} />

      {schemas.map((s, i) => (
        <script key={i} type="application/ld+json">
          {JSON.stringify(s)}
        </script>
      ))}
    </Helmet>
  );
}

/* ── Structured data builders ── */

export function productJsonLd(p: {
  name: string;
  brand?: string;
  description?: string;
  image?: string;
  sku?: string;
  url: string;
  priceRub?: number;
  inStock?: boolean;
  deliveryPriceRub?: number;
  deliveryDaysMin?: number;
  deliveryDaysMax?: number;
  freeDeliveryFrom?: number;
}) {
  // priceValidUntil = 1 year from today — required for Google price rich snippet
  const priceValidUntil = new Date(Date.now() + 365 * 24 * 3600 * 1000)
    .toISOString()
    .slice(0, 10);

  return {
    "@context": "https://schema.org",
    "@type": "Product",
    name: p.name,
    ...(p.brand ? { brand: { "@type": "Brand", name: p.brand } } : {}),
    ...(p.description ? { description: p.description } : {}),
    ...(p.image ? { image: p.image } : {}),
    ...(p.sku ? { sku: p.sku, mpn: p.sku } : {}),
    url: `${BASE_URL}${p.url}`,
    offers: {
      "@type": "Offer",
      priceCurrency: "RUB",
      ...(p.priceRub ? { price: String(p.priceRub) } : {}),
      priceValidUntil,
      itemCondition: "https://schema.org/NewCondition",
      availability: p.inStock
        ? "https://schema.org/InStock"
        : "https://schema.org/OutOfStock",
      seller: {
        "@type": "Organization",
        name: SITE_NAME,
        url: BASE_URL,
      },
      url: `${BASE_URL}${p.url}`,
      hasMerchantReturnPolicy: {
        "@type": "MerchantReturnPolicy",
        applicableCountry: "RU",
        returnPolicyCategory: "https://schema.org/MerchantReturnFiniteReturnWindow",
        merchantReturnDays: 14,
        returnMethod: "https://schema.org/ReturnByMail",
        returnFees: "https://schema.org/FreeReturn",
      },
      shippingDetails: {
        "@type": "OfferShippingDetails",
        shippingRate: {
          "@type": "MonetaryAmount",
          value: p.deliveryPriceRub != null && p.priceRub && p.freeDeliveryFrom && p.priceRub >= p.freeDeliveryFrom
            ? "0"
            : String(p.deliveryPriceRub ?? 350),
          currency: "RUB",
        },
        shippingDestination: {
          "@type": "DefinedRegion",
          addressCountry: "RU",
        },
        deliveryTime: {
          "@type": "ShippingDeliveryTime",
          handlingTime: { "@type": "QuantitativeValue", minValue: 0, maxValue: 1, unitCode: "DAY" },
          transitTime: {
            "@type": "QuantitativeValue",
            minValue: p.deliveryDaysMin ?? 1,
            maxValue: p.deliveryDaysMax ?? 5,
            unitCode: "DAY",
          },
        },
      },
    },
    // aggregateRating is omitted here — ProductPage injects real data when available
  };
}

export function breadcrumbJsonLd(items: { name: string; url: string }[]) {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: items.map((item, i) => ({
      "@type": "ListItem",
      position: i + 1,
      name: item.name,
      item: `${BASE_URL}${item.url}`,
    })),
  };
}
