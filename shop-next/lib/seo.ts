import type { ShopProduct, ShopSettings } from "./types";
import { toProductSlug } from "./slug";

export const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL ?? "https://magicvibes.ru";
export const SITE_NAME = "Magic Vibes";
export const DEFAULT_DESC = "Оригинальная парфюмерия мировых брендов с доставкой по России. Chanel, Dior, Tom Ford, Montale, Creed, Byredo и тысячи других ароматов. Гарантия подлинности. Рейтинг 4.9 на Ozon.";

export function productJsonLd(p: ShopProduct, settings?: ShopSettings | null) {
  const priceValidUntil = new Date(Date.now() + 365 * 24 * 3600 * 1000).toISOString().slice(0, 10);
  const isFree = settings?.freeDeliveryFrom && p.priceRub >= settings.freeDeliveryFrom;
  const deliveryPrice = isFree ? "0" : String(settings?.deliveryPriceRub ?? 350);
  const url = `${SITE_URL}/product/${toProductSlug(p.name, p.offerId)}`;

  const schema: Record<string, unknown> = {
    "@context": "https://schema.org",
    "@type": "Product",
    name: p.name,
    ...(p.brand ? { brand: { "@type": "Brand", name: p.brand } } : {}),
    description: p.description?.slice(0, 300) || undefined,
    image: p.images[0] || undefined,
    sku: p.offerId,
    mpn: p.offerId,
    url,
    offers: {
      "@type": "Offer",
      priceCurrency: "RUB",
      price: String(p.priceRub),
      priceValidUntil,
      itemCondition: "https://schema.org/NewCondition",
      availability: p.inStock ? "https://schema.org/InStock" : "https://schema.org/OutOfStock",
      seller: { "@type": "Organization", name: SITE_NAME, url: SITE_URL },
      url,
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
        shippingRate: { "@type": "MonetaryAmount", value: deliveryPrice, currency: "RUB" },
        shippingDestination: { "@type": "DefinedRegion", addressCountry: "RU" },
        deliveryTime: {
          "@type": "ShippingDeliveryTime",
          handlingTime: { "@type": "QuantitativeValue", minValue: 0, maxValue: 1, unitCode: "DAY" },
          transitTime: {
            "@type": "QuantitativeValue",
            minValue: settings?.deliveryDaysMin ?? 1,
            maxValue: settings?.deliveryDays ?? 5,
            unitCode: "DAY",
          },
        },
      },
    },
  };

  if (p.rating && p.reviewCount) {
    schema.aggregateRating = {
      "@type": "AggregateRating",
      ratingValue: p.rating,
      reviewCount: p.reviewCount,
      bestRating: "5",
    };
  }

  return schema;
}

export function breadcrumbJsonLd(items: { name: string; url: string }[]) {
  return {
    "@context": "https://schema.org",
    "@type": "BreadcrumbList",
    itemListElement: items.map((item, i) => ({
      "@type": "ListItem",
      position: i + 1,
      name: item.name,
      item: `${SITE_URL}${item.url}`,
    })),
  };
}
