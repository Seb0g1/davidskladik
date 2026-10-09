import { toProductSlug } from "./slug";

/** Where-to-buy pages linked from Telegram posts. */
export const STORE_LINKS = {
  site: "https://magicvibes.ru",
  ozon: "https://www.ozon.ru/seller/magic-stick/",
  yandex: "https://market.yandex.ru/business--parfumerius/171782339",
} as const;

export type BuyChannel = keyof typeof STORE_LINKS;

export const CHANNEL_LABEL: Record<BuyChannel, string> = {
  site: "magicvibes.ru",
  yandex: "Яндекс Маркет",
  ozon: "Ozon",
};

export const buyPath = (p?: { name: string; offerId: string }) => (p ? `/buy/${toProductSlug(p.name, p.offerId)}` : "/buy");

/** price: exact buyer price, null when the platform does not expose it. */
export interface BuyOffer { price: number | null; url?: string; inStock: boolean }

export interface BuyCompare {
  product: {
    offerId: string; name: string; brand: string; image: string | null; volume: string | null;
    priceRub: number; oldPriceRub: number | null; inStock: boolean;
  };
  offers: Record<BuyChannel, BuyOffer | null>;
}
