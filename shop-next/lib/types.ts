export interface ShopProduct {
  id: string;
  offerId: string;
  name: string;
  brand: string;
  description: string;
  images: string[];
  priceRub: number;
  oldPriceRub?: number;
  inStock: boolean;
  stockQty: number;
  volume?: string;
  category?: string;
  categoryLabel?: string;
  tags?: string[];
  rating?: number;
  reviewCount?: number;
}

export interface ShopBanner {
  id: string;
  imageUrl: string;
  title?: string;
  subtitle?: string;
  linkUrl?: string;
  linkText?: string;
  endDate?: string;
  promoCode?: string;
  holidayKey?: string;
  active: boolean;
  order: number;
}

export interface ShopCategory {
  id: string;
  name: string;
  slug: string;
  icon?: string;
  imageUrl?: string;
  order: number;
  filterTag?: string;
}

export interface ShopSettings {
  markup: number;
  shopName: string;
  shopDescription: string;
  contactEmail?: string;
  contactPhone?: string;
  deliveryDays?: number;
  deliveryDaysMin?: number;
  deliveryPriceRub?: number;
  freeDeliveryFrom?: number;
  features?: { giftBuilder?: boolean; vipClub?: boolean };
  deliveryMode?: "fixed" | "ozon";
}

export interface CartItem {
  product: ShopProduct;
  quantity: number;
}

export type PaymentMethod = "ozon_pay" | "sbp" | "cash";

export interface ShopOrderPayload {
  items: { offerId: string; quantity: number; priceRub: number }[];
  delivery: {
    type?: "courier" | "pickup" | "ozon_pay";
    firstName: string;
    lastName?: string;
    phone: string;
    email: string;
    address?: string;
    city?: string;
    postalCode?: string;
    pvzId?: string;
    pvzName?: string;
  };
  comment?: string;
  paymentMethod?: PaymentMethod;
  refCode?: string;
  promoCode?: string;
  /** 152-ФЗ consent (required) and 38-ФЗ ads opt-in, with the document version */
  consents?: { pd: boolean; ads: boolean; version: string };
}

export interface ShopOrder {
  id: string;
  status: string;
  paymentUrl?: string;
  totalRub: number;
  createdAt: string;
  items?: { offerId: string; quantity: number; priceRub: number; name?: string; brand?: string; image?: string | null; volume?: string | null; slug?: string | null }[];
  delivery?: { type?: string; firstName?: string; lastName?: string; city?: string; address?: string; phone?: string; email?: string; pvzId?: string; pvzName?: string; priceRub?: number;
    carrier?: string; carrierTitle?: string; daysMin?: number | null; daysMax?: number | null;
    shipment?: { carrier: string; id: string; status?: string; statusLabel?: string; number?: string | null; trackingUrl?: string | null } };
  /** /track/<id>?k=… — страница отслеживания на сайте (есть, когда заказ передан в службу) */
  trackUrl?: string;
  comment?: string;
}

export interface TelegramNewsPost {
  id: string;
  text: string;
  photoUrl?: string | null;
  publishedAt: string;
}

export interface ShopReview {
  id: string;
  offerId?: string | null;
  productName?: string | null;
  productImg?: string | null;
  rating: number;
  text: string;
  photoUrl?: string | null;
  createdAt: string;
  author?: string;
}

export interface MarketplaceReview {
  id: string;
  author: string;
  rating: number;
  text: string;
  advantages?: string;
  disadvantages?: string;
  createdAt: string;
  source: "ozon" | "yandex";
  photos?: string[];
  videoUrl?: string;
}

export interface ProductQAItem {
  id: string;
  question: string;
  answer: string;
  createdAt: string;
}

export interface FragranceNotes {
  topNotes: string[];
  middleNotes: string[];
  baseNotes: string[];
  accords: string[];
  gender?: string;
  seasons?: string[];
}

export interface CatalogFacets {
  brands: { name: string; count: number }[];
  volumes: { ml: number; count: number }[];
  price: { min: number; max: number };
}

export interface CatalogResponse {
  products: ShopProduct[];
  total: number;
  page: number;
  pageSize: number;
  brands: string[];
  facets?: CatalogFacets;
}

export interface BlogPost {
  id: string;
  slug: string;
  title: string;
  excerpt?: string;
  content?: string;
  coverUrl?: string;
  tags: string[];
  publishedAt?: string;
  createdAt?: string;
  published?: boolean;
}

export interface ShopCustomer {
  id: string;
  email: string;
  firstName?: string | null;
  lastName?: string | null;
  phone?: string | null;
  avatarUrl?: string | null;
}

export interface LoyaltyData {
  points: number;
  tier: "silver" | "gold" | "platinum";
  nextTier: number | null;
  transactions: { id: string; points: number; reason: string; createdAt: string }[];
}

export interface AutoCategory {
  slug: string;
  label: string;
  count: number;
}

export interface ShopContest {
  id: string;
  title: string;
  badge?: string;
  description?: string;
  prize?: string;
  imageUrl?: string;
  linkUrl?: string;
  linkText?: string;
  rulesUrl?: string;
  startDate?: string;
  endDate?: string;
  steps: { title: string; desc: string }[];
}
