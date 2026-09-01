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

export interface AutoCategory {
  slug: string;
  label: string;
  count: number;
}

export interface ShopBanner {
  id: string;
  imageUrl: string;
  title?: string;
  subtitle?: string;
  linkUrl?: string;
  linkText?: string;
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
  freeDeliveryFrom?: number;
}

export interface CartItem {
  product: ShopProduct;
  quantity: number;
}

export type PaymentMethod = "ozon_pay" | "sbp" | "cash";

export interface ShopOrderPayload {
  items: { offerId: string; quantity: number; priceRub: number }[];
  delivery: {
    type?: "courier" | "pickup";
    firstName: string;
    lastName?: string;
    phone: string;
    email: string;
    address?: string;
    city?: string;
    postalCode?: string;
    pvzId?: string;
  };
  comment?: string;
  paymentMethod?: PaymentMethod;
}

export interface ShopOrderItem {
  offerId: string;
  quantity: number;
  priceRub: number;
  name?: string;
}

export interface ShopOrder {
  id: string;
  status: string;
  paymentUrl?: string;
  totalRub: number;
  createdAt: string;
  items?: ShopOrderItem[];
  delivery?: {
    firstName?: string;
    lastName?: string;
    city?: string;
    address?: string;
    phone?: string;
    email?: string;
  };
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
}

export interface CatalogResponse {
  products: ShopProduct[];
  total: number;
  page: number;
  pageSize: number;
  brands: string[];
}
