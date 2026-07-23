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

export interface ShopOrderPayload {
  items: { offerId: string; quantity: number; priceRub: number }[];
  delivery: {
    firstName: string;
    lastName: string;
    phone: string;
    email: string;
    address: string;
    city: string;
    postalCode: string;
  };
  comment?: string;
}

export interface ShopOrder {
  id: string;
  status: string;
  paymentUrl?: string;
  totalRub: number;
  createdAt: string;
}

export interface CatalogResponse {
  products: ShopProduct[];
  total: number;
  page: number;
  pageSize: number;
  brands: string[];
}
