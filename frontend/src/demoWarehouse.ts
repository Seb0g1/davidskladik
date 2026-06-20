import type { Product } from "./types";

const now = "2026-06-12T09:30:00.000Z";

function link(article: string, supplierName: string, price = 43.5, stockOnly = false) {
  return {
    id: `demo-link-${article.toLowerCase()}`,
    article,
    supplierArticle: article,
    supplierName,
    partnerId: supplierName.toLowerCase().replace(/\s+/g, "-"),
    rowId: `pm-${article.toLowerCase()}`,
    sourceRowId: `pm-${article.toLowerCase()}`,
    exactName: `${article} PriceMaster row`,
    matchType: "selected_row",
    keyword: "",
    priceCurrency: "USD",
    price,
    available: true,
    pricingMode: stockOnly ? "stock_only" : "normal",
    stockOnly,
    priceEligible: !stockOnly,
    stockEligible: true,
    stockOnlyCount: stockOnly ? 1 : 0,
    priceEligibleCount: stockOnly ? 0 : 1,
    createdAt: now,
    updatedAt: now,
    createdBy: "demo",
    updatedBy: "demo",
  };
}

function supplier(name: string, article: string, price: number, finalPrice: number) {
  return {
    id: `demo-supplier-${name.toLowerCase().replace(/\s+/g, "-")}`,
    name,
    supplierName: name,
    partnerName: name,
    partnerId: name.toLowerCase().replace(/\s+/g, "-"),
    article,
    currency: "USD",
    priceCurrency: "USD",
    price,
    effectiveFinalPrice: finalPrice,
    calculatedPrice: finalPrice,
    markupCoefficient: 1.72,
    baseMarkupCoefficient: 1.65,
    trustFactor: 96,
    orderCutoffTime: "18:00",
    reseller: false,
    impactProductCount: 1,
    pricingMode: "normal",
    active: true,
    stopped: false,
  };
}

function product(input: {
  id: string;
  marketplace: "ozon" | "yandex";
  offerId: string;
  name: string;
  brand: string;
  imageUrl: string;
  currentPrice: number;
  targetPrice: number;
  stock: number;
  supplierName: string;
  supplierArticle: string;
  supplierPrice: number;
  finalPrice: number;
  status?: string;
  archived?: boolean;
  changed?: boolean;
  stockOnly?: boolean;
}): Product {
  const selected = supplier(input.supplierName, input.supplierArticle, input.supplierPrice, input.finalPrice);
  const marketplaceLabel = input.marketplace === "ozon" ? "Ozon" : "Yandex Market";
  const statusCode = input.archived ? "archived" : (input.status || (input.stock > 0 ? "active" : "out_of_stock"));
  const statusLabel = input.archived ? "В архиве" : (input.stock > 0 ? "Активен" : "Нет остатка");
  return {
    id: input.id,
    marketplace: input.marketplace,
    target: input.marketplace,
    targetName: marketplaceLabel,
    offerId: input.offerId,
    sku: input.offerId,
    barcode: `${input.offerId}-BAR`,
    productId: `${input.marketplace}-${input.offerId}`,
    name: input.name,
    brand: input.brand,
    imageUrl: input.imageUrl,
    images: [input.imageUrl],
    currentPrice: input.currentPrice,
    newPrice: input.changed ? input.targetPrice : null,
    targetPrice: input.targetPrice,
    targetStock: input.stock,
    stock: input.stock,
    autoPriceEnabled: true,
    archived: Boolean(input.archived),
    status: statusCode,
    updatedAt: now,
    marketplaceState: { code: statusCode, label: statusLabel, stock: input.stock },
    links: [link(input.supplierArticle, input.supplierName, input.supplierPrice, input.stockOnly)],
    suppliers: [selected],
    supplierAlternatives: [
      selected,
      supplier("Иванна", `${input.supplierArticle}-ALT`, input.supplierPrice + 2.2, input.finalPrice + 210),
      supplier("Наш склад", `${input.supplierArticle}-SKL`, input.supplierPrice + 4.1, input.finalPrice + 420),
    ],
    selectedSupplier: selected,
    stockOnlyFallbackActive: Boolean(input.stockOnly),
    stockOnlyManualPriceMissing: false,
    stockOnlyManualPrices: input.stockOnly ? { default: input.targetPrice, ozon: input.targetPrice, yandex: input.targetPrice } : {},
    stockOnlyAvailableSupplierCount: input.stockOnly ? 1 : 0,
    noSupplierAutomation: {},
    aiImages: [
      {
        id: `${input.id}-ai-1`,
        status: "completed",
        sourceImageUrl: input.imageUrl,
        resultUrl: input.imageUrl,
        prompt: "Premium marketplace packshot, centered bottle, clean studio light.",
        batchId: "demo-ai",
        variantIndex: 1,
        variantTotal: 5,
        createdAt: now,
      },
    ],
    priceFormula: {
      markupCoefficient: 1.72,
      baseMarkupCoefficient: 1.65,
      usdRate: 91.4,
      selectedSupplierPrice: input.supplierPrice,
      selectedSupplierCurrency: "USD",
      selectedSupplierName: input.supplierName,
      targetPrice: input.targetPrice,
      stockOnlyFallbackActive: Boolean(input.stockOnly),
      stockOnlyManualPrice: input.stockOnly ? input.targetPrice : null,
    },
    priceSource: "PriceMaster demo",
    lastPriceSendAt: "2026-06-10T10:15:00.000Z",
    hasSnoozedLinks: false,
  } as Product;
}

export const demoWarehouseProducts: Product[] = [
  product({
    id: "demo-ozon-cedrat",
    marketplace: "ozon",
    offerId: "CEDRAT120",
    name: "Mancera Cedrat Boise 120 ml",
    brand: "Mancera",
    imageUrl: "https://images.unsplash.com/photo-1619994403073-2cec844b8e63?auto=format&fit=crop&w=420&q=80",
    currentPrice: 7290,
    targetPrice: 7390,
    stock: 34,
    supplierName: "Инна",
    supplierArticle: "CEDRAT120",
    supplierPrice: 43.5,
    finalPrice: 4350,
    changed: true,
  }),
  product({
    id: "demo-yandex-cedrat",
    marketplace: "yandex",
    offerId: "CEDRAT120",
    name: "Mancera Cedrat Boise 120 ml",
    brand: "Mancera",
    imageUrl: "https://images.unsplash.com/photo-1619994403073-2cec844b8e63?auto=format&fit=crop&w=420&q=80",
    currentPrice: 6990,
    targetPrice: 7090,
    stock: 31,
    supplierName: "Инна",
    supplierArticle: "CEDRAT120",
    supplierPrice: 43.5,
    finalPrice: 4350,
    changed: true,
  }),
  product({
    id: "demo-ozon-montale",
    marketplace: "ozon",
    offerId: "INTCAFE100",
    name: "Montale Intense Cafe 100 ml",
    brand: "Montale",
    imageUrl: "https://images.unsplash.com/photo-1595425970377-c9703cf48b6d?auto=format&fit=crop&w=420&q=80",
    currentPrice: 7490,
    targetPrice: 7590,
    stock: 27,
    supplierName: "Иванна",
    supplierArticle: "INTCAFE100",
    supplierPrice: 41.5,
    finalPrice: 4150,
  }),
  product({
    id: "demo-yandex-montale",
    marketplace: "yandex",
    offerId: "INTCAFE100",
    name: "Montale Intense Cafe 100 ml",
    brand: "Montale",
    imageUrl: "https://images.unsplash.com/photo-1595425970377-c9703cf48b6d?auto=format&fit=crop&w=420&q=80",
    currentPrice: 7190,
    targetPrice: 7290,
    stock: 24,
    supplierName: "Иванна",
    supplierArticle: "INTCAFE100",
    supplierPrice: 41.5,
    finalPrice: 4150,
  }),
  product({
    id: "demo-ozon-armaf",
    marketplace: "ozon",
    offerId: "CDNINT105",
    name: "Armaf Club de Nuit Intense 105 ml",
    brand: "Armaf",
    imageUrl: "https://images.unsplash.com/photo-1541643600914-78b084683601?auto=format&fit=crop&w=420&q=80",
    currentPrice: 5990,
    targetPrice: 5990,
    stock: 15,
    supplierName: "Наш склад",
    supplierArticle: "CDNINT105",
    supplierPrice: 32.5,
    finalPrice: 3250,
    stockOnly: true,
  }),
  product({
    id: "demo-yandex-armaf",
    marketplace: "yandex",
    offerId: "CDNINT105",
    name: "Armaf Club de Nuit Intense 105 ml",
    brand: "Armaf",
    imageUrl: "https://images.unsplash.com/photo-1541643600914-78b084683601?auto=format&fit=crop&w=420&q=80",
    currentPrice: 5690,
    targetPrice: 5690,
    stock: 15,
    supplierName: "Наш склад",
    supplierArticle: "CDNINT105",
    supplierPrice: 32.5,
    finalPrice: 3250,
    stockOnly: true,
  }),
  product({
    id: "demo-ozon-zarko",
    marketplace: "ozon",
    offerId: "MOLEC23400",
    name: "Zarkoperfume Molecule 234.100 ml",
    brand: "Zarkoperfume",
    imageUrl: "https://images.unsplash.com/photo-1592945403244-b3fbafd7f539?auto=format&fit=crop&w=420&q=80",
    currentPrice: 6890,
    targetPrice: 6590,
    stock: 8,
    supplierName: "Инна",
    supplierArticle: "MOLEC23400",
    supplierPrice: 39.8,
    finalPrice: 3980,
    changed: true,
  }),
  product({
    id: "demo-ozon-byredo",
    marketplace: "ozon",
    offerId: "BALDAFR100",
    name: "Byredo Bal d'Afrique 100 ml",
    brand: "Byredo",
    imageUrl: "https://images.unsplash.com/photo-1590736704728-f4730bb30770?auto=format&fit=crop&w=420&q=80",
    currentPrice: 0,
    targetPrice: 12990,
    stock: 0,
    supplierName: "Иванна",
    supplierArticle: "BALDAFR100",
    supplierPrice: 68.9,
    finalPrice: 6890,
    status: "out_of_stock",
  }),
  product({
    id: "demo-yandex-tom-ford",
    marketplace: "yandex",
    offerId: "OUDWOOD100",
    name: "Tom Ford Oud Wood 100 ml",
    brand: "Tom Ford",
    imageUrl: "https://images.unsplash.com/photo-1585386959984-a4155224a1ad?auto=format&fit=crop&w=420&q=80",
    currentPrice: 12490,
    targetPrice: 11990,
    stock: 5,
    supplierName: "Наш склад",
    supplierArticle: "OUDWOOD100",
    supplierPrice: 89.5,
    finalPrice: 8950,
    changed: true,
  }),
];

export function isDemoWarehouseProduct(product: Product | undefined): boolean {
  return Boolean(product?.id && String(product.id).startsWith("demo-"));
}
