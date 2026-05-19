import { z } from "zod";

export const LinkSchema = z.object({
  id: z.coerce.string().optional().default(""),
  article: z.coerce.string().optional().default(""),
  supplierArticle: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  keyword: z.coerce.string().optional().default(""),
  priceCurrency: z.coerce.string().optional().default("USD"),
  updatedBy: z.coerce.string().optional().nullable(),
  createdBy: z.coerce.string().optional().nullable(),
  updatedAt: z.coerce.string().optional().nullable(),
  createdAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const SupplierSchema = z.object({
  id: z.coerce.string().optional(),
  name: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().nullable(),
  active: z.boolean().optional(),
  stopReason: z.coerce.string().optional().nullable(),
}).passthrough();

export const AiImageSchema = z.object({
  id: z.coerce.string(),
  status: z.coerce.string().optional().default("pending"),
  sourceImageUrl: z.coerce.string().optional().default(""),
  resultUrl: z.coerce.string().optional().default(""),
  prompt: z.coerce.string().optional().default(""),
  batchId: z.coerce.string().optional().nullable(),
  variantIndex: z.number().optional().nullable(),
  variantTotal: z.number().optional().nullable(),
  createdAt: z.string().optional().nullable(),
}).passthrough();

export const ProductSchema = z.object({
  id: z.coerce.string(),
  marketplace: z.coerce.string().optional().default(""),
  target: z.coerce.string().optional().nullable(),
  offerId: z.coerce.string().optional().default(""),
  sku: z.coerce.string().optional().nullable(),
  barcode: z.coerce.string().optional().nullable(),
  productId: z.coerce.string().optional().nullable(),
  name: z.coerce.string().optional().default(""),
  brand: z.coerce.string().optional().nullable(),
  imageUrl: z.coerce.string().optional().nullable(),
  images: z.array(z.string()).optional().default([]),
  currentPrice: z.number().optional().nullable(),
  newPrice: z.number().optional().nullable(),
  targetPrice: z.number().optional().nullable(),
  oldPrice: z.number().optional().nullable(),
  targetStock: z.number().optional().nullable(),
  stock: z.number().optional().nullable(),
  autoPriceEnabled: z.boolean().optional(),
  archived: z.boolean().optional(),
  status: z.string().optional().nullable(),
  updatedAt: z.string().optional().nullable(),
  raw: z.unknown().optional(),
  ozon: z.unknown().optional(),
  yandex: z.unknown().optional(),
  marketplaceState: z.record(z.string(), z.unknown()).optional().default({}),
  links: z.array(LinkSchema).optional().default([]),
  suppliers: z.array(SupplierSchema).optional().default([]),
  selectedSupplier: z.unknown().optional().nullable(),
  noSupplierAutomation: z.record(z.string(), z.unknown()).optional().default({}),
  aiImages: z.array(AiImageSchema).optional().default([]),
}).passthrough();

export const WarehousePageSchema = z.object({
  page: z.number().optional().default(1),
  pageSize: z.number().optional().default(60),
  total: z.number().optional().default(0),
  totalAll: z.number().optional().default(0),
  hasMore: z.boolean().optional().default(false),
  ready: z.number().optional().default(0),
  changed: z.number().optional().default(0),
  withoutSupplier: z.number().optional().default(0),
  linkedArchived: z.number().optional().default(0),
  ozonArchived: z.number().optional().default(0),
  ozonInactive: z.number().optional().default(0),
  usdRate: z.number().optional().nullable(),
  updatedAt: z.string().optional().nullable(),
  sourceError: z.string().optional().default(""),
  items: z.array(ProductSchema).optional().default([]),
}).passthrough();

export const GroupDetailSchema = z.object({
  products: z.array(ProductSchema).default([]),
  suppliers: z.array(SupplierSchema).optional().default([]),
}).passthrough();

export const DiagnosticsSchema = z.record(z.string(), z.unknown());

export const AiImagesResponseSchema = z.object({
  ok: z.boolean().optional(),
  draft: AiImageSchema.optional(),
  drafts: z.array(AiImageSchema).optional().default([]),
  batchId: z.string().optional(),
  product: ProductSchema.optional(),
}).passthrough();

export const MutationProductResponseSchema = z.object({
  ok: z.boolean().optional(),
  product: ProductSchema.optional(),
  products: z.array(ProductSchema).optional().default([]),
  warehouse: z.unknown().optional(),
}).passthrough();

export type Product = z.infer<typeof ProductSchema>;
export type ProductLink = z.infer<typeof LinkSchema>;
export type WarehousePage = z.infer<typeof WarehousePageSchema>;
export type GroupDetail = z.infer<typeof GroupDetailSchema>;
export type AiImage = z.infer<typeof AiImageSchema>;

export type Filters = {
  q: string;
  marketplace: string;
  linked: string;
  state: string;
  brand: string;
  autoOnly: boolean;
  page: number;
};
