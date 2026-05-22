import { z } from "zod";

export const LinkSchema = z.object({
  id: z.coerce.string().optional().default(""),
  article: z.coerce.string().optional().default(""),
  supplierArticle: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  rowId: z.coerce.string().optional().default(""),
  sourceRowId: z.coerce.string().optional().default(""),
  exactName: z.coerce.string().optional().default(""),
  matchType: z.coerce.string().optional().default(""),
  keyword: z.coerce.string().optional().default(""),
  priceCurrency: z.coerce.string().optional().default("USD"),
  pricingMode: z.coerce.string().optional().default("normal"),
  stockOnly: z.boolean().optional(),
  priceEligible: z.boolean().optional(),
  stockEligible: z.boolean().optional(),
  stockOnlyCount: z.number().optional().default(0),
  priceEligibleCount: z.number().optional().default(0),
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
  stopped: z.boolean().optional(),
  pricingMode: z.coerce.string().optional().default("normal"),
  stockOnly: z.boolean().optional(),
  impactProductCount: z.number().optional().default(0),
  stopReason: z.coerce.string().optional().nullable(),
}).passthrough();

export const SuppliersResponseSchema = z.object({
  suppliers: z.array(SupplierSchema).optional().default([]),
  supplierSync: z.record(z.string(), z.unknown()).optional().default({}),
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
  stockOnlyFallbackActive: z.boolean().optional().default(false),
  stockOnlyManualPriceMissing: z.boolean().optional().default(false),
  stockOnlyManualPrices: z.record(z.string(), z.unknown()).optional().default({}),
  stockOnlyAvailableSupplierCount: z.number().optional().default(0),
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

export const WarehouseBrandsSchema = z.object({
  brands: z.array(z.coerce.string()).optional().default([]),
  source: z.coerce.string().optional().default(""),
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

export const AiImageJobSchema = z.object({
  id: z.coerce.string().optional().default(""),
  jobId: z.coerce.string().optional().default(""),
  productId: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  batchId: z.coerce.string().optional().default(""),
  status: z.coerce.string().optional().default("queued"),
  progress: z.number().optional().default(0),
  variantIndex: z.number().optional().default(0),
  variantTotal: z.number().optional().default(5),
  draftIds: z.array(z.coerce.string()).optional().default([]),
  lastError: z.record(z.string(), z.unknown()).optional().nullable(),
  model: z.coerce.string().optional().default(""),
  endpoint: z.coerce.string().optional().default(""),
  presetId: z.coerce.string().optional().default(""),
  presetLabel: z.coerce.string().optional().default(""),
  sourceImageUrl: z.coerce.string().optional().default(""),
  startedAt: z.string().optional().nullable(),
  updatedAt: z.string().optional().nullable(),
  finishedAt: z.string().optional().nullable(),
}).passthrough();

export const AiImageJobResponseSchema = z.object({
  ok: z.boolean().optional(),
  jobId: z.coerce.string().optional().default(""),
  status: z.coerce.string().optional().default(""),
  productId: z.coerce.string().optional().default(""),
  batchId: z.coerce.string().optional().default(""),
  job: AiImageJobSchema.optional(),
  product: ProductSchema.optional().nullable(),
}).passthrough();

export const AiAssistantResponseSchema = z.object({
  ok: z.boolean().optional(),
  productId: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  marketplace: z.coerce.string().optional().default("yandex"),
  draft: z.record(z.string(), z.unknown()).optional().default({}),
  product: ProductSchema.optional().nullable(),
  before: z.record(z.string(), z.unknown()).optional().default({}),
  after: z.record(z.string(), z.unknown()).optional().default({}),
  reasons: z.array(z.coerce.string()).optional().default([]),
  checklist: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  photoPresets: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  provider: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

export const MutationProductResponseSchema = z.object({
  ok: z.boolean().optional(),
  product: ProductSchema.optional(),
  products: z.array(ProductSchema).optional().default([]),
  warehouse: z.unknown().optional(),
}).passthrough();

export const PriceMasterSearchRowSchema = z.object({
  id: z.coerce.string(),
  rowId: z.coerce.string().optional().default(""),
  article: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  keyword: z.coerce.string().optional().default(""),
  name: z.coerce.string().optional().default(""),
  price: z.number().optional().nullable(),
  originalPrice: z.number().optional().nullable(),
  currency: z.coerce.string().optional().default("USD"),
  priceCurrency: z.coerce.string().optional().default("USD"),
  available: z.boolean().optional().default(false),
  updatedAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const PriceMasterSearchSchema = z.object({
  ok: z.boolean().optional(),
  rows: z.array(PriceMasterSearchRowSchema).optional().default([]),
  total: z.number().optional().default(0),
}).passthrough();

export const OperationsSchema = z.object({
  ok: z.boolean().optional(),
  jobs: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  total: z.number().optional().default(0),
}).passthrough();

export const OperationCreateSchema = z.object({
  ok: z.boolean().optional(),
  job: z.record(z.string(), z.unknown()).optional(),
}).passthrough();

export const OperationDetailSchema = z.object({
  ok: z.boolean().optional(),
  job: z.record(z.string(), z.unknown()).optional(),
}).passthrough();

export const OzonUnarchiveQueueSchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  dailyLimit: z.number().optional().default(100),
  total: z.number().optional().default(0),
  due: z.number().optional().default(0),
  future: z.number().optional().default(0),
  availableToday: z.number().optional().default(0),
  nextRetryAt: z.coerce.string().optional().nullable(),
  targets: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const PricePreviewSchema = z.object({
  ok: z.boolean().optional(),
  dryRun: z.boolean().optional(),
  selected: z.number().optional().default(0),
  readyToSend: z.number().optional().default(0),
  stockReadyToSend: z.number().optional().default(0),
  skipped: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  ozonSent: z.number().optional().default(0),
  ozonFailed: z.number().optional().default(0),
  ozonSkipped: z.number().optional().default(0),
  yandexSent: z.number().optional().default(0),
  yandexFailed: z.number().optional().default(0),
  yandexSkipped: z.number().optional().default(0),
}).passthrough();

export const ProductRepairSchema = z.object({
  ok: z.boolean().optional(),
  productIds: z.array(z.coerce.string()).optional().default([]),
  linksSynced: z.number().optional().default(0),
  priceSent: z.number().optional().default(0),
  stockSent: z.number().optional().default(0),
  unarchiveStatus: z.coerce.string().optional().default(""),
  pending: z.boolean().optional().default(false),
  errors: z.array(z.unknown()).optional().default([]),
  nextRetryAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const SystemStatusSchema = z.object({
  ok: z.boolean().optional(),
  time: z.coerce.string().optional().default(""),
  health: z.record(z.string(), z.unknown()).optional().default({}),
  dailySync: z.record(z.string(), z.unknown()).optional().default({}),
  queues: z.record(z.string(), z.unknown()).optional().default({}),
  operations: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

export const SupplierCartRowSchema = z.object({
  key: z.coerce.string(),
  marketplace: z.coerce.string().optional().default(""),
  accountName: z.coerce.string().optional().default(""),
  orderId: z.coerce.string().optional().default(""),
  postingNumber: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  productName: z.coerce.string().optional().default(""),
  quantity: z.number().optional().default(1),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  offerRowId: z.coerce.string().optional().default(""),
  price: z.number().optional().default(0),
  priceCurrency: z.coerce.string().optional().default("USD"),
  ready: z.boolean().optional().default(false),
  alreadyCommitted: z.boolean().optional().default(false),
  skipReason: z.coerce.string().optional().default(""),
  requestDocId: z.coerce.string().optional().default(""),
  requestRowId: z.coerce.string().optional().default(""),
}).passthrough();

export const SupplierCartPreviewSchema = z.object({
  ok: z.boolean().optional(),
  from: z.coerce.string().optional().default(""),
  to: z.coerce.string().optional().default(""),
  total: z.number().optional().default(0),
  ready: z.number().optional().default(0),
  skipped: z.number().optional().default(0),
  alreadyCommitted: z.number().optional().default(0),
  rows: z.array(SupplierCartRowSchema).optional().default([]),
  warnings: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const SupplierCartCommitSchema = z.object({
  ok: z.boolean().optional(),
  inserted: z.number().optional().default(0),
  skipped: z.number().optional().default(0),
  docIds: z.array(z.union([z.string(), z.number()])).optional().default([]),
  rows: z.array(SupplierCartRowSchema).optional().default([]),
}).passthrough();

export const SupplierCartHistorySchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  totalProcessed: z.number().optional().default(0),
  history: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const SettingsResponseSchema = z.object({
  ok: z.boolean().optional(),
  settings: z.record(z.string(), z.unknown()).default({}),
  priceAffectingChanged: z.boolean().optional().default(false),
  priceRepriceQueued: z.boolean().optional().default(false),
  priceRepriceReason: z.coerce.string().optional().default(""),
  priceRepriceQueueError: z.coerce.string().optional().default(""),
}).passthrough();

export const UsersResponseSchema = z.object({
  ok: z.boolean().optional(),
  users: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const UserStatsRowSchema = z.object({
  username: z.coerce.string(),
  role: z.coerce.string().optional().default(""),
  active: z.boolean().optional().default(true),
  source: z.coerce.string().optional().default(""),
  deleted: z.boolean().optional().default(false),
  hardDeleted: z.boolean().optional().default(false),
  deletedAt: z.coerce.string().optional().nullable(),
  currentLinksCreated: z.number().optional().default(0),
  currentLinksUpdated: z.number().optional().default(0),
  actionsTotal: z.number().optional().default(0),
  linksAdded: z.number().optional().default(0),
  linksUpdated: z.number().optional().default(0),
  linksDeleted: z.number().optional().default(0),
  affectedProducts: z.number().optional().default(0),
  affectedOfferIds: z.number().optional().default(0),
  lastActionAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const UsersStatsResponseSchema = z.object({
  ok: z.boolean().optional(),
  period: z.coerce.string().optional().default("30d"),
  periodDays: z.number().optional().nullable(),
  from: z.coerce.string().optional().nullable(),
  to: z.coerce.string().optional().nullable(),
  summary: z.record(z.string(), z.unknown()).optional().default({}),
  includeInactive: z.boolean().optional().default(true),
  includeDeleted: z.boolean().optional().default(true),
  selectedUsers: z.array(z.coerce.string()).optional().default([]),
  users: z.array(UserStatsRowSchema).optional().default([]),
}).passthrough();

export const AuditLogSchema = z.object({
  audit: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  total: z.number().optional().default(0),
}).passthrough();

export const PriceRetryQueueSchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  total: z.number().optional().default(0),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const PriceHistorySchema = z.object({
  ok: z.boolean().optional(),
  total: z.number().optional().default(0),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const SyncStatusSchema = z.record(z.string(), z.unknown());

export const NoSupplierSchema = z.object({
  createdAt: z.coerce.string().optional().nullable(),
  total: z.number().optional().default(0),
  withoutSupplier: z.number().optional().default(0),
  alerts: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const AiDraftsSchema = z.object({
  ok: z.boolean().optional(),
  drafts: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  total: z.number().optional().default(0),
}).passthrough();

export const YandexQualityCandidatesSchema = z.object({
  ok: z.boolean().optional(),
  cached: z.boolean().optional(),
  threshold: z.number().optional(),
  checked: z.number().optional().default(0),
  qualityLoaded: z.number().optional().default(0),
  total: z.number().optional().default(0),
  errors: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  products: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export type Product = z.infer<typeof ProductSchema>;
export type ProductLink = z.infer<typeof LinkSchema>;
export type WarehousePage = z.infer<typeof WarehousePageSchema>;
export type GroupDetail = z.infer<typeof GroupDetailSchema>;
export type AiImage = z.infer<typeof AiImageSchema>;
export type PriceMasterSearchRow = z.infer<typeof PriceMasterSearchRowSchema>;

export type Filters = {
  q: string;
  marketplace: string;
  linked: string;
  state: string;
  brand: string;
  autoOnly: boolean;
  page: number;
};
