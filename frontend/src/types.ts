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
  snooze: z.object({ snoozedAt: z.string().nullable().optional(), snoozedUntil: z.string(), days: z.number() }).nullable().optional(),
}).passthrough();

export const SupplierSchema = z.object({
  id: z.coerce.string().optional(),
  name: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().nullable(),
  active: z.boolean().optional(),
  stopped: z.boolean().optional(),
  pricingMode: z.coerce.string().optional().default("normal"),
  stockOnly: z.boolean().optional(),
  trustFactor: z.number().optional().default(100),
  orderCutoffTime: z.coerce.string().optional().default(""),
  reseller: z.boolean().optional().default(false),
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
  supplierAlternatives: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  selectedSupplier: z.unknown().optional().nullable(),
  stockOnlyFallbackActive: z.boolean().optional().default(false),
  stockOnlyManualPriceMissing: z.boolean().optional().default(false),
  stockOnlyManualPrices: z.record(z.string(), z.unknown()).optional().default({}),
  stockOnlyAvailableSupplierCount: z.number().optional().default(0),
  noSupplierAutomation: z.record(z.string(), z.unknown()).optional().default({}),
  hasSnoozedLinks: z.boolean().optional().default(false),
  aiImages: z.array(AiImageSchema).optional().default([]),
}).passthrough();

export const ProductGroupPageItemSchema = z.object({
  groupKey: z.coerce.string(),
  offerId: z.coerce.string().optional().default(""),
  manualGroupId: z.coerce.string().optional().default(""),
  name: z.coerce.string().optional().default(""),
  brand: z.coerce.string().optional().nullable(),
  imageUrl: z.coerce.string().optional().nullable(),
  marketplaces: z.array(z.coerce.string()).optional().default([]),
  products: z.array(ProductSchema).optional().default([]),
  links: z.array(LinkSchema).optional().default([]),
  primary: ProductSchema.optional().nullable(),
  statusSummary: z.object({
    total: z.number().optional().default(0),
    linked: z.number().optional().default(0),
    archived: z.number().optional().default(0),
    ready: z.number().optional().default(0),
    changed: z.number().optional().default(0),
    withoutSupplier: z.number().optional().default(0),
    marketplaces: z.array(z.coerce.string()).optional().default([]),
  }).passthrough().optional(),
}).passthrough();

export const WarehousePageItemSchema = z.union([ProductGroupPageItemSchema, ProductSchema]);

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
  grouped: z.boolean().optional().default(false),
  rowTotal: z.number().optional().default(0),
  groupTotal: z.number().optional().default(0),
  items: z.array(WarehousePageItemSchema).optional().default([]),
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
  isTester: z.boolean().optional().default(false),
  isOtlivant: z.boolean().optional().default(false),
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
  dailyLimit: z.number().nullable().optional().default(null),
  total: z.number().optional().default(0),
  due: z.number().optional().default(0),
  future: z.number().optional().default(0),
  verificationPending: z.number().optional().default(0),
  warningCounts: z.record(z.string(), z.number()).optional().default({}),
  availableToday: z.number().nullable().optional().default(null),
  nextRetryAt: z.coerce.string().optional().nullable(),
  autoEnabled: z.boolean().optional().default(true),
  autoRunning: z.boolean().optional().default(false),
  lastAutoRunAt: z.coerce.string().optional().nullable(),
  nextAutoRunAt: z.coerce.string().optional().nullable(),
  lastAutoResult: z.record(z.string(), z.unknown()).optional().nullable(),
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

export const SalesAutomationSummarySchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  autoEnabled: z.boolean().optional().default(true),
  total: z.number().optional().default(0),
  updatedAt: z.coerce.string().optional().nullable(),
  retryTotal: z.number().optional().default(0),
  ozonUnarchiveQueued: z.number().optional().default(0),
  reasons: z.record(z.string(), z.number()).optional().default({}),
  statuses: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const SalesAutomationItemsSchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  total: z.number().optional().default(0),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const ProblemProductsSchema = z.object({
  ok: z.boolean().optional(),
  total: z.number().optional().default(0),
  summary: z.record(z.string(), z.number()).optional().default({}),
  items: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const BrandIndexStatusSchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  indexed: z.number().optional().default(0),
  products: z.number().optional().default(0),
  ready: z.boolean().optional().default(false),
  stale: z.boolean().optional().default(false),
}).passthrough();

export const ProductRepairSchema = z.object({
  ok: z.boolean().optional(),
  accepted: z.boolean().optional().default(false),
  repaired: z.number().optional().default(0),
  failed: z.number().optional().default(0),
  job: z.record(z.string(), z.unknown()).optional().default({}),
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
  postgresTables: z.record(z.string(), z.unknown()).optional().nullable(),
  redis: z.record(z.string(), z.unknown()).optional().nullable(),
  bullmq: z.record(z.string(), z.unknown()).optional().nullable(),
  jobs: z.record(z.string(), z.unknown()).optional().default({}),
  slowEndpoints: z.record(z.string(), z.unknown()).optional().default({}),
  memory: z.record(z.string(), z.unknown()).optional().nullable(),
  runtime: z.record(z.string(), z.unknown()).optional().nullable(),
  lastPriceRun: z.record(z.string(), z.unknown()).optional().nullable(),
  lastOzonVerification: z.coerce.string().optional().nullable(),
  lastAutoarchiveRun: z.record(z.string(), z.unknown()).optional().nullable(),
  stateWarnings: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  dailySync: z.record(z.string(), z.unknown()).optional().default({}),
  queues: z.record(z.string(), z.unknown()).optional().default({}),
  operations: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

export const FinanceSummarySchema = z.object({
  ok: z.boolean().optional(),
  period: z.coerce.string().optional().default("30d"),
  source: z.coerce.string().optional().default(""),
  summary: z.record(z.string(), z.unknown()).optional().default({}),
  updatedAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const DashboardSummarySchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  salesToday: z.object({
    orders: z.number().optional().default(0),
    income: z.number().optional().default(0),
    profit: z.number().optional().default(0),
  }).optional().default({ orders: 0, income: 0, profit: 0 }),
  salesWeek: z.object({
    orders: z.number().optional().default(0),
    income: z.number().optional().default(0),
    profit: z.number().optional().default(0),
  }).optional().default({ orders: 0, income: 0, profit: 0 }),
  topSuppliers: z.array(z.object({
    supplierName: z.coerce.string().optional().default(""),
    orders: z.number().optional().default(0),
    income: z.number().optional().default(0),
    profit: z.number().optional().default(0),
  })).optional().default([]),
  priceQueue: z.number().optional().default(0),
  archiveBacklog: z.object({
    yandex: z.number().optional().default(0),
    ozon: z.number().optional().default(0),
    ozonDue: z.number().optional().default(0),
  }).optional().default({ yandex: 0, ozon: 0, ozonDue: 0 }),
  notifications: z.object({
    unread: z.number().optional().default(0),
    byType: z.record(z.string(), z.number()).optional().default({}),
  }).optional().default({ unread: 0, byType: {} }),
  priceHealth: z.object({
    stalePriceLinked: z.number().optional().default(0),
    staleHours: z.number().optional().default(1),
    alertThreshold: z.number().optional().default(50),
    alert: z.boolean().optional().default(false),
    oldestPriceJobAgeMs: z.number().optional().default(0),
    oldestPriceJobAlertThresholdMs: z.number().optional().default(10 * 60_000),
    oldestPriceJobAlert: z.boolean().optional().default(false),
  }).optional().default({
    stalePriceLinked: 0,
    staleHours: 1,
    alertThreshold: 50,
    alert: false,
    oldestPriceJobAgeMs: 0,
    oldestPriceJobAlertThresholdMs: 10 * 60_000,
    oldestPriceJobAlert: false,
  }),
}).passthrough();

export const LiveStatusSchema = z.object({
  ok: z.boolean().optional(),
  now: z.coerce.string().optional().nullable(),
  warehouse: z.object({
    updatedAt: z.coerce.string().optional().nullable(),
    createdAt: z.coerce.string().optional().nullable(),
    products: z.number().optional().default(0),
    suppliers: z.number().optional().default(0),
    source: z.coerce.string().optional().nullable(),
  }).optional().default({ products: 0, suppliers: 0 }),
  priceMaster: z.object({
    syncId: z.coerce.string().optional().nullable(),
    updatedAt: z.coerce.string().optional().nullable(),
    items: z.number().optional().default(0),
    changes: z.number().optional().default(0),
    error: z.coerce.string().optional().nullable(),
  }).optional().default({ items: 0, changes: 0 }),
  dailySync: z.object({
    updatedAt: z.coerce.string().optional().nullable(),
    status: z.coerce.string().optional().default("idle"),
    running: z.boolean().optional().default(false),
    lastRunAt: z.coerce.string().optional().nullable(),
    nextRunAt: z.coerce.string().optional().nullable(),
    error: z.coerce.string().optional().nullable(),
  }).optional().default({ status: "idle", running: false }),
  autoSync: z.object({
    running: z.boolean().optional().default(false),
    nextRunAt: z.coerce.string().optional().nullable(),
  }).optional().default({ running: false }),
  marketplaceMaintenance: z.object({
    enabled: z.boolean().optional().default(false),
    running: z.boolean().optional().default(false),
    everyHours: z.number().optional().nullable(),
    nextRunAt: z.coerce.string().optional().nullable(),
  }).optional().default({ enabled: false, running: false }),
  queue: z.object({
    enabled: z.boolean().optional().default(false),
    degraded: z.boolean().optional().default(false),
    producerReady: z.boolean().optional().default(false),
    consumerReady: z.boolean().optional().default(false),
    counts: z.record(z.string(), z.number()).optional().nullable(),
    error: z.coerce.string().optional().nullable(),
  }).optional().default({ enabled: false, degraded: false, producerReady: false, consumerReady: false }),
}).passthrough();

export const FinanceOrderSchema = z.object({
  id: z.coerce.string(),
  marketplace: z.coerce.string().optional().default(""),
  target: z.coerce.string().optional().default(""),
  orderId: z.coerce.string().optional().default(""),
  postingNumber: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  productName: z.coerce.string().optional().default(""),
  quantity: z.number().optional().default(1),
  saleAmount: z.number().nullable().optional(),
  payoutAmount: z.number().nullable().optional(),
  purchaseCost: z.number().nullable().optional(),
  feesAmount: z.number().nullable().optional(),
  taxAmount: z.number().nullable().optional(),
  penaltiesAmount: z.number().nullable().optional(),
  refundsAmount: z.number().nullable().optional(),
  profitAmount: z.number().nullable().optional(),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  source: z.coerce.string().optional().default("manual"),
  status: z.coerce.string().optional().default("open"),
  createdAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const FinanceExpenseSchema = z.object({
  id: z.coerce.string(),
  type: z.coerce.string().optional().default("manual_purchase"),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  productName: z.coerce.string().optional().default(""),
  quantity: z.number().optional().default(1),
  amount: z.number().optional().default(0),
  currency: z.coerce.string().optional().default("RUB"),
  note: z.coerce.string().optional().default(""),
  source: z.coerce.string().optional().default("manual"),
  status: z.coerce.string().optional().default("confirmed"),
  spentAt: z.coerce.string().optional().nullable(),
}).passthrough();

export const FinanceOrdersSchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  total: z.number().optional().default(0),
  orders: z.array(FinanceOrderSchema).optional().default([]),
}).passthrough();

export const FinanceExpensesSchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  total: z.number().optional().default(0),
  expenses: z.array(FinanceExpenseSchema).optional().default([]),
}).passthrough();

export const FinanceExpenseCreateSchema = z.object({
  ok: z.boolean().optional(),
  expense: FinanceExpenseSchema.optional(),
}).passthrough();

export const SupplierLedgerEntrySchema = z.object({
  id: z.coerce.string(),
  sourceKey: z.coerce.string().optional().default(""),
  entryType: z.coerce.string().optional().default("adjustment"),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  amount: z.number().optional().default(0),
  currency: z.coerce.string().optional().default("RUB"),
  pickingKey: z.coerce.string().optional().default(""),
  financeOrderId: z.coerce.string().optional().default(""),
  orderId: z.coerce.string().optional().default(""),
  postingNumber: z.coerce.string().optional().default(""),
  offerId: z.coerce.string().optional().default(""),
  productName: z.coerce.string().optional().default(""),
  quantity: z.number().optional().default(1),
  note: z.coerce.string().optional().default(""),
  status: z.coerce.string().optional().default("active"),
  occurredAt: z.coerce.string().optional().nullable(),
  voidedAt: z.coerce.string().optional().nullable(),
  createdBy: z.coerce.string().optional().default(""),
}).passthrough();

export const SupplierLedgerSummarySchema = z.object({
  balance: z.number().optional().default(0),
  debtTotal: z.number().optional().default(0),
  paidTotal: z.number().optional().default(0),
  entries: z.number().optional().default(0),
  lastPaymentAt: z.coerce.string().optional().nullable(),
  lastDebtAt: z.coerce.string().optional().nullable(),
}).passthrough();

const emptySupplierLedgerSummary = {
  balance: 0,
  debtTotal: 0,
  paidTotal: 0,
  entries: 0,
  lastPaymentAt: null,
  lastDebtAt: null,
};

export const SupplierLedgerResponseSchema = z.object({
  ok: z.boolean().optional(),
  source: z.coerce.string().optional().default(""),
  total: z.number().optional().default(0),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  summary: SupplierLedgerSummarySchema.optional().default(emptySupplierLedgerSummary),
  entries: z.array(SupplierLedgerEntrySchema).optional().default([]),
}).passthrough();

export const SupplierLedgerPaymentSchema = z.object({
  ok: z.boolean().optional(),
  entry: SupplierLedgerEntrySchema.optional(),
  summary: SupplierLedgerSummarySchema.optional().default(emptySupplierLedgerSummary),
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
  trustFactor: z.number().optional().default(100),
  orderCutoffTime: z.coerce.string().optional().default(""),
  reseller: z.boolean().optional().default(false),
  supplierScore: z.number().optional().default(0),
  ready: z.boolean().optional().default(false),
  alreadyCommitted: z.boolean().optional().default(false),
  stockOnlyFallback: z.boolean().optional().default(false),
  skipReason: z.coerce.string().optional().default(""),
  requestDocId: z.coerce.string().optional().default(""),
  requestRowId: z.coerce.string().optional().default(""),
}).passthrough();

export const SupplierCartPreviewSchema = z.object({
  ok: z.boolean().optional(),
  draftId: z.coerce.string().optional().default(""),
  generatedAt: z.coerce.string().optional().nullable(),
  generatedBy: z.coerce.string().optional().default(""),
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
  pickingCreated: z.number().optional().default(0),
  docIds: z.array(z.union([z.string(), z.number()])).optional().default([]),
  verifiedInPriceMaster: z.boolean().optional().default(false),
  verifiedRows: z.number().optional().default(0),
  priceMasterDb: z.coerce.string().optional().default(""),
  rows: z.array(SupplierCartRowSchema).optional().default([]),
}).passthrough();

export const SupplierCartHistorySchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  totalProcessed: z.number().optional().default(0),
  history: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

export const SupplierPickingRowSchema = z.object({
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
  trustFactor: z.number().optional().default(100),
  orderCutoffTime: z.coerce.string().optional().default(""),
  reseller: z.boolean().optional().default(false),
  supplierScore: z.number().optional().default(0),
  requestDocId: z.coerce.string().optional().default(""),
  requestRowId: z.coerce.string().optional().default(""),
  status: z.coerce.string().optional().default("open"),
  createdAt: z.coerce.string().optional().default(""),
  pickedBy: z.coerce.string().optional().default(""),
  pickedAt: z.coerce.string().optional().nullable(),
  missingBy: z.coerce.string().optional().default(""),
  missingAt: z.coerce.string().optional().nullable(),
  missingReason: z.coerce.string().optional().default(""),
  nextRetryAt: z.coerce.string().optional().nullable(),
  replacementFor: z.coerce.string().optional().default(""),
  replacementKey: z.coerce.string().optional().default(""),
}).passthrough();

export const SupplierPickingListSchema = z.object({
  ok: z.boolean().optional(),
  updatedAt: z.coerce.string().optional().nullable(),
  total: z.number().optional().default(0),
  rows: z.array(SupplierPickingRowSchema).optional().default([]),
  suppliers: z.array(z.string()).optional().default([]),
  supplierLedger: z.record(z.string(), SupplierLedgerSummarySchema).optional().default({}),
  summary: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

export const SupplierCartCancelSchema = z.object({
  ok: z.boolean().optional(),
  cancelled: z.boolean().optional().default(false),
  key: z.coerce.string().optional().default(""),
  sourceCartKey: z.coerce.string().optional().default(""),
  priceMaster: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

export const SupplierPickingInvoiceSchema = z.object({
  ok: z.boolean().optional(),
  period: z.coerce.string().optional().default("30d"),
  total: z.number().optional().default(0),
  groups: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  rows: z.array(SupplierPickingRowSchema).optional().default([]),
}).passthrough();

export const SupplierPickingUpdateSchema = z.object({
  ok: z.boolean().optional(),
  row: SupplierPickingRowSchema,
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

export const ConsignmentItemSchema = z.object({
  id: z.coerce.string(),
  name: z.coerce.string().optional().default(""),
  article: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  purchasePrice: z.number().optional().default(0),
  salePrice: z.number().optional().default(0),
  quantity: z.number().optional().default(0),
  note: z.coerce.string().optional().default(""),
  archived: z.boolean().optional().default(false),
  createdAt: z.string().optional().nullable(),
  updatedAt: z.string().optional().nullable(),
}).passthrough();

export const ConsignmentOperationSchema = z.object({
  id: z.coerce.string(),
  itemId: z.coerce.string().optional().nullable(),
  itemName: z.coerce.string().optional().default(""),
  type: z.coerce.string().optional().default(""),
  quantity: z.number().optional().default(0),
  unitPurchase: z.number().optional().default(0),
  unitSale: z.number().optional().default(0),
  balanceDelta: z.number().optional().default(0),
  sponsorDelta: z.number().optional().default(0),
  myDelta: z.number().optional().default(0),
  note: z.coerce.string().optional().default(""),
  status: z.coerce.string().optional().default("active"),
  relatedOperationId: z.coerce.string().optional().nullable(),
  createdBy: z.coerce.string().optional().default(""),
  createdAt: z.string().optional().nullable(),
}).passthrough();

export const ConsignmentSummarySchema = z.object({
  ok: z.boolean().optional(),
  summary: z.object({
    items: z.number().optional().default(0),
    stockQuantity: z.number().optional().default(0),
    capitalization: z.number().optional().default(0),
    stockSaleValue: z.number().optional().default(0),
    balance: z.number().optional().default(0),
    sponsorProfit: z.number().optional().default(0),
    myProfit: z.number().optional().default(0),
    soldQuantity: z.number().optional().default(0),
    returnedQuantity: z.number().optional().default(0),
    writeoffQuantity: z.number().optional().default(0),
    salesRevenue: z.number().optional().default(0),
    profitTotal: z.number().optional().default(0),
    purchasesFromBalance: z.number().optional().default(0),
    sponsorPayouts: z.number().optional().default(0),
    sponsorProfitPaidOut: z.number().optional().default(0),
    myProfitPaidOut: z.number().optional().default(0),
  }).passthrough().optional(),
}).passthrough();

export const ConsignmentItemsSchema = z.object({
  ok: z.boolean().optional(),
  items: z.array(ConsignmentItemSchema).optional().default([]),
}).passthrough();

export const ConsignmentOperationsSchema = z.object({
  ok: z.boolean().optional(),
  operations: z.array(ConsignmentOperationSchema).optional().default([]),
}).passthrough();

export const ConsignmentMutationSchema = z.object({
  ok: z.boolean().optional(),
  item: ConsignmentItemSchema.optional().nullable(),
  operation: ConsignmentOperationSchema.optional().nullable(),
}).passthrough();

export const ConsignmentSuppliersSchema = z.object({
  ok: z.boolean().optional(),
  suppliers: z.array(z.coerce.string()).optional().default([]),
}).passthrough();

export const ConsignmentPmSearchSchema = z.object({
  ok: z.boolean().optional(),
  items: z.array(z.object({
    article: z.coerce.string().optional().default(""),
    name: z.coerce.string().optional().default(""),
    supplierName: z.coerce.string().optional().default(""),
    partnerId: z.coerce.string().optional().default(""),
    price: z.number().optional().nullable(),
    currency: z.coerce.string().optional().default("USD"),
    priceRub: z.number().optional().nullable(),
  }).passthrough()).optional().default([]),
}).passthrough();

export const SupplierAlternativeOptionSchema = z.object({
  partnerId: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  rowId: z.coerce.string().optional().default(""),
  price: z.number().optional().default(0),
  originalPrice: z.number().optional().default(0),
  priceCurrency: z.coerce.string().optional().default("USD"),
  available: z.boolean().optional().default(true),
  trustFactor: z.number().optional().default(100),
  orderCutoffTime: z.coerce.string().optional().default(""),
  reseller: z.boolean().optional().default(false),
  stockOnly: z.boolean().optional().default(false),
  blocked: z.boolean().optional().default(false),
  cutoffPassed: z.boolean().optional().default(false),
  score: z.number().optional().default(0),
  orderable: z.boolean().optional().default(false),
}).passthrough();

export const SupplierAlternativesSchema = z.object({
  ok: z.boolean().optional(),
  offerId: z.coerce.string().optional().default(""),
  skipReason: z.coerce.string().optional().default(""),
  options: z.array(SupplierAlternativeOptionSchema).optional().default([]),
}).passthrough();

export const SupplierCartOverrideSchema = z.object({
  ok: z.boolean().optional(),
  row: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

export const SupplierReplaceResponseSchema = z.object({
  ok: z.boolean().optional(),
  oldKey: z.coerce.string().optional().default(""),
  supplierName: z.coerce.string().optional().default(""),
  inserted: z.number().optional().default(0),
  docIds: z.array(z.union([z.string(), z.number()])).optional().default([]),
  verifiedInPriceMaster: z.boolean().optional().default(false),
  row: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

export const ConsignmentMpSyncSchema = z.object({
  status: z.coerce.string().optional().default(""),
  matched: z.number().optional().default(0),
  created: z.number().optional().default(0),
  noStock: z.number().optional().default(0),
  alreadyBooked: z.number().optional().default(0),
  errors: z.number().optional().default(0),
}).passthrough();

export type ConsignmentItem = z.infer<typeof ConsignmentItemSchema>;
export type ConsignmentOperation = z.infer<typeof ConsignmentOperationSchema>;

export type Product = z.infer<typeof ProductSchema>;
export type ProductLink = z.infer<typeof LinkSchema>;
export type ProductGroupPageItem = z.infer<typeof ProductGroupPageItemSchema>;
export type WarehousePageItem = z.infer<typeof WarehousePageItemSchema>;
export type WarehousePage = z.infer<typeof WarehousePageSchema>;

export function isProductGroupPageItem(item: WarehousePageItem): item is ProductGroupPageItem {
  return typeof (item as ProductGroupPageItem).groupKey === "string" && Boolean((item as ProductGroupPageItem).groupKey);
}

export function isProductPageItem(item: WarehousePageItem): item is Product {
  return typeof (item as Product).id === "string" && Boolean((item as Product).id);
}
export type GroupDetail = z.infer<typeof GroupDetailSchema>;
export type AiImage = z.infer<typeof AiImageSchema>;
export type PriceMasterSearchRow = z.infer<typeof PriceMasterSearchRowSchema>;

export type Filters = {
  q: string;
  marketplace: string;
  linked: string;
  state: string;
  brand: string;
  sort: string;
  autoOnly: boolean;
  page: number;
};
