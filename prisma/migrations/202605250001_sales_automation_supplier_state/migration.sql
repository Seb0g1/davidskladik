-- Sales automation status per marketplace SKU.
CREATE TABLE IF NOT EXISTS "sales_automation_sku_states" (
  "id" TEXT NOT NULL,
  "product_id" TEXT,
  "marketplace" "Marketplace" NOT NULL,
  "target" TEXT,
  "offer_id" TEXT NOT NULL,
  "current_price" INTEGER,
  "target_price" INTEGER,
  "target_stock" INTEGER,
  "price_status" "QueueStatus" NOT NULL DEFAULT 'pending',
  "stock_status" "QueueStatus" NOT NULL DEFAULT 'pending',
  "unarchive_status" "QueueStatus" NOT NULL DEFAULT 'pending',
  "reason" TEXT NOT NULL DEFAULT 'ok',
  "last_calculated_at" TIMESTAMP(3),
  "last_price_sent_at" TIMESTAMP(3),
  "last_stock_sent_at" TIMESTAMP(3),
  "last_error" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "sales_automation_sku_states_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "sales_automation_sku_states_marketplace_target_offer_id_key"
  ON "sales_automation_sku_states"("marketplace", "target", "offer_id");
CREATE INDEX IF NOT EXISTS "sales_automation_sku_states_product_id_idx" ON "sales_automation_sku_states"("product_id");
CREATE INDEX IF NOT EXISTS "sales_automation_sku_states_marketplace_reason_idx" ON "sales_automation_sku_states"("marketplace", "reason");
CREATE INDEX IF NOT EXISTS "sales_automation_sku_states_price_status_stock_status_idx" ON "sales_automation_sku_states"("price_status", "stock_status");
CREATE INDEX IF NOT EXISTS "sales_automation_sku_states_updated_at_idx" ON "sales_automation_sku_states"("updated_at");

ALTER TABLE "sales_automation_sku_states"
  ADD CONSTRAINT "sales_automation_sku_states_product_id_fkey"
  FOREIGN KEY ("product_id") REFERENCES "warehouse_products"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- Persistent Ozon autoarchive queue.
CREATE TABLE IF NOT EXISTS "ozon_unarchive_queue" (
  "id" TEXT NOT NULL,
  "queue_key" TEXT NOT NULL,
  "product_id" TEXT,
  "offer_id" TEXT NOT NULL,
  "target" TEXT,
  "status" "QueueStatus" NOT NULL DEFAULT 'pending',
  "queued_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "next_retry_at" TIMESTAMP(3),
  "last_attempt_at" TIMESTAMP(3),
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "warning" TEXT,
  "error" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "ozon_unarchive_queue_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "ozon_unarchive_queue_queue_key_key" ON "ozon_unarchive_queue"("queue_key");
CREATE INDEX IF NOT EXISTS "ozon_unarchive_queue_status_next_retry_at_idx" ON "ozon_unarchive_queue"("status", "next_retry_at");
CREATE INDEX IF NOT EXISTS "ozon_unarchive_queue_target_idx" ON "ozon_unarchive_queue"("target");
CREATE INDEX IF NOT EXISTS "ozon_unarchive_queue_offer_id_idx" ON "ozon_unarchive_queue"("offer_id");

-- Supplier cart drafts.
CREATE TABLE IF NOT EXISTS "supplier_cart_drafts" (
  "id" TEXT NOT NULL,
  "generated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "generated_by" TEXT,
  "marketplace" TEXT NOT NULL DEFAULT 'all',
  "from" TIMESTAMP(3),
  "to" TIMESTAMP(3),
  "summary" JSONB,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "params" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "supplier_cart_drafts_pkey" PRIMARY KEY ("id")
);
CREATE INDEX IF NOT EXISTS "supplier_cart_drafts_active_generated_at_idx" ON "supplier_cart_drafts"("active", "generated_at");

CREATE TABLE IF NOT EXISTS "supplier_cart_draft_rows" (
  "id" TEXT NOT NULL,
  "draft_id" TEXT NOT NULL,
  "cart_key" TEXT NOT NULL,
  "marketplace" TEXT,
  "account_name" TEXT,
  "order_id" TEXT,
  "posting_number" TEXT,
  "offer_id" TEXT,
  "product_name" TEXT,
  "quantity" INTEGER NOT NULL DEFAULT 1,
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "offer_row_id" TEXT,
  "price" DECIMAL(14,4),
  "price_currency" TEXT,
  "supplier_score" DECIMAL(14,4),
  "ready" BOOLEAN NOT NULL DEFAULT false,
  "already_committed" BOOLEAN NOT NULL DEFAULT false,
  "skip_reason" TEXT,
  "request_doc_id" TEXT,
  "request_row_id" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "supplier_cart_draft_rows_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "supplier_cart_draft_rows_draft_id_cart_key_key" ON "supplier_cart_draft_rows"("draft_id", "cart_key");
CREATE INDEX IF NOT EXISTS "supplier_cart_draft_rows_cart_key_idx" ON "supplier_cart_draft_rows"("cart_key");
CREATE INDEX IF NOT EXISTS "supplier_cart_draft_rows_offer_id_idx" ON "supplier_cart_draft_rows"("offer_id");
CREATE INDEX IF NOT EXISTS "supplier_cart_draft_rows_ready_idx" ON "supplier_cart_draft_rows"("ready");
ALTER TABLE "supplier_cart_draft_rows"
  ADD CONSTRAINT "supplier_cart_draft_rows_draft_id_fkey"
  FOREIGN KEY ("draft_id") REFERENCES "supplier_cart_drafts"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- Supplier picking and SKU supplier blocks.
CREATE TABLE IF NOT EXISTS "supplier_picking_rows" (
  "id" TEXT NOT NULL,
  "picking_key" TEXT NOT NULL,
  "marketplace" TEXT,
  "account_name" TEXT,
  "order_id" TEXT,
  "posting_number" TEXT,
  "offer_id" TEXT,
  "product_name" TEXT,
  "quantity" INTEGER NOT NULL DEFAULT 1,
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "offer_row_id" TEXT,
  "price" DECIMAL(14,4),
  "price_currency" TEXT,
  "trust_factor" INTEGER NOT NULL DEFAULT 100,
  "order_cutoff_time" TEXT,
  "reseller" BOOLEAN NOT NULL DEFAULT false,
  "supplier_score" DECIMAL(14,4),
  "request_doc_id" TEXT,
  "request_row_id" TEXT,
  "status" TEXT NOT NULL DEFAULT 'open',
  "created_by" TEXT,
  "picked_by" TEXT,
  "picked_at" TIMESTAMP(3),
  "missing_by" TEXT,
  "missing_at" TIMESTAMP(3),
  "missing_reason" TEXT,
  "next_retry_at" TIMESTAMP(3),
  "replacement_for" TEXT,
  "replacement_key" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "supplier_picking_rows_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "supplier_picking_rows_picking_key_key" ON "supplier_picking_rows"("picking_key");
CREATE INDEX IF NOT EXISTS "supplier_picking_rows_status_created_at_idx" ON "supplier_picking_rows"("status", "created_at");
CREATE INDEX IF NOT EXISTS "supplier_picking_rows_supplier_name_idx" ON "supplier_picking_rows"("supplier_name");
CREATE INDEX IF NOT EXISTS "supplier_picking_rows_partner_id_idx" ON "supplier_picking_rows"("partner_id");
CREATE INDEX IF NOT EXISTS "supplier_picking_rows_offer_id_idx" ON "supplier_picking_rows"("offer_id");

CREATE TABLE IF NOT EXISTS "supplier_blocks" (
  "id" TEXT NOT NULL,
  "block_key" TEXT NOT NULL,
  "offer_id" TEXT NOT NULL,
  "partner_id" TEXT NOT NULL,
  "supplier_name" TEXT,
  "reason" TEXT,
  "source_key" TEXT,
  "blocked_by" TEXT,
  "blocked_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "expires_at" TIMESTAMP(3) NOT NULL,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "supplier_blocks_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "supplier_blocks_block_key_key" ON "supplier_blocks"("block_key");
CREATE INDEX IF NOT EXISTS "supplier_blocks_offer_id_active_expires_at_idx" ON "supplier_blocks"("offer_id", "active", "expires_at");
CREATE INDEX IF NOT EXISTS "supplier_blocks_partner_id_idx" ON "supplier_blocks"("partner_id");

-- Brand index.
CREATE TABLE IF NOT EXISTS "brand_index_items" (
  "id" TEXT NOT NULL,
  "normalized_brand" TEXT NOT NULL,
  "display_brand" TEXT NOT NULL,
  "product_id" TEXT NOT NULL,
  "marketplace" "Marketplace" NOT NULL,
  "offer_id" TEXT NOT NULL,
  "source" TEXT NOT NULL,
  "confidence" INTEGER NOT NULL DEFAULT 80,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "brand_index_items_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "brand_index_items_normalized_brand_product_id_source_key" ON "brand_index_items"("normalized_brand", "product_id", "source");
CREATE INDEX IF NOT EXISTS "brand_index_items_normalized_brand_idx" ON "brand_index_items"("normalized_brand");
CREATE INDEX IF NOT EXISTS "brand_index_items_display_brand_idx" ON "brand_index_items"("display_brand");
CREATE INDEX IF NOT EXISTS "brand_index_items_product_id_idx" ON "brand_index_items"("product_id");
CREATE INDEX IF NOT EXISTS "brand_index_items_marketplace_idx" ON "brand_index_items"("marketplace");
ALTER TABLE "brand_index_items"
  ADD CONSTRAINT "brand_index_items_product_id_fkey"
  FOREIGN KEY ("product_id") REFERENCES "warehouse_products"("id") ON DELETE CASCADE ON UPDATE CASCADE;
