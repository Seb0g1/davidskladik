CREATE TYPE "UserRole" AS ENUM ('admin', 'manager');
CREATE TYPE "Marketplace" AS ENUM ('ozon', 'yandex');
CREATE TYPE "PriceCurrency" AS ENUM ('USD', 'RUB');
CREATE TYPE "QueueStatus" AS ENUM ('pending', 'processing', 'success', 'failed', 'delayed');
CREATE TYPE "SyncStatus" AS ENUM ('running', 'success', 'failed', 'skipped');

CREATE TABLE "app_users" (
  "id" TEXT NOT NULL,
  "username" TEXT NOT NULL,
  "password_hash" TEXT NOT NULL,
  "role" "UserRole" NOT NULL DEFAULT 'manager',
  "active" BOOLEAN NOT NULL DEFAULT true,
  "source" TEXT NOT NULL DEFAULT 'postgres',
  "protected" BOOLEAN NOT NULL DEFAULT false,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "app_users_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "warehouse_products" (
  "id" TEXT NOT NULL,
  "marketplace" "Marketplace" NOT NULL,
  "target" TEXT,
  "offer_id" TEXT NOT NULL,
  "product_id" TEXT,
  "name" TEXT NOT NULL,
  "brand" TEXT,
  "images" JSONB,
  "marketplace_state" JSONB,
  "current_price" INTEGER,
  "target_price" INTEGER,
  "target_stock" INTEGER,
  "status" TEXT,
  "archived" BOOLEAN NOT NULL DEFAULT false,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "warehouse_products_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "product_links" (
  "id" TEXT NOT NULL,
  "product_id" TEXT NOT NULL,
  "supplier_article" TEXT NOT NULL,
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "price_currency" "PriceCurrency" NOT NULL DEFAULT 'USD',
  "keyword" TEXT,
  "raw" JSONB,
  "created_by_id" TEXT,
  "updated_by_id" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "product_links_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "managed_suppliers" (
  "id" TEXT NOT NULL,
  "partner_id" TEXT,
  "name" TEXT NOT NULL,
  "active" BOOLEAN NOT NULL DEFAULT true,
  "default_currency" "PriceCurrency" NOT NULL DEFAULT 'USD',
  "stop_reason" TEXT,
  "note" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "managed_suppliers_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "app_settings" (
  "key" TEXT NOT NULL,
  "value" JSONB NOT NULL,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "app_settings_pkey" PRIMARY KEY ("key")
);

CREATE TABLE "audit_logs" (
  "id" TEXT NOT NULL,
  "username" TEXT NOT NULL,
  "user_id" TEXT,
  "action" TEXT NOT NULL,
  "entity_type" TEXT,
  "entity_id" TEXT,
  "old_value" JSONB,
  "new_value" JSONB,
  "details" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "audit_logs_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "price_retry_queue" (
  "id" TEXT NOT NULL,
  "queue_key" TEXT NOT NULL,
  "marketplace" "Marketplace" NOT NULL,
  "target" TEXT,
  "product_id" TEXT,
  "offer_id" TEXT NOT NULL,
  "price" INTEGER NOT NULL,
  "old_price" INTEGER,
  "status" "QueueStatus" NOT NULL DEFAULT 'pending',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "error" TEXT,
  "payload" JSONB,
  "next_retry_at" TIMESTAMP(3),
  "last_attempt_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "price_retry_queue_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "price_history" (
  "id" TEXT NOT NULL,
  "product_id" TEXT,
  "marketplace" "Marketplace" NOT NULL,
  "target" TEXT,
  "offer_id" TEXT NOT NULL,
  "old_price" INTEGER,
  "new_price" INTEGER NOT NULL,
  "status" "QueueStatus" NOT NULL,
  "response" JSONB,
  "error" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "price_history_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "sync_runs" (
  "id" TEXT NOT NULL,
  "trigger" TEXT NOT NULL,
  "status" "SyncStatus" NOT NULL,
  "started_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finished_at" TIMESTAMP(3),
  "stats" JSONB,
  "error" TEXT,
  CONSTRAINT "sync_runs_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "pm_snapshot_items" (
  "id" TEXT NOT NULL,
  "row_id" TEXT,
  "article" TEXT NOT NULL,
  "partner_id" TEXT,
  "partner_name" TEXT,
  "native_name" TEXT,
  "price" DECIMAL(14,4),
  "currency" "PriceCurrency" NOT NULL DEFAULT 'USD',
  "doc_date" TIMESTAMP(3),
  "active" BOOLEAN NOT NULL DEFAULT true,
  "raw" JSONB,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "pm_snapshot_items_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "app_users_username_key" ON "app_users"("username");
CREATE UNIQUE INDEX "managed_suppliers_partner_id_key" ON "managed_suppliers"("partner_id");
CREATE UNIQUE INDEX "price_retry_queue_queue_key_key" ON "price_retry_queue"("queue_key");
CREATE UNIQUE INDEX "product_links_identity_key" ON "product_links"("product_id", "supplier_article", "partner_id", "supplier_name", "keyword", "price_currency");
CREATE UNIQUE INDEX "pm_snapshot_items_identity_key" ON "pm_snapshot_items"("article", "partner_id", "row_id");

CREATE INDEX "warehouse_products_marketplace_target_idx" ON "warehouse_products"("marketplace", "target");
CREATE INDEX "warehouse_products_offer_id_idx" ON "warehouse_products"("offer_id");
CREATE INDEX "warehouse_products_product_id_idx" ON "warehouse_products"("product_id");
CREATE INDEX "warehouse_products_brand_idx" ON "warehouse_products"("brand");
CREATE INDEX "product_links_product_id_idx" ON "product_links"("product_id");
CREATE INDEX "product_links_supplier_article_idx" ON "product_links"("supplier_article");
CREATE INDEX "product_links_partner_id_idx" ON "product_links"("partner_id");
CREATE INDEX "managed_suppliers_name_idx" ON "managed_suppliers"("name");
CREATE INDEX "audit_logs_username_idx" ON "audit_logs"("username");
CREATE INDEX "audit_logs_action_idx" ON "audit_logs"("action");
CREATE INDEX "audit_logs_entity_idx" ON "audit_logs"("entity_type", "entity_id");
CREATE INDEX "audit_logs_created_at_idx" ON "audit_logs"("created_at");
CREATE INDEX "price_retry_queue_status_next_retry_at_idx" ON "price_retry_queue"("status", "next_retry_at");
CREATE INDEX "price_retry_queue_marketplace_target_idx" ON "price_retry_queue"("marketplace", "target");
CREATE INDEX "price_retry_queue_offer_id_idx" ON "price_retry_queue"("offer_id");
CREATE INDEX "price_history_product_id_idx" ON "price_history"("product_id");
CREATE INDEX "price_history_marketplace_target_idx" ON "price_history"("marketplace", "target");
CREATE INDEX "price_history_created_at_idx" ON "price_history"("created_at");
CREATE INDEX "sync_runs_trigger_idx" ON "sync_runs"("trigger");
CREATE INDEX "sync_runs_status_idx" ON "sync_runs"("status");
CREATE INDEX "sync_runs_started_at_idx" ON "sync_runs"("started_at");
CREATE INDEX "pm_snapshot_items_article_idx" ON "pm_snapshot_items"("article");
CREATE INDEX "pm_snapshot_items_partner_id_idx" ON "pm_snapshot_items"("partner_id");
CREATE INDEX "pm_snapshot_items_partner_name_idx" ON "pm_snapshot_items"("partner_name");
CREATE INDEX "pm_snapshot_items_native_name_idx" ON "pm_snapshot_items"("native_name");

ALTER TABLE "product_links" ADD CONSTRAINT "product_links_product_id_fkey" FOREIGN KEY ("product_id") REFERENCES "warehouse_products"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "product_links" ADD CONSTRAINT "product_links_created_by_id_fkey" FOREIGN KEY ("created_by_id") REFERENCES "app_users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "product_links" ADD CONSTRAINT "product_links_updated_by_id_fkey" FOREIGN KEY ("updated_by_id") REFERENCES "app_users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "audit_logs" ADD CONSTRAINT "audit_logs_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "app_users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "price_retry_queue" ADD CONSTRAINT "price_retry_queue_product_id_fkey" FOREIGN KEY ("product_id") REFERENCES "warehouse_products"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "price_history" ADD CONSTRAINT "price_history_product_id_fkey" FOREIGN KEY ("product_id") REFERENCES "warehouse_products"("id") ON DELETE SET NULL ON UPDATE CASCADE;
