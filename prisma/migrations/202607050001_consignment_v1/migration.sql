CREATE TABLE IF NOT EXISTS "consignment_items" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "article" TEXT,
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "purchase_price" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "sale_price" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "quantity" INTEGER NOT NULL DEFAULT 0,
  "note" TEXT,
  "archived" BOOLEAN NOT NULL DEFAULT false,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "consignment_items_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "consignment_items_article_idx" ON "consignment_items"("article");
CREATE INDEX IF NOT EXISTS "consignment_items_supplier_name_idx" ON "consignment_items"("supplier_name");
CREATE INDEX IF NOT EXISTS "consignment_items_archived_updated_at_idx" ON "consignment_items"("archived", "updated_at");

CREATE TABLE IF NOT EXISTS "consignment_operations" (
  "id" TEXT NOT NULL,
  "item_id" TEXT,
  "item_name" TEXT,
  "type" TEXT NOT NULL,
  "quantity" INTEGER NOT NULL DEFAULT 0,
  "unit_purchase" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "unit_sale" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "balance_delta" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "sponsor_delta" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "my_delta" DECIMAL(14,2) NOT NULL DEFAULT 0,
  "note" TEXT,
  "status" TEXT NOT NULL DEFAULT 'active',
  "related_operation_id" TEXT,
  "created_by" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "consignment_operations_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "consignment_operations_item_id_idx" ON "consignment_operations"("item_id");
CREATE INDEX IF NOT EXISTS "consignment_operations_type_idx" ON "consignment_operations"("type");
CREATE INDEX IF NOT EXISTS "consignment_operations_status_idx" ON "consignment_operations"("status");
CREATE INDEX IF NOT EXISTS "consignment_operations_created_at_idx" ON "consignment_operations"("created_at");
CREATE INDEX IF NOT EXISTS "consignment_operations_related_operation_id_idx" ON "consignment_operations"("related_operation_id");

ALTER TABLE "consignment_operations"
  ADD CONSTRAINT "consignment_operations_item_id_fkey"
  FOREIGN KEY ("item_id") REFERENCES "consignment_items"("id") ON DELETE SET NULL ON UPDATE CASCADE;
