-- Finance v1: marketplace profitability and manual purchases.
CREATE TABLE IF NOT EXISTS "finance_orders" (
  "id" TEXT NOT NULL,
  "marketplace" "Marketplace",
  "target" TEXT,
  "order_id" TEXT NOT NULL,
  "posting_number" TEXT,
  "offer_id" TEXT,
  "product_name" TEXT,
  "quantity" INTEGER NOT NULL DEFAULT 1,
  "sale_amount" DECIMAL(14,2),
  "payout_amount" DECIMAL(14,2),
  "purchase_cost" DECIMAL(14,2),
  "fees_amount" DECIMAL(14,2),
  "tax_amount" DECIMAL(14,2),
  "penalties_amount" DECIMAL(14,2),
  "refunds_amount" DECIMAL(14,2),
  "profit_amount" DECIMAL(14,2),
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "source" TEXT NOT NULL DEFAULT 'manual',
  "status" TEXT NOT NULL DEFAULT 'open',
  "raw" JSONB,
  "sold_at" TIMESTAMP(3),
  "received_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "finance_orders_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "finance_orders_marketplace_target_order_id_offer_id_key"
  ON "finance_orders"("marketplace", "target", "order_id", "offer_id");
CREATE INDEX IF NOT EXISTS "finance_orders_marketplace_status_idx" ON "finance_orders"("marketplace", "status");
CREATE INDEX IF NOT EXISTS "finance_orders_order_id_idx" ON "finance_orders"("order_id");
CREATE INDEX IF NOT EXISTS "finance_orders_offer_id_idx" ON "finance_orders"("offer_id");
CREATE INDEX IF NOT EXISTS "finance_orders_supplier_name_idx" ON "finance_orders"("supplier_name");
CREATE INDEX IF NOT EXISTS "finance_orders_created_at_idx" ON "finance_orders"("created_at");

CREATE TABLE IF NOT EXISTS "finance_expenses" (
  "id" TEXT NOT NULL,
  "type" TEXT NOT NULL DEFAULT 'manual_purchase',
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "offer_id" TEXT,
  "product_name" TEXT,
  "quantity" INTEGER NOT NULL DEFAULT 1,
  "amount" DECIMAL(14,2) NOT NULL,
  "currency" TEXT NOT NULL DEFAULT 'RUB',
  "note" TEXT,
  "source" TEXT NOT NULL DEFAULT 'manual',
  "status" TEXT NOT NULL DEFAULT 'confirmed',
  "spent_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "finance_expenses_pkey" PRIMARY KEY ("id")
);
CREATE INDEX IF NOT EXISTS "finance_expenses_type_idx" ON "finance_expenses"("type");
CREATE INDEX IF NOT EXISTS "finance_expenses_supplier_name_idx" ON "finance_expenses"("supplier_name");
CREATE INDEX IF NOT EXISTS "finance_expenses_offer_id_idx" ON "finance_expenses"("offer_id");
CREATE INDEX IF NOT EXISTS "finance_expenses_spent_at_idx" ON "finance_expenses"("spent_at");
