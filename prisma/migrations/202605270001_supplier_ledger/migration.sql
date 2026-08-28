CREATE TABLE IF NOT EXISTS "supplier_ledger_entries" (
  "id" TEXT NOT NULL,
  "source_key" TEXT NOT NULL,
  "entry_type" TEXT NOT NULL,
  "supplier_name" TEXT,
  "partner_id" TEXT,
  "amount" DECIMAL(14,2) NOT NULL,
  "currency" TEXT NOT NULL DEFAULT 'RUB',
  "picking_key" TEXT,
  "finance_order_id" TEXT,
  "order_id" TEXT,
  "posting_number" TEXT,
  "offer_id" TEXT,
  "product_name" TEXT,
  "quantity" INTEGER NOT NULL DEFAULT 1,
  "note" TEXT,
  "status" TEXT NOT NULL DEFAULT 'active',
  "occurred_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "voided_at" TIMESTAMP(3),
  "created_by" TEXT,
  "raw" JSONB,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "supplier_ledger_entries_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "supplier_ledger_entries_source_key_key" ON "supplier_ledger_entries"("source_key");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_supplier_name_idx" ON "supplier_ledger_entries"("supplier_name");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_partner_id_idx" ON "supplier_ledger_entries"("partner_id");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_entry_type_idx" ON "supplier_ledger_entries"("entry_type");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_status_idx" ON "supplier_ledger_entries"("status");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_occurred_at_idx" ON "supplier_ledger_entries"("occurred_at");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_picking_key_idx" ON "supplier_ledger_entries"("picking_key");
CREATE INDEX IF NOT EXISTS "supplier_ledger_entries_finance_order_id_idx" ON "supplier_ledger_entries"("finance_order_id");
