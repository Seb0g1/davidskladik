-- Consignment invoices can be unposted, edited and posted again.
ALTER TABLE "consignment_invoices" ADD COLUMN IF NOT EXISTS "status" TEXT NOT NULL DEFAULT 'posted';
ALTER TABLE "consignment_invoices" ADD COLUMN IF NOT EXISTS "from_balance" BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE "consignment_invoices" ADD COLUMN IF NOT EXISTS "posted_at" TIMESTAMP(3);
ALTER TABLE "consignment_invoices" ADD COLUMN IF NOT EXISTS "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Operations created by an invoice were linked to it only by the note "Накладная ПН-001".
UPDATE "consignment_operations" o
SET "raw" = COALESCE(o."raw", '{}'::jsonb) || jsonb_build_object('invoiceId', i."id")
FROM "consignment_invoices" i
WHERE o."note" = 'Накладная ' || i."number"
  AND o."type" IN ('receive', 'purchase')
  AND (o."raw" IS NULL OR NOT (o."raw" ? 'invoiceId'));

UPDATE "consignment_invoices" i
SET "from_balance" = EXISTS (
  SELECT 1 FROM "consignment_operations" o
  WHERE o."raw"->>'invoiceId' = i."id" AND o."type" = 'purchase'
);

-- An invoice whose operations were all deleted by hand is effectively unposted.
UPDATE "consignment_invoices" i
SET "status" = CASE WHEN EXISTS (
    SELECT 1 FROM "consignment_operations" o WHERE o."raw"->>'invoiceId' = i."id"
  ) THEN 'posted' ELSE 'draft' END,
  "posted_at" = CASE WHEN EXISTS (
    SELECT 1 FROM "consignment_operations" o WHERE o."raw"->>'invoiceId' = i."id"
  ) THEN i."created_at" ELSE NULL END;

CREATE INDEX IF NOT EXISTS "consignment_invoices_status_idx" ON "consignment_invoices"("status");
