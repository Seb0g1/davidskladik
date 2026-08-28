ALTER TABLE "consignment_operations" ADD COLUMN IF NOT EXISTS "source_key" TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS "consignment_operations_source_key_key" ON "consignment_operations"("source_key");
