ALTER TABLE "warehouse_products" ADD COLUMN "ever_had_links" BOOLEAN NOT NULL DEFAULT false;

UPDATE "warehouse_products" AS wp
SET "ever_had_links" = true
WHERE EXISTS (
  SELECT 1
  FROM "product_links" AS pl
  WHERE pl."product_id" = wp."id"
);
