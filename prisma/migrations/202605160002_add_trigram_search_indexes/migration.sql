CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS "warehouse_products_offer_id_trgm_idx" ON "warehouse_products" USING GIN ("offer_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "warehouse_products_product_id_trgm_idx" ON "warehouse_products" USING GIN ("product_id" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "warehouse_products_name_trgm_idx" ON "warehouse_products" USING GIN ("name" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "warehouse_products_brand_trgm_idx" ON "warehouse_products" USING GIN ("brand" gin_trgm_ops);

CREATE INDEX IF NOT EXISTS "product_links_supplier_article_trgm_idx" ON "product_links" USING GIN ("supplier_article" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "product_links_supplier_name_trgm_idx" ON "product_links" USING GIN ("supplier_name" gin_trgm_ops);

CREATE INDEX IF NOT EXISTS "pm_snapshot_items_article_trgm_idx" ON "pm_snapshot_items" USING GIN ("article" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "pm_snapshot_items_row_id_idx" ON "pm_snapshot_items" ("row_id");
CREATE INDEX IF NOT EXISTS "pm_snapshot_items_native_name_trgm_idx" ON "pm_snapshot_items" USING GIN ("native_name" gin_trgm_ops);
CREATE INDEX IF NOT EXISTS "pm_snapshot_items_partner_name_trgm_idx" ON "pm_snapshot_items" USING GIN ("partner_name" gin_trgm_ops);
