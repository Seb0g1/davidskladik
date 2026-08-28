CREATE INDEX IF NOT EXISTS "warehouse_products_marketplace_status_updated_at_idx" ON "warehouse_products"("marketplace", "status", "updated_at");
CREATE INDEX IF NOT EXISTS "warehouse_products_marketplace_archived_updated_at_idx" ON "warehouse_products"("marketplace", "archived", "updated_at");
CREATE INDEX IF NOT EXISTS "warehouse_products_target_status_idx" ON "warehouse_products"("target", "status");

CREATE INDEX IF NOT EXISTS "product_links_supplier_article_partner_id_idx" ON "product_links"("supplier_article", "partner_id");
CREATE INDEX IF NOT EXISTS "product_links_supplier_name_idx" ON "product_links"("supplier_name");
CREATE INDEX IF NOT EXISTS "product_links_updated_at_idx" ON "product_links"("updated_at");

CREATE INDEX IF NOT EXISTS "pm_snapshot_items_article_partner_id_idx" ON "pm_snapshot_items"("article", "partner_id");
CREATE INDEX IF NOT EXISTS "pm_snapshot_items_active_article_idx" ON "pm_snapshot_items"("active", "article");
CREATE INDEX IF NOT EXISTS "pm_snapshot_items_updated_at_idx" ON "pm_snapshot_items"("updated_at");
