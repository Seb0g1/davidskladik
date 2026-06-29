-- Expression indexes on JSONB fields used by fetchCrossMarketplaceSiblingRows.
-- Without these, every warehouse page load runs 300-760ms seq scans on 31K rows (939MB).
--
-- 1. manualGroupId: 39% of products have 'auto-pair-*' groups, seq scan costs 760ms/call.
CREATE INDEX IF NOT EXISTS warehouse_products_manual_group_id_idx
  ON warehouse_products ((raw->>'manualGroupId'));

-- 2. yandex.extra.sourceProductId: used to find Yandex twin for each Ozon product,
--    currently requires a marketplace-filtered bitmap heap scan on 10K yandex rows (300ms/call).
CREATE INDEX IF NOT EXISTS warehouse_products_yandex_source_product_id_idx
  ON warehouse_products ((raw->'yandex'->'extra'->>'sourceProductId'));

ANALYZE warehouse_products;
