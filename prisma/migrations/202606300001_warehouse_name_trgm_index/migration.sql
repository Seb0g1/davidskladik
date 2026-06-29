-- Enable pg_trgm extension for trigram-based text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN trigram index speeds up name ILIKE queries (duplicate-name exclusion + search).
-- NOTE: This acquires an AccessShareLock; on a 939MB table it may take 2-5 minutes.
CREATE INDEX IF NOT EXISTS warehouse_products_name_trgm_idx
  ON warehouse_products USING gin (lower(name) gin_trgm_ops);

-- Refresh planner statistics so the new index is considered immediately.
ANALYZE warehouse_products;
