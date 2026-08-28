-- Add typed columns for sourceRowId and exactName to product_links
ALTER TABLE product_links ADD COLUMN IF NOT EXISTS source_row_id TEXT;
ALTER TABLE product_links ADD COLUMN IF NOT EXISTS exact_name TEXT;

-- Partial indexes (only index non-null rows for efficiency)
CREATE INDEX IF NOT EXISTS idx_product_links_source_row_id ON product_links(source_row_id) WHERE source_row_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_product_links_exact_name ON product_links(exact_name) WHERE exact_name IS NOT NULL;

-- Backfill from raw JSON
UPDATE product_links SET source_row_id = (raw->>'sourceRowId') WHERE raw IS NOT NULL AND raw->>'sourceRowId' IS NOT NULL AND source_row_id IS NULL;
UPDATE product_links SET exact_name = (raw->>'exactName') WHERE raw IS NOT NULL AND raw->>'exactName' IS NOT NULL AND exact_name IS NULL;
