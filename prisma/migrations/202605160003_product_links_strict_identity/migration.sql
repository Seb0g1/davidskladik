CREATE OR REPLACE FUNCTION public.product_links_strict_identity_key(
  _product_id TEXT,
  _supplier_article TEXT,
  _partner_id TEXT,
  _supplier_name TEXT,
  _keyword TEXT,
  _price_currency "PriceCurrency"
)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT
    btrim(coalesce(_product_id, '')) || '|' ||
    lower(btrim(coalesce(_supplier_article, '')) COLLATE "C") || '|' ||
    lower(btrim(coalesce(_partner_id, '')) COLLATE "C") || '|' ||
    lower(btrim(coalesce(_supplier_name, '')) COLLATE "C") || '|' ||
    lower(btrim(coalesce(_keyword, '')) COLLATE "C") || '|' ||
    CASE WHEN _price_currency = 'RUB'::"PriceCurrency" THEN 'RUB' ELSE 'USD' END;
$$;

DELETE FROM "product_links" p
USING (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY
        public.product_links_strict_identity_key(
          "product_id",
          "supplier_article",
          "partner_id",
          "supplier_name",
          "keyword",
          "price_currency"
        )
      ORDER BY "updated_at" DESC, "created_at" DESC, "id" DESC
    ) AS rn
  FROM "product_links"
) d
WHERE p.ctid = d.ctid
  AND d.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS "product_links_strict_identity_idx"
  ON "product_links" (public.product_links_strict_identity_key(
    "product_id",
    "supplier_article",
    "partner_id",
    "supplier_name",
    "keyword",
    "price_currency"
  ));
