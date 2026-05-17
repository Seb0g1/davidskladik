DELETE FROM "product_links" p
USING (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY
        "product_id",
        lower("supplier_article"),
        lower(coalesce("partner_id", '')),
        lower(coalesce("supplier_name", '')),
        lower(coalesce("keyword", '')),
        coalesce("price_currency"::text, 'USD')
      ORDER BY "updated_at" DESC, "created_at" DESC, "id" DESC
    ) AS rn
  FROM "product_links"
) d
WHERE p.ctid = d.ctid
  AND d.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS "product_links_strict_identity_idx"
  ON "product_links" (
    "product_id",
    lower("supplier_article"),
    lower(coalesce("partner_id", '')),
    lower(coalesce("supplier_name", '')),
    lower(coalesce("keyword", '')),
    coalesce("price_currency"::text, 'USD')
  );
