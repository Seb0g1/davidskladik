function normalizePriceMasterPartnerRows(rows = []) {
  const unique = new Map();
  for (const row of rows || []) {
    const partnerId = cleanText(row.partnerId ?? row.PartnerID ?? row.id);
    const name = cleanText(row.name ?? row.PartnerName ?? row.partnerName);
    if (!name) continue;
    const key = partnerId || normalizeSupplierName(name);
    if (!unique.has(key)) unique.set(key, { partnerId, name });
  }
  return Array.from(unique.values()).sort((left, right) =>
    String(left.name).localeCompare(String(right.name), "ru", { sensitivity: "base" }),
  );
}

async function listPriceMasterPartners() {
  const queries = [
    {
      label: "partners_with_offers",
      sql: `
        SELECT DISTINCT
          p.PartnerID AS partnerId,
          p.PartnerName AS name
        FROM Partners p
        JOIN OfferDocs d ON d.PartnerID = p.PartnerID
        WHERE p.PartnerName IS NOT NULL AND TRIM(p.PartnerName) <> ''
        ORDER BY p.PartnerName
      `,
    },
    {
      label: "partners_all",
      sql: `
        SELECT DISTINCT
          PartnerID AS partnerId,
          PartnerName AS name
        FROM Partners
        WHERE PartnerName IS NOT NULL AND TRIM(PartnerName) <> ''
        ORDER BY PartnerName
      `,
    },
    {
      label: "offer_docs_partners",
      sql: `
        SELECT DISTINCT
          d.PartnerID AS partnerId,
          COALESCE(NULLIF(TRIM(p.PartnerName), ''), CONCAT('Partner ', d.PartnerID)) AS name
        FROM OfferDocs d
        JOIN OfferRows r ON r.DocID = d.DocID
        LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
        WHERE d.PartnerID IS NOT NULL AND r.Ignored = 0
        ORDER BY name
      `,
      allowEmpty: true,
    },
  ];
  let lastError = null;
  for (const query of queries) {
    try {
      const [rows] = await pool.query(query.sql);
      const partners = normalizePriceMasterPartnerRows(rows);
      if (partners.length || query.allowEmpty) {
        if (query.label !== "partners_with_offers") {
          logger.info("PriceMaster partners loaded with fallback", { source: query.label, partners: partners.length });
        }
        return partners;
      }
      logger.warn("PriceMaster partners query returned no rows, trying fallback", { source: query.label });
    } catch (error) {
      lastError = error;
      logger.warn("PriceMaster partners query failed, trying fallback", { source: query.label, detail: error.message });
    }
  }
  if (lastError) throw lastError;
  return [];
}

async function listBrandFallbackCandidates(query, limit = 40) {
  const q = normalizeSupplierName(query);
  const unique = new Map();
  try {
    const warehouse = await readWarehouse();
    for (const product of warehouse.products || []) {
      const values = [
        cleanText(product?.ozon?.vendor),
        cleanText(product?.yandex?.vendor),
      ].filter(Boolean);
      for (const brand of values) {
        const key = normalizeSupplierName(brand);
        if (!key || (q && !key.includes(q)) || unique.has(key)) continue;
        unique.set(key, brand);
        if (unique.size >= limit) break;
      }
      if (unique.size >= limit) break;
    }
  } catch (_error) {
    // fallback should never block the request
  }
  return Array.from(unique.values()).slice(0, limit);
}
