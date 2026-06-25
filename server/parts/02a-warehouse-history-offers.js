async function appendHistory(syncResult) {
  await fs.mkdir(dataDir, { recursive: true });
  const lines = syncResult.changes.map((change) =>
    JSON.stringify({
      syncId: syncResult.syncId,
      createdAt: syncResult.createdAt,
      type: change.type,
      article: change.current?.article || change.previous?.article || null,
      barcode: change.current?.barcode || change.previous?.barcode || null,
      name: change.current?.name || change.previous?.name || null,
      partnerId: change.current?.partnerId || change.previous?.partnerId || null,
      partnerName: change.current?.partnerName || change.previous?.partnerName || null,
      oldPrice: change.previous?.price ?? null,
      newPrice: change.current?.price ?? null,
      oldActive: change.previous?.active ?? null,
      newActive: change.current?.active ?? null,
      previousDocDate: change.previous?.docDate || null,
      currentDocDate: change.current?.docDate || null,
    }),
  );

  if (lines.length) {
    await fs.appendFile(historyPath, `${lines.join("\n")}\n`, "utf8");
  }
}

async function readHistory(limit = 300) {
  try {
    const content = await fs.readFile(historyPath, "utf8");
    const lines = content.trim().split("\n").filter(Boolean);
    return lines
      .slice(-limit)
      .reverse()
      .map((line) => JSON.parse(line));
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
}

function offerKey(row) {
  const identity = [row.article || "", row.barcode || "", row.name || ""]
    .map((value) => String(value).trim().toLowerCase())
    .join("|");
  return `${row.partnerId}:${identity}`;
}

let offerDocsActiveColumn = null;
let offerDocsActiveFilterSuffix = null; // " = 0" for lock-type columns, " != 0" for active-type

async function discoverOfferDocsActiveColumn() {
  if (offerDocsActiveColumn !== null) return offerDocsActiveColumn;
  let connection;
  try {
    connection = await pool.getConnection();
    // Inactive/lock columns: Locked=1 means inactive, so condition is "= 0"
    const [lockCols] = await connection.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'OfferDocs'
       AND COLUMN_NAME IN ('Locked', 'IsLocked', 'Disabled', 'IsDisabled')
       ORDER BY ORDINAL_POSITION LIMIT 1`,
    );
    if (lockCols.length) {
      offerDocsActiveColumn = String(lockCols[0].COLUMN_NAME);
      offerDocsActiveFilterSuffix = " = 0";
      logger.info("OfferDocs active column discovery", { found: true, column: offerDocsActiveColumn, type: "lock_flag" });
      return offerDocsActiveColumn;
    }
    // Active columns: Active=1 means active, so condition is "!= 0"
    const [activeCols] = await connection.query(
      `SELECT COLUMN_NAME FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'OfferDocs'
       AND COLUMN_NAME IN ('Active', 'IsActive', 'IsCurrent', 'Enabled', 'IsEnabled')
       ORDER BY ORDINAL_POSITION LIMIT 1`,
    );
    if (activeCols.length) {
      offerDocsActiveColumn = String(activeCols[0].COLUMN_NAME);
      offerDocsActiveFilterSuffix = " != 0";
      logger.info("OfferDocs active column discovery", { found: true, column: offerDocsActiveColumn, type: "active_flag" });
      return offerDocsActiveColumn;
    }
    offerDocsActiveColumn = "";
    offerDocsActiveFilterSuffix = "";
    logger.info("OfferDocs active column discovery", { found: false });
  } catch (error) {
    offerDocsActiveColumn = "";
    offerDocsActiveFilterSuffix = "";
    logger.warn("OfferDocs schema discovery failed", { detail: error?.message || String(error) });
  } finally {
    if (connection) connection.release();
  }
  return offerDocsActiveColumn;
}

async function getCurrentOffers(connection) {
  await discoverOfferDocsActiveColumn();
  const col = offerDocsActiveColumn;
  const suffix = offerDocsActiveFilterSuffix;
  const latestDocsWhere = col ? `WHERE ${col}${suffix}` : "";
  const latestDocIdsAnd = col ? `AND d.${col}${suffix}` : "";

  const [rows] = await connection.query(`
    WITH latest_docs AS (
      SELECT PartnerID, MAX(DocDate) AS LatestDocDate
      FROM OfferDocs
      ${latestDocsWhere}
      GROUP BY PartnerID
    ),
    latest_doc_ids AS (
      SELECT d.PartnerID, MAX(d.DocID) AS DocID
      FROM OfferDocs d
      JOIN latest_docs l
        ON l.PartnerID = d.PartnerID
       AND l.LatestDocDate = d.DocDate
      ${latestDocIdsAnd}
      GROUP BY d.PartnerID
    )
    SELECT
      r.RowID AS rowId,
      r.NativeID AS article,
      r.BarCode AS barcode,
      r.NativeName AS name,
      r.ProductID AS productId,
      r.NativePrice AS price,
      r.Active AS active,
      r.IsNew AS isNew,
      r.Ignored AS ignored,
      d.DocDate AS docDate,
      d.PartnerID AS partnerId,
      p.PartnerName AS partnerName
    FROM latest_doc_ids ld
    JOIN OfferDocs d ON d.DocID = ld.DocID
    JOIN OfferRows r ON r.DocID = d.DocID
    LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
    WHERE r.Ignored = 0
    ORDER BY d.DocDate DESC, p.PartnerName, r.NativeName, r.RowID DESC
  `);

  return rows.map((row) => ({
    ...row,
    key: offerKey(row),
    price: Number(row.price || 0),
    active: Boolean(row.active),
    isNew: Boolean(row.isNew),
    ignored: Boolean(row.ignored),
  }));
}


