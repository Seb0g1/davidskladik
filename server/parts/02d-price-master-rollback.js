
function uniqueNumericStrings(values = []) {
  return Array.from(new Set(values
    .map((value) => cleanText(value))
    .filter((value) => /^\d+$/.test(value))));
}

function priceMasterConfigPublic() {
  return {
    host: cleanText(process.env.PM_DB_HOST),
    port: Number(process.env.PM_DB_PORT || 3306),
    database: cleanText(process.env.PM_DB_NAME),
    user: cleanText(process.env.PM_DB_USER),
    configured: Boolean(cleanText(process.env.PM_DB_HOST) && cleanText(process.env.PM_DB_NAME)),
  };
}

async function verifyPriceMasterInsertedRows(rowIds = [], docIds = [], connection = null) {
  const numericRowIds = uniqueNumericStrings(rowIds);
  const numericDocIds = uniqueNumericStrings(docIds);
  const ownConnection = connection || await pool.getConnection();
  try {
    const [[dbRow]] = await ownConnection.query("SELECT DATABASE() AS dbName");
    const rows = numericRowIds.length
      ? (await ownConnection.query(
          `SELECT r.RowID, r.DocID, r.OfferRowID, r.RequestQuant, r.RequestPrice, r.RequestComment,
                  d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered
             FROM RequestRows r
             LEFT JOIN RequestDocs d ON d.DocID = r.DocID
            WHERE r.RowID IN (${numericRowIds.map(() => "?").join(",")})
            ORDER BY r.RowID DESC`,
          numericRowIds.map(Number),
        ))[0]
      : [];
    const docs = numericDocIds.length
      ? (await ownConnection.query(
          `SELECT d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered,
                  COUNT(r.RowID) AS rowCount
             FROM RequestDocs d
             LEFT JOIN RequestRows r ON r.DocID = d.DocID
            WHERE d.DocID IN (${numericDocIds.map(() => "?").join(",")})
            GROUP BY d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered
            ORDER BY d.DocID DESC`,
          numericDocIds.map(Number),
        ))[0]
      : [];
    return {
      ok: rows.length === numericRowIds.length && (!numericDocIds.length || docs.length === numericDocIds.length),
      db: dbRow?.dbName || "",
      expectedRows: numericRowIds.length,
      verifiedRows: rows.length,
      expectedDocs: numericDocIds.length,
      verifiedDocs: docs.length,
      rowIds: numericRowIds,
      docIds: numericDocIds,
      rows,
      docs,
    };
  } finally {
    if (!connection) ownConnection.release();
  }
}

async function getPriceMasterBasketStatus() {
  const connection = await pool.getConnection();
  try {
    const [[dbRow]] = await connection.query("SELECT DATABASE() AS dbName");
    const [[docsTable]] = await connection.query("SHOW TABLES LIKE 'RequestDocs'");
    const [[rowsTable]] = await connection.query("SHOW TABLES LIKE 'RequestRows'");
    const davidskladDocs = docsTable
      ? (await connection.query(
          `SELECT d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered,
                  COUNT(r.RowID) AS rowCount
             FROM RequestDocs d
             LEFT JOIN RequestRows r ON r.DocID = d.DocID
            WHERE d.Comment LIKE 'ДавидСклад автокорзина%'
            GROUP BY d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered
            ORDER BY d.DocID DESC
            LIMIT 20`,
        ))[0]
      : [];
    const latestDocs = docsTable
      ? (await connection.query(
          `SELECT d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered,
                  COUNT(r.RowID) AS rowCount
             FROM RequestDocs d
             LEFT JOIN RequestRows r ON r.DocID = d.DocID
            GROUP BY d.DocID, d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered
            ORDER BY d.DocID DESC
            LIMIT 20`,
        ))[0]
      : [];
    const latestRows = rowsTable
      ? (await connection.query(
          `SELECT r.RowID, r.DocID, r.OfferRowID, r.RequestQuant, r.RequestPrice, r.RequestComment,
                  d.PartnerID, d.Sended, d.Recieved, d.Comment, d.DocDate, d.Registered
             FROM RequestRows r
             LEFT JOIN RequestDocs d ON d.DocID = r.DocID
            ORDER BY r.RowID DESC
            LIMIT 20`,
        ))[0]
      : [];
    return {
      ok: Boolean(docsTable && rowsTable),
      config: priceMasterConfigPublic(),
      db: dbRow?.dbName || "",
      tables: { requestDocs: Boolean(docsTable), requestRows: Boolean(rowsTable) },
      davidskladDocs,
      latestDocs,
      latestRows,
    };
  } finally {
    connection.release();
  }
}

function supplierCartRollbackEmptyState() {
  return normalizeSupplierCartState({
    updatedAt: new Date().toISOString(),
    processed: {},
    supplierBlocks: {},
    draft: null,
    history: [],
  });
}

async function collectSupplierCartRollbackPlan() {
  const [cartState, pickingState] = await Promise.all([
    readSupplierCartState(),
    readSupplierPickingState(),
  ]);
  const cartProcessed = Object.values(cartState.processed || {}).filter((item) => item && typeof item === "object");
  const draftRows = cartState.draft?.rows || [];
  const pickingRows = Object.values(pickingState.rows || {}).map(normalizeSupplierPickingRow);
  const rowIds = uniqueNumericStrings([
    ...cartProcessed.map((row) => row.requestRowId),
    ...draftRows.map((row) => row.requestRowId),
    ...pickingRows.map((row) => row.requestRowId),
  ]);
  const docIds = uniqueNumericStrings([
    ...cartProcessed.map((row) => row.requestDocId),
    ...draftRows.map((row) => row.requestDocId),
    ...pickingRows.map((row) => row.requestDocId),
  ]);
  const pm = { rowIds, docIds, davidskladDocIds: [], davidskladRowIds: [] };
  try {
    const connection = await pool.getConnection();
    try {
      const [docs] = await connection.query(
        `SELECT d.DocID
           FROM RequestDocs d
          WHERE d.Comment LIKE 'ДавидСклад автокорзина%'
            AND COALESCE(d.Sended, 0) = 0`,
      );
      pm.davidskladDocIds = uniqueNumericStrings(docs.map((row) => row.DocID));
      if (pm.davidskladDocIds.length) {
        const [rows] = await connection.query(
          `SELECT RowID FROM RequestRows WHERE DocID IN (${pm.davidskladDocIds.map(() => "?").join(",")})`,
          pm.davidskladDocIds.map(Number),
        );
        pm.davidskladRowIds = uniqueNumericStrings(rows.map((row) => row.RowID));
      }
    } finally {
      connection.release();
    }
  } catch (error) {
    pm.error = error?.message || String(error);
  }
  pm.rowIds = uniqueNumericStrings([...pm.rowIds, ...pm.davidskladRowIds]);
  pm.docIds = uniqueNumericStrings([...pm.docIds, ...pm.davidskladDocIds]);

  let postgresCounts = null;
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const [drafts, draftRowsCount, picking, blocks, finance, ledger] = await Promise.all([
        prisma.supplierCartDraft.count(),
        prisma.supplierCartDraftRow.count(),
        prisma.supplierPickingRow.count(),
        prisma.supplierBlock.count({ where: { active: true } }),
        prisma.financeOrder.count({ where: { source: "supplier_picking" } }),
        prisma.supplierLedgerEntry.count({ where: { sourceKey: { startsWith: "picking:" }, status: "active" } }),
      ]);
      postgresCounts = { drafts, draftRows: draftRowsCount, picking, blocks, finance, ledger };
    } catch (error) {
      postgresCounts = { error: error?.message || String(error) };
    }
  }

  return {
    cartProcessed: cartProcessed.length,
    draftRows: draftRows.length,
    pickingRows: pickingRows.length,
    supplierBlocks: Object.keys(cartState.supplierBlocks || {}).length,
    jsonHistory: (cartState.history || []).length,
    pm,
    postgres: postgresCounts,
  };
}

async function deletePriceMasterDavidskladCart(plan = {}) {
  const rowIds = uniqueNumericStrings(plan?.pm?.rowIds || []);
  const docIds = uniqueNumericStrings(plan?.pm?.docIds || []);
  const connection = await pool.getConnection();
  let lockAcquired = false;
  try {
    const [lockRows] = await connection.query("SELECT GET_LOCK('davidsklad_supplier_cart', 10) AS locked");
    lockAcquired = Number(lockRows?.[0]?.locked || 0) === 1;
    if (!lockAcquired) {
      const error = new Error("PriceMaster cart is busy. Try again in a few seconds.");
      error.statusCode = 409;
      throw error;
    }
    await connection.beginTransaction();
    let deletedRows = 0;
    if (rowIds.length) {
      const [result] = await connection.query(
        `DELETE FROM RequestRows WHERE RowID IN (${rowIds.map(() => "?").join(",")})`,
        rowIds.map(Number),
      );
      deletedRows += Number(result?.affectedRows || 0);
    }
    if (docIds.length) {
      const [result] = await connection.query(
        `DELETE r
           FROM RequestRows r
           JOIN RequestDocs d ON d.DocID = r.DocID
          WHERE d.DocID IN (${docIds.map(() => "?").join(",")})
            AND d.Comment LIKE 'ДавидСклад автокорзина%'
            AND COALESCE(d.Sended, 0) = 0`,
        docIds.map(Number),
      );
      deletedRows += Number(result?.affectedRows || 0);
    }
    const [docResult] = await connection.query(
      `DELETE d
         FROM RequestDocs d
         LEFT JOIN RequestRows r ON r.DocID = d.DocID
        WHERE r.RowID IS NULL
          AND d.Comment LIKE 'ДавидСклад автокорзина%'
          AND COALESCE(d.Sended, 0) = 0`,
    );
    await connection.commit();
    return { deletedRows, deletedDocs: Number(docResult?.affectedRows || 0), rowIds, docIds };
  } catch (error) {
    try { await connection.rollback(); } catch (_rollbackError) {}
    throw error;
  } finally {
    if (lockAcquired) {
      try { await connection.query("SELECT RELEASE_LOCK('davidsklad_supplier_cart')"); } catch (_releaseError) {}
    }
    connection.release();
  }
}

async function rollbackSupplierCartAll(request = null, { dryRun = true } = {}) {
  const before = await collectSupplierCartRollbackPlan();
  if (dryRun) return { ok: true, dryRun: true, before };

  const pm = await deletePriceMasterDavidskladCart(before);
  let postgres = null;
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      await prisma.$transaction(async (tx) => {
        await tx.supplierCartDraftRow.deleteMany({});
        await tx.supplierCartDraft.deleteMany({});
        await tx.supplierPickingRow.deleteMany({});
        await tx.supplierBlock.updateMany({ where: { active: true }, data: { active: false, raw: { rollbackBy: requestUsername(request), rollbackAt: new Date().toISOString() } } });
        await tx.financeOrder.deleteMany({ where: { source: "supplier_picking" } });
        await tx.supplierLedgerEntry.updateMany({
          where: { sourceKey: { startsWith: "picking:" }, status: "active" },
          data: { status: "voided", voidedAt: new Date(), raw: { rollbackBy: requestUsername(request), rollbackAt: new Date().toISOString() } },
        });
      }, { timeout: stateWriteTransactionTimeoutMs, maxWait: stateWriteTransactionMaxWaitMs });
      postgres = { ok: true };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      postgres = { ok: false, error: error?.message || String(error) };
    }
  }

  await fs.mkdir(dataDir, { recursive: true });
  await fs.writeFile(supplierCartStatePath, JSON.stringify(supplierCartRollbackEmptyState(), null, 2), "utf8");
  await fs.writeFile(supplierPickingListPath, JSON.stringify({ updatedAt: new Date().toISOString(), rows: {}, invoices: [] }, null, 2), "utf8");
  const financeState = await readFinanceJsonFallback();
  await writeFinanceJsonFallback({
    ...financeState,
    orders: (financeState.orders || []).filter((order) => order.source !== "supplier_picking"),
  });

  suppliersListCache = null;
  const after = await collectSupplierCartRollbackPlan();
  const result = { ok: true, dryRun: false, before, after, pm, postgres };
  await appendAudit(request || { session: { username: "system", role: "admin" } }, "supplier_cart.rollback_all", {
    entityType: "supplier_cart",
    entityId: "all",
    oldValue: before,
    newValue: result,
  }).catch((error) => logger.warn("supplier cart rollback audit failed", { detail: error?.message || String(error) }));
  return result;
}

