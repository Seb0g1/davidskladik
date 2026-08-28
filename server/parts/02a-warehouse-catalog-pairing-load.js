async function loadWarehouseCatalogPairingRows(prisma, select = {}, where = {}, options = {}) {
  const batchSize = Math.max(1000, Math.min(10000, Number(process.env.WAREHOUSE_GROUP_COUNT_BATCH_SIZE || 5000) || 5000));
  const configuredMaxRows = Math.max(0, Number(options.maxRows ?? warehouseGroupCountMaxScanRows) || 0);
  const maxRows = serverUnderMemoryPressure()
    ? Math.min(configuredMaxRows || batchSize, batchSize)
    : configuredMaxRows;
  const rows = [];
  let cursorId = "";
  while (true) {
    const batch = await prisma.warehouseProduct.findMany({
      where,
      select,
      orderBy: { id: "asc" },
      take: batchSize,
      ...(cursorId ? { skip: 1, cursor: { id: cursorId } } : {}),
    });
    if (!batch.length) break;
    cursorId = batch[batch.length - 1].id;
    rows.push(...batch);
    if (maxRows > 0 && rows.length >= maxRows) break;
    if (batch.length < batchSize) break;
  }
  return rows;
}


