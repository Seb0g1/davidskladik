async function writeWarehouseToPostgres(prisma, payload) {
  const products = Array.isArray(payload.products) ? payload.products : [];
  const suppliers = Array.isArray(payload.suppliers) ? payload.suppliers : [];
  const chunkSize = Math.max(25, Math.min(250, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 100) || 100));
  const changedProducts = products.filter((product) =>
    !warehousePostgresHashCache.has(product.id)
    || cleanText(product.updatedAt) !== warehousePostgresUpdatedAtCache.get(product.id)
  );
  if (changedProducts.length) {
    logger.info("warehouse postgres write delta", { products: changedProducts.length, suppliers: suppliers.length, chunkSize });
  }
  const writeConcurrency = warehousePostgresWriteConcurrency();
  const supplierRows = suppliers.map(supplierToPostgresData);
  await runWithLimitedConcurrency(supplierRows, writeConcurrency, async (data) => {
    await prisma.managedSupplier.upsert({
      where: { partnerId: data.partnerId || data.name },
      create: data,
      update: {
        name: data.name,
        active: data.active,
        defaultCurrency: data.defaultCurrency,
        stopReason: data.stopReason,
        note: data.note,
        raw: data.raw,
        updatedAt: data.updatedAt,
      },
    });
  });
  for (const productChunk of chunkArray(changedProducts, chunkSize)) {
    await runWithLimitedConcurrency(productChunk, writeConcurrency, async (product) => {
      await upsertWarehouseProductPostgres(prisma, product);
    });
    markWarehousePostgresProductsWritten(productChunk);
  }
}

function scheduleWarehousePostgresWrite(prisma, payload) {
  warehousePostgresWriteQueuedPayload = payload;
  if (warehousePostgresWriteRunning) return;
  warehousePostgresWriteRunning = true;
  setImmediate(async () => {
    try {
      while (warehousePostgresWriteQueuedPayload) {
        const nextPayload = warehousePostgresWriteQueuedPayload;
        warehousePostgresWriteQueuedPayload = null;
        await writeWarehouseToPostgres(prisma, nextPayload);
      }
    } catch (error) {
      logger.warn("write warehouse postgres failed, keeping JSON fallback", { detail: error?.message || String(error) });
    } finally {
      warehousePostgresWriteRunning = false;
      if (warehousePostgresWriteQueuedPayload) scheduleWarehousePostgresWrite(prisma, warehousePostgresWriteQueuedPayload);
    }
  });
}

