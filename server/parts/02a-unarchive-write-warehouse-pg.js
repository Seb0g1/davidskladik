async function writeWarehouseToPostgres(prisma, payload) {
  // Маркер для event_loop_blocked: дельта-запись склада в Postgres гоняет
  // хеширование и сериализацию тысяч product.raw — кандидат в блокировщики.
  const closeMarker = setEventLoopBlockMarker("warehouse_postgres_write");
  try {
    await writeWarehouseToPostgresInner(prisma, payload);
  } finally {
    closeMarker();
  }
}

async function writeWarehouseToPostgresInner(prisma, payload) {
  // Прогрев персистентного кэша записей: без него первый write после
  // рестарта считает изменившимся весь каталог.
  await loadWarehousePgWrittenCache();
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
    const partnerKey = data.partnerId || data.name;
    // api и worker держат независимые in-memory копии склада: bulk-запись из
    // устаревшей копии не должна откатывать более свежую строку поставщика
    // (например валюту, выставленную на странице «Поставщики»).
    const updated = await prisma.managedSupplier.updateMany({
      where: { partnerId: partnerKey, updatedAt: { lte: data.updatedAt } },
      data: {
        name: data.name,
        active: data.active,
        defaultCurrency: data.defaultCurrency,
        stopReason: data.stopReason,
        note: data.note,
        raw: data.raw,
        updatedAt: data.updatedAt,
      },
    });
    if (!updated.count) {
      const existing = await prisma.managedSupplier.findUnique({ where: { partnerId: partnerKey }, select: { id: true } });
      if (!existing) {
        await prisma.managedSupplier.create({ data }).catch((error) => {
          if (error?.code !== "P2002") throw error;
        });
      }
    }
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

