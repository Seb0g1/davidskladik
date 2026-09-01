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
  // Cap at 10 so each chunk takes ~2-3 s at ~250ms/upsert; setImmediate between chunks
  // prevents 6-8 s event-loop blocks seen at chunkSize=25 on the worker.
  const chunkSize = Math.max(3, Math.min(10, Number(process.env.WAREHOUSE_POSTGRES_WRITE_CHUNK_SIZE || 10) || 10));
  // Стриминговый фильтр без pre-allocation массива changedProducts:
  // при дельте 8-11k предварительная фильтрация держала 400-800 МБ в памяти
  // параллельно с полным каталогом reconciler'а → heap 4+ GB → GC-паузы 22 с.
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
  let productChunk = [];
  let totalWritten = 0;
  for (const product of products) {
    const cachedUpdatedAt = warehousePostgresUpdatedAtCache.get(product.id);
    const upToDate = cachedUpdatedAt && cleanText(product.updatedAt) === cachedUpdatedAt;
    if (upToDate) continue;
    productChunk.push(product);
    if (productChunk.length >= chunkSize) {
      for (const p of productChunk) {
        await upsertWarehouseProductPostgres(prisma, p);
        await new Promise((r) => setImmediate(r));
      }
      markWarehousePostgresProductsWritten(productChunk);
      totalWritten += productChunk.length;
      productChunk = [];
    }
  }
  if (productChunk.length) {
    for (const p of productChunk) {
      await upsertWarehouseProductPostgres(prisma, p);
      await new Promise((r) => setImmediate(r));
    }
    markWarehousePostgresProductsWritten(productChunk);
    totalWritten += productChunk.length;
  }
  if (totalWritten) {
    logger.info("warehouse postgres write delta", { products: totalWritten, suppliers: suppliers.length, chunkSize });
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

