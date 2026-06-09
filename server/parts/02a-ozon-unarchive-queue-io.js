async function readOzonUnarchiveQueue() {
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = await prisma.ozonUnarchiveQueueItem.findMany({
        where: { status: { in: ["pending", "processing", "failed", "delayed"] } },
        orderBy: [{ nextRetryAt: "asc" }, { queuedAt: "asc" }],
        take: 5000,
      });
      const daily = await readOzonUnarchiveDailyState();
      const updatedAt = rows.reduce((latest, row) => {
        const time = row.updatedAt ? row.updatedAt.getTime() : 0;
        return time > latest ? time : latest;
      }, 0);
      return normalizeOzonUnarchiveQueue({
        updatedAt: updatedAt ? new Date(updatedAt).toISOString() : null,
        daily,
        items: rows.map(ozonUnarchiveQueueItemFromPostgres),
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      rememberStateWarning("ozon_unarchive_queue_read_postgres", error);
      logger.warn("read ozon unarchive queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const text = await fs.readFile(ozonUnarchiveQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, daily: {}, items: [] };
    return normalizeOzonUnarchiveQueue(JSON.parse(text));
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, daily: {}, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("ozon unarchive queue is invalid, resetting in memory", { detail: error.message });
      return { updatedAt: null, daily: {}, items: [] };
    }
    throw error;
  }
}

async function writeOzonUnarchiveQueue(queue = {}) {
  const payload = normalizeOzonUnarchiveQueue({
    ...queue,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = payload.items.map(ozonUnarchiveQueueItemToPostgres);
      const queueKeys = rows.map((item) => item.queueKey).filter(Boolean);
      if (queueKeys.length) {
        await prisma.ozonUnarchiveQueueItem.deleteMany({ where: { queueKey: { notIn: queueKeys }, status: { not: "success" } } });
      } else {
        await prisma.ozonUnarchiveQueueItem.deleteMany({ where: { status: { not: "success" } } });
      }
      await writePrismaStateChunks(rows, ozonUnarchiveQueueWriteChunkSize, (data) =>
        prisma.ozonUnarchiveQueueItem.upsert({
          where: { queueKey: data.queueKey },
          create: data,
          update: ozonUnarchiveQueueItemUpdateData(data),
        })
      );
      await writeOzonUnarchiveDailyState(payload.daily);
      if (!jsonFallbackEnabled() || !stateJsonBackupOnPostgresWrite) return payload;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      rememberStateWarning("ozon_unarchive_queue_write_postgres", error, { items: payload.items.length });
      logger.warn("write ozon unarchive queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  return writeOzonUnarchiveQueueJson(payload);
}

async function writeOzonUnarchiveQueueJson(payload = {}) {
  await fs.mkdir(dataDir, { recursive: true });
  await writeOzonUnarchiveDailyState(payload.daily);
  const tmpPath = `${ozonUnarchiveQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, ozonUnarchiveQueuePath);
  return payload;
}

function pickOzonUnarchiveQueueItems(queue = {}, products = []) {
  const wanted = new Set((Array.isArray(products) ? products : []).map(ozonUnarchiveQueueKey).filter(Boolean));
  if (!wanted.size) return [];
  return normalizeOzonUnarchiveQueue(queue).items.filter((item) => wanted.has(ozonUnarchiveQueueKey(item)));
}

async function writeOzonUnarchiveQueueDelta(queue = {}, { upsertProducts = [], removeProducts = [] } = {}) {
  const payload = normalizeOzonUnarchiveQueue({
    ...queue,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const removeKeys = (Array.isArray(removeProducts) ? removeProducts : []).map(ozonUnarchiveQueueKey).filter(Boolean);
      for (const chunk of chunkArray(removeKeys, ozonUnarchiveQueueWriteChunkSize)) {
        await prisma.ozonUnarchiveQueueItem.deleteMany({ where: { queueKey: { in: chunk } } });
      }
      const rows = pickOzonUnarchiveQueueItems(payload, upsertProducts).map(ozonUnarchiveQueueItemToPostgres);
      await writePrismaStateChunks(rows, ozonUnarchiveQueueWriteChunkSize, (data) =>
        prisma.ozonUnarchiveQueueItem.upsert({
          where: { queueKey: data.queueKey },
          create: data,
          update: ozonUnarchiveQueueItemUpdateData(data),
        })
      );
      await writeOzonUnarchiveDailyState(payload.daily);
      if (!jsonFallbackEnabled()) return payload;
      if (stateJsonBackupOnPostgresWrite) return writeOzonUnarchiveQueueJson(payload);
      return payload;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      rememberStateWarning("ozon_unarchive_queue_delta_write_postgres", error, {
        upsert: Array.isArray(upsertProducts) ? upsertProducts.length : 0,
        remove: Array.isArray(removeProducts) ? removeProducts.length : 0,
      });
      logger.warn("write ozon unarchive queue delta postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  return writeOzonUnarchiveQueueJson(payload);
}
