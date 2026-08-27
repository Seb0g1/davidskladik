function priceMasterSnapshotSearchHaystack(row = {}) {
  const fields = priceMasterSnapshotRowFields(row);
  return normalizeSearchText([fields.article, fields.name, fields.partnerName].join(" "));
}

function snapshotRowMatchesPriceMasterSearch(row = {}, { q = "", supplier = "", tokenGroups = null } = {}) {
  const fields = priceMasterSnapshotRowFields(row);
  const supplierLower = normalizeSearchText(supplier);
  if (supplierLower && !normalizeSearchText(fields.partnerName).includes(supplierLower)) return false;
  if (tokenGroups && tokenGroups.length) {
    const hay = [fields.name, fields.article].join(" ");
    return pmPassesSearchFilter(hay, tokenGroups);
  }
  const search = normalizeSearchText(q);
  if (!search) return true;
  const source = priceMasterSnapshotSearchHaystack(row);
  return search.split(" ").every((token) => source.includes(token));
}

function searchPriceMasterSnapshotJsonRows(rows = [], { q = "", supplier = "", limit = 100, usdRate = 95 } = {}) {
  const tokenGroups = q ? pmQueryToTokenGroups(q) : null;
  const unique = new Map();
  for (const row of rows) {
    if (!snapshotRowMatchesPriceMasterSearch(row, { q, supplier, tokenGroups })) continue;
    const mapped = mapPriceMasterSearchResponseRow(row, usdRate);
    const key = [mapped.rowId, mapped.article, mapped.supplierName, mapped.name].join("|");
    if (!unique.has(key)) unique.set(key, mapped);
    if (unique.size >= limit) break;
  }
  return Array.from(unique.values());
}

async function readSnapshot() {
  if (priceMasterSnapshotMemoryCache) return priceMasterSnapshotMemoryCache;
  try {
    const text = await fs.readFile(snapshotPath, "utf8");
    // Десятки МБ одним JSON.parse — секунды блокировки event loop (watchdog
    // считает worker мёртвым); маркер атрибутирует блокировку в логе.
    const closeMarker = setEventLoopBlockMarker("pricemaster_snapshot_parse");
    try {
      priceMasterSnapshotMemoryCache = JSON.parse(text);
    } finally {
      closeMarker();
    }
    return priceMasterSnapshotMemoryCache;
  } catch (error) {
    if (error.code === "ENOENT" || error instanceof SyntaxError) {
      // Битый снапшот (процесс убит на середине 77-МБ записи до перехода на
      // атомарный write) валил daily sync каждый день: «Unterminated string in
      // JSON». Карантиним файл и начинаем с пустого — следующий синк PriceMaster
      // пересоберёт его из MySQL.
      if (error instanceof SyntaxError) {
        logger.warn("price master snapshot corrupted, quarantining", { detail: error.message });
        await fs.rename(snapshotPath, `${snapshotPath}.corrupt`).catch(() => {});
      }
      priceMasterSnapshotMemoryCache = { createdAt: null, items: {}, changes: [] };
      return priceMasterSnapshotMemoryCache;
    }
    throw error;
  }
}

async function writeSnapshot(snapshot) {
  await fs.mkdir(dataDir, { recursive: true });
  // Атомарно: tmp + rename — обрыв процесса на середине записи (~77 МБ) не
  // оставляет усечённый snapshot.json (см. паттерн writeOzonUnarchiveDailyState).
  const tmpPath = `${snapshotPath}.${process.pid}.${Date.now()}.${crypto.randomUUID()}.tmp`;
  // Компактный JSON вместо pretty-print: 77 МБ файла — во многом отступы, а
  // stringify(…, null, 2) блокировал event loop в разы дольше компактного.
  const closeMarker = setEventLoopBlockMarker("pricemaster_snapshot_stringify");
  let serialized;
  try {
    serialized = JSON.stringify(snapshot);
  } finally {
    closeMarker();
  }
  await fs.writeFile(tmpPath, serialized, "utf8");
  try {
    await fs.rename(tmpPath, snapshotPath);
  } catch (renameError) {
    if (renameError?.code !== "ENOENT") throw renameError;
    await fs.unlink(tmpPath).catch(() => {});
  }
  priceMasterSnapshotMemoryCache = snapshot;
  priceMasterArticleIndexCache = null;
  priceMasterSnapshotIndexCache = null;
  priceMasterLinkLookupCache.clear();
  priceMasterSearchCache.clear();
  await writePriceMasterSnapshotToPostgres(snapshot).catch((error) => {
    logger.warn("PriceMaster postgres snapshot write failed", { detail: error?.message || String(error) });
  });
  invalidateWarehouseViewCache();
}

function stablePriceMasterSnapshotId(row = {}) {
  return crypto
    .createHash("sha1")
    .update([
      cleanText(row.article || row.NativeID || row.nativeId),
      cleanText(row.partnerId || row.PartnerID),
      cleanText(row.rowId || row.RowID),
      cleanText(row.name || row.nativeName || row.NativeName),
      cleanText(row.docDate || row.DocDate),
    ].join("|"))
    .digest("hex");
}

function priceMasterSnapshotArticleKey(row = {}) {
  const article = cleanText(row.article || row.NativeID || row.nativeId);
  if (article) return article;
  const rowId = cleanText(row.rowId || row.RowID);
  if (rowId) return `__no_article__:${rowId}`;
  return `__no_article__:${stablePriceMasterSnapshotId(row)}`;
}

function normalizePriceMasterSnapshotItemForPostgres(row = {}, updatedAt = new Date()) {
  const article = priceMasterSnapshotArticleKey(row);
  const rawPrice = row.price ?? row.NativePrice;
  const price = rawPrice === undefined || rawPrice === null || rawPrice === "" ? null : String(rawPrice);
  const currency = cleanText(row.currency || row.priceCurrency).toUpperCase();
  return {
    id: stablePriceMasterSnapshotId(row),
    rowId: cleanText(row.rowId || row.RowID) || null,
    article,
    partnerId: cleanText(row.partnerId || row.PartnerID) || null,
    partnerName: cleanText(row.partnerName || row.PartnerName) || null,
    nativeName: cleanText(row.name || row.nativeName || row.NativeName) || null,
    price,
    currency: currency === "RUB" || currency === "RUR" ? "RUB" : "USD",
    docDate: toDateOrNull(row.docDate || row.DocDate),
    active: row.active !== false && row.Active !== false && row.Active !== 0,
    raw: row,
    updatedAt,
  };
}

async function writePriceMasterSnapshotToPostgres(snapshot = {}) {
  if (!shouldUsePostgresStorage()) return { skipped: true, reason: "postgres_disabled" };
  const prisma = getPrisma();
  if (!prisma) return { skipped: true, reason: "no_prisma" };
  const rows = Object.values(snapshot.items || {});
  if (!rows.length) return { skipped: true, reason: "empty_snapshot" };

  const existingCount = await prisma.priceMasterSnapshotItem.count();
  const changes = Array.isArray(snapshot.changes) ? snapshot.changes.length : 0;
  if (existingCount === rows.length && changes === 0) {
    return { skipped: true, reason: "unchanged", items: rows.length };
  }

  // Capture partner row counts before the wipe so we can detect partner disappearances.
  const partnerCountsBefore = await prisma.$queryRawUnsafe(`
    SELECT partner_id, MAX(partner_name) AS partner_name,
           COUNT(*) FILTER (WHERE active = true)::int AS active_count
    FROM pm_snapshot_items
    WHERE partner_id IS NOT NULL
    GROUP BY partner_id
  `).catch(() => []);

  const updatedAt = toDateOrNull(snapshot.createdAt) || new Date();
  const normalizedRows = rows
    .map((row) => normalizePriceMasterSnapshotItemForPostgres(row, updatedAt))
    .filter(Boolean);
  await prisma.priceMasterSnapshotItem.deleteMany({});
  for (const chunk of chunkArray(normalizedRows, 2000)) {
    await prisma.priceMasterSnapshotItem.createMany({ data: chunk, skipDuplicates: true });
  }

  // Detect partners that lost a significant number of active rows.
  if (partnerCountsBefore.length) {
    const afterCountsByPartner = new Map();
    for (const row of normalizedRows) {
      if (row.active && row.partnerId) {
        const entry = afterCountsByPartner.get(row.partnerId) || { count: 0, name: row.partnerName };
        entry.count += 1;
        afterCountsByPartner.set(row.partnerId, entry);
      }
    }
    const drops = [];
    for (const before of partnerCountsBefore) {
      const beforeN = Number(before.active_count || 0);
      if (beforeN < 5) continue; // ignore tiny partners — noise
      const after = afterCountsByPartner.get(before.partner_id);
      const afterN = after ? after.count : 0;
      const lostPct = beforeN > 0 ? (beforeN - afterN) / beforeN : 0;
      if (lostPct >= 0.2 && beforeN - afterN >= 5) {
        drops.push({
          name: before.partner_name || before.partner_id,
          before: beforeN,
          after: afterN,
          lost: beforeN - afterN,
        });
      }
    }
    if (drops.length) {
      const msg = drops.map((d) => `• ${d.name}: было ${d.before} → стало ${d.after} (−${d.lost})`).join("\n");
      logger.warn("pm_partner_rows_dropped", { drops });
      // sendHealthAlertTelegram is defined later in the assembled module (02f-health-alert-monitor.js)
      // and is always available by the time any snapshot write runs (server already fully started).
      if (typeof sendHealthAlertTelegram === "function") {
        sendHealthAlertTelegram(
          `⚠️ DavidSklad: поставщики потеряли строки в PriceMaster:\n${msg}\nПроверьте — товары могут потерять цену.`
        ).catch(() => {});
      }
    }
  }

  logger.info("PriceMaster postgres snapshot written", {
    items: normalizedRows.length,
    changes,
    previousItems: existingCount,
  });
  return { items: normalizedRows.length, changes };
}

async function getPriceMasterSnapshotMeta() {
  const snapshot = await readSnapshot();
  const items = snapshot.items || {};
  const changes = Array.isArray(snapshot.changes) ? snapshot.changes : [];
  return {
    syncId: snapshot.syncId || null,
    updatedAt: snapshot.createdAt || null,
    items: Object.keys(items).length,
    changes: changes.length,
  };
}

async function getPriceMasterSnapshotMetaFast() {
  if (priceMasterSnapshotMemoryCache) return getPriceMasterSnapshotMeta();
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      if (prisma) {
        const [items, aggregate] = await Promise.all([
          prisma.priceMasterSnapshotItem.count(),
          prisma.priceMasterSnapshotItem.aggregate({ _max: { updatedAt: true } }),
        ]);
        return {
          syncId: null,
          updatedAt: aggregate?._max?.updatedAt ? aggregate._max.updatedAt.toISOString() : null,
          items,
          changes: 0,
        };
      }
    } catch (error) {
      logger.warn("fast PriceMaster snapshot meta failed", { detail: error?.message || String(error) });
    }
  }
  return { syncId: null, updatedAt: null, items: 0, changes: 0 };
}

function sortPriceMasterSnapshotRows(rows = []) {
  rows.sort((a, b) => new Date(b.docDate || 0) - new Date(a.docDate || 0) || Number(b.rowId || 0) - Number(a.rowId || 0));
  return rows;
}

async function getPriceMasterSnapshotIndexes() {
  const snapshot = await readSnapshot();
  if (priceMasterSnapshotIndexCache?.syncId === snapshot.syncId && priceMasterSnapshotIndexCache?.createdAt === snapshot.createdAt) {
    return priceMasterSnapshotIndexCache.indexes;
  }
  // Индексация ~267k строк — заметная синхронная работа: маркер атрибутирует
  // блокировку event loop в логе event_loop_blocked.
  const closeMarker = setEventLoopBlockMarker("pricemaster_snapshot_index_build");
  const indexes = {
    byArticle: new Map(),
    byName: new Map(),
    byRowId: new Map(),
    rows: Object.values(snapshot.items || {}),
  };
  try {
    for (const row of indexes.rows) {
      const article = cleanText(row.article || row.NativeID || row.nativeId);
      if (article) {
        if (!indexes.byArticle.has(article)) indexes.byArticle.set(article, []);
        indexes.byArticle.get(article).push(row);
      }
      const name = cleanText(row.name || row.nativeName || row.NativeName).toLowerCase();
      if (name) {
        if (!indexes.byName.has(name)) indexes.byName.set(name, []);
        indexes.byName.get(name).push(row);
      }
      const rowId = cleanText(row.rowId || row.RowID);
      if (rowId) {
        if (!indexes.byRowId.has(rowId)) indexes.byRowId.set(rowId, []);
        indexes.byRowId.get(rowId).push(row);
      }
    }
    for (const rows of indexes.byArticle.values()) sortPriceMasterSnapshotRows(rows);
    for (const rows of indexes.byName.values()) sortPriceMasterSnapshotRows(rows);
    for (const rows of indexes.byRowId.values()) sortPriceMasterSnapshotRows(rows);
  } finally {
    closeMarker();
  }
  priceMasterSnapshotIndexCache = {
    syncId: snapshot.syncId || null,
    createdAt: snapshot.createdAt || null,
    indexes,
  };
  priceMasterArticleIndexCache = {
    syncId: snapshot.syncId || null,
    createdAt: snapshot.createdAt || null,
    index: indexes.byArticle,
  };
  return indexes;
}

async function getPriceMasterArticleIndex() {
  const snapshot = await readSnapshot();
  if (priceMasterArticleIndexCache?.syncId === snapshot.syncId && priceMasterArticleIndexCache?.createdAt === snapshot.createdAt) {
    return priceMasterArticleIndexCache.index;
  }
  return (await getPriceMasterSnapshotIndexes()).byArticle;
}

async function readPriceRetryQueue() {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().priceRetryQueueItem.findMany({
        where: { status: { in: ["pending", "processing", "failed", "delayed"] } },
        orderBy: { createdAt: "desc" },
        take: 5000,
      });
      const updatedAt = rows.reduce((latest, row) => {
        const time = row.updatedAt ? row.updatedAt.getTime() : 0;
        return time > latest ? time : latest;
      }, 0);
      return {
        updatedAt: updatedAt ? new Date(updatedAt).toISOString() : null,
        items: rows.map(priceRetryQueueItemFromPostgres),
      };
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      rememberStateWarning("price_retry_queue_read_postgres", error);
      logger.warn("read price retry queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    const text = await fs.readFile(priceRetryQueuePath, "utf8");
    if (!text.trim()) return { updatedAt: null, items: [] };
    const parsed = JSON.parse(text);
    return {
      updatedAt: parsed.updatedAt || null,
      items: Array.isArray(parsed.items) ? parsed.items : [],
    };
  } catch (error) {
    if (error.code === "ENOENT") return { updatedAt: null, items: [] };
    if (error instanceof SyntaxError) {
      logger.warn("price retry queue is invalid, resetting in memory", { detail: error.message });
      return { updatedAt: null, items: [] };
    }
    throw error;
  }
}

async function writePriceRetryQueue(queue) {
  const payload = {
    updatedAt: new Date().toISOString(),
    items: Array.isArray(queue.items) ? queue.items : [],
  };
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const rows = payload.items.map(priceRetryQueueItemToPostgres);
      const queueKeys = rows.map((item) => item.queueKey).filter(Boolean);
      if (queueKeys.length) {
        await prisma.priceRetryQueueItem.deleteMany({ where: { queueKey: { notIn: queueKeys } } });
      } else {
        await prisma.priceRetryQueueItem.deleteMany({});
      }
      await writePrismaStateChunks(rows, priceRetryQueueWriteChunkSize, (data) =>
        prisma.priceRetryQueueItem.upsert({
          where: { queueKey: data.queueKey },
          create: data,
          update: priceRetryQueueItemUpdateData(data),
        })
      );
      if (!jsonFallbackEnabled() || !stateJsonBackupOnPostgresWrite) return payload;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      rememberStateWarning("price_retry_queue_write_postgres", error, { items: payload.items.length });
      logger.warn("write price retry queue postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const tmpPath = `${priceRetryQueuePath}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmpPath, priceRetryQueuePath);
  return payload;
}

