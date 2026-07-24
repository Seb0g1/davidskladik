function normalizeSupplierCartState(input = {}) {
  const processed = input.processed && typeof input.processed === "object" && !Array.isArray(input.processed)
    ? input.processed
    : {};
  const supplierBlocks = input.supplierBlocks && typeof input.supplierBlocks === "object" && !Array.isArray(input.supplierBlocks)
    ? input.supplierBlocks
    : {};
  const draft = input.draft && typeof input.draft === "object" && !Array.isArray(input.draft)
    ? input.draft
    : null;
  const history = Array.isArray(input.history) ? input.history : [];
  return {
    updatedAt: input.updatedAt || null,
    processed,
    supplierBlocks,
    draft: draft ? {
      id: cleanText(draft.id) || crypto.randomUUID(),
      generatedAt: draft.generatedAt || null,
      generatedBy: cleanText(draft.generatedBy),
      params: draft.params && typeof draft.params === "object" ? draft.params : {},
      rows: Array.isArray(draft.rows) ? draft.rows.map(normalizeSupplierCartPreviewRow) : [],
      summary: draft.summary && typeof draft.summary === "object" ? draft.summary : {},
    } : null,
    history: history
      .filter((item) => item && typeof item === "object")
      .slice(-1000),
  };
}

function supplierCartDraftRowToPostgres(draftId, row = {}) {
  const normalized = normalizeSupplierCartPreviewRow(row);
  return {
    draftId,
    cartKey: normalized.key,
    marketplace: normalized.marketplace || null,
    accountName: normalized.accountName || null,
    orderId: normalized.orderId || null,
    postingNumber: normalized.postingNumber || null,
    offerId: normalized.offerId || null,
    productName: normalized.productName || null,
    quantity: normalized.quantity,
    supplierName: normalized.supplierName || null,
    partnerId: normalized.partnerId || null,
    offerRowId: normalized.offerRowId || null,
    price: normalized.price || null,
    priceCurrency: normalized.priceCurrency || null,
    supplierScore: normalized.supplierScore || null,
    ready: Boolean(normalized.ready),
    alreadyCommitted: Boolean(normalized.alreadyCommitted),
    skipReason: normalized.skipReason || null,
    requestDocId: normalized.requestDocId || null,
    requestRowId: normalized.requestRowId || null,
    raw: normalized,
  };
}

function supplierCartDraftRowFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return normalizeSupplierCartPreviewRow({
    ...raw,
    key: row.cartKey || raw.key,
    marketplace: row.marketplace || raw.marketplace,
    accountName: row.accountName || raw.accountName,
    orderId: row.orderId || raw.orderId,
    postingNumber: row.postingNumber || raw.postingNumber,
    offerId: row.offerId || raw.offerId,
    productName: row.productName || raw.productName,
    quantity: row.quantity ?? raw.quantity,
    supplierName: row.supplierName || raw.supplierName,
    partnerId: row.partnerId || raw.partnerId,
    offerRowId: row.offerRowId || raw.offerRowId,
    price: row.price === null || row.price === undefined ? raw.price : Number(row.price),
    priceCurrency: row.priceCurrency || raw.priceCurrency,
    supplierScore: row.supplierScore === null || row.supplierScore === undefined ? raw.supplierScore : Number(row.supplierScore),
    ready: row.ready,
    alreadyCommitted: row.alreadyCommitted,
    skipReason: row.skipReason || raw.skipReason,
    requestDocId: row.requestDocId || raw.requestDocId,
    requestRowId: row.requestRowId || raw.requestRowId,
  });
}

function supplierBlockFromPostgres(row = {}) {
  const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
  return {
    ...raw,
    key: row.blockKey,
    offerId: row.offerId,
    partnerId: row.partnerId,
    supplierName: row.supplierName || raw.supplierName || "",
    reason: row.reason || raw.reason || "",
    sourceKey: row.sourceKey || raw.sourceKey || "",
    blockedBy: row.blockedBy || raw.blockedBy || "",
    blockedAt: row.blockedAt?.toISOString?.() || raw.blockedAt || "",
    expiresAt: row.expiresAt?.toISOString?.() || raw.expiresAt || "",
    active: row.active !== false,
  };
}

async function readSupplierCartState() {
  if (shouldUsePostgresStorage()) {
    try {
      const [draft, blocks] = await Promise.all([
        getPrisma().supplierCartDraft.findFirst({
          where: { active: true },
          include: { rows: { orderBy: { createdAt: "asc" } } },
          orderBy: { generatedAt: "desc" },
        }),
        getPrisma().supplierBlock.findMany({
          where: { active: true, expiresAt: { gt: new Date() } },
          orderBy: { expiresAt: "asc" },
          take: 5000,
        }),
      ]);
      const jsonState = await fs.readFile(supplierCartStatePath, "utf8")
        .then((text) => normalizeSupplierCartState(JSON.parse(text || "{}")))
        .catch(() => normalizeSupplierCartState());
      const supplierBlocks = { ...(jsonState.supplierBlocks || {}) };
      for (const block of blocks.map(supplierBlockFromPostgres)) {
        if (block.key) supplierBlocks[block.key] = block;
      }
      return normalizeSupplierCartState({
        ...jsonState,
        supplierBlocks,
        draft: draft ? {
          id: draft.id,
          generatedAt: draft.generatedAt?.toISOString?.() || null,
          generatedBy: draft.generatedBy || "",
          params: draft.params || {},
          rows: (draft.rows || []).map(supplierCartDraftRowFromPostgres),
          summary: draft.summary || {},
        } : jsonState.draft,
      });
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("read supplier cart state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  try {
    return normalizeSupplierCartState(JSON.parse(await fs.readFile(supplierCartStatePath, "utf8")));
  } catch (error) {
    if (error.code === "ENOENT") return normalizeSupplierCartState();
    throw error;
  }
}

async function writeSupplierCartState(state = {}) {
  const normalized = normalizeSupplierCartState({
    ...state,
    updatedAt: new Date().toISOString(),
  });
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      if (normalized.draft?.id) {
        await prisma.$transaction(async (tx) => {
          await tx.supplierCartDraft.updateMany({ where: { active: true, id: { not: normalized.draft.id } }, data: { active: false } });
          await tx.supplierCartDraft.upsert({
            where: { id: normalized.draft.id },
            create: {
              id: normalized.draft.id,
              generatedAt: toDateOrNull(normalized.draft.generatedAt) || new Date(),
              generatedBy: normalized.draft.generatedBy || null,
              marketplace: cleanText(normalized.draft.params?.marketplace || "all") || "all",
              from: toDateOrNull(normalized.draft.params?.from),
              to: toDateOrNull(normalized.draft.params?.to),
              summary: normalized.draft.summary || {},
              params: normalized.draft.params || {},
              active: true,
            },
            update: {
              generatedAt: toDateOrNull(normalized.draft.generatedAt) || new Date(),
              generatedBy: normalized.draft.generatedBy || null,
              marketplace: cleanText(normalized.draft.params?.marketplace || "all") || "all",
              from: toDateOrNull(normalized.draft.params?.from),
              to: toDateOrNull(normalized.draft.params?.to),
              summary: normalized.draft.summary || {},
              params: normalized.draft.params || {},
              active: true,
            },
          });
          await tx.supplierCartDraftRow.deleteMany({ where: { draftId: normalized.draft.id } });
          const rows = (normalized.draft.rows || []).map((row) => supplierCartDraftRowToPostgres(normalized.draft.id, row));
          for (const batch of chunkArray(rows, 500)) {
            if (batch.length) await tx.supplierCartDraftRow.createMany({ data: batch, skipDuplicates: true });
          }
        });
      }
      const blocks = Object.values(normalized.supplierBlocks || {}).filter((block) => block && typeof block === "object");
      for (const block of blocks) {
        const key = cleanText(block.key || supplierBlockKey(block.offerId, block.partnerId));
        const offerId = cleanText(block.offerId);
        const partnerId = cleanText(block.partnerId);
        const expiresAt = toDateOrNull(block.expiresAt);
        if (!key || !offerId || !partnerId || !expiresAt) continue;
        await prisma.supplierBlock.upsert({
          where: { blockKey: key },
          create: {
            blockKey: key,
            offerId,
            partnerId,
            supplierName: cleanText(block.supplierName) || null,
            reason: cleanText(block.reason) || null,
            sourceKey: cleanText(block.sourceKey) || null,
            blockedBy: cleanText(block.blockedBy) || null,
            blockedAt: toDateOrNull(block.blockedAt) || new Date(),
            expiresAt,
            active: block.active !== false,
            raw: block,
          },
          update: {
            supplierName: cleanText(block.supplierName) || null,
            reason: cleanText(block.reason) || null,
            sourceKey: cleanText(block.sourceKey) || null,
            blockedBy: cleanText(block.blockedBy) || null,
            blockedAt: toDateOrNull(block.blockedAt) || new Date(),
            expiresAt,
            active: block.active !== false,
            raw: block,
          },
        });
      }
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write supplier cart state postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  // Prune processed entries older than 30 days to keep the JSON file small.
  // Orders that old are always fulfilled — old keys can't reappear in the cart.
  const processedCutoffMs = Date.now() - 30 * 24 * 60 * 60 * 1000;
  const prunedProcessed = {};
  for (const [key, entry] of Object.entries(normalized.processed || {})) {
    const committedAt = toDateOrNull(entry?.committedAt);
    if (!committedAt || committedAt.getTime() >= processedCutoffMs) {
      prunedProcessed[key] = entry;
    }
  }
  const toWrite = { ...normalized, processed: prunedProcessed };
  await fs.mkdir(dataDir, { recursive: true });
  const temporaryPath = `${supplierCartStatePath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(toWrite), "utf8");
  await fs.rename(temporaryPath, supplierCartStatePath);
  return normalized;
}

function supplierBlockKey(offerId = "", partnerId = "") {
  return `${cleanText(offerId).toLowerCase()}|${cleanText(partnerId).toLowerCase()}`;
}

function activeSupplierBlocksForOffer(state = {}, offerId = "", now = new Date()) {
  const normalizedOffer = cleanText(offerId).toLowerCase();
  const blocks = state.supplierBlocks && typeof state.supplierBlocks === "object" ? state.supplierBlocks : {};
  const active = new Set();
  for (const block of Object.values(blocks)) {
    if (!block || typeof block !== "object") continue;
    if (cleanText(block.offerId).toLowerCase() !== normalizedOffer) continue;
    const expiresAt = toDateOrNull(block.expiresAt);
    if (expiresAt && expiresAt.getTime() <= now.getTime()) continue;
    const partnerId = cleanText(block.partnerId).toLowerCase();
    if (partnerId) active.add(partnerId);
  }
  return active;
}

