// «Конвейер» страницы «Фрагрантика»: много ароматов за раз, минимум кликов.
//
//   POST   /api/fragrantica/drafts            — в конвейер: { perfumeIds } или { query } (фильтры списка, «выбрать все»)
//                                               + targets (магазины). Аромат раскладывается на объёмы из PriceMaster.
//   GET    /api/fragrantica/drafts            — черновики со стадиями и статусами отправки
//   PATCH  /api/fragrantica/drafts/:id        — правки (цена, название, магазины, привязки, описание, бренд, тип/объём)
//   POST   /api/fragrantica/drafts/:id/rebuild — собрать заново ({ newDescription } — переписать описание)
//   DELETE /api/fragrantica/drafts/:id
//   POST   /api/fragrantica/drafts/volume      — добавить объём аромату вручную
//   POST   /api/fragrantica/drafts/approve     — одобрить { ids } → worker создаёт карточки
//   POST   /api/fragrantica/drafts/clear       — убрать отправленные / пропущенные
//
// Сборка идёт на worker (runFragranticaDraftsTick): форма Ozon → привязка PriceMaster + цена → фото
// (флакон + «Пирамида аромата» каждого магазина, без ручного одобрения) → описание ИИ (одно на аромат:
// другие объёмы берут уже написанное) → «готово» или «нужно внимание» (чего не хватает).
//
// Статусы: queued → working → ready | attention | skipped; approved → sending → sent | failed.

const fragranticaDraftsEnabled = process.env.FRAGRANTICA_DRAFTS_ENABLED !== "false";
const fragranticaDraftsTickMs = 10_000;
const fragranticaDraftsParallel = Math.max(1, Number(process.env.FRAGRANTICA_DRAFTS_PARALLEL || 2) || 2);
const FRAG_DRAFT_ACTIVE = "('queued', 'working', 'ready', 'attention', 'approved', 'sending', 'failed')";
let fragranticaDraftsTablesReady = false;
let fragranticaDraftsRunning = false;

async function requireFragranticaDraftTables() {
  const prisma = await requireFragranticaTables();
  if (fragranticaDraftsTablesReady) return prisma;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS fragrantica_drafts (
      id BIGSERIAL PRIMARY KEY,
      perfume_id INTEGER NOT NULL,
      kind TEXT NOT NULL DEFAULT 'card',
      volume_ml NUMERIC,
      tester BOOLEAN NOT NULL DEFAULT false,
      type_key TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      stage TEXT,
      targets JSONB NOT NULL DEFAULT '[]'::jsonb,
      data JSONB,
      error TEXT,
      export_ids JSONB,
      created_by TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_drafts_status_idx ON fragrantica_drafts (status, id)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS fragrantica_drafts_perfume_idx ON fragrantica_drafts (perfume_id)`);
  fragranticaDraftsTablesReady = true;
  return prisma;
}

function fragranticaDraftTargets(keys = []) {
  const all = fragranticaTargets();
  return [...new Set((Array.isArray(keys) ? keys : []).map(cleanText))].map((key) => all.find((t) => t.key === key)).filter(Boolean);
}

async function updateFragranticaDraft(id, fields = {}) {
  const prisma = await requireFragranticaDraftTables();
  const sets = [];
  const params = [Number(id)];
  for (const [column, value] of Object.entries(fields)) {
    params.push(value !== null && typeof value === "object" && !(value instanceof Date) ? JSON.stringify(value) : value);
    sets.push(`${column} = $${params.length}${["data", "targets", "export_ids"].includes(column) ? "::jsonb" : ""}`);
  }
  const rows = await prisma.$queryRawUnsafe(`UPDATE fragrantica_drafts SET ${sets.join(", ")}, updated_at = now() WHERE id = $1 RETURNING *`, ...params);
  return rows[0] || null;
}

async function readFragranticaDraft(id) {
  const prisma = await requireFragranticaDraftTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_drafts WHERE id = $1`, Number(id));
  return rows[0] || null;
}

async function insertFragranticaDraft({ perfumeId, kind = "card", volume = null, tester = false, typeKey = null, targets = [], createdBy = null, data = null }) {
  const prisma = await requireFragranticaDraftTables();
  const rows = await prisma.$queryRawUnsafe(
    `INSERT INTO fragrantica_drafts (perfume_id, kind, volume_ml, tester, type_key, status, targets, created_by, data)
     VALUES ($1, $2, $3, $4, $5, 'queued', $6::jsonb, $7, $8::jsonb) RETURNING *`,
    Number(perfumeId), kind, volume === null ? null : Number(volume), Boolean(tester), typeKey, JSON.stringify(targets), createdBy, data ? JSON.stringify(data) : null,
  );
  return rows[0];
}

/** Rows of this perfume's live exports (for «уже есть в магазине» by volume). */
async function fragranticaPerfumePresence(perfumeId) {
  const prisma = await requireFragranticaDraftTables();
  const exports = await prisma.$queryRawUnsafe(
    `SELECT account_id AS "accountId", volume_ml AS volume, tester, status FROM fragrantica_exports WHERE perfume_id = $1`,
    Number(perfumeId),
  );
  const [row] = await prisma.$queryRawUnsafe(`SELECT shop_stock FROM fragrantica_perfumes WHERE id = $1`, Number(perfumeId));
  return { exports: exports.map((e) => ({ ...e, volume: e.volume === null ? null : Number(e.volume) })), stock: row?.shop_stock || {} };
}

function fragranticaTargetsMissingVolume(targets, presence, volume, tester = false) {
  return targets.filter((t) => !fragranticaVolumeInShop({ ...presence, shopId: t.id, volume, tester }));
}

// ─── Сборка ────────────────────────────────────────────────────────────────

// Аромат → черновики по объёмам из PriceMaster (только тех магазинов, где этого объёма ещё нет).
async function expandFragranticaPerfumeDraft(draft) {
  let perfume;
  try {
    perfume = await fragranticaPerfumeForExport(Number(draft.perfume_id));
  } catch (error) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Страница аромата не скачана: ${error?.message || error}. Откройте аромат и загрузите его закладкой «→ Склад».` });
  }
  if (!perfume.notes && !perfume.accords) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: "Фрагрантика не отдала страницу аромата (нет пирамиды). Откройте аромат и загрузите его закладкой «→ Склад», затем «Собрать заново»." });
  }
  const { rows } = await fragranticaSearchPmRows(fragNameWithBrand(perfume), 80);
  const plan = planFragranticaVolumes(rows, perfume);
  const typeKey = FRAG_OZON_TYPES.some((t) => t.key === draft.type_key) ? draft.type_key : plan.typeKey;
  const targets = fragranticaDraftTargets(draft.targets);
  const presence = await fragranticaPerfumePresence(draft.perfume_id);
  const prisma = await requireFragranticaDraftTables();
  const active = await prisma.$queryRawUnsafe(
    `SELECT volume_ml AS volume, tester FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'card' AND status IN ${FRAG_DRAFT_ACTIVE}`,
    Number(draft.perfume_id),
  );
  let created = 0;
  const skipped = [];
  for (const { volume } of plan.volumes) {
    if (active.some((a) => !a.tester && Math.abs(Number(a.volume) - volume) < 0.01)) continue;
    const missing = fragranticaTargetsMissingVolume(targets, presence, volume);
    if (!missing.length) {
      skipped.push(volume);
      continue;
    }
    await insertFragranticaDraft({ perfumeId: draft.perfume_id, volume, typeKey, targets: missing.map((t) => t.key), createdBy: draft.created_by });
    created += 1;
  }
  if (created) {
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE id = $1`, Number(draft.id));
    return null;
  }
  const reason = !plan.volumes.length
    ? "В PriceMaster нет подходящих строк с объёмом — добавьте объём вручную."
    : `Все объёмы из PriceMaster (${plan.volumes.map((v) => v.volume).join(", ")} мл) уже есть в выбранных магазинах.`;
  return updateFragranticaDraft(draft.id, { status: "skipped", stage: null, type_key: typeKey, error: reason, data: { skippedVolumes: skipped } });
}

async function buildFragranticaCardDraft(draft) {
  const perfumeId = Number(draft.perfume_id);
  const targets = fragranticaDraftTargets(draft.targets);
  const volume = fragFormatVolume(draft.volume_ml);
  const tester = Boolean(draft.tester);
  const stage = (name) => updateFragranticaDraft(draft.id, { stage: name });
  const warnings = [];

  await stage("form");
  const ozonTarget = targets.find((t) => t.kind === "ozon");
  const form = await buildFragranticaFormData({ perfumeId, typeKey: draft.type_key, volume, tester, accountId: ozonTarget?.id });
  const typeKey = form.typeKey;

  await stage("links");
  let linkRows = [];
  let selected = [];
  let preview = null;
  try {
    const suggestions = await fragranticaLinkSuggestionsData({ perfumeId, typeKey, volume, tester });
    linkRows = suggestions.rows.slice(0, 12);
    selected = suggestions.suggested.filter((id) => linkRows.some((r) => r.id === id));
    const rows = linkRows.filter((r) => selected.includes(r.id));
    if (rows.length) preview = await fragranticaPricePreview(rows);
    else warnings.push("Подходящих строк поставщика нет — цену поставьте вручную или отметьте строку.");
  } catch (error) {
    warnings.push(`Привязка: ${error?.message || error}`);
  }

  await stage("photos");
  const styles = [...new Set(targets.map((t) => t.style))];
  let images = { main: form.sourceImage, notes: {} };
  if (styles.length) {
    const result = await runFragranticaImageJob({}, { perfumeId, styles, refresh: false });
    images = { main: result.main || form.sourceImage, notes: result.notes || {}, specs: result.specs || {}, closeup: result.closeup || null };
    warnings.push(...(result.warnings || []));
  }

  await stage("description");
  // Одно описание на аромат: если другой объём уже написан/отправлен — берём его, ИИ не зовём
  let description = await readFragranticaCardDescription(perfumeId).catch(() => "");
  let descriptionSource = description ? "shared" : "";
  if (!description) {
    try {
      const res = await generateFragranticaDescription({ perfumeId, typeKey, tester, marketplace: targets.some((t) => t.kind === "yandex") ? "yandex" : "ozon" });
      description = res.description;
      descriptionSource = "ai";
    } catch (error) {
      description = cleanText((form.attributes.find((a) => a.id === FRAG_OZON_ATTR.annotation)?.values || [])[0]?.value);
      descriptionSource = description ? "fragrantica" : "";
      warnings.push(`Описание ИИ не получилось: ${error?.message || error}`);
    }
  }

  const data = {
    name: form.name,
    offerId: form.offerId,
    typeKey,
    volume,
    tester,
    dims: form.dims,
    attributes: form.attributes.filter((a) => a.values.length).map((a) => ({ id: a.id, values: a.values })),
    requiredAttrs: form.attributes.filter((a) => a.required).map((a) => ({ id: a.id, name: a.name })),
    brandMatched: form.brandMatched,
    brandCandidates: form.brandCandidates,
    country: form.country,
    vatByTarget: form.vatByTarget,
    sourceImage: form.sourceImage,
    price: preview?.price || 0,
    oldPrice: preview?.oldPrice || 0,
    yandexPrice: preview?.yandexPrice || preview?.price || 0,
    supplierName: preview?.supplierName || "",
    markup: preview?.markup || 0,
    linkRows,
    selectedLinks: selected,
    images,
    description,
    descriptionSource,
    barcode: "",
    warnings,
  };
  const built = { ...draft, perfumeId, data };
  const missing = fragranticaDraftMissing(built, targets);
  if (!form.brandMatched && !missing.includes("Бренд")) missing.unshift("Бренд");
  return updateFragranticaDraft(draft.id, {
    status: missing.length ? "attention" : "ready",
    stage: null,
    type_key: typeKey,
    error: missing.length ? `Не хватает: ${missing.join(", ")}` : null,
    data: { ...data, missing },
  });
}

async function sendFragranticaDraft(draft) {
  const targets = fragranticaDraftTargets(draft.targets);
  const built = { ...draft, perfumeId: Number(draft.perfume_id) };
  const missing = fragranticaDraftMissing(built, targets);
  if (missing.length) {
    return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Не хватает: ${missing.join(", ")}`, data: { ...(draft.data || {}), missing } });
  }
  try {
    const body = buildFragranticaDraftExportBody(built, targets);
    const result = await createFragranticaExports(body, { session: { username: cleanText(draft.created_by) || "fragrantica" } });
    return updateFragranticaDraft(draft.id, { status: "sent", stage: null, error: null, export_ids: result.exports.map((e) => e.id) });
  } catch (error) {
    const detail = error?.detail?.missing;
    if (Array.isArray(detail) && detail.length) {
      return updateFragranticaDraft(draft.id, { status: "attention", stage: null, error: `Не хватает: ${detail.join(", ")}`, data: { ...(draft.data || {}), missing: detail } });
    }
    return updateFragranticaDraft(draft.id, { status: "failed", stage: null, error: String(error?.message || error).slice(0, 2000) });
  }
}

async function processFragranticaDraft(draft) {
  try {
    if (draft.status === "sending") return await sendFragranticaDraft(draft);
    if (draft.kind === "perfume") return await expandFragranticaPerfumeDraft(draft);
    return await buildFragranticaCardDraft(draft);
  } catch (error) {
    logger.warn("fragrantica draft failed", { id: Number(draft.id), detail: error?.message || String(error) });
    return updateFragranticaDraft(draft.id, {
      status: draft.status === "sending" ? "failed" : "attention",
      stage: null,
      error: String(error?.message || error).slice(0, 2000),
    });
  }
}

// Worker: одобренные — отправить; в очереди — собрать (по ароматам параллельно, объёмы одного аромата — по очереди,
// чтобы фото и описание делались один раз).
async function runFragranticaDraftsTick() {
  if (fragranticaDraftsRunning) return { status: "already_running" };
  fragranticaDraftsRunning = true;
  try {
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(
      `UPDATE fragrantica_drafts SET status = CASE WHEN status = 'sending' THEN 'approved' ELSE 'queued' END, stage = NULL, updated_at = now()
       WHERE status IN ('working', 'sending') AND updated_at < now() - interval '15 minutes'`,
    );
    for (let i = 0; i < 20; i += 1) {
      const [next] = await prisma.$queryRawUnsafe(
        `UPDATE fragrantica_drafts SET status = 'sending', updated_at = now()
         WHERE id = (SELECT id FROM fragrantica_drafts WHERE status = 'approved' ORDER BY updated_at, id LIMIT 1 FOR UPDATE SKIP LOCKED)
         RETURNING *`,
      );
      if (!next) break;
      await processFragranticaDraft(next);
    }
    const claimed = await prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'working', stage = 'start', updated_at = now()
       WHERE id IN (
         SELECT DISTINCT ON (d.perfume_id) d.id FROM fragrantica_drafts d
          WHERE d.status = 'queued'
            AND NOT EXISTS (SELECT 1 FROM fragrantica_drafts w WHERE w.perfume_id = d.perfume_id AND w.status = 'working')
          ORDER BY d.perfume_id, (d.kind = 'perfume') DESC, d.id
          LIMIT ${fragranticaDraftsParallel})
       RETURNING *`,
    );
    await Promise.all(claimed.map((draft) => processFragranticaDraft(draft)));
    return { status: "ok", built: claimed.length };
  } finally {
    fragranticaDraftsRunning = false;
  }
}

function scheduleFragranticaDrafts(delayMs = fragranticaDraftsTickMs) {
  if (!fragranticaDraftsEnabled) return;
  setTimeout(async () => {
    let busy = false;
    try {
      const result = await runFragranticaDraftsTick();
      busy = Boolean(result?.built);
    } catch (error) {
      logger.warn("fragrantica drafts tick failed", { detail: error?.message });
    } finally {
      scheduleFragranticaDrafts(busy ? 1_000 : fragranticaDraftsTickMs);
    }
  }, Math.max(1_000, Number(delayMs) || fragranticaDraftsTickMs)).unref?.();
}

// ─── Роуты ─────────────────────────────────────────────────────────────────

function fragranticaDraftResponse(row, exportsById = new Map()) {
  const data = row.data || {};
  return {
    id: Number(row.id),
    perfumeId: Number(row.perfume_id),
    brand: row.brand || "",
    perfumeName: row.perfume_name || "",
    thumb: fragranticaMediaUrl("thumbs", `${Number(row.perfume_id)}.jpg`),
    kind: row.kind,
    volume: row.volume_ml === null ? null : Number(row.volume_ml),
    tester: Boolean(row.tester),
    typeKey: row.type_key || data.typeKey || "",
    status: row.status,
    stage: row.stage || null,
    targets: Array.isArray(row.targets) ? row.targets : [],
    error: row.error || null,
    data,
    exports: (Array.isArray(row.export_ids) ? row.export_ids : []).map((id) => exportsById.get(Number(id))).filter(Boolean),
    updatedAt: row.updated_at,
  };
}

app.get("/api/fragrantica/drafts", requireAdmin, async (_request, response, next) => {
  try {
    const prisma = await requireFragranticaDraftTables();
    const rows = await prisma.$queryRawUnsafe(
      `SELECT d.*, p.brand, p.name AS perfume_name FROM fragrantica_drafts d
         LEFT JOIN fragrantica_perfumes p ON p.id = d.perfume_id
        WHERE d.status <> 'sent' OR d.updated_at > now() - interval '3 days'
        ORDER BY d.id LIMIT 600`,
    );
    const ids = [...new Set(rows.flatMap((r) => (Array.isArray(r.export_ids) ? r.export_ids : []).map(Number)))];
    let exportRows = ids.length ? await prisma.$queryRawUnsafe(`SELECT * FROM fragrantica_exports WHERE id = ANY($1::bigint[])`, ids) : [];
    // Ozon checks a new card for a minute or two: ask it about a few waiting ones on each poll
    const waiting = exportRows.filter((e) => e.status === "pending" && Date.now() - new Date(e.updated_at).getTime() > 20_000).slice(0, 4);
    if (waiting.length) {
      const fresh = await Promise.all(waiting.map((e) => refreshFragranticaExport(e).catch(() => e)));
      const byId = new Map(fresh.map((e) => [Number(e.id), e]));
      exportRows = exportRows.map((e) => byId.get(Number(e.id)) || e);
    }
    const exportsById = new Map(exportRows.map((e) => [Number(e.id), fragranticaExportFromRow(e)]));
    const counts = {};
    for (const row of rows) counts[row.status] = (counts[row.status] || 0) + 1;
    response.json({ ok: true, items: rows.map((r) => fragranticaDraftResponse(r, exportsById)), counts, targets: fragranticaTargets() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const targets = fragranticaDraftTargets(body.targets);
    if (!targets.length) return response.status(400).json({ error: "Выберите хотя бы один магазин." });
    let perfumeIds = (Array.isArray(body.perfumeIds) ? body.perfumeIds : []).map(Number).filter((id) => id > 0);
    if (!perfumeIds.length && body.query && typeof body.query === "object") {
      // «Выбрать все» по текущим фильтрам списка — не больше 300 ароматов за раз
      const max = Math.min(300, Math.max(1, Number(body.limit) || 300));
      for (let page = 1; perfumeIds.length < max; page += 1) {
        const list = await listFragranticaPerfumes({ ...body.query, limit: 120, page });
        perfumeIds.push(...list.items.map((item) => item.id));
        if (!list.hasMore) break;
      }
      perfumeIds = perfumeIds.slice(0, max);
    }
    perfumeIds = [...new Set(perfumeIds)].slice(0, 300);
    if (!perfumeIds.length) return response.status(400).json({ error: "Нет ароматов для конвейера." });
    const prisma = await requireFragranticaDraftTables();
    const busy = await prisma.$queryRawUnsafe(
      `SELECT DISTINCT perfume_id AS id FROM fragrantica_drafts WHERE perfume_id = ANY($1::int[]) AND status IN ${FRAG_DRAFT_ACTIVE}`,
      perfumeIds,
    );
    const busyIds = new Set(busy.map((r) => Number(r.id)));
    const username = cleanText(request.session?.username) || null;
    let added = 0;
    for (const perfumeId of perfumeIds) {
      if (busyIds.has(perfumeId)) continue;
      await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'perfume' AND status = 'skipped'`, perfumeId);
      await insertFragranticaDraft({ perfumeId, kind: "perfume", typeKey: cleanText(body.typeKey) || null, targets: targets.map((t) => t.key), createdBy: username });
      added += 1;
    }
    await appendAudit(request, "fragrantica.drafts.add", { entityType: "fragrantica_draft", newValue: { added, alreadyInQueue: busyIds.size, shops: targets.map((t) => t.label) } });
    response.json({ ok: true, added, alreadyInQueue: busyIds.size });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/volume", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const perfumeId = Number(body.perfumeId);
    const volume = Number(fragFormatVolume(body.volume));
    const targets = fragranticaDraftTargets(body.targets);
    if (!perfumeId || !volume) return response.status(400).json({ error: "Укажите объём." });
    if (!targets.length) return response.status(400).json({ error: "Выберите хотя бы один магазин." });
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE perfume_id = $1 AND kind = 'perfume' AND status IN ('skipped', 'attention')`, perfumeId);
    const draft = await insertFragranticaDraft({
      perfumeId, volume, tester: Boolean(body.tester), typeKey: FRAG_OZON_TYPES.some((t) => t.key === body.typeKey) ? body.typeKey : null,
      targets: targets.map((t) => t.key), createdBy: cleanText(request.session?.username) || null,
    });
    response.json({ ok: true, id: Number(draft.id) });
  } catch (error) {
    next(error);
  }
});

// Правки черновика. Тип, объём или тестер меняют карточку целиком — она собирается заново (описание остаётся).
app.patch("/api/fragrantica/drafts/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    if (["working", "sending", "sent"].includes(draft.status)) return response.status(409).json({ error: "Черновик сейчас собирается или уже отправлен." });
    const body = request.body || {};
    const data = { ...(draft.data || {}) };
    const fields = {};
    let rebuild = false;
    if (body.targets !== undefined) {
      const targets = fragranticaDraftTargets(body.targets);
      fields.targets = targets.map((t) => t.key);
      // a shop of a new style needs its «Пирамида аромата» — rebuild only the photos part (cached otherwise)
      if (targets.some((t) => !data.images?.notes?.[t.style])) rebuild = true;
    }
    if (body.typeKey !== undefined && FRAG_OZON_TYPES.some((t) => t.key === body.typeKey) && body.typeKey !== (draft.type_key || data.typeKey)) {
      fields.type_key = body.typeKey;
      rebuild = true;
    }
    if (body.volume !== undefined && Number(fragFormatVolume(body.volume)) && Math.abs(Number(fragFormatVolume(body.volume)) - Number(draft.volume_ml)) > 0.001) {
      fields.volume_ml = Number(fragFormatVolume(body.volume));
      rebuild = true;
    }
    if (body.tester !== undefined && Boolean(body.tester) !== Boolean(draft.tester)) {
      fields.tester = Boolean(body.tester);
      rebuild = true;
    }
    for (const key of ["name", "barcode"]) if (body[key] !== undefined) data[key] = cleanText(body[key]);
    for (const key of ["price", "oldPrice", "yandexPrice"]) if (body[key] !== undefined) data[key] = Math.max(0, Math.round(Number(body[key]) || 0));
    if (body.dims && typeof body.dims === "object") {
      data.dims = { ...(data.dims || {}) };
      for (const key of ["depth", "width", "height", "weight"]) if (body.dims[key] !== undefined) data.dims[key] = Math.max(0, Math.round(Number(body.dims[key]) || 0));
    }
    if (Array.isArray(body.selectedLinks)) {
      data.selectedLinks = body.selectedLinks.map(String).filter((id) => (data.linkRows || []).some((r) => r.id === id));
      const rows = (data.linkRows || []).filter((r) => data.selectedLinks.includes(r.id));
      if (rows.length && !body.keepPrice) {
        const preview = await fragranticaPricePreview(rows);
        Object.assign(data, { price: preview.price, oldPrice: preview.oldPrice, yandexPrice: preview.yandexPrice || preview.price, supplierName: preview.supplierName, markup: preview.markup });
      }
    }
    if (body.brand && Number(body.brand.id)) {
      const value = { dictionary_value_id: Number(body.brand.id), value: cleanText(body.brand.value) };
      data.attributes = [...(data.attributes || []).filter((a) => Number(a.id) !== FRAG_OZON_ATTR.brand), { id: FRAG_OZON_ATTR.brand, values: [value] }];
      data.brandMatched = true;
    }
    const prisma = await requireFragranticaDraftTables();
    if (typeof body.description === "string") {
      // Описание общее для аромата: правка уходит во все его черновики и в следующие объёмы
      data.description = body.description;
      data.descriptionSource = "edited";
      await saveFragranticaCardDescription(draft.perfume_id, body.description);
      await prisma.$executeRawUnsafe(
        `UPDATE fragrantica_drafts SET data = jsonb_set(jsonb_set(data, '{description}', to_jsonb($2::text)), '{descriptionSource}', '"edited"'), updated_at = now()
          WHERE perfume_id = $1 AND id <> $3 AND kind = 'card' AND data IS NOT NULL AND status IN ('ready', 'attention', 'failed')`,
        Number(draft.perfume_id), body.description, Number(draft.id),
      );
    }
    if (rebuild) {
      fields.status = "queued";
      fields.stage = null;
      fields.error = null;
    } else {
      const targets = fragranticaDraftTargets(fields.targets || draft.targets);
      const missing = fragranticaDraftMissing({ ...draft, perfumeId: Number(draft.perfume_id), data }, targets);
      if (data.brandMatched === false && !missing.includes("Бренд")) missing.unshift("Бренд");
      data.missing = missing;
      if (["ready", "attention", "failed"].includes(draft.status)) {
        fields.status = missing.length ? "attention" : "ready";
        fields.error = missing.length ? `Не хватает: ${missing.join(", ")}` : null;
      }
    }
    fields.data = data;
    const updated = await updateFragranticaDraft(draft.id, fields);
    response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/:id/rebuild", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const draft = await readFragranticaDraft(request.params.id);
    if (!draft) return response.status(404).json({ error: "Черновик не найден." });
    if (["working", "sending", "sent"].includes(draft.status)) return response.status(409).json({ error: "Черновик сейчас собирается или уже отправлен." });
    if (request.body?.newDescription) {
      // a new AI text replaces the shared description (generateFragranticaDescription saves it) in every draft
      const prisma = await requireFragranticaDraftTables();
      const res = await generateFragranticaDescription({ perfumeId: draft.perfume_id, typeKey: draft.type_key, tester: draft.tester, marketplace: fragranticaDraftTargets(draft.targets).some((t) => t.kind === "yandex") ? "yandex" : "ozon" });
      await prisma.$executeRawUnsafe(
        `UPDATE fragrantica_drafts SET data = jsonb_set(jsonb_set(data, '{description}', to_jsonb($2::text)), '{descriptionSource}', '"ai"'), updated_at = now()
          WHERE perfume_id = $1 AND kind = 'card' AND data IS NOT NULL AND status IN ('ready', 'attention', 'failed')`,
        Number(draft.perfume_id), res.description,
      );
      const updated = await readFragranticaDraft(draft.id);
      return response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
    }
    const updated = await updateFragranticaDraft(draft.id, { status: "queued", stage: null, error: null });
    response.json({ ok: true, draft: fragranticaDraftResponse(updated) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/fragrantica/drafts/:id", requireAdmin, async (request, response, next) => {
  try {
    if (!/^\d+$/.test(request.params.id)) return next();
    const prisma = await requireFragranticaDraftTables();
    await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE id = $1 AND status NOT IN ('working', 'sending')`, Number(request.params.id));
    response.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/approve", requireAdmin, async (request, response, next) => {
  try {
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter((id) => id > 0).slice(0, 600);
    if (!ids.length) return response.status(400).json({ error: "Нечего одобрять." });
    const prisma = await requireFragranticaDraftTables();
    const rows = await prisma.$queryRawUnsafe(
      `UPDATE fragrantica_drafts SET status = 'approved', error = NULL, created_by = COALESCE($2, created_by), updated_at = now()
        WHERE id = ANY($1::bigint[]) AND kind = 'card' AND status IN ('ready', 'failed') RETURNING id`,
      ids, cleanText(request.session?.username) || null,
    );
    await appendAudit(request, "fragrantica.drafts.approve", { entityType: "fragrantica_draft", entityId: rows.map((r) => r.id).join(","), newValue: { approved: rows.length } });
    response.json({ ok: true, approved: rows.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/fragrantica/drafts/clear", requireAdmin, async (request, response, next) => {
  try {
    const allowed = ["sent", "skipped", "attention", "ready", "failed", "queued"];
    const statuses = (Array.isArray(request.body?.statuses) ? request.body.statuses : ["sent", "skipped"]).filter((s) => allowed.includes(s));
    if (!statuses.length) return response.status(400).json({ error: "statuses required" });
    const prisma = await requireFragranticaDraftTables();
    const removed = await prisma.$executeRawUnsafe(`DELETE FROM fragrantica_drafts WHERE status = ANY($1::text[])`, statuses);
    response.json({ ok: true, removed: Number(removed) || 0 });
  } catch (error) {
    next(error);
  }
});
