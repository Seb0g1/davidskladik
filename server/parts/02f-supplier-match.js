// «Подбор поставщиков» — новые строки PriceMaster для товаров склада, привязка только после одобрения.
//
//   Вкладка «Запустить в продажу» (tab = launch): у товара нет привязок или ни одна привязка не держится
//     на живой строке последнего прайса — товар не продаётся. Ищем строки, на которых его можно запустить.
//   Вкладка «Новые привязки» (tab = extra): товар продаётся — ищем строки поставщиков, которых у него ещё нет.
//
// Кто такой товар: fragrantica_card_matches (карточка → аромат Фрагрантики, объём, тестер; строит
// runFragranticaShopStockMatch). Строки ищем тем же сопоставлением, что и конвейер (matchFragranticaPmIndex:
// бренд, все слова названия, без фланкеров / клонов / лосьонов, лишние числа = другой аромат), плюс
// тот же объём, тестер = тестер, без пробников, концентрация, проверка цены-заглушки, без остановленных
// поставщиков. «Надёжное» совпадение — объём и концентрация совпали и нет замечаний.
//
// Скан — в воркере раз в SUPPLIER_MATCH_HOURS (деф. 3) или по кнопке (запрос через supplier_match_state).
// Одобрение — в API: привязка ко всем карточкам товара (все магазины), затем обычная активация привязанных.

const supplierMatchEnabled = process.env.SUPPLIER_MATCH_ENABLED !== "false";
const supplierMatchIntervalMs = Math.max(1, Number(process.env.SUPPLIER_MATCH_HOURS || 3) || 3) * 3_600_000;
const SUPPLIER_MATCH_EXTRA_PER_PRODUCT = 5;
let supplierMatchRunning = false;
let supplierMatchTablesReady = false;
let supplierMatchApproveJob = null;

async function requireSupplierMatchTables() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres недоступен");
  if (supplierMatchTablesReady) return prisma;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS supplier_match_suggestions (
      id BIGSERIAL PRIMARY KEY,
      offer_key TEXT NOT NULL,
      row_key TEXT NOT NULL,
      tab TEXT NOT NULL,
      product_ids JSONB NOT NULL DEFAULT '[]',
      shops JSONB NOT NULL DEFAULT '[]',
      product_name TEXT NOT NULL DEFAULT '',
      card_status TEXT NOT NULL DEFAULT '',
      archived BOOLEAN NOT NULL DEFAULT false,
      sold30 INTEGER NOT NULL DEFAULT 0,
      row_data JSONB NOT NULL DEFAULT '{}',
      ozon_price INTEGER,
      confidence TEXT NOT NULL DEFAULT 'probable',
      issues JSONB NOT NULL DEFAULT '[]',
      status TEXT NOT NULL DEFAULT 'new',
      error TEXT,
      seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      decided_at TIMESTAMPTZ,
      decided_by TEXT,
      UNIQUE (offer_key, row_key)
    )`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS supplier_match_suggestions_tab_idx ON supplier_match_suggestions (tab, status, sold30 DESC)`);
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS supplier_match_state (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL DEFAULT '{}',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  supplierMatchTablesReady = true;
  return prisma;
}

async function readSupplierMatchState(key) {
  const prisma = await requireSupplierMatchTables();
  const [row] = await prisma.$queryRawUnsafe(`SELECT value FROM supplier_match_state WHERE key = $1`, key);
  return row?.value || {};
}

async function writeSupplierMatchState(key, value) {
  const prisma = await requireSupplierMatchTables();
  await prisma.$executeRawUnsafe(
    `INSERT INTO supplier_match_state (key, value, updated_at) VALUES ($1, $2::jsonb, now())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
    key, JSON.stringify(value || {}),
  );
}

// shorthand concentrations of supplier price lists («YSL Opium lady dt», «т/в», «п/в»)
function supplierMatchRowConcentration(text) {
  const t = String(text || "").toLowerCase().replace(/ё/g, "е");
  const found = fragRowConcentration(fragRowTokens(t), t);
  if (found) return found;
  if (/(^|[^a-zа-я])(dt|т\s*[/.]\s*в)([^a-zа-я]|$)/.test(t)) return "edt";
  if (/(^|[^a-zа-я])(dp|п\s*[/.]\s*в)([^a-zа-я]|$)/.test(t)) return "edp";
  return "";
}

// damaged / discounted goods: a fine row to sell from, but never an «exact» one
const SUPPLIER_MATCH_DEFECT_RE = /(подмят|мят(ая|ый|ой)?\s*(короб|упак)|без\s*(короб|упак|крыш|слюд|целлофан)|уценк|брак|дефект|царап|поврежд|витрин|damaged|no\s*box|without\s*box|unbox)/i;

// Suppliers never offered in «Подбор поставщиков» and Фрагрантика link suggestions (shared list, editable on both pages).
// Until the list is saved for the first time these are excluded.
const SUPPLIER_MATCH_DEFAULT_EXCLUDED = [
  { partnerId: "77", name: "Сима 2218" },
  { partnerId: "49", name: "Армен Арютюрян" },
  { partnerId: "277", name: "Сафронова (марка)" },
];
let supplierExcludedCache = { at: 0, list: null };

async function supplierMatchExcludedList() {
  if (supplierExcludedCache.list && Date.now() - supplierExcludedCache.at < 60_000) return supplierExcludedCache.list;
  const state = await readSupplierMatchState("excluded").catch(() => ({}));
  const list = Array.isArray(state.suppliers) ? state.suppliers : SUPPLIER_MATCH_DEFAULT_EXCLUDED;
  supplierExcludedCache = { at: Date.now(), list };
  return list;
}

/** Keys (partner id / supplier name, normalised) of the excluded suppliers. */
async function supplierMatchExcludedKeys() {
  const keys = new Set();
  for (const s of await supplierMatchExcludedList()) {
    if (s.partnerId) keys.add(supplierMatchKey(s.partnerId));
    if (s.name) keys.add(supplierMatchKey(s.name));
  }
  return keys;
}

/** Pure: is a PriceMaster row (partnerId / supplierName) from an excluded supplier. */
function supplierRowExcluded(row = {}, keys = new Set()) {
  return [row.partnerId, row.supplierName, row.partnerName].some((v) => v && keys.has(supplierMatchKey(v)));
}

const supplierMatchKey = (value) => String(value ?? "").trim().toLowerCase().replace(/\s+/g, " ");

/** All active rows of the latest price lists with what a link needs (article, row id, partner, price, currency). */
async function loadSupplierMatchPmRows(suppliers = []) {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const cte = await pmLatestDocsCteSql();
  const [rows] = await pool.query(
    `${cte}
     SELECT r.NativeID AS article, r.NativeName AS name, r.NativePrice AS price, r.Active AS active, r.RowID AS rowId,
            d.DocDate AS docDate, d.PartnerID AS partnerId, p.PartnerName AS partnerName
     FROM pm_latest_docs ld
     JOIN OfferDocs d ON d.DocID = ld.DocID
     JOIN OfferRows r ON r.DocID = d.DocID
     LEFT JOIN Partners p ON p.PartnerID = d.PartnerID
     WHERE r.Ignored = 0 AND r.Active != 0 AND r.NativePrice > 0`,
  );
  const maps = managedSupplierMaps(suppliers);
  return { usdRate, settings, rows: rows.map((row) => mapPriceMasterSearchResponseRow(row, usdRate, maps)).filter((row) => row.available) };
}

/**
 * Pure: candidate rows for one product. group = { name, brand, perfumeName, volume, tester, links }, index = PM index
 * of mapped rows. Returns [{ row, confidence, issues }] (already linked rows and stopped suppliers excluded).
 */
function supplierMatchCandidates(group, index, { stoppedPartners = new Set(), newSuppliersOnly = false } = {}) {
  if (!group.volume) return [];
  const perfume = { brand: group.brand, name: group.perfumeName };
  const m = matchFragranticaPmIndex(index, perfume, { strictNumbers: true, keepTesters: true });
  if (!m.count) return [];
  const linkedRows = new Set();
  const linkedKeys = new Set();
  const linkedPartners = new Set();
  for (const link of group.links || []) {
    if (link.sourceRowId) linkedRows.add(String(link.sourceRowId));
    const partners = [supplierMatchKey(link.partnerId), supplierMatchKey(link.supplierName)].filter(Boolean);
    for (const p of partners) {
      linkedPartners.add(p);
      if (link.supplierArticle) linkedKeys.add(`a|${supplierMatchKey(link.supplierArticle)}|${p}`);
      if (link.exactName) linkedKeys.add(`n|${supplierMatchKey(link.exactName)}|${p}`);
    }
  }
  const cardConcentration = supplierMatchRowConcentration(group.name);
  // a name word that is also a brand word must be there twice: «Ormonde Jayne Ormonde Elixir» ≠ «Ormonde Jayne Ta'if Elixir»
  const brandWords = new Set(fragRowTokens(group.brand));
  const doubledWords = [...new Set(fragRowTokens(group.perfumeName))].filter((w) => brandWords.has(w));
  const typeKey = cardConcentration || "edp";
  const out = [];
  for (const i of m.matched || []) {
    const row = index.rows[i];
    const partners = [supplierMatchKey(row.partnerId), supplierMatchKey(row.supplierName)].filter(Boolean);
    if (partners.some((p) => stoppedPartners.has(p))) continue;
    if (row.rowId && linkedRows.has(String(row.rowId))) continue;
    if (partners.some((p) => linkedKeys.has(`a|${supplierMatchKey(row.article)}|${p}`) || linkedKeys.has(`n|${supplierMatchKey(row.name)}|${p}`))) continue;
    if (newSuppliersOnly && partners.some((p) => linkedPartners.has(p))) continue;
    // a tester card takes tester rows only, a bottle — never a tester / sample / decant
    if (isTesterOrDecantSupplierRowName(row.name) !== Boolean(group.tester)) continue;
    if (isSingleSampleName(row.name)) continue;
    if (FRAG_SET_NAME_RE.test(row.name)) continue;
    const volumes = fragPmVolumes(row.name);
    if (!volumes.some((v) => Math.abs(v - Number(group.volume)) < 0.01)) continue;
    if (supplierRowVolumeMismatch(group.name, row.name)) continue;
    if (priceGuardRowProblem(row, group.name)) continue;
    if (doubledWords.length) {
      const rowTokens = fragRowTokens(row.name);
      if (doubledWords.some((w) => rowTokens.filter((t) => t === w).length < 2)) continue;
    }
    const check = assessFragranticaSupplierRow(row.name, { brand: group.brand, name: group.perfumeName, typeKey });
    if (check.clone || check.notPerfume) continue;
    const rowConcentration = check.concentration || supplierMatchRowConcentration(row.name);
    if (rowConcentration && cardConcentration && rowConcentration !== cardConcentration) continue;
    const issues = [];
    if (SUPPLIER_MATCH_DEFECT_RE.test(row.name)) issues.push("уценка / повреждение");
    if (!rowConcentration) issues.push("концентрация не указана");
    if (!cardConcentration) issues.push("у карточки не указана концентрация");
    out.push({ row, confidence: issues.length ? "probable" : "exact", issues });
  }
  return out;
}

async function runSupplierMatchScan(trigger = "schedule") {
  if (supplierMatchRunning) return { status: "already_running" };
  supplierMatchRunning = true;
  const startedAt = Date.now();
  try {
    const prisma = await requireSupplierMatchTables();
    await writeSupplierMatchState("scan", { ...(await readSupplierMatchState("scan")), running: true, startedAt: new Date(startedAt).toISOString() });
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier);
    const stoppedPartners = new Set();
    for (const s of suppliers) {
      if (s.stopped !== true && s.active !== false) continue;
      if (s.partnerId) stoppedPartners.add(supplierMatchKey(s.partnerId));
      if (s.name) stoppedPartners.add(supplierMatchKey(s.name));
    }
    for (const key of await supplierMatchExcludedKeys()) stoppedPartners.add(key);
    const pm = await loadSupplierMatchPmRows(warehouse.suppliers || []);
    const index = buildFragranticaPmIndex(pm.rows);
    const liveIndex = await getLiveSupplierIndex();

    // cards that know their perfume, grouped by article (one product = its cards in every shop)
    const cards = await prisma.$queryRawUnsafe(`
      SELECT w.id, w.offer_id AS "offerId", w.target, w.marketplace::text AS marketplace, w.name, w.archived,
             m.volume_ml AS volume, m.tester, p.brand, p.name AS "perfumeName"
        FROM fragrantica_card_matches m
        JOIN warehouse_products w ON w.target = m.target AND w.offer_id = m.offer_id
        JOIN fragrantica_perfumes p ON p.id = m.perfume_id`);
    const links = await prisma.$queryRawUnsafe(`
      SELECT l.product_id AS "productId", l.supplier_article AS "supplierArticle", l.supplier_name AS "supplierName",
             l.partner_id AS "partnerId", l.source_row_id AS "sourceRowId", l.exact_name AS "exactName"
        FROM product_links l
        JOIN warehouse_products w ON w.id = l.product_id
        JOIN fragrantica_card_matches m ON m.target = w.target AND m.offer_id = w.offer_id`);
    const linksByProduct = new Map();
    for (const l of links) {
      if (!linksByProduct.has(l.productId)) linksByProduct.set(l.productId, []);
      linksByProduct.get(l.productId).push(l);
    }
    const sales = await prisma.$queryRawUnsafe(`
      SELECT lower(offer_id) AS offer, sum(quantity)::int AS qty FROM finance_orders
       WHERE source = 'marketplace_sync' AND offer_id IS NOT NULL AND coalesce(sold_at, created_at) > now() - interval '30 days'
         AND status NOT ILIKE '%cancel%'
       GROUP BY 1`).catch(() => []);
    const soldByOffer = new Map(sales.map((r) => [r.offer, Number(r.qty || 0)]));

    const groups = new Map();
    for (const card of cards) {
      const key = supplierMatchKey(card.offerId);
      if (!key) continue;
      if (!groups.has(key)) {
        groups.set(key, { key, name: card.name, brand: card.brand, perfumeName: card.perfumeName, volume: card.volume === null ? null : Number(card.volume), tester: Boolean(card.tester), ids: [], shops: [], links: [], archived: true });
      }
      const g = groups.get(key);
      g.ids.push(card.id);
      g.shops.push(card.target);
      if (!card.archived) g.archived = false;
      // the volume / tester must agree on every card, otherwise the article is ambiguous
      if (g.volume !== (card.volume === null ? null : Number(card.volume)) || g.tester !== Boolean(card.tester)) g.volume = null;
      g.links.push(...(linksByProduct.get(card.id) || []));
    }

    const records = [];
    let launchGroups = 0;
    let extraGroups = 0;
    for (const g of groups.values()) {
      const live = g.links.length ? productHasLiveSupplierRow({ links: g.links.map((l) => ({ ...l, article: l.supplierArticle })) }, liveIndex) : null;
      const tab = live === true ? "extra" : "launch";
      const cardStatus = !g.links.length ? "no_links" : live === true ? "selling" : "no_supplier";
      let candidates = supplierMatchCandidates(g, index, { stoppedPartners, newSuppliersOnly: tab === "extra" });
      if (!candidates.length) continue;
      // one row per supplier (the cheapest), exact first; «extra» keeps the best few
      const bySupplier = new Map();
      for (const c of candidates) {
        const supplierKey = supplierMatchKey(c.row.partnerId || c.row.supplierName);
        const prev = bySupplier.get(supplierKey);
        const better = !prev || (c.confidence === "exact" && prev.confidence !== "exact")
          || (c.confidence === prev.confidence && Number(c.row.price) < Number(prev.row.price));
        if (better) bySupplier.set(supplierKey, c);
      }
      candidates = [...bySupplier.values()].sort((a, b) => Number(b.confidence === "exact") - Number(a.confidence === "exact") || Number(a.row.price) - Number(b.row.price));
      if (tab === "extra") candidates = candidates.slice(0, SUPPLIER_MATCH_EXTRA_PER_PRODUCT);
      if (tab === "extra") extraGroups += 1; else launchGroups += 1;
      for (const c of candidates) {
        const { price } = fragranticaRowPrice(c.row, { usdRate: pm.usdRate, settings: pm.settings });
        records.push({
          offerKey: g.key,
          rowKey: c.row.rowId ? `r:${c.row.rowId}` : `a:${supplierMatchKey(c.row.article)}|${supplierMatchKey(c.row.partnerId || c.row.supplierName)}`,
          tab,
          productIds: g.ids,
          shops: [...new Set(g.shops)],
          productName: g.name,
          cardStatus,
          archived: g.archived,
          sold30: soldByOffer.get(g.key) || 0,
          row: { rowId: c.row.rowId, article: c.row.article, name: c.row.name, supplierName: c.row.supplierName, partnerId: c.row.partnerId, price: c.row.price, priceCurrency: c.row.priceCurrency, updatedAt: c.row.updatedAt },
          ozonPrice: Number.isFinite(price) ? Math.round(price) : null,
          confidence: c.confidence,
          issues: c.issues,
        });
      }
    }

    const scanAt = new Date(startedAt).toISOString();
    for (let i = 0; i < records.length; i += 1000) {
      await prisma.$executeRawUnsafe(
        `INSERT INTO supplier_match_suggestions (offer_key, row_key, tab, product_ids, shops, product_name, card_status, archived, sold30, row_data, ozon_price, confidence, issues, seen_at)
         SELECT x->>'offerKey', x->>'rowKey', x->>'tab', x->'productIds', x->'shops', x->>'productName', x->>'cardStatus', (x->>'archived')::boolean,
                (x->>'sold30')::int, x->'row', NULLIF(x->>'ozonPrice', '')::int, x->>'confidence', x->'issues', $2::timestamptz
           FROM jsonb_array_elements($1::jsonb) x
         ON CONFLICT (offer_key, row_key) DO UPDATE SET
           tab = EXCLUDED.tab, product_ids = EXCLUDED.product_ids, shops = EXCLUDED.shops, product_name = EXCLUDED.product_name,
           card_status = EXCLUDED.card_status, archived = EXCLUDED.archived, sold30 = EXCLUDED.sold30, row_data = EXCLUDED.row_data,
           ozon_price = EXCLUDED.ozon_price, confidence = EXCLUDED.confidence, issues = EXCLUDED.issues, seen_at = EXCLUDED.seen_at,
           status = CASE WHEN supplier_match_suggestions.status IN ('failed', 'linked') THEN 'new' ELSE supplier_match_suggestions.status END,
           error = NULL`,
        JSON.stringify(records.slice(i, i + 1000)), scanAt,
      );
    }
    // a suggestion that is gone (row left the price list, product got linked) is not offered any more
    const removed = await prisma.$executeRawUnsafe(`DELETE FROM supplier_match_suggestions WHERE status IN ('new', 'failed') AND seen_at < $1::timestamptz`, scanAt);
    const result = {
      status: "ok", trigger, pmRows: pm.rows.length, products: groups.size, launchProducts: launchGroups, extraProducts: extraGroups,
      suggestions: records.length, removed: Number(removed || 0), elapsedMs: Date.now() - startedAt,
    };
    await writeSupplierMatchState("scan", { ...result, running: false, at: new Date().toISOString() });
    logger.info("supplier match scan complete", result);
    return result;
  } catch (error) {
    logger.warn("supplier match scan failed", { detail: error?.message || String(error) });
    await writeSupplierMatchState("scan", { running: false, status: "error", error: error?.message || String(error), at: new Date().toISOString() }).catch(() => {});
    return { status: "error", error: error?.message || String(error) };
  } finally {
    supplierMatchRunning = false;
  }
}

/** Worker: every minute — a requested scan (button) or the regular one when the interval has passed. */
function scheduleSupplierMatch(delayMs = 60_000) {
  if (!supplierMatchEnabled) return;
  setTimeout(async () => {
    try {
      const scan = await readSupplierMatchState("scan");
      const request = await readSupplierMatchState("scan_request");
      const lastAt = scan.at ? new Date(scan.at).getTime() : 0;
      const requestedAt = request.at ? new Date(request.at).getTime() : 0;
      if (requestedAt > lastAt || Date.now() - lastAt >= supplierMatchIntervalMs) {
        await runSupplierMatchScan(requestedAt > lastAt ? "button" : "schedule");
      }
    } catch (error) {
      logger.warn("supplier match tick failed", { detail: error?.message || String(error) });
    } finally {
      scheduleSupplierMatch(60_000);
    }
  }, Math.max(10_000, Number(delayMs) || 60_000)).unref?.();
}

// ─── Одобрение: привязать строку ко всем карточкам товара ────────────────────

async function addSupplierMatchLinks(productIds = [], rows = [], username = "supplier-match") {
  const settings = await readAppSettings();
  const usdRate = Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95;
  const changed = [];
  for (const productId of productIds) {
    await withWarehouseProductMutationLock([productId], async () => {
      const current = await findWarehouseProductById(productId);
      if (!current) return;
      await hydrateWarehouseProductsForIds([productId], { expandGroups: true });
      const warehouse = await readWarehouse();
      const context = normalizeWarehouseProduct(current);
      const now = new Date().toISOString();
      current.links = Array.isArray(current.links) ? current.links : [];
      let added = 0;
      for (const row of rows) {
        let link = normalizeWarehouseLink(fragranticaLinkDraft(row));
        link = await resolvePriceMasterLinkForSave(link, usdRate, warehouse.suppliers, { live: true, timeoutMs: 2500, cacheEmpty: false, productContext: context });
        if (current.links.some((item) => warehouseLinksEqualForSave(item, link))) continue;
        current.links.push(normalizeWarehouseLink({ ...link, raw: { ...(link.raw || {}), createdBy: "supplier_match" }, createdAt: now, updatedAt: now, createdBy: username, updatedBy: username }));
        added += 1;
      }
      if (!added) return;
      current.links = compactWarehouseLinks(current.links);
      current.autoPriceEnabled = true;
      current.everHadLinks = true;
      current.updatedAt = now;
      current.userUpdatedAt = now;
      await withWarehouseMutation(async () => {
        await writeWarehouseProductPatch([current], { reason: "warehouse_link_save" });
      });
      changed.push(productId);
    });
  }
  if (changed.length) {
    await queueLinkedProductActivation(changed, "supplier_match_link", warehouseLinkActivationRequestMeta(changed, { username }))
      .catch((error) => logger.warn("supplier match activation failed", { detail: error?.message }));
    void triggerLinkedProductStockSync(changed, "supplier_match_link").catch(() => {});
  }
  return changed;
}

/** Approves suggestions (grouped per product): links, then marks them linked / failed. */
async function approveSupplierMatchIds(ids = [], username = "supplier-match", onProgress = () => {}) {
  const prisma = await requireSupplierMatchTables();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id, offer_key, product_ids, row_data FROM supplier_match_suggestions WHERE id = ANY($1::bigint[]) AND status = 'new'`,
    ids.map((id) => String(Number(id))).filter((id) => id !== "NaN"),
  );
  const byOffer = new Map();
  for (const r of rows) {
    if (!byOffer.has(r.offer_key)) byOffer.set(r.offer_key, { productIds: r.product_ids || [], items: [] });
    byOffer.get(r.offer_key).items.push(r);
  }
  const result = { linked: 0, failed: 0, products: 0 };
  for (const group of byOffer.values()) {
    const groupIds = group.items.map((r) => String(r.id));
    try {
      const changed = await addSupplierMatchLinks(group.productIds, group.items.map((r) => r.row_data), username);
      await prisma.$executeRawUnsafe(
        `UPDATE supplier_match_suggestions SET status = 'linked', decided_at = now(), decided_by = $2, error = NULL WHERE id = ANY($1::bigint[])`,
        groupIds, username,
      );
      result.linked += group.items.length;
      if (changed.length) result.products += 1;
    } catch (error) {
      result.failed += group.items.length;
      await prisma.$executeRawUnsafe(
        `UPDATE supplier_match_suggestions SET status = 'failed', error = $2 WHERE id = ANY($1::bigint[])`,
        groupIds, String(error?.message || error).slice(0, 300),
      );
    }
    onProgress(result);
  }
  return result;
}

// ─── Роуты ───────────────────────────────────────────────────────────────────

/** «Только от поставщиков»: "77,49" or ["77", "Сима 2218"] → normalised keys (partner id or name). */
function supplierMatchOnlyKeys(value) {
  const list = Array.isArray(value) ? value : String(value || "").split(",");
  return [...new Set(list.map((v) => supplierMatchKey(v)).filter(Boolean))].slice(0, 100);
}

app.get("/api/supplier-match", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireSupplierMatchTables();
    const tab = request.query.tab === "extra" ? "extra" : "launch";
    const exactOnly = request.query.confidence === "exact";
    const q = cleanText(request.query.q).toLowerCase();
    const limit = Math.min(100, Math.max(1, Number(request.query.limit) || 40));
    const offset = Math.max(0, Number(request.query.offset) || 0);
    const excluded = [...(await supplierMatchExcludedKeys())];
    // «Только от поставщиков»: partner ids / names, empty = everyone
    const only = supplierMatchOnlyKeys(request.query.only);
    const where = `tab = $1 AND status IN ('new', 'failed') AND ($2::boolean = false OR confidence = 'exact')
      AND NOT (lower(coalesce(row_data->>'partnerId', '')) = ANY($6::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($6::text[]))
      AND (cardinality($7::text[]) = 0 OR lower(coalesce(row_data->>'partnerId', '')) = ANY($7::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($7::text[]))
      AND ($3 = '' OR lower(product_name) LIKE '%' || $3 || '%' OR offer_key LIKE '%' || $3 || '%' OR lower(row_data->>'supplierName') LIKE '%' || $3 || '%')`;
    const offers = await prisma.$queryRawUnsafe(
      `SELECT offer_key, max(sold30) AS sold30 FROM supplier_match_suggestions WHERE ${where}
        GROUP BY offer_key ORDER BY max(sold30) DESC, offer_key LIMIT $4 OFFSET $5`,
      tab, exactOnly, q, limit, offset, excluded, only,
    );
    const keys = offers.map((o) => o.offer_key);
    const items = keys.length ? await prisma.$queryRawUnsafe(
      `SELECT * FROM supplier_match_suggestions WHERE offer_key = ANY($1::text[]) AND tab = $2 AND status IN ('new', 'failed')
         AND ($3::boolean = false OR confidence = 'exact')
         AND NOT (lower(coalesce(row_data->>'partnerId', '')) = ANY($4::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($4::text[]))
         AND (cardinality($5::text[]) = 0 OR lower(coalesce(row_data->>'partnerId', '')) = ANY($5::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($5::text[]))
        ORDER BY (confidence = 'exact') DESC, ozon_price NULLS LAST`,
      keys, tab, exactOnly, excluded, only,
    ) : [];
    const byKey = new Map(keys.map((k) => [k, null]));
    for (const it of items) {
      if (!byKey.get(it.offer_key)) {
        byKey.set(it.offer_key, {
          offerKey: it.offer_key, productName: it.product_name, productIds: it.product_ids, shops: it.shops,
          cardStatus: it.card_status, archived: it.archived, sold30: it.sold30, suggestions: [],
        });
      }
      byKey.get(it.offer_key).suggestions.push({
        id: String(it.id), row: it.row_data, ozonPrice: it.ozon_price, confidence: it.confidence, issues: it.issues, status: it.status, error: it.error,
      });
    }
    const [counts] = await prisma.$queryRawUnsafe(`
      SELECT count(DISTINCT offer_key) FILTER (WHERE tab = 'launch')::int AS "launchProducts",
             count(*) FILTER (WHERE tab = 'launch' AND confidence = 'exact')::int AS "launchExact",
             count(DISTINCT offer_key) FILTER (WHERE tab = 'extra')::int AS "extraProducts",
             count(*) FILTER (WHERE tab = 'extra' AND confidence = 'exact')::int AS "extraExact",
             count(DISTINCT offer_key) FILTER (WHERE tab = $1 AND ($2::boolean = false OR confidence = 'exact'))::int AS "filtered",
             count(*) FILTER (WHERE tab = $1 AND confidence = 'exact' AND (cardinality($4::text[]) = 0
               OR lower(coalesce(row_data->>'partnerId', '')) = ANY($4::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($4::text[])))::int AS "exactSelected"
        FROM supplier_match_suggestions WHERE status IN ('new', 'failed')
         AND NOT (lower(coalesce(row_data->>'partnerId', '')) = ANY($3::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($3::text[]))`, tab, exactOnly, excluded, only);
    // suppliers of this tab for the «Только от поставщиков» picker
    const suppliers = await prisma.$queryRawUnsafe(`
      SELECT coalesce(row_data->>'partnerId', '') AS "partnerId", max(row_data->>'supplierName') AS name,
             count(DISTINCT offer_key)::int AS products, count(*) FILTER (WHERE confidence = 'exact')::int AS exact
        FROM supplier_match_suggestions WHERE tab = $1 AND status IN ('new', 'failed')
         AND NOT (lower(coalesce(row_data->>'partnerId', '')) = ANY($2::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($2::text[]))
       GROUP BY 1 ORDER BY 3 DESC`, tab, excluded);
    response.json({
      ok: true,
      tab,
      products: [...byKey.values()].filter(Boolean),
      counts,
      suppliers,
      scan: await readSupplierMatchState("scan"),
      scanRequest: await readSupplierMatchState("scan_request"),
      approveJob: supplierMatchApproveJob,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-match/scan", requireAdmin, async (request, response, next) => {
  try {
    await writeSupplierMatchState("scan_request", { at: new Date().toISOString(), by: requestUsername(request) });
    response.json({ ok: true, queued: true });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-match/approve", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireSupplierMatchTables();
    const username = requestUsername(request) || "admin";
    if (request.body?.allExact) {
      if (supplierMatchApproveJob?.running) return response.status(409).json({ error: "Уже идёт массовое одобрение." });
      const tab = request.body.tab === "extra" ? "extra" : "launch";
      const only = supplierMatchOnlyKeys(request.body.only);
      const rows = await prisma.$queryRawUnsafe(
        `SELECT id FROM supplier_match_suggestions WHERE tab = $1 AND status = 'new' AND confidence = 'exact'
           AND NOT (lower(coalesce(row_data->>'partnerId', '')) = ANY($2::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($2::text[]))
           AND (cardinality($3::text[]) = 0 OR lower(coalesce(row_data->>'partnerId', '')) = ANY($3::text[]) OR lower(coalesce(row_data->>'supplierName', '')) = ANY($3::text[]))
         ORDER BY sold30 DESC, id`,
        tab, [...(await supplierMatchExcludedKeys())], only,
      );
      const ids = rows.map((r) => String(r.id));
      supplierMatchApproveJob = { running: true, tab, total: ids.length, linked: 0, failed: 0, products: 0, startedAt: new Date().toISOString() };
      response.json({ ok: true, queued: ids.length });
      approveSupplierMatchIds(ids, username, (p) => Object.assign(supplierMatchApproveJob, p))
        .then((r) => { Object.assign(supplierMatchApproveJob, r, { running: false, finishedAt: new Date().toISOString() }); })
        .catch((error) => { Object.assign(supplierMatchApproveJob, { running: false, error: error?.message || String(error) }); });
      await appendAudit(request, "supplier_match.approve_all", { entityType: "supplier_match", entityId: tab, newValue: { total: ids.length } }).catch(() => {});
      return;
    }
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).slice(0, 100);
    if (!ids.length) return response.status(400).json({ error: "Не выбраны предложения." });
    const result = await approveSupplierMatchIds(ids, username);
    await appendAudit(request, "supplier_match.approve", { entityType: "supplier_match", entityId: ids.join(","), newValue: result }).catch(() => {});
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/supplier-match/reject", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireSupplierMatchTables();
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map((id) => String(Number(id))).filter((id) => id !== "NaN");
    if (!ids.length) return response.status(400).json({ error: "Не выбраны предложения." });
    const updated = await prisma.$executeRawUnsafe(
      `UPDATE supplier_match_suggestions SET status = 'rejected', decided_at = now(), decided_by = $2 WHERE id = ANY($1::bigint[]) AND status IN ('new', 'failed')`,
      ids, requestUsername(request) || "admin",
    );
    response.json({ ok: true, rejected: Number(updated || 0) });
  } catch (error) {
    next(error);
  }
});

// ─── Исключённые поставщики (общий список: «Подбор поставщиков» + Фрагрантика) ──

app.get("/api/supplier-match/excluded", requireAdmin, async (request, response, next) => {
  try {
    const warehouse = await readWarehouse();
    const suppliers = (warehouse.suppliers || []).map(normalizeManagedSupplier)
      .filter((s) => s.name)
      .map((s) => ({ partnerId: cleanText(s.partnerId), name: s.name }))
      .sort((a, b) => a.name.localeCompare(b.name, "ru"));
    response.json({ ok: true, excluded: await supplierMatchExcludedList(), suppliers });
  } catch (error) {
    next(error);
  }
});

app.put("/api/supplier-match/excluded", requireAdmin, async (request, response, next) => {
  try {
    const list = (Array.isArray(request.body?.suppliers) ? request.body.suppliers : [])
      .map((s) => ({ partnerId: cleanText(s?.partnerId), name: cleanText(s?.name) }))
      .filter((s) => s.partnerId || s.name)
      .slice(0, 200);
    await writeSupplierMatchState("excluded", { suppliers: list, by: requestUsername(request), at: new Date().toISOString() });
    supplierExcludedCache = { at: 0, list: null };
    await appendAudit(request, "supplier_match.excluded", { entityType: "supplier_match", entityId: "excluded", newValue: { suppliers: list.map((s) => s.name) } }).catch(() => {});
    response.json({ ok: true, excluded: list });
  } catch (error) {
    next(error);
  }
});
