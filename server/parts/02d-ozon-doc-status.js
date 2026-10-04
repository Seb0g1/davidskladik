// «Документы Ozon» (/app/ozon-docs): какие товары привязаны к декларациям / сертификатам и что с ними —
// на проверке, одобрено, отклонено (и почему). Ozon показывает это только в кабинете по одному товару.
//
//   GET  /api/ozon-docs         — товары с фильтрами (account, status, q, cert, sort, page) + счётчики
//   POST /api/ozon-docs/sync    — собрать заново сейчас (в фоне; статус в GET → sync)
//
// Сбор (runOzonDocStatusSync, worker раз в OZON_DOC_STATUS_MINUTES, деф. 30): по каждому кабинету
// /v1/product/certificate/list → для документов с товарами /v1/product/certificate/products/list →
// /v3/product/info/list (артикул, название, фото, статус карточки «Убран из продажи…»). Справочники
// причин и статусов — /v1/product/certificate/rejection_reasons/list, …/status/list, …/product_status/list.
// Снимок — таблица ozon_doc_status; строки, которых в новом сборе нет, удаляются.

const ozonDocStatusEnabled = process.env.OZON_DOC_STATUS_ENABLED !== "false";
const ozonDocStatusIntervalMs = Math.max(5, Number(process.env.OZON_DOC_STATUS_MINUTES || 30) || 30) * 60_000;
let ozonDocStatusTablesReady = false;
let ozonDocStatusRunning = null;

const OZON_DOC_PRODUCT_STATUS = { approved: "Одобрено", declined: "Отклонено", awaiting_verification: "На проверке" };

async function requireOzonDocStatusTables() {
  const prisma = getPrisma();
  if (ozonDocStatusTablesReady) return prisma;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS ozon_doc_status (
      account_id TEXT NOT NULL,
      product_id BIGINT NOT NULL,
      certificate_id BIGINT NOT NULL,
      account_name TEXT NOT NULL DEFAULT '',
      offer_id TEXT,
      sku BIGINT,
      name TEXT,
      image TEXT,
      certificate_number TEXT,
      certificate_name TEXT,
      certificate_type TEXT,
      certificate_status TEXT,
      certificate_reason TEXT,
      certificate_comment TEXT,
      expire_date TIMESTAMPTZ,
      product_status TEXT,
      product_reason TEXT,
      ozon_status TEXT,
      ozon_status_text TEXT,
      status_changed_at TIMESTAMPTZ,
      seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (account_id, product_id, certificate_id)
    )`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS ozon_doc_status_status_idx ON ozon_doc_status (product_status, account_id)`);
  // the page's default order and the per-account filter
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS ozon_doc_status_changed_idx ON ozon_doc_status (status_changed_at DESC NULLS LAST, product_id DESC)`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS ozon_doc_status_account_idx ON ozon_doc_status (account_id, product_status, status_changed_at DESC)`);
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS ozon_doc_status_state (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  ozonDocStatusTablesReady = true;
  return prisma;
}

async function writeOzonDocStatusState(value) {
  const prisma = await requireOzonDocStatusTables();
  await prisma.$executeRawUnsafe(
    `INSERT INTO ozon_doc_status_state (key, value, updated_at) VALUES ('sync', $1::jsonb, now())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
    JSON.stringify(value || {}),
  );
}

async function readOzonDocStatusState() {
  const prisma = await requireOzonDocStatusTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT value FROM ozon_doc_status_state WHERE key = 'sync'`);
  return rows[0]?.value || {};
}

/** code → Russian name from an Ozon dictionary answer ({ result: [{ code, name }] }). */
function ozonDocDictionary(data) {
  const list = Array.isArray(data?.result) ? data.result : Array.isArray(data?.result?.items) ? data.result.items : [];
  return Object.fromEntries(list.filter((x) => x && x.code).map((x) => [String(x.code), String(x.name || x.code)]));
}

/** Product reason: the product's own refusal fields when Ozon sends them, else the document's reason + comment. */
function ozonDocProductReason(item = {}, cert = {}, reasons = {}) {
  const own = [item.rejection_reason, item.rejection_reason_code, item.error, item.error_message, item.comment, item.verification_comment]
    .map((v) => (v && typeof v === "object" ? v.name || v.message || v.code : v))
    .map((v) => cleanText(v))
    .filter(Boolean)
    .map((v) => reasons[v] || v);
  if (own.length) return [...new Set(own)].join("; ");
  const status = cleanText(item.product_status_code);
  if (status !== "declined" && cleanText(cert.status_code) !== "declined") return "";
  const parts = [reasons[cleanText(cert.rejection_reason_code)] || cleanText(cert.rejection_reason_code), cleanText(cert.verification_comment)].filter(Boolean);
  return parts.join(". ") || "Ozon не указал причину";
}

async function ozonDocListCertificates(account) {
  const out = [];
  for (let page = 1; page <= 200; page += 1) {
    const data = await ozonRequest("/v1/product/certificate/list", { page, page_size: 100 }, account);
    const list = data?.result?.certificates || [];
    out.push(...list);
    const pages = Number(data?.result?.page_count || 0);
    if (!list.length || (pages && page >= pages) || list.length < 100) break;
  }
  return out;
}

// Products of one certificate: last_id + limit (up to 1000 per request; page / page_size are deprecated)
async function ozonDocListCertificateProducts(account, certificateId) {
  const out = [];
  const seen = new Set();
  let lastId = 0;
  for (let round = 0; round < 500; round += 1) {
    const body = { certificate_id: Number(certificateId), limit: 1000 };
    if (lastId) body.last_id = lastId;
    const data = await ozonRequest("/v1/product/certificate/products/list", body, account);
    const items = data?.result?.items || [];
    let fresh = 0;
    for (const item of items) {
      const id = Number(item.product_id);
      if (seen.has(id)) continue;
      seen.add(id);
      out.push(item);
      fresh += 1;
    }
    const nextId = Number(items[items.length - 1]?.product_id) || 0;
    if (items.length < 1000 || !fresh || !nextId || nextId === lastId) break;
    lastId = nextId;
  }
  return out;
}

async function syncOzonDocStatusAccount(account, startedAt, progress) {
  const prisma = await requireOzonDocStatusTables();
  const dict = async (path) => ozonDocDictionary(await ozonRequest(path, {}, account).catch(() => ({})));
  const [reasons, certStatuses] = [await dict("/v1/product/certificate/rejection_reasons/list"), await dict("/v1/product/certificate/status/list")];
  const certificates = await ozonDocListCertificates(account);
  const rows = [];
  // a few certificates at a time — each one is a chain of last_id requests
  const queue = certificates.filter((cert) => Number(cert.products_count || 0) > 0);
  const workers = Array.from({ length: Math.min(3, queue.length) }, async () => {
    for (let cert = queue.shift(); cert; cert = queue.shift()) {
      const items = await ozonDocListCertificateProducts(account, cert.certificate_id).catch((error) => {
        logger.warn("ozon doc status: products list failed", { account: account.id, certificate: cert.certificate_id, detail: error?.message });
        return [];
      });
      for (const item of items) rows.push({ cert, item });
      progress.products += items.length;
    }
  });
  await Promise.all(workers);
  const info = rows.length
    ? await getOzonProductInfoMapByProductIds([...new Set(rows.map((r) => String(r.item.product_id)))], account, { continueOnError: true })
    : new Map();
  const infoFor = (productId) => info.get(String(productId)) || {};
  const accountName = cleanText(account.name) || `Ozon ${cleanText(account.clientId)}`;
  const records = rows.map(({ cert, item }) => {
    const card = infoFor(item.product_id);
    const statuses = card.statuses || {};
    const image = Array.isArray(card.primary_image) ? card.primary_image[0] : card.primary_image || (Array.isArray(card.images) ? card.images[0] : "");
    return {
      account_id: cleanText(account.id),
      account_name: accountName,
      product_id: Number(item.product_id),
      certificate_id: Number(cert.certificate_id),
      offer_id: cleanText(card.offer_id) || null,
      sku: Number(item.sku || card.sku || (card.sources || [])[0]?.sku || 0) || null,
      name: cleanText(card.name) || null,
      image: cleanText(image) || null,
      certificate_number: cleanText(cert.certificate_number) || null,
      certificate_name: cleanText(cert.certificate_name) || null,
      certificate_type: cleanText(cert.type_code) || null,
      certificate_status: certStatuses[cleanText(cert.status_code)] ? `${cleanText(cert.status_code)}|${certStatuses[cleanText(cert.status_code)]}` : cleanText(cert.status_code) || null,
      certificate_reason: reasons[cleanText(cert.rejection_reason_code)] || cleanText(cert.rejection_reason_code) || null,
      certificate_comment: cleanText(cert.verification_comment) || null,
      expire_date: cert.expire_date || null,
      product_status: cleanText(item.product_status_code) || "awaiting_verification",
      product_reason: ozonDocProductReason(item, cert, reasons) || null,
      ozon_status: cleanText(statuses.status_name || statuses.status) || null,
      ozon_status_text: [statuses.status_description, statuses.status_tooltip].map(cleanText).filter(Boolean).join(". ") || null,
    };
  });
  for (let i = 0; i < records.length; i += 500) {
    const chunk = records.slice(i, i + 500);
    await prisma.$executeRawUnsafe(
      `INSERT INTO ozon_doc_status (account_id, account_name, product_id, certificate_id, offer_id, sku, name, image, certificate_number,
         certificate_name, certificate_type, certificate_status, certificate_reason, certificate_comment, expire_date, product_status,
         product_reason, ozon_status, ozon_status_text, status_changed_at, seen_at)
       SELECT x.account_id, x.account_name, x.product_id, x.certificate_id, x.offer_id, x.sku, x.name, x.image, x.certificate_number,
         x.certificate_name, x.certificate_type, x.certificate_status, x.certificate_reason, x.certificate_comment, x.expire_date,
         x.product_status, x.product_reason, x.ozon_status, x.ozon_status_text, now(), now()
       FROM jsonb_to_recordset($1::jsonb) AS x(account_id text, account_name text, product_id bigint, certificate_id bigint, offer_id text,
         sku bigint, name text, image text, certificate_number text, certificate_name text, certificate_type text, certificate_status text,
         certificate_reason text, certificate_comment text, expire_date timestamptz, product_status text, product_reason text,
         ozon_status text, ozon_status_text text)
       ON CONFLICT (account_id, product_id, certificate_id) DO UPDATE SET
         account_name = EXCLUDED.account_name, offer_id = COALESCE(EXCLUDED.offer_id, ozon_doc_status.offer_id),
         sku = COALESCE(EXCLUDED.sku, ozon_doc_status.sku), name = COALESCE(EXCLUDED.name, ozon_doc_status.name),
         image = COALESCE(EXCLUDED.image, ozon_doc_status.image), certificate_number = EXCLUDED.certificate_number,
         certificate_name = EXCLUDED.certificate_name, certificate_type = EXCLUDED.certificate_type,
         certificate_status = EXCLUDED.certificate_status, certificate_reason = EXCLUDED.certificate_reason,
         certificate_comment = EXCLUDED.certificate_comment, expire_date = EXCLUDED.expire_date,
         status_changed_at = CASE WHEN ozon_doc_status.product_status IS DISTINCT FROM EXCLUDED.product_status THEN now() ELSE ozon_doc_status.status_changed_at END,
         product_status = EXCLUDED.product_status, product_reason = EXCLUDED.product_reason,
         ozon_status = EXCLUDED.ozon_status, ozon_status_text = EXCLUDED.ozon_status_text, seen_at = now()`,
      JSON.stringify(chunk),
    );
  }
  // unbound since the last sync — gone from Ozon
  await prisma.$executeRawUnsafe(`DELETE FROM ozon_doc_status WHERE account_id = $1 AND seen_at < $2`, cleanText(account.id), startedAt);
  return { account: accountName, certificates: certificates.length, products: records.length };
}

async function runOzonDocStatusSync({ reason = "schedule" } = {}) {
  if (ozonDocStatusRunning) return ozonDocStatusRunning;
  ozonDocStatusRunning = (async () => {
    const startedAt = new Date();
    const progress = { products: 0 };
    await writeOzonDocStatusState({ running: true, startedAt: startedAt.toISOString(), reason, ...(await readOzonDocStatusState().then((s) => ({ lastDoneAt: s.lastDoneAt || s.doneAt || null, accounts: s.accounts || [] })).catch(() => ({}))) });
    const accounts = [];
    for (const account of getOzonAccounts()) {
      try {
        accounts.push(await syncOzonDocStatusAccount(account, startedAt, progress));
      } catch (error) {
        logger.warn("ozon doc status sync failed", { account: account.id, detail: error?.message || String(error) });
        accounts.push({ account: cleanText(account.name) || cleanText(account.id), error: String(error?.message || error).slice(0, 300) });
      }
    }
    ozonDocAggregatesCache.clear();
    const result = { running: false, doneAt: new Date().toISOString(), lastDoneAt: new Date().toISOString(), startedAt: startedAt.toISOString(), elapsedMs: Date.now() - startedAt.getTime(), accounts, reason };
    await writeOzonDocStatusState(result);
    logger.info("ozon doc status sync done", { accounts: accounts.map((a) => `${a.account}:${a.products ?? a.error}`).join(", "), elapsedMs: result.elapsedMs });
    return result;
  })().finally(() => { ozonDocStatusRunning = null; });
  return ozonDocStatusRunning;
}

function scheduleOzonDocStatus(delayMs = ozonDocStatusIntervalMs) {
  if (!ozonDocStatusEnabled) return;
  setTimeout(async () => {
    try {
      await runOzonDocStatusSync();
    } catch (error) {
      logger.warn("ozon doc status tick failed", { detail: error?.message });
    } finally {
      scheduleOzonDocStatus(ozonDocStatusIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || ozonDocStatusIntervalMs)).unref?.();
}

// Totals of the page (per account / status, certificates) — the same for every filter, cached for a minute
const ozonDocAggregatesCache = new Map();
async function ozonDocAggregates(prisma, account) {
  const key = account || "*";
  const hit = ozonDocAggregatesCache.get(key);
  if (hit && Date.now() - hit.at < 60_000) return hit.value;
  const [counts, notSelling, certificates] = await Promise.all([
    prisma.$queryRawUnsafe(
      `SELECT account_id AS "accountId", MAX(account_name) AS "accountName", product_status AS status, COUNT(*)::int AS n
         FROM ozon_doc_status GROUP BY account_id, product_status`,
    ),
    prisma.$queryRawUnsafe(
      `SELECT account_id AS "accountId", COUNT(*)::int AS n FROM ozon_doc_status
        WHERE product_status <> 'declined' AND ozon_status IS NOT NULL AND ozon_status !~* 'продается|продаётся|selling' GROUP BY account_id`,
    ),
    prisma.$queryRawUnsafe(
      `SELECT certificate_number AS number, MAX(certificate_type) AS type, COUNT(*)::int AS n,
              COUNT(*) FILTER (WHERE product_status = 'declined')::int AS declined
         FROM ozon_doc_status ${account ? "WHERE account_id = $1" : ""} GROUP BY certificate_number ORDER BY COUNT(*) DESC LIMIT 300`,
      ...(account ? [account] : []),
    ),
  ]);
  const value = { counts, notSelling, certificates };
  ozonDocAggregatesCache.set(key, { at: Date.now(), value });
  return value;
}

app.get("/api/ozon-docs", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireOzonDocStatusTables();
    const where = [];
    const params = [];
    const add = (sql, value) => { params.push(value); where.push(sql.replaceAll("?", `$${params.length}`)); };
    const account = cleanText(request.query.account);
    const status = cleanText(request.query.status);
    const q = cleanText(request.query.q).toLowerCase();
    const cert = cleanText(request.query.cert);
    if (account) add("account_id = ?", account);
    if (["approved", "declined", "awaiting_verification"].includes(status)) add("product_status = ?", status);
    if (status === "not_selling") where.push("product_status <> 'declined' AND ozon_status IS NOT NULL AND ozon_status !~* 'продается|продаётся|selling'");
    if (q) add("(lower(coalesce(name, '')) LIKE ? OR lower(coalesce(offer_id, '')) LIKE ? OR coalesce(sku::text, '') LIKE ? OR product_id::text LIKE ?)", `%${q}%`);
    if (cert) add("lower(coalesce(certificate_number, '')) LIKE ?", `%${cert.toLowerCase()}%`);
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const order = {
      changed: "status_changed_at DESC NULLS LAST, product_id DESC",
      name: "name ASC NULLS LAST",
      status: "CASE product_status WHEN 'declined' THEN 0 WHEN 'awaiting_verification' THEN 1 ELSE 2 END, status_changed_at DESC NULLS LAST",
    }[cleanText(request.query.sort)] || "CASE product_status WHEN 'declined' THEN 0 WHEN 'awaiting_verification' THEN 1 ELSE 2 END, status_changed_at DESC NULLS LAST";
    const limit = Math.min(200, Math.max(1, Number(request.query.limit) || 100));
    const page = Math.max(1, Number(request.query.page) || 1);
    const items = await prisma.$queryRawUnsafe(
      `SELECT * FROM ozon_doc_status ${whereSql} ORDER BY ${order} LIMIT ${limit + 1} OFFSET ${(page - 1) * limit}`,
      ...params,
    );
    const [{ total }] = await prisma.$queryRawUnsafe(`SELECT COUNT(*)::int AS total FROM ozon_doc_status ${whereSql}`, ...params);
    const { counts, notSelling, certificates } = await ozonDocAggregates(prisma, account);
    const sync = await readOzonDocStatusState().catch(() => ({}));
    response.json({
      ok: true,
      items: items.slice(0, limit).map((row) => ({
        accountId: row.account_id,
        accountName: row.account_name,
        productId: Number(row.product_id),
        certificateId: Number(row.certificate_id),
        offerId: row.offer_id,
        sku: row.sku ? Number(row.sku) : null,
        name: row.name,
        image: row.image,
        certificateNumber: row.certificate_number,
        certificateName: row.certificate_name,
        certificateType: row.certificate_type,
        certificateStatus: String(row.certificate_status || "").split("|").pop() || null,
        certificateReason: row.certificate_reason,
        certificateComment: row.certificate_comment,
        expireDate: row.expire_date,
        status: row.product_status,
        statusLabel: OZON_DOC_PRODUCT_STATUS[row.product_status] || row.product_status,
        reason: row.product_reason,
        ozonStatus: row.ozon_status,
        ozonStatusText: row.ozon_status_text,
        changedAt: row.status_changed_at,
        seenAt: row.seen_at,
      })),
      hasMore: items.length > limit,
      total,
      page,
      counts,
      notSelling,
      certificates,
      accounts: getOzonAccounts().map((a) => ({ id: cleanText(a.id), name: cleanText(a.name) || `Ozon ${cleanText(a.clientId)}` })),
      sync,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ozon-docs/sync", requireAdmin, async (request, response, next) => {
  try {
    const state = await readOzonDocStatusState().catch(() => ({}));
    // another process (worker) may be collecting right now — the state row says so for up to 20 min
    if (state.running && Date.now() - Date.parse(state.startedAt || 0) < 20 * 60_000) return response.json({ ok: true, started: false, running: true });
    void runOzonDocStatusSync({ reason: `manual:${cleanText(request.session?.username) || "admin"}` }).catch((error) => logger.warn("ozon doc status manual sync failed", { detail: error?.message }));
    response.json({ ok: true, started: true });
  } catch (error) {
    next(error);
  }
});
