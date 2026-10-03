// «Ошибки карточек» (/app/card-health): что не так с карточками на Маркете, как это починить, и починка —
// по кнопке или сама (переключатель на каждый тип ошибки, по умолчанию выключен).
//
//   Скан (worker, раз в CARD_HEALTH_SCAN_HOURS, деф. 2 ч): все карточки каждого магазина Маркета →
//   offer-cards (ошибки, предупреждения, рейтинг) → правила CARD_HEALTH_RULES → таблица card_issues.
//   Ошибка пропала с Маркета → status 'fixed'. Новые карточки попадают сюда сами — так они чинятся и потом.
//
//   Починки: resend_card — переотправка карточки с Ozon по текущим правилам переноса (тот же путь, что у
//   автопереноса, с его жёсткими фильтрами: скрытый бренд, < 20 мл, тестер, пробник); reprice — цена через
//   sendWarehousePrices (защита цен «Проверка цен» действует); confirm_quarantine — подтверждение цены из
//   карантина ТОЛЬКО если она совпадает с ценой склада (±2 %) и строка поставщика прошла защиту цен.
//   Скрытие за подлинность, фото/видео против правил, GTIN — только показ с подсказкой, что делать.

const cardHealthEnabled = process.env.CARD_HEALTH_ENABLED !== "false";
const cardHealthScanMs = Math.max(1, Number(process.env.CARD_HEALTH_SCAN_HOURS || 2) || 2) * 3_600_000;
let cardHealthTablesReady = false;
let cardHealthRunning = false;

const CARD_HEALTH_RULES = [
  { code: "variant_volume", re: /не указаны отличия|дубль варианта/i, title: "Группа вариантов: нет объёма или дубль", fix: "resend_card", howTo: "Переотправим карточку: объём флакона и «тестер» берутся из названия, группа — по аромату и бренду." },
  { code: "variant_brand", re: /общие признаки не совпадают/i, title: "Группа вариантов: разные бренды или признаки", fix: "resend_card", howTo: "Переотправим карточку: группа строится заново из бренда, аромата и концентрации — чужие варианты выйдут из неё." },
  { code: "tnved", re: /тн вэд/i, title: "Нет кода ТН ВЭД", fix: "resend_card", howTo: "Переотправим карточку: ТН ВЭД и ОКПД 2 подставятся по типу товара." },
  { code: "dimensions", re: /габарит|не указан вес/i, title: "Нет габаритов или веса", fix: "resend_card", howTo: "Переотправим карточку: габариты и вес — из карточки Ozon или шаблона по объёму." },
  { code: "caps_name", re: /большими .*буквами/i, title: "Название заглавными буквами", fix: "resend_card", howTo: "Переотправим карточку: название приводится к обычному регистру (бренд остаётся как есть)." },
  { code: "required_field", re: /не заполнено обязательное поле|указаны лишние значения/i, title: "Не заполнено обязательное поле", fix: "resend_card", howTo: "Переотправим карточку: «Тип», «Пол» и другие поля категории заполнятся по правилам переноса." },
  { code: "image_unavailable", re: /изображение недоступно|нет изображения/i, title: "Фото не скачивается", fix: "resend_card", howTo: "Переотправим карточку со свежими ссылками на фото с Ozon." },
  { code: "price_missing", re: /цена не указана/i, title: "Цена не указана", fix: "reprice", howTo: "Пересчитаем цену по поставщику и отправим (через «Проверку цен» — подозрительная цена не уйдёт)." },
  { code: "vat", re: /ставка ндс/i, title: "Неправильная ставка НДС", fix: "reprice", howTo: "Переотправим цену: НДС берётся из настроек кабинета." },
  { code: "price_drop", re: /цена сильно снизилась/i, title: "Карантин: цена сильно снизилась", fix: "confirm_quarantine", howTo: "Подтвердим цену, только если она совпадает с ценой склада (±2 %) и поставщик прошёл проверку цен.", defaultAuto: true },
  { code: "order_not_accepted", re: /не приняли заказ/i, title: "Не принят заказ — Маркет считает, что товар закончился", fix: "", howTo: "Проверьте наличие у поставщика. Если товар есть — остаток уйдёт со следующей синхронизацией; если нет — остаток будет 0." },
  { code: "authenticity", re: /подлинност|сомневаемся/i, title: "Скрыт за сомнения в подлинности", fix: "", howTo: "Нужны документы от поставщика (накладная, сертификат) через поддержку Маркета. Скрытые бренды склад сам убирает с Маркета (список «скрытых брендов»)." },
  { code: "hidden_staff", re: /скрыт[оа]? сотрудником|скрыт с витрины|предложение скрыто|маркировк/i, title: "Скрыт сотрудником Маркета", fix: "", howTo: "Напишите в поддержку Маркета по теме из текста ошибки." },
  { code: "gtin", re: /gtin/i, title: "Нужен штрихкод производителя (GTIN)", fix: "", howTo: "Укажите GTIN с упаковки или из «Честного знака» — без него маркируемый товар не продаётся." },
  { code: "media_rules", re: /не по правилам|видео не прошло/i, title: "Фото или видео против правил", fix: "", howTo: "Замените фото/видео в карточке — Маркет не опубликует такие." },
  { code: "category", re: /определить категорию|уточните информацию/i, title: "Маркет не понял категорию", fix: "resend_card", howTo: "Переотправим карточку с категорией по типу товара и главным фото флакона." },
  { code: "shipping_place", re: /нельзя отгружать/i, title: "Нельзя отгружать с выбранного места", fix: "", howTo: "Смените место отгрузки в кабинете Маркета для этой категории." },
];
// «Относится к N магазинам» — контекст, а не ошибка
const CARD_HEALTH_IGNORE = /^относится к \d+ магазин/i;

function classifyCardIssue(message = "") {
  const text = cleanText(message);
  if (!text || CARD_HEALTH_IGNORE.test(text)) return null;
  return CARD_HEALTH_RULES.find((rule) => rule.re.test(text)) || { code: "other", title: "Другое", fix: "", howTo: "Посмотрите текст ошибки в кабинете Маркета." };
}

async function requireCardHealthTables() {
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) throw new Error("Нужен Postgres.");
  if (cardHealthTablesReady) return prisma;
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS card_issues (
      id BIGSERIAL PRIMARY KEY,
      marketplace TEXT NOT NULL,
      shop_id TEXT NOT NULL,
      offer_id TEXT NOT NULL,
      code TEXT NOT NULL,
      severity TEXT NOT NULL,
      message TEXT NOT NULL,
      fix TEXT,
      status TEXT NOT NULL DEFAULT 'open',
      name TEXT,
      content_rating INTEGER,
      error TEXT,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      applied_at TIMESTAMPTZ,
      UNIQUE (marketplace, shop_id, offer_id, code)
    )`);
  await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS card_issues_status_idx ON card_issues (status, code)`);
  // Market content rating of every card (the improvement queue starts from the weakest)
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS card_quality (
      shop_id TEXT NOT NULL,
      offer_id TEXT NOT NULL,
      content_rating INTEGER,
      errors INTEGER NOT NULL DEFAULT 0,
      warnings INTEGER NOT NULL DEFAULT 0,
      checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (shop_id, offer_id)
    )`);
  await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS card_health_state (key TEXT PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`);
  cardHealthTablesReady = true;
  return prisma;
}

/** Numbers for the «Улучшение карточек» block. */
async function cardHealthImproveStats() {
  const prisma = await requireCardHealthTables();
  const settings = await readCardHealthState("improve");
  const [ready] = await prisma.$queryRawUnsafe(`SELECT to_regclass('fragrantica_card_matches') IS NOT NULL AS m, to_regclass('fragrantica_drafts') IS NOT NULL AS d, to_regclass('fragrantica_exports') IS NOT NULL AS e`);
  const one = async (sql) => (ready.m && ready.d && ready.e ? Number((await prisma.$queryRawUnsafe(sql))[0]?.n || 0) : 0);
  const [cards, matched, drafts, review, improved, rating] = await Promise.all([
    prisma.$queryRawUnsafe(`SELECT count(*)::int AS n FROM warehouse_products WHERE archived = false`).then((r) => Number(r[0]?.n || 0)),
    one(`SELECT count(DISTINCT lower(offer_id))::int AS n FROM fragrantica_card_matches WHERE volume_ml IS NOT NULL AND offer_id !~* '^FR[0-9]'`),
    one(`SELECT count(*)::int AS n FROM fragrantica_drafts WHERE data ? 'existing' AND status IN ('queued', 'working')`),
    one(`SELECT count(*)::int AS n FROM fragrantica_drafts WHERE data ? 'existing' AND status IN ('ready', 'attention')`),
    one(`SELECT count(*)::int AS n FROM fragrantica_exports WHERE item->>'improve' = 'true' AND status = 'imported'`),
    prisma.$queryRawUnsafe(`SELECT round(avg(content_rating))::int AS avg, count(*) FILTER (WHERE content_rating < 70)::int AS weak, count(*)::int AS n FROM card_quality`).then((r) => r[0] || {}),
  ]);
  return {
    enabled: settings.enabled !== false,
    perScan: Number(settings.perScan) || FRAG_IMPROVE_PER_SCAN,
    cards, matched, building: drafts, review, improved,
    rating: { avg: rating.avg === null ? null : Number(rating.avg), weak: Number(rating.weak || 0), checked: Number(rating.n || 0) },
  };
}

async function readCardHealthState(key) {
  const prisma = await requireCardHealthTables();
  const rows = await prisma.$queryRawUnsafe(`SELECT value FROM card_health_state WHERE key = $1`, key);
  return rows[0]?.value || {};
}

async function writeCardHealthState(key, value) {
  const prisma = await requireCardHealthTables();
  await prisma.$executeRawUnsafe(
    `INSERT INTO card_health_state (key, value, updated_at) VALUES ($1, $2::jsonb, now()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
    key, JSON.stringify(value || {}),
  );
}

/** code → auto on/off (defaults: only the guarded quarantine confirm). */
async function cardHealthAutoSettings() {
  const saved = await readCardHealthState("auto");
  const out = {};
  for (const rule of CARD_HEALTH_RULES) if (rule.fix) out[rule.code] = saved[rule.code] !== undefined ? Boolean(saved[rule.code]) : Boolean(rule.defaultAuto);
  return out;
}

// ─── Скан ───────────────────────────────────────────────────────────────────

async function runCardHealthScan({ autoFix = true } = {}) {
  if (cardHealthRunning) return { status: "already_running" };
  cardHealthRunning = true;
  const startedAt = Date.now();
  const summary = { shops: [], issues: 0, fixed: 0, autoApplied: 0 };
  try {
    const prisma = await requireCardHealthTables();
    for (const shop of uniqueYandexShopsByBusiness()) {
      const mappings = await getYandexOfferMappings(shop);
      const names = new Map(mappings.map((m) => [cleanText(m.offer?.offerId || m.offerId), cleanText(m.offer?.name)]));
      const offerIds = [...names.keys()].filter(Boolean);
      const seen = [];
      for (const chunk of chunkArray(offerIds, 200)) {
        const cards = await getYandexOfferCardsContentStatus(shop, chunk, { withRecommendations: false }).catch((error) => {
          logger.warn("card health offer-cards failed", { shop: shop.id, detail: error?.message });
          return [];
        });
        const rows = [];
        if (cards.length) {
          await prisma.$executeRawUnsafe(
            `INSERT INTO card_quality (shop_id, offer_id, content_rating, errors, warnings, checked_at)
             SELECT $1, x->>'o', NULLIF(x->>'r', '')::int, (x->>'e')::int, (x->>'w')::int, now() FROM jsonb_array_elements($2::jsonb) x
             ON CONFLICT (shop_id, offer_id) DO UPDATE SET content_rating = EXCLUDED.content_rating, errors = EXCLUDED.errors, warnings = EXCLUDED.warnings, checked_at = now()`,
            shop.id, JSON.stringify(cards.filter((c) => c.offerId).map((c) => ({ o: c.offerId, r: Number.isFinite(Number(c.contentRating)) ? Number(c.contentRating) : "", e: (c.errors || []).length, w: (c.warnings || []).length }))),
          ).catch((error) => logger.warn("card quality save failed", { detail: error?.message }));
        }
        for (const card of cards) {
          const all = [
            ...(card.errors || []).map((e) => ({ severity: "error", text: [e.message, e.comment].filter(Boolean).join(": ") })),
            ...(card.warnings || []).map((e) => ({ severity: "warning", text: [e.message, e.comment].filter(Boolean).join(": ") })),
          ];
          const byCode = new Map();
          for (const entry of all) {
            const rule = classifyCardIssue(entry.text);
            if (!rule) continue;
            const prev = byCode.get(rule.code);
            if (!prev || (prev.severity === "warning" && entry.severity === "error")) byCode.set(rule.code, { ...entry, rule });
          }
          for (const [code, entry] of byCode) {
            rows.push({ offerId: card.offerId, code, severity: entry.severity, message: entry.text.slice(0, 1500), fix: entry.rule.fix || null, name: names.get(card.offerId) || "", rating: card.contentRating || null });
            seen.push(`${card.offerId}|${code}`);
          }
        }
        if (rows.length) {
          await prisma.$executeRawUnsafe(
            `INSERT INTO card_issues (marketplace, shop_id, offer_id, code, severity, message, fix, name, content_rating, status, last_seen)
             SELECT 'yandex', $1, x->>'offerId', x->>'code', x->>'severity', x->>'message', NULLIF(x->>'fix', ''), x->>'name', NULLIF(x->>'rating', '')::int, 'open', now()
               FROM jsonb_array_elements($2::jsonb) x
             ON CONFLICT (marketplace, shop_id, offer_id, code) DO UPDATE SET
               severity = EXCLUDED.severity, message = EXCLUDED.message, fix = EXCLUDED.fix, name = EXCLUDED.name,
               content_rating = EXCLUDED.content_rating, last_seen = now(),
               -- dismissed stays dismissed; a fix that did not help reopens the issue
               status = CASE WHEN card_issues.status = 'dismissed' THEN 'dismissed' ELSE 'open' END`,
            shop.id, JSON.stringify(rows),
          );
        }
        await new Promise((resolve) => setImmediate(resolve));
      }
      // what the scan did not see any more is fixed
      const gone = await prisma.$executeRawUnsafe(
        `UPDATE card_issues SET status = 'fixed' WHERE marketplace = 'yandex' AND shop_id = $1 AND status IN ('open', 'applied', 'failed')
           AND last_seen < to_timestamp($2::double precision / 1000)`,
        shop.id, startedAt,
      );
      summary.shops.push({ shop: shop.id, offers: offerIds.length, issues: seen.length });
      summary.issues += seen.length;
      summary.fixed += Number(gone) || 0;
    }
    if (autoFix) summary.autoApplied = await applyCardHealthAuto().catch((error) => {
      logger.warn("card health auto fix failed", { detail: error?.message });
      return 0;
    });
    summary.quarantine = await runGuardedQuarantineConfirm().catch((error) => ({ error: error?.message || String(error) }));
    // «Улучшение карточек»: the weakest old cards go to the conveyor as drafts — nothing is sent until approved
    const improve = await readCardHealthState("improve");
    if (improve?.enabled !== false && typeof enqueueFragranticaImprovements === "function") {
      summary.improve = await enqueueFragranticaImprovements({ limit: Number(improve?.perScan) || undefined }).catch((error) => ({ error: error?.message || String(error) }));
    }
    summary.elapsedMs = Date.now() - startedAt;
    await writeCardHealthState("scan", { ...summary, at: new Date().toISOString() });
    logger.info("card health scan complete", summary);
    return summary;
  } finally {
    cardHealthRunning = false;
  }
}

function scheduleCardHealthScan(delayMs = cardHealthScanMs) {
  if (!cardHealthEnabled) return;
  setTimeout(async () => {
    try {
      await runCardHealthScan();
    } catch (error) {
      logger.warn("card health scan failed", { detail: error?.message || String(error) });
    } finally {
      scheduleCardHealthScan(cardHealthScanMs);
    }
  }, Math.max(30_000, Number(delayMs) || cardHealthScanMs)).unref?.();
}

// ─── Починка ────────────────────────────────────────────────────────────────

/** Ozon card(s) behind Market offers (same offer id); Fragrantica cards are rebuilt by their own flow. */
async function cardHealthOzonProducts(offerIds = []) {
  // straight from Postgres: the API process keeps only a light warehouse cache
  const prisma = await requireCardHealthTables();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id FROM warehouse_products WHERE marketplace = 'ozon' AND archived = false AND offer_id = ANY($1::text[])`,
    [...new Set(offerIds.map(cleanText).filter(Boolean))],
  );
  const products = await readWarehouseProductsFromPostgresByIds(rows.map((r) => r.id));
  const byOffer = new Map();
  for (const product of products) {
    const key = cleanText(product.offerId).toLowerCase();
    // the main Ozon cabinet wins when two cabinets share the offer id
    if (!byOffer.has(key) || cleanText(product.target) === "ozon") byOffer.set(key, product);
  }
  return byOffer;
}

/** Applies the fix of the given issues. Returns { applied, failed, skipped, details }. */
async function applyCardIssues(issueIds = [], { source = "manual" } = {}) {
  const prisma = await requireCardHealthTables();
  const issues = await prisma.$queryRawUnsafe(
    `SELECT * FROM card_issues WHERE id = ANY($1::bigint[]) AND status IN ('open', 'failed') AND fix IS NOT NULL`,
    issueIds.map((id) => Number(id)).filter(Boolean),
  );
  const result = { applied: 0, failed: 0, skipped: 0, details: [] };
  if (!issues.length) return result;
  const shops = new Map(uniqueYandexShopsByBusiness().map((s) => [cleanText(s.id), s]));
  const mark = async (ids, status, error = null) => {
    if (!ids.length) return;
    await prisma.$executeRawUnsafe(
      `UPDATE card_issues SET status = $2, error = $3, applied_at = now() WHERE id = ANY($1::bigint[])`,
      ids.map(Number), status, error ? String(error).slice(0, 500) : null,
    );
  };

  // resend_card — one export per shop for all its offers
  const resend = issues.filter((i) => i.fix === "resend_card");
  for (const [shopId, list] of cardHealthGroupBy(resend, (i) => i.shop_id)) {
    const shop = shops.get(shopId);
    const fr = list.filter((i) => /^FR\d/i.test(i.offer_id));
    if (fr.length) {
      await mark(fr.map((i) => i.id), "failed", "Карточка из «Фрагрантики» — обновите её там («Обновить медиа» / отправить ещё раз)");
      result.skipped += fr.length;
    }
    const rest = list.filter((i) => !/^FR\d/i.test(i.offer_id));
    if (!shop || !rest.length) continue;
    const ozon = await cardHealthOzonProducts(rest.map((i) => i.offer_id));
    const noOzon = rest.filter((i) => !ozon.has(cleanText(i.offer_id).toLowerCase()));
    if (noOzon.length) {
      await mark(noOzon.map((i) => i.id), "failed", "Нет карточки Ozon с таким артикулом — исправьте в кабинете Маркета");
      result.failed += noOzon.length;
    }
    const products = [...new Map(rest.filter((i) => ozon.has(cleanText(i.offer_id).toLowerCase())).map((i) => [cleanText(i.offer_id).toLowerCase(), ozon.get(cleanText(i.offer_id).toLowerCase())])).values()];
    if (!products.length) continue;
    const exported = await exportOzonProductsToYandex(products, [shop], { reason: `card_health_${source}`, contentOnly: true })
      .catch((error) => ({ error: error?.message || String(error), sentOfferIds: new Set() }));
    const ok = rest.filter((i) => exported.sentOfferIds?.has(cleanText(i.offer_id).toLowerCase()));
    const bad = rest.filter((i) => ozon.has(cleanText(i.offer_id).toLowerCase()) && !ok.includes(i));
    await mark(ok.map((i) => i.id), "applied");
    await mark(bad.map((i) => i.id), "failed", exported.error || "Маркет не принял карточку или она не прошла правила переноса (скрытый бренд, < 20 мл, тестер, нет обязательных полей)");
    result.applied += ok.length;
    result.failed += bad.length;
  }

  // reprice — the warehouse price send (the price guard applies)
  const reprice = issues.filter((i) => i.fix === "reprice");
  if (reprice.length) {
    const ids = reprice.map((i) => {
      const shop = shops.get(i.shop_id);
      return shop ? yandexWarehouseProductId(shop, i.offer_id) : null;
    }).filter(Boolean);
    const sent = ids.length
      ? await sendWarehousePrices({ productIds: ids, livePriceMaster: true, force: true, reason: `card-health-${source}`, sourceEvent: "card-health" }).catch((error) => ({ error: error?.message || String(error) }))
      : { error: "Товар не найден на складе" };
    const SKIP_TEXT = {
      price_guard_hold: "цена ждёт решения на «Проверке цен»",
      no_supplier: "нет живого поставщика — цену не из чего посчитать",
      no_pricemaster_link: "товар не привязан к поставщику — привяжите на складе",
      pm_live_timeout: "PriceMaster не ответил — повторим при следующей проверке",
      not_ready: "цена ещё не готова",
    };
    const why = new Map((sent.skipped || []).filter((s) => SKIP_TEXT[s.reason]).map((s) => [String(s.offerId), SKIP_TEXT[s.reason]]));
    const ok = reprice.filter((i) => !sent.error && !why.has(String(i.offer_id)));
    await mark(ok.map((i) => i.id), "applied");
    for (const i of reprice.filter((x) => !ok.includes(x))) await mark([i.id], "failed", sent.error || `Цена не ушла: ${why.get(String(i.offer_id)) || "причина в журнале цен"}`);
    const bad = reprice.filter((i) => !ok.includes(i));
    result.applied += ok.length;
    result.failed += bad.length;
  }

  // confirm_quarantine — only the guarded way
  const quarantine = issues.filter((i) => i.fix === "confirm_quarantine");
  if (quarantine.length) {
    const r = await runGuardedQuarantineConfirm({ onlyOfferIds: quarantine.map((i) => i.offer_id) });
    // already out of quarantine (released by the scan a minute ago) counts as done
    const confirmed = new Set([...(r.confirmedOfferIds || []), ...quarantine.map((i) => i.offer_id).filter((id) => !(r.inQuarantine || []).includes(String(id)))].map(String));
    const ok = quarantine.filter((i) => confirmed.has(String(i.offer_id)));
    const bad = quarantine.filter((i) => !confirmed.has(String(i.offer_id)));
    await mark(ok.map((i) => i.id), "applied");
    for (const i of bad) await mark([i.id], "failed", (r.refused || {})[i.offer_id] || "Цена не подтверждена: не совпадает с ценой склада");
    result.applied += ok.length;
    result.failed += bad.length;
  }
  return result;
}

function cardHealthGroupBy(list, keyFn) {
  const map = new Map();
  for (const item of list) {
    const key = keyFn(item);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }
  return map;
}

/** Fixes the open issues of every type switched to «чинить само» (at most 500 per scan). */
async function applyCardHealthAuto() {
  const auto = await cardHealthAutoSettings();
  const codes = Object.entries(auto).filter(([code, on]) => on && code !== "price_drop").map(([code]) => code);
  if (!codes.length) return 0;
  const prisma = await requireCardHealthTables();
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id FROM card_issues WHERE status = 'open' AND fix IS NOT NULL AND code = ANY($1::text[]) ORDER BY id LIMIT 500`, codes,
  );
  if (!rows.length) return 0;
  const r = await applyCardIssues(rows.map((r) => r.id), { source: "auto" });
  return r.applied;
}

// ─── Карантин цен: подтверждение только проверенной цены ──────────────────────

/**
 * Market price quarantine (business + campaign level): an offer is confirmed only if the quarantined price
 * equals the warehouse price for it (±2 %) and the selected supplier row passed the price guard. Everything
 * else stays in quarantine — that is exactly what the quarantine is for (230 403 ₽ on 2026-10-02).
 */
async function runGuardedQuarantineConfirm({ onlyOfferIds = null } = {}) {
  const auto = await cardHealthAutoSettings();
  if (!onlyOfferIds && !auto.price_drop) return { skipped: "auto_off" };
  const only = onlyOfferIds ? new Set(onlyOfferIds.map(String)) : null;
  const out = { checked: 0, confirmed: 0, confirmedOfferIds: [], refused: {}, inQuarantine: [] };
  for (const shop of uniqueYandexShopsByBusiness()) {
    const levels = [{ path: `/v2/businesses/${shop.businessId}/price-quarantine` }];
    for (const s of getYandexShops().filter((x) => cleanText(x.businessId) === cleanText(shop.businessId) && x.campaignId)) {
      levels.push({ path: `/v2/campaigns/${s.campaignId}/price-quarantine` });
    }
    for (const level of levels) {
      const offers = [];
      let pageToken = "";
      for (let i = 0; i < 50; i += 1) {
        const data = await yandexRequest(shop, "POST", `${level.path}?limit=200${pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : ""}`, {}).catch(() => null);
        const result = data?.result || {};
        offers.push(...(result.offers || []));
        pageToken = result.paging?.nextPageToken || "";
        if (!pageToken) break;
      }
      const candidates = offers.filter((o) => !only || only.has(String(o.offerId)));
      out.inQuarantine.push(...candidates.map((o) => String(o.offerId)));
      if (!candidates.length) continue;
      const ids = candidates.map((o) => yandexWarehouseProductId(shop, o.offerId));
      const settings = await readAppSettings();
      const fresh = await buildFreshWarehouseProducts(ids, { usdRate: Number(settings.fixedUsdRate || process.env.DEFAULT_USD_RATE || 95) || 95, livePriceMaster: true, batchPriceMaster: true }).catch(() => []);
      const byOffer = new Map(fresh.map((p) => [cleanText(p.offerId), p]));
      const ok = [];
      for (const offer of candidates) {
        out.checked += 1;
        const quarantined = Number(offer.currentPrice?.value || 0);
        const product = byOffer.get(cleanText(offer.offerId));
        const expected = Math.round(Number(product?.nextPrice || product?.targetPrice || 0));
        const rowProblem = product?.selectedSupplier ? priceGuardRowProblem(product.selectedSupplier, product.name) : "нет живого поставщика";
        if (!product || !expected) { out.refused[offer.offerId] = "склад не знает цену этого товара"; continue; }
        if (rowProblem) { out.refused[offer.offerId] = rowProblem; continue; }
        if (Math.abs(quarantined - expected) > Math.max(50, expected * 0.02)) { out.refused[offer.offerId] = `на Маркете ${quarantined} ₽, а по складу ${expected} ₽`; continue; }
        ok.push(cleanText(offer.offerId));
      }
      for (const chunk of chunkArray(ok, 200)) {
        await yandexRequest(shop, "POST", `${level.path}/confirm`, { offerIds: chunk });
        out.confirmed += chunk.length;
        out.confirmedOfferIds.push(...chunk);
      }
    }
  }
  if (out.confirmed || Object.keys(out.refused).length) logger.info("guarded quarantine confirm", { checked: out.checked, confirmed: out.confirmed, refused: Object.keys(out.refused).length });
  return out;
}

// ─── API ────────────────────────────────────────────────────────────────────

app.get("/api/card-health", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireCardHealthTables();
    const status = ["open", "applied", "failed", "fixed", "dismissed"].includes(request.query.status) ? request.query.status : "open";
    const code = cleanText(request.query.code);
    const q = cleanText(request.query.q).toLowerCase();
    const params = [status];
    let where = "status = $1";
    if (code) { params.push(code); where += ` AND code = $${params.length}`; }
    if (q) { params.push(`%${q}%`); where += ` AND (lower(offer_id) LIKE $${params.length} OR lower(name) LIKE $${params.length})`; }
    const [items, groups, statuses, auto, scan] = await Promise.all([
      prisma.$queryRawUnsafe(`SELECT * FROM card_issues WHERE ${where} ORDER BY severity, code, offer_id LIMIT 500`, ...params),
      prisma.$queryRawUnsafe(`SELECT code, count(*)::int AS n, count(*) FILTER (WHERE severity = 'error')::int AS errors FROM card_issues WHERE status = $1 GROUP BY code ORDER BY n DESC`, status),
      prisma.$queryRawUnsafe(`SELECT status, count(*)::int AS n FROM card_issues GROUP BY status`),
      cardHealthAutoSettings(),
      readCardHealthState("scan"),
    ]);
    const improve = await cardHealthImproveStats().catch((error) => ({ error: error?.message || String(error) }));
    const rules = Object.fromEntries([...CARD_HEALTH_RULES, { code: "other", title: "Другое", fix: "", howTo: "Посмотрите текст ошибки в кабинете Маркета." }]
      .map((r) => [r.code, { title: r.title, fix: r.fix || "", howTo: r.howTo }]));
    response.json({
      ok: true,
      items: items.map((i) => ({ ...i, id: Number(i.id) })),
      groups,
      statuses: Object.fromEntries(statuses.map((s) => [s.status, s.n])),
      rules,
      auto,
      scan: { ...scan, running: cardHealthRunning },
      improve,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/card-health/apply", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireCardHealthTables();
    let ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter(Boolean).slice(0, 2000);
    const code = cleanText(request.body?.code);
    if (!ids.length && code) {
      ids = (await prisma.$queryRawUnsafe(`SELECT id FROM card_issues WHERE status IN ('open', 'failed') AND code = $1 AND fix IS NOT NULL LIMIT 2000`, code)).map((r) => Number(r.id));
    }
    if (!ids.length) return response.status(400).json({ error: "Нечего чинить." });
    const result = await applyCardIssues(ids, { source: "manual" });
    await appendAudit(request, "card_health.apply", { entityType: "card_issue", entityId: code || ids.slice(0, 10).join(","), newValue: { requested: ids.length, ...result, details: undefined } });
    response.json({ ok: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.post("/api/card-health/dismiss", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireCardHealthTables();
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter(Boolean).slice(0, 2000);
    const restore = request.body?.restore === true;
    await prisma.$executeRawUnsafe(`UPDATE card_issues SET status = $2 WHERE id = ANY($1::bigint[])`, ids, restore ? "open" : "dismissed");
    response.json({ ok: true, count: ids.length });
  } catch (error) {
    next(error);
  }
});

app.post("/api/card-health/auto", requireAdmin, async (request, response, next) => {
  try {
    const code = cleanText(request.body?.code);
    if (!CARD_HEALTH_RULES.some((r) => r.code === code && r.fix)) return response.status(400).json({ error: "Для этой ошибки нет автопочинки." });
    const saved = await readCardHealthState("auto");
    saved[code] = request.body?.enabled === true;
    await writeCardHealthState("auto", saved);
    await appendAudit(request, "card_health.auto", { entityType: "card_issue_code", entityId: code, newValue: { enabled: saved[code] } });
    response.json({ ok: true, auto: await cardHealthAutoSettings() });
  } catch (error) {
    next(error);
  }
});

// «Улучшение карточек»: on/off, how many per scan, «добавить сейчас»
app.post("/api/card-health/improve", requireAdmin, async (request, response, next) => {
  try {
    const saved = await readCardHealthState("improve");
    if (typeof request.body?.enabled === "boolean") saved.enabled = request.body.enabled;
    if (request.body?.perScan !== undefined) saved.perScan = Math.max(0, Math.min(500, Number(request.body.perScan) || 0));
    await writeCardHealthState("improve", saved);
    let queued = null;
    const now = Math.max(0, Math.min(1000, Number(request.body?.queueNow) || 0));
    if (now) queued = await enqueueFragranticaImprovements({ limit: now, createdBy: cleanText(request.session?.username) || "улучшение" });
    await appendAudit(request, "card_health.improve", { entityType: "card_improve", entityId: "settings", newValue: { ...saved, queueNow: now || undefined, queued: queued?.queued } });
    response.json({ ok: true, queued, improve: await cardHealthImproveStats() });
  } catch (error) {
    next(error);
  }
});

app.post("/api/card-health/scan", requireAdmin, async (_request, response, next) => {
  try {
    if (cardHealthRunning) return response.json({ ok: true, status: "already_running" });
    // the scan reads ~15 000 cards (a few minutes) — run in the background, the page polls
    runCardHealthScan({ autoFix: false }).catch((error) => logger.warn("card health manual scan failed", { detail: error?.message }));
    response.json({ ok: true, status: "started" });
  } catch (error) {
    next(error);
  }
});

// ─── Страница «Улучшение карточек» ───────────────────────────────────────────
// Черновики улучшения (data.existing) с «было → станет»: название, фото, описание. Одобрение — общий
// /api/fragrantica/drafts/approve; «Не улучшать» оставляет черновик со статусом skipped, чтобы эта карточка
// больше не попадала в очередь.

const CARD_IMPROVE_TABS = {
  review: ["ready"],
  attention: ["attention", "failed"],
  queue: ["queued", "working"],
  sending: ["approved", "sending"],
  sent: ["sent"],
  skipped: ["skipped"],
};

app.get("/api/card-improve", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireCardHealthTables();
    const tab = CARD_IMPROVE_TABS[request.query.tab] ? request.query.tab : "review";
    const q = cleanText(request.query.q).toLowerCase();
    const params = [CARD_IMPROVE_TABS[tab]];
    let where = `d.data ? 'existing' AND d.status = ANY($1::text[])`;
    if (q) {
      params.push(`%${q}%`);
      where += ` AND (lower(coalesce(p.brand, '') || ' ' || coalesce(p.name, '')) LIKE $2 OR lower(d.data->'existing'->>'offerId') LIKE $2)`;
    }
    const [rows, counts] = await Promise.all([
      prisma.$queryRawUnsafe(
        `SELECT d.*, p.brand, p.name AS perfume_name FROM fragrantica_drafts d LEFT JOIN fragrantica_perfumes p ON p.id = d.perfume_id
          WHERE ${where} ORDER BY coalesce((d.data->'existing'->>'sold')::int, 0) DESC, (d.data->'existing'->>'rating')::int ASC NULLS LAST, d.id LIMIT 200`,
        ...params,
      ),
      prisma.$queryRawUnsafe(`SELECT status, count(*)::int AS n FROM fragrantica_drafts WHERE data ? 'existing' GROUP BY status`),
    ]);
    const exportIds = [...new Set(rows.flatMap((r) => (Array.isArray(r.export_ids) ? r.export_ids : []).map(Number)))];
    const exportRows = exportIds.length ? await prisma.$queryRawUnsafe(`SELECT id, status, error, account_name FROM fragrantica_exports WHERE id = ANY($1::bigint[])`, exportIds) : [];
    const exportsById = new Map(exportRows.map((e) => [Number(e.id), { status: e.status, error: e.error || null, shop: e.account_name }]));
    const targets = fragranticaTargets();
    const byStatus = Object.fromEntries(counts.map((c) => [c.status, c.n]));
    const tabCounts = Object.fromEntries(Object.entries(CARD_IMPROVE_TABS).map(([key, list]) => [key, list.reduce((s, st) => s + (byStatus[st] || 0), 0)]));
    response.json({
      ok: true,
      tab,
      counts: tabCounts,
      items: rows.map((r) => {
        const d = r.data || {};
        const ex = d.existing || {};
        const target = targets.find((t) => t.kind === ex.marketplace && t.id === cleanText(ex.target)) || {};
        const shops = fragranticaExistingTargets(ex).map((t) => targets.find((x) => x.kind === t.marketplace && x.id === t.target)?.label || t.target);
        const style = target.style || "parfumerius";
        // the same order the export uses: own photos first (the first replaces the bottle), then the pyramid and cards
        const own = Array.isArray(d.customPhotos) ? d.customPhotos : [];
        const photos = (d.onlyCustomPhotos && own.length
          ? own
          : d.ownBottleOnly
            ? [...own, d.images?.notes?.[style], ...fragranticaExtraPhotos(Number(r.perfume_id), style).filter((u) => !/-(specs|closeup)-/.test(u))]
            : [own[0] || d.images?.main || d.sourceImage, ...own.slice(1), d.images?.notes?.[style], ...fragranticaExtraPhotos(Number(r.perfume_id), style)])
          .filter(Boolean).map((u) => fragranticaAbsoluteUrl(u));
        return {
          id: Number(r.id),
          status: r.status,
          stage: r.stage || null,
          error: r.error || null,
          perfumeId: Number(r.perfume_id),
          brand: r.brand || "",
          perfumeName: r.perfume_name || "",
          volume: r.volume_ml === null ? null : Number(r.volume_ml),
          tester: Boolean(r.tester),
          shop: shops.join(", ") || target.label || ex.target || "",
          sold: Number(ex.sold || 0),
          marketplace: ex.marketplace || "",
          offerId: ex.offerId || "",
          rating: ex.rating ?? null,
          price: Number(d.price || 0),
          yandexPrice: Number(d.yandexPrice || 0),
          before: ex.state?.before || null,
          after: {
            name: ex.marketplace === "yandex" ? d.marketName || d.name || "" : d.name || "",
            photos,
            description: String(d.description || ""),
          },
          missing: Array.isArray(d.missing) ? d.missing : [],
          customPhotos: own,
          onlyCustomPhotos: Boolean(d.onlyCustomPhotos),
          keptExistingPhotos: Number(d.keptExistingPhotos || 0),
          blurryExistingPhotos: Number(d.blurryExistingPhotos || 0),
          ownBottleOnly: Boolean(d.ownBottleOnly),
          notes: (Array.isArray(d.warnings) ? d.warnings : []).filter((w) => /нет — /.test(w)),
          exports: (Array.isArray(r.export_ids) ? r.export_ids : []).map((id) => exportsById.get(Number(id))).filter(Boolean),
          updatedAt: r.updated_at,
        };
      }),
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/card-improve/skip", requireAdmin, async (request, response, next) => {
  try {
    const prisma = await requireCardHealthTables();
    const ids = (Array.isArray(request.body?.ids) ? request.body.ids : []).map(Number).filter((id) => id > 0).slice(0, 600);
    const restore = request.body?.restore === true;
    if (!ids.length) return response.status(400).json({ error: "Ничего не выбрано." });
    const rows = await prisma.$queryRawUnsafe(
      restore
        ? `UPDATE fragrantica_drafts SET status = 'queued', stage = NULL, error = NULL, updated_at = now() WHERE id = ANY($1::bigint[]) AND data ? 'existing' AND status = 'skipped' RETURNING id`
        : `UPDATE fragrantica_drafts SET status = 'skipped', updated_at = now() WHERE id = ANY($1::bigint[]) AND data ? 'existing' AND status IN ('ready', 'attention', 'failed', 'queued') RETURNING id`,
      ids,
    );
    await appendAudit(request, restore ? "card_improve.restore" : "card_improve.skip", { entityType: "fragrantica_draft", entityId: rows.map((r) => r.id).join(","), newValue: { count: rows.length } });
    response.json({ ok: true, count: rows.length });
  } catch (error) {
    next(error);
  }
});
