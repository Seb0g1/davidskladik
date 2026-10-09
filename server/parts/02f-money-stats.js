// «Прибыль»: the money the marketplaces actually accrue, by day, with every expense, and the net result.
//
// Ozon: /v1/finance/accrual/by-day (the v3 transaction list was switched off 2026-09-08). Each day's accruals
// replace that day's rows: a posting's sale / return, the sale commission and its logistics services; item
// and non-item fees (acquiring, promotion, storage, fines…) by their type in /v1/finance/accrual/types.
// Yandex Market: /v2/campaigns/{id}/stats/orders. A delivered order brings the seller's price (BUYER +
// MARKETPLACE + CASHBACK), returned items take it back, the actual commissions are the expenses; dated by the
// order's last status change (delivery or return). Fresh orders get their commissions within about a day.
// Orders and cancellations: Ozon FBS postings and Yandex orders by their creation day.
// Purchase cost (себестоимость) is not stored: the summary takes it from finance_orders by posting / order
// (the picking's real purchase first, else the current supplier's price), so later picking fixes it.

const moneySyncEnabled = process.env.MONEY_SYNC_ENABLED !== "false";
const moneySyncIntervalMs = Math.max(15 * 60_000, Number(process.env.MONEY_SYNC_MINUTES || 60) * 60_000 || 60 * 60_000);
// how far back the first runs fill the history, a chunk of days per run so the Ozon queue isn't held for long
const MONEY_HISTORY_DAYS = Math.max(30, Math.min(400, Number(process.env.MONEY_HISTORY_DAYS || 150) || 150));
const MONEY_BACKFILL_DAYS_PER_RUN = 25;
const MONEY_RECENT_DAYS = 6;
const MONEY_SETTINGS_KEY = "money";
const MONEY_STATE_KEY = "money-sync";

const MONEY_CATEGORIES = ["sales", "returns", "commission", "logistics", "promotion", "storage", "penalties", "services", "compensation", "other"];

// Ozon accrual types (ids of /v1/finance/accrual/types) → the summary's lines
const OZON_ACCRUAL_CATEGORY = (() => {
  const groups = {
    commission: [1, 22, 26, 63, 66, 68, 69],
    logistics: [2, 6, 9, 12, 13, 16, 17, 21, 28, 29, 30, 32, 38, 39, 40, 42, 43, 44, 45, 53, 56, 58, 59, 62, 64, 65, 67, 71, 73, 77, 84, 85, 86, 88, 97, 98, 99, 100, 101, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 120, 121, 122, 123, 124, 131, 132, 133, 134, 135],
    promotion: [3, 4, 5, 19, 23, 27, 31, 33, 36, 41, 47, 48, 49, 50, 54, 55, 61, 70, 74, 75, 80, 87, 95, 96, 116, 118, 119, 130],
    storage: [15, 46, 60, 78, 79, 102, 103],
    penalties: [14, 89, 90, 91, 92, 93, 94],
    services: [18, 20, 24, 34, 35, 37, 51, 52, 76, 82, 83, 105, 117, 125, 126, 127, 128],
    compensation: [8, 10, 25, 81, 104],
  };
  const map = new Map();
  for (const [category, ids] of Object.entries(groups)) for (const id of ids) map.set(id, category);
  return map;
})();

const YANDEX_COMMISSION_CATEGORY = {
  FEE: "commission", AGENCY: "commission", PAYMENT_TRANSFER: "commission", INSTALLMENT: "commission",
  LOYALTY_PARTICIPATION_FEE: "promotion", AUCTION_PROMOTION: "promotion",
  DELIVERY_TO_CUSTOMER: "logistics", EXPRESS_DELIVERY_TO_CUSTOMER: "logistics", SORTING: "logistics", INTAKE_SORTING: "logistics",
  RETURN_PROCESSING: "logistics", CROSSREGIONAL_DELIVERY: "logistics", FULFILLMENT: "logistics", FULFILLMENT_WITHDRAW: "logistics",
  RETURNED_ORDERS_STORAGE: "storage", ITEM_BOOKING: "storage",
};
const YANDEX_COMMISSION_NAME = {
  FEE: "Размещение товара", AGENCY: "Приём платежа", PAYMENT_TRANSFER: "Перевод платежа", INSTALLMENT: "Рассрочка",
  LOYALTY_PARTICIPATION_FEE: "Программа лояльности и отзывы за баллы", AUCTION_PROMOTION: "Буст продаж",
  DELIVERY_TO_CUSTOMER: "Доставка покупателю", EXPRESS_DELIVERY_TO_CUSTOMER: "Экспресс-доставка", SORTING: "Обработка заказа",
  INTAKE_SORTING: "Забор заказов со склада", RETURN_PROCESSING: "Обработка возвратов", CROSSREGIONAL_DELIVERY: "Доставка средней мили",
  FULFILLMENT: "Складская обработка", FULFILLMENT_WITHDRAW: "Вывоз со склада", RETURNED_ORDERS_STORAGE: "Хранение невыкупов и возвратов",
  ITEM_BOOKING: "Бронирование товара", ILLIQUID_GOODS_SALE: "Продажа невывезенных товаров",
};

let moneySyncTimer = null;
let moneySyncRunning = false;
let moneyTablesReady = null;

const moneyNum = (value) => {
  const n = Number(typeof value === "object" && value !== null ? value.amount : value);
  return Number.isFinite(n) ? n : 0;
};
const moneyRound = (value) => Math.round(Number(value || 0) * 100) / 100;
/** Pure: the Moscow calendar day of a timestamp («2026-10-08T22:30:00Z» → 2026-10-09). */
function moneyMoscowDay(value) {
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return "";
  return new Date(time + 3 * 3600_000).toISOString().slice(0, 10);
}
function moneyDayShift(day, delta) {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}
const moneyToday = () => moneyMoscowDay(Date.now());

async function requireMoneyTables() {
  if (!moneyTablesReady) {
    moneyTablesReady = (async () => {
      const prisma = getPrisma();
      await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS money_ops (
        id text PRIMARY KEY, marketplace text NOT NULL, account text NOT NULL, day date NOT NULL, category text NOT NULL,
        amount numeric(14,2) NOT NULL, ref text, units integer, type_name text, updated_at timestamptz NOT NULL DEFAULT now())`);
      await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS money_ops_day_idx ON money_ops (day)`);
      await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS money_ops_ref_idx ON money_ops (marketplace, account, ref)`);
      await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS money_orders (
        id text PRIMARY KEY, marketplace text NOT NULL, account text NOT NULL, day date NOT NULL, status text,
        amount numeric(14,2) NOT NULL DEFAULT 0, units integer NOT NULL DEFAULT 0, cancelled boolean NOT NULL DEFAULT false,
        returned boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT now())`);
      await prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS money_orders_day_idx ON money_orders (day)`);
      return prisma;
    })();
    moneyTablesReady.catch(() => { moneyTablesReady = null; });
  }
  return moneyTablesReady;
}

async function moneyReadSetting(key, fallback) {
  const row = await getPrisma().appSetting.findUnique({ where: { key } }).catch(() => null);
  return row?.value && typeof row.value === "object" ? { ...fallback, ...row.value } : { ...fallback };
}
async function moneyWriteSetting(key, value) {
  await getPrisma().appSetting.upsert({ where: { key }, create: { key, value }, update: { value } });
  return value;
}

async function moneyUpsertOps(rows) {
  const prisma = getPrisma();
  for (let i = 0; i < rows.length; i += 500) {
    await prisma.$executeRawUnsafe(
      `INSERT INTO money_ops (id, marketplace, account, day, category, amount, ref, units, type_name, updated_at)
       SELECT id, marketplace, account, day::date, category, amount, ref, units, type_name, now()
         FROM jsonb_to_recordset($1::jsonb) AS x(id text, marketplace text, account text, day text, category text, amount numeric, ref text, units int, type_name text)
       ON CONFLICT (id) DO UPDATE SET day = EXCLUDED.day, category = EXCLUDED.category, amount = EXCLUDED.amount, ref = EXCLUDED.ref,
         units = EXCLUDED.units, type_name = EXCLUDED.type_name, updated_at = now()`,
      JSON.stringify(rows.slice(i, i + 500)),
    );
  }
}
async function moneyUpsertOrders(rows) {
  const prisma = getPrisma();
  for (let i = 0; i < rows.length; i += 500) {
    await prisma.$executeRawUnsafe(
      `INSERT INTO money_orders (id, marketplace, account, day, status, amount, units, cancelled, returned, updated_at)
       SELECT id, marketplace, account, day::date, status, amount, units, cancelled, returned, now()
         FROM jsonb_to_recordset($1::jsonb) AS x(id text, marketplace text, account text, day text, status text, amount numeric, units int, cancelled boolean, returned boolean)
       ON CONFLICT (id) DO UPDATE SET day = EXCLUDED.day, status = EXCLUDED.status, amount = EXCLUDED.amount, units = EXCLUDED.units,
         cancelled = EXCLUDED.cancelled, returned = EXCLUDED.returned, updated_at = now()`,
      JSON.stringify(rows.slice(i, i + 500)),
    );
  }
}

// ─── Ozon ───────────────────────────────────────────────────────────────────

let ozonAccrualTypesCache = null;
async function ozonAccrualTypeNames(account) {
  if (ozonAccrualTypesCache && Date.now() - ozonAccrualTypesCache.at < 24 * 3600_000) return ozonAccrualTypesCache.names;
  const data = await ozonRequest("/v1/finance/accrual/types", {}, account);
  const names = new Map((data?.accrual_types || []).map((t) => [Number(t.id), cleanText(t.description || t.name)]));
  ozonAccrualTypesCache = { at: Date.now(), names };
  return names;
}

/** Pure: one Ozon accrual → summary rows (one per line of the accrual, same lines added up). */
function ozonAccrualMoneyRows(account, accrual, typeNames = new Map()) {
  const rows = new Map();
  const day = cleanText(accrual.date).slice(0, 10);
  const ref = cleanText(accrual.unit_number);
  const add = (key, category, amount, typeName, units = null) => {
    const value = moneyNum(amount);
    if (!value) return;
    const id = `oz:${account}:${accrual.accrual_id}:${key}`;
    const row = rows.get(id) || { id, marketplace: "ozon", account, day, category, amount: 0, ref, units, type_name: typeName };
    row.amount += value;
    rows.set(id, row);
  };
  const fee = (fees) => {
    for (const f of fees || []) {
      const typeId = Number(f.type_id);
      add(`t${typeId}`, OZON_ACCRUAL_CATEGORY.get(typeId) || "other", f.accrued, typeNames.get(typeId) || `Начисление ${typeId}`);
    }
  };
  for (const product of accrual.posting?.products || []) {
    const sale = moneyNum(product.commission?.sale_amount);
    if (sale > 0) add("sale", "sales", sale, "Продажи", Number(product.quantity) || 1);
    else if (sale < 0) add("return", "returns", sale, "Возвраты", Number(product.quantity) || 1);
    add("commission", "commission", product.commission?.sale_commission, "Вознаграждение за продажу");
    fee(product.delivery?.services);
  }
  for (const item of accrual.item_fees?.fees || []) fee(item.fees);
  fee(accrual.container_fees?.fees);
  if (accrual.non_item_fee) fee([accrual.non_item_fee]);
  // whatever the parts don't explain stays visible as «прочее»
  const parts = [...rows.values()].reduce((sum, r) => sum + r.amount, 0);
  const rest = moneyRound(moneyNum(accrual.total_amount) - parts);
  if (Math.abs(rest) >= 0.01) add("rest", "other", rest, "Прочие начисления");
  return [...rows.values()].map((r) => ({ ...r, amount: moneyRound(r.amount) }));
}

async function syncOzonMoneyDay(account, day) {
  const typeNames = await ozonAccrualTypeNames(account).catch(() => new Map());
  const accruals = [];
  let lastId = "";
  for (let page = 0; page < 200; page += 1) {
    const data = await ozonRequest("/v1/finance/accrual/by-day", { date: day, last_id: lastId }, account);
    const list = data?.accruals || [];
    accruals.push(...list);
    if (!list.length || !data?.last_id || data.last_id === lastId) break;
    lastId = data.last_id;
  }
  const rows = accruals.flatMap((a) => ozonAccrualMoneyRows(account.id, a, typeNames));
  // the day's accruals replace what was stored for it
  await getPrisma().$executeRawUnsafe(`DELETE FROM money_ops WHERE marketplace = 'ozon' AND account = $1 AND day = $2::date`, account.id, day);
  await moneyUpsertOps(rows);
  return rows.length;
}

async function syncOzonMoneyOrders(account, fromDay, toDay) {
  let stored = 0;
  // the postings list takes ranges of up to a month
  for (let start = fromDay, end = ""; start <= toDay; start = moneyDayShift(end, 1)) {
    end = moneyDayShift(start, 27) < toDay ? moneyDayShift(start, 27) : toDay;
    for (let offset = 0; offset < 50_000; offset += 1000) {
      const data = await ozonRequest("/v3/posting/fbs/list", {
        dir: "ASC",
        filter: { since: `${start}T00:00:00+03:00`, to: `${end}T23:59:59+03:00` },
        limit: 1000,
        offset,
        with: { analytics_data: false, financial_data: false },
      }, account);
      const postings = data?.result?.postings || [];
      const rows = postings.map((p) => {
        const products = p.products || [];
        const status = cleanText(p.status);
        return {
          id: `oz:${account.id}:${cleanText(p.posting_number)}`,
          marketplace: "ozon",
          account: account.id,
          day: moneyMoscowDay(p.in_process_at || p.created_at || p.shipment_date),
          status,
          amount: moneyRound(products.reduce((s, x) => s + moneyNum(x.price) * (Number(x.quantity) || 1), 0)),
          units: products.reduce((s, x) => s + (Number(x.quantity) || 1), 0),
          cancelled: status === "cancelled",
          returned: false,
        };
      }).filter((r) => r.day);
      await moneyUpsertOrders(rows);
      stored += rows.length;
      if (!data?.result?.has_next && postings.length < 1000) break;
    }
  }
  return stored;
}

// ─── Yandex Market ──────────────────────────────────────────────────────────

const yandexSellerUnitPrice = (item) => (item.prices || [])
  .filter((p) => ["BUYER", "MARKETPLACE", "CASHBACK"].includes(cleanText(p.type).toUpperCase()))
  .reduce((s, p) => s + moneyNum(p.costPerItem), 0);

/** Pure: one Yandex order (stats/orders) → its money rows and its orders-table row. */
function yandexOrderMoney(shopId, order) {
  const status = cleanText(order.status).toUpperCase();
  const orderId = String(order.id);
  const day = cleanText(order.statusUpdateDate).slice(0, 10) || cleanText(order.creationDate).slice(0, 10);
  const rows = [];
  const push = (key, category, amount, typeName, units = null) => {
    const value = moneyRound(amount);
    if (value) rows.push({ id: `ym:${shopId}:${orderId}:${key}`, marketplace: "yandex", account: shopId, day, category, amount: value, ref: orderId, units, type_name: typeName });
  };
  const items = order.items || [];
  let orderAmount = 0;
  let orderUnits = 0;
  if (["DELIVERED", "PARTIALLY_DELIVERED", "PARTIALLY_RETURNED", "RETURNED"].includes(status)) {
    let sold = 0, soldUnits = 0, returned = 0, returnedUnits = 0;
    for (const item of items) {
      const unit = yandexSellerUnitPrice(item);
      // count: what is left in the order after the seller's removals (initialCount is what was ordered)
      const count = Number(item.count) || 0;
      const details = item.details || [];
      const backCount = details.filter((d) => cleanText(d.itemStatus) === "RETURNED").reduce((s, d) => s + (Number(d.itemCount) || 0), 0);
      const rejectedCount = details.filter((d) => cleanText(d.itemStatus) === "REJECTED").reduce((s, d) => s + (Number(d.itemCount) || 0), 0);
      // a whole order RETURNED without item details: it came back after delivery
      const back = status === "RETURNED" && !details.length ? count : backCount;
      // a rejected item (невыкуп) was never sold
      const kept = Math.max(0, count - rejectedCount);
      sold += unit * kept; soldUnits += kept;
      returned += unit * Math.min(back, kept); returnedUnits += Math.min(back, kept);
    }
    push("sale", "sales", sold, "Продажи", soldUnits);
    push("return", "returns", -returned, "Возвраты", returnedUnits);
  }
  const byType = new Map();
  for (const c of order.commissions || []) {
    const type = cleanText(c.type).toUpperCase();
    byType.set(type, (byType.get(type) || 0) + moneyNum(c.actual ?? c.predicted));
  }
  for (const [type, total] of byType) push(`c-${type}`, YANDEX_COMMISSION_CATEGORY[type] || "other", -total, YANDEX_COMMISSION_NAME[type] || type);
  for (const item of items) {
    const count = Number(item.initialCount ?? item.count) || 1;
    orderAmount += yandexSellerUnitPrice(item) * count;
    orderUnits += count;
  }
  const order_ = {
    id: `ym:${shopId}:${orderId}`, marketplace: "yandex", account: shopId,
    day: cleanText(order.creationDate).slice(0, 10), status: status.toLowerCase(), amount: moneyRound(orderAmount), units: orderUnits,
    cancelled: status.startsWith("CANCELLED"), returned: status === "RETURNED" || status === "PARTIALLY_RETURNED",
  };
  return { rows, order: order_ };
}

async function syncYandexMoney(shop, body) {
  let pageToken = "";
  let orders = 0;
  for (let page = 0; page < 500; page += 1) {
    const query = `limit=200${pageToken ? `&page_token=${encodeURIComponent(pageToken)}` : ""}`;
    const data = await yandexRequest(shop, "POST", `/v2/campaigns/${shop.campaignId}/stats/orders?${query}`, body);
    const list = (data?.result?.orders || []).filter((o) => !o.fake);
    const parsed = list.map((o) => yandexOrderMoney(shop.id, o));
    const refs = list.map((o) => String(o.id));
    if (refs.length) {
      // an order's rows are rebuilt from its current state (delivered → returned moves the money)
      await getPrisma().$executeRawUnsafe(`DELETE FROM money_ops WHERE marketplace = 'yandex' AND account = $1 AND ref = ANY($2::text[])`, shop.id, refs);
      await moneyUpsertOps(parsed.flatMap((p) => p.rows));
      await moneyUpsertOrders(parsed.map((p) => p.order).filter((o) => o.day));
    }
    orders += list.length;
    pageToken = data?.result?.paging?.nextPageToken || "";
    if (!pageToken || !list.length) break;
  }
  return orders;
}

// ─── The run ────────────────────────────────────────────────────────────────

async function runMoneySync({ source = "schedule", recentDays = MONEY_RECENT_DAYS, backfill = true } = {}) {
  if (moneySyncRunning) return { status: "already_running" };
  if (!shouldUsePostgresStorage() || !getPrisma()) return { status: "postgres_disabled" };
  moneySyncRunning = true;
  const startedAt = Date.now();
  const result = { ozonOps: 0, ozonOrders: 0, yandexOrders: 0, errors: [] };
  try {
    await requireMoneyTables();
    const state = await moneyReadSetting(MONEY_STATE_KEY, { ozonFilledTo: {}, yandexFilledTo: {} });
    const today = moneyToday();
    const floor = moneyDayShift(today, -MONEY_HISTORY_DAYS);
    for (const account of getOzonAccounts()) {
      try {
        for (let d = moneyDayShift(today, -recentDays); d <= today; d = moneyDayShift(d, 1)) result.ozonOps += await syncOzonMoneyDay(account, d);
        result.ozonOrders += await syncOzonMoneyOrders(account, moneyDayShift(today, -Math.max(recentDays, 10)), today);
        // history: a chunk of older days per run, newest first, down to MONEY_HISTORY_DAYS
        const filledTo = state.ozonFilledTo[account.id] || moneyDayShift(today, -MONEY_RECENT_DAYS);
        if (backfill && filledTo > floor) {
          const chunkFrom = moneyDayShift(filledTo, -MONEY_BACKFILL_DAYS_PER_RUN) > floor ? moneyDayShift(filledTo, -MONEY_BACKFILL_DAYS_PER_RUN) : floor;
          for (let d = moneyDayShift(filledTo, -1); d >= chunkFrom; d = moneyDayShift(d, -1)) result.ozonOps += await syncOzonMoneyDay(account, d);
          result.ozonOrders += await syncOzonMoneyOrders(account, chunkFrom, moneyDayShift(filledTo, -1));
          state.ozonFilledTo[account.id] = chunkFrom;
        }
      } catch (error) {
        result.errors.push(`Ozon ${account.name || account.id}: ${error?.message || error}`);
        logger.warn("money sync ozon failed", { account: account.id, detail: error?.message || String(error) });
      }
    }
    for (const shop of getYandexShops()) {
      if (!shop.campaignId) continue;
      try {
        if (backfill && !state.yandexFilledTo[shop.id]) {
          // the whole history once, by the orders' creation date
          result.yandexOrders += await syncYandexMoney(shop, { dateFrom: floor, dateTo: today });
          state.yandexFilledTo[shop.id] = floor;
        }
        // orders whose status changed lately (delivered, returned, cancelled; commissions arrive later)
        result.yandexOrders += await syncYandexMoney(shop, { updateFrom: moneyDayShift(today, -Math.max(recentDays, 10)), updateTo: today });
      } catch (error) {
        result.errors.push(`Маркет ${shop.name || shop.id}: ${error?.message || error}`);
        logger.warn("money sync yandex failed", { shop: shop.id, detail: error?.message || String(error) });
      }
    }
    state.lastRunAt = new Date().toISOString();
    state.lastResult = { ...result, source, elapsedMs: Date.now() - startedAt };
    await moneyWriteSetting(MONEY_STATE_KEY, state);
    logger.info("money_sync_complete", state.lastResult);
    return { status: result.errors.length ? "partial" : "ok", ...state.lastResult };
  } catch (error) {
    logger.warn("money sync failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    moneySyncRunning = false;
  }
}

function scheduleMoneySync(delayMs = moneySyncIntervalMs) {
  if (!moneySyncEnabled) return;
  if (moneySyncTimer) clearTimeout(moneySyncTimer);
  moneySyncTimer = setTimeout(async () => {
    try {
      await runMoneySync({ source: "schedule" });
    } finally {
      scheduleMoneySync(moneySyncIntervalMs);
    }
  }, Math.max(30_000, Number(delayMs) || moneySyncIntervalMs));
  moneySyncTimer.unref?.();
}

// ─── Summary ────────────────────────────────────────────────────────────────

/** Pure: the bucket a day falls in — the day, its ISO week's Monday, or the month's first day. */
function moneyBucket(day, group) {
  if (group === "month") return `${day.slice(0, 7)}-01`;
  if (group === "week") {
    const d = new Date(`${day}T00:00:00Z`);
    const shift = (d.getUTCDay() + 6) % 7;
    return moneyDayShift(day, -shift);
  }
  return day;
}

/** Purchase cost per sold posting / order: the picking's real purchase first, else the current supplier's price. */
async function moneyPurchaseCosts(refs) {
  const out = new Map();
  for (const marketplace of ["ozon", "yandex"]) {
    const list = [...new Set(refs.filter((r) => r.marketplace === marketplace).map((r) => r.ref))];
    if (!list.length) continue;
    const column = marketplace === "ozon" ? "posting_number" : "order_id";
    const rows = await getPrisma().$queryRawUnsafe(
      `SELECT ${column} AS ref, source, sum(coalesce(purchase_cost, 0))::float8 AS cost, sum(coalesce(sale_amount, 0))::float8 AS sale,
              count(*) FILTER (WHERE coalesce(purchase_cost, 0) <= 0)::int AS missing
         FROM finance_orders WHERE marketplace::text = $1 AND ${column} = ANY($2::text[]) GROUP BY 1, 2`,
      marketplace, list,
    );
    for (const row of rows) {
      const key = `${marketplace}:${row.ref}`;
      const prev = out.get(key);
      // picking rows are the purchase that really happened
      if (prev && prev.source === "supplier_picking") continue;
      if (row.source !== "supplier_picking" && prev) continue;
      out.set(key, { cost: Number(row.cost) || 0, sale: Number(row.sale) || 0, missing: Number(row.missing) || 0, source: row.source });
    }
  }
  return out;
}

async function moneySummary({ from, to, group = "day", account = "all" }) {
  await requireMoneyTables();
  const prisma = getPrisma();
  const accountFilter = account && account !== "all";
  const where = `day BETWEEN $1::date AND $2::date${accountFilter ? " AND (account = $3 OR marketplace = $3)" : ""}`;
  const args = accountFilter ? [from, to, account] : [from, to];
  const [ops, types, refs, orders, expenses, settings, state] = await Promise.all([
    prisma.$queryRawUnsafe(`SELECT to_char(day, 'YYYY-MM-DD') AS day, marketplace, account, category, sum(amount)::float8 AS amount FROM money_ops WHERE ${where} GROUP BY 1, 2, 3, 4`, ...args),
    prisma.$queryRawUnsafe(`SELECT category, marketplace, type_name, sum(amount)::float8 AS amount FROM money_ops WHERE ${where} AND category NOT IN ('sales', 'returns') GROUP BY 1, 2, 3 ORDER BY 4`, ...args),
    prisma.$queryRawUnsafe(`SELECT to_char(day, 'YYYY-MM-DD') AS day, marketplace, account, ref, category, sum(amount)::float8 AS amount FROM money_ops WHERE ${where} AND category IN ('sales', 'returns') AND coalesce(ref, '') <> '' GROUP BY 1, 2, 3, 4, 5`, ...args),
    prisma.$queryRawUnsafe(`SELECT to_char(day, 'YYYY-MM-DD') AS day, marketplace, account, count(*)::int AS orders, sum(amount)::float8 AS amount,
        count(*) FILTER (WHERE cancelled)::int AS cancelled, coalesce(sum(amount) FILTER (WHERE cancelled), 0)::float8 AS cancelled_amount,
        count(*) FILTER (WHERE returned)::int AS returned
      FROM money_orders WHERE ${where} GROUP BY 1, 2, 3`, ...args),
    accountFilter ? [] : prisma.$queryRawUnsafe(`SELECT id, type, amount::float8 AS amount, note, to_char(spent_at AT TIME ZONE 'Europe/Moscow', 'YYYY-MM-DD') AS day
        FROM finance_expenses WHERE source = 'money' AND (spent_at AT TIME ZONE 'Europe/Moscow')::date BETWEEN $1::date AND $2::date ORDER BY spent_at DESC`, from, to),
    moneyReadSetting(MONEY_SETTINGS_KEY, { taxMode: "none", taxRate: 0 }),
    moneyReadSetting(MONEY_STATE_KEY, {}),
  ]);

  const emptyLine = () => Object.fromEntries([...MONEY_CATEGORIES, "cost", "expenses"].map((c) => [c, 0]));
  const buckets = new Map();
  const bucket = (day) => {
    const key = moneyBucket(day, group);
    if (!buckets.has(key)) buckets.set(key, { key, ...emptyLine(), orders: 0, ordersAmount: 0, cancelled: 0, cancelledAmount: 0, returnedOrders: 0, units: 0 });
    return buckets.get(key);
  };
  const totals = emptyLine();
  const byAccount = new Map();
  const accountLine = (marketplace, acc) => {
    const key = `${marketplace}:${acc}`;
    if (!byAccount.has(key)) byAccount.set(key, { key, marketplace, account: acc, ...emptyLine(), orders: 0, cancelled: 0 });
    return byAccount.get(key);
  };
  for (const op of ops) {
    const amount = Number(op.amount) || 0;
    bucket(op.day)[op.category] += amount;
    totals[op.category] += amount;
    accountLine(op.marketplace, op.account)[op.category] += amount;
  }

  // purchase cost of what was sold (and given back by returns), by the sale's day
  const costs = await moneyPurchaseCosts(refs);
  let soldRefs = 0, refsWithoutCost = 0;
  for (const r of refs) {
    const info = costs.get(`${r.marketplace}:${r.ref}`);
    const amount = Number(r.amount) || 0;
    if (r.category === "sales") {
      soldRefs += 1;
      if (!info || info.cost <= 0) { refsWithoutCost += 1; continue; }
    }
    if (!info || info.cost <= 0) continue;
    // a return gives back the share of the purchase it takes back
    const share = r.category === "sales" ? 1 : Math.min(1, Math.abs(amount) / Math.max(info.sale, Math.abs(amount), 1));
    const cost = (r.category === "sales" ? -1 : 1) * info.cost * share;
    bucket(r.day).cost += cost;
    totals.cost += cost;
    accountLine(r.marketplace, r.account).cost += cost;
  }
  for (const e of expenses) {
    bucket(e.day).expenses -= Number(e.amount) || 0;
    totals.expenses -= Number(e.amount) || 0;
  }
  const orderTotals = { orders: 0, ordersAmount: 0, cancelled: 0, cancelledAmount: 0, returnedOrders: 0 };
  for (const o of orders) {
    const b = bucket(o.day);
    b.orders += o.orders; b.ordersAmount += Number(o.amount) || 0; b.cancelled += o.cancelled; b.cancelledAmount += Number(o.cancelled_amount) || 0; b.returnedOrders += o.returned;
    orderTotals.orders += o.orders; orderTotals.ordersAmount += Number(o.amount) || 0; orderTotals.cancelled += o.cancelled;
    orderTotals.cancelledAmount += Number(o.cancelled_amount) || 0; orderTotals.returnedOrders += o.returned;
    const a = accountLine(o.marketplace, o.account);
    a.orders += o.orders; a.cancelled += o.cancelled;
  }

  const taxRate = Math.max(0, Math.min(100, Number(settings.taxRate) || 0)) / 100;
  const finish = (line) => {
    const payout = MONEY_CATEGORIES.reduce((s, c) => s + (line[c] || 0), 0);
    const revenue = (line.sales || 0) + (line.returns || 0);
    const beforeTax = payout + (line.cost || 0) + (line.expenses || 0);
    const tax = settings.taxMode === "income" ? -Math.max(0, revenue) * taxRate : settings.taxMode === "profit" ? -Math.max(0, beforeTax) * taxRate : 0;
    const out = { ...line, payout, revenue, tax, net: beforeTax + tax };
    for (const k of Object.keys(out)) if (typeof out[k] === "number") out[k] = moneyRound(out[k]);
    return out;
  };
  const accounts = [...getOzonAccounts({ includeSyncDisabled: true }), ...getYandexShops({ includeSyncDisabled: true })]
    .map((a) => ({ id: a.id, name: a.name || a.id, marketplace: a.marketplace }));
  const nameOf = new Map(accounts.map((a) => [a.id, a.name]));
  return {
    from, to, group, account,
    totals: { ...finish(totals), ...orderTotals, soldRefs, refsWithoutCost },
    series: [...buckets.values()].sort((a, b) => a.key.localeCompare(b.key)).map(finish),
    byAccount: [...byAccount.values()].map((line) => ({ ...finish(line), name: nameOf.get(line.account) || line.account })),
    breakdown: types.map((t) => ({ category: t.category, marketplace: t.marketplace, name: t.type_name || "—", amount: moneyRound(t.amount) })),
    expenses: expenses.map((e) => ({ id: e.id, type: e.type, amount: Number(e.amount) || 0, note: e.note || "", day: e.day })),
    settings: { taxMode: settings.taxMode, taxRate: Number(settings.taxRate) || 0 },
    accounts,
    sync: { lastRunAt: state.lastRunAt || null, lastResult: state.lastResult || null, running: moneySyncRunning, historyDays: MONEY_HISTORY_DAYS },
  };
}

const MONEY_DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

app.get("/api/money/summary", requireAdmin, async (request, response, next) => {
  try {
    const today = moneyToday();
    const to = MONEY_DAY_RE.test(String(request.query.to || "")) ? String(request.query.to) : today;
    const from = MONEY_DAY_RE.test(String(request.query.from || "")) ? String(request.query.from) : moneyDayShift(to, -29);
    const group = ["day", "week", "month"].includes(String(request.query.group)) ? String(request.query.group) : "day";
    response.json({ ok: true, ...(await moneySummary({ from: from <= to ? from : to, to, group, account: cleanText(request.query.account || "all") })) });
  } catch (error) {
    next(error);
  }
});

app.post("/api/money/sync", requireAdmin, async (request, response, next) => {
  try {
    if (moneySyncRunning) return response.json({ ok: true, status: "already_running" });
    // the fresh days only; the history is filled by the worker
    runMoneySync({ source: "manual", recentDays: 3, backfill: false }).catch(() => {});
    await appendAudit(request, "money.sync", { entityType: "money", entityId: "sync" });
    response.json({ ok: true, status: "started" });
  } catch (error) {
    next(error);
  }
});

app.post("/api/money/settings", requireAdmin, async (request, response, next) => {
  try {
    const taxMode = ["none", "income", "profit"].includes(String(request.body?.taxMode)) ? String(request.body.taxMode) : "none";
    const taxRate = Math.max(0, Math.min(100, Number(request.body?.taxRate) || 0));
    const value = await moneyWriteSetting(MONEY_SETTINGS_KEY, { taxMode, taxRate });
    await appendAudit(request, "money.settings", { entityType: "money", entityId: "settings", newValue: value });
    response.json({ ok: true, settings: value });
  } catch (error) {
    next(error);
  }
});

const MONEY_EXPENSE_TYPES = ["advertising", "salary", "rent", "packaging", "delivery", "services", "tax", "other"];

app.post("/api/money/expenses", requireAdmin, async (request, response, next) => {
  try {
    const amount = moneyRound(Number(request.body?.amount));
    if (!(amount > 0)) return response.status(400).json({ error: "Укажите сумму больше нуля." });
    const day = MONEY_DAY_RE.test(String(request.body?.day || "")) ? String(request.body.day) : moneyToday();
    const type = MONEY_EXPENSE_TYPES.includes(String(request.body?.type)) ? String(request.body.type) : "other";
    const row = await getPrisma().financeExpense.create({
      data: {
        type, amount, currency: "RUB", note: cleanText(request.body?.note).slice(0, 500) || null, source: "money", status: "confirmed",
        spentAt: new Date(`${day}T12:00:00+03:00`), raw: { day, type },
      },
    });
    await appendAudit(request, "money.expense.create", { entityType: "finance_expense", entityId: row.id, newValue: { type, amount, day } });
    response.status(201).json({ ok: true, id: row.id });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/money/expenses/:id", requireAdmin, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id);
    const removed = await getPrisma().financeExpense.deleteMany({ where: { id, source: "money" } });
    await appendAudit(request, "money.expense.delete", { entityType: "finance_expense", entityId: id });
    response.json({ ok: true, removed: removed.count });
  } catch (error) {
    next(error);
  }
});
