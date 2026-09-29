// Yandex Commerce Protocol (YCP) — checkout from Alice AI / Yandex Search / Yandex Ritm.
// Spec: https://yandex.ru/support/merchants-ru-ycp/ru/openapi/ycp-swagger.openapi.json (v0.0.14).
// Yandex calls THESE endpoints on our side; every request carries `Authorization: Bearer <token>`,
// the token we generate and paste into the YCP cabinet (env YCP_INBOUND_TOKEN).
// Answer within 5 s. Base URL for the cabinet: https://davidsklad.ru/api/ycp (both
// <base>/<method> and <base>/api/v1/<method> are served — the spec's servers entry is "api/v1").
//
// Orders live in the regular shop_orders table (visible in the shop admin and the buyer's cabinet):
//   status "ycp_draft" = checkout session, then paid / pending / cancelled / delivered;
//   YCP data is kept in delivery.ycp { session_id, order_id, order_number, history[] … }.
// Globals: app, express, getPrisma, cleanText, logger, getShopFeedProducts, findShopProductByOfferId,
//          shopYmlOfferId, readShopSettings, sendOrderSequenceEmail

const YCP_WAREHOUSE_ID = "mv-main";
const YCP_DRAFT = "ycp_draft";

function ycpAuth(request, response, next) {
  const expected = String(process.env.YCP_INBOUND_TOKEN || "");
  const header = String(request.headers.authorization || "");
  const got = header.startsWith("Bearer ") ? header.slice(7).trim() : "";
  if (!expected || !got) return response.status(401).json({ error: "Unauthorized" });
  const a = Buffer.from(got), b = Buffer.from(expected);
  if (a.length !== b.length || !require("crypto").timingSafeEqual(a, b)) return response.status(401).json({ error: "Unauthorized" });
  next();
}

// ── product lookup ──────────────────────────────────────────────────────────
async function ycpProductIndex() {
  const feed = await getShopFeedProducts();
  if (ycpProductIndex._feed === feed) return ycpProductIndex._idx;
  const byFeedId = new Map(), byOffer = new Map();
  for (const p of feed) {
    byFeedId.set(shopYmlOfferId(p.offerId), p);
    byOffer.set(p.offerId.toLowerCase(), p);
  }
  ycpProductIndex._feed = feed;
  ycpProductIndex._idx = { byFeedId, byOffer };
  return ycpProductIndex._idx;
}

/** id from Yandex → { feedItem, live } ; live = current price/stock straight from the DB */
async function ycpResolve(id, fromMerchantCenter) {
  const { byFeedId, byOffer } = await ycpProductIndex();
  const key = cleanText(id);
  const feedItem = fromMerchantCenter
    ? byFeedId.get(key) || byOffer.get(key.toLowerCase())
    : byOffer.get(key.toLowerCase()) || byFeedId.get(key);
  const offerId = feedItem?.offerId || key;
  const live = await findShopProductByOfferId(offerId, { fast: true }).catch(() => null);
  return { feedItem, live, offerId };
}

// No dimensions in the catalogue: estimate a boxed bottle from its volume (Yandex needs them for delivery).
function ycpDimensions(volume, category) {
  const ml = Number(String(volume || "").replace(",", ".").match(/[\d.]+/)?.[0] || 0);
  if (category === "sets") return { width: 200, height: 160, depth: 80, weight: 800 };
  if (ml && ml <= 10) return { width: 60, height: 35, depth: 30, weight: 60 };
  if (ml && ml <= 50) return { width: 110, height: 65, depth: 45, weight: 250 };
  if (ml && ml <= 100) return { width: 140, height: 85, depth: 60, weight: 450 };
  return { width: 160, height: 100, depth: 75, weight: 650 };
}

const YCP_VAT = Number.isFinite(Number(process.env.YCP_VAT)) && process.env.YCP_VAT !== "" ? Number(process.env.YCP_VAT) : 0; // УСН — без НДС

function ycpAvailable(live) {
  if (!live || !live.inStock || !(live.priceRub > 0)) return 0;
  return Math.max(1, Math.min(10, Number(live.stockQty) || 1));
}

function ycpBasketItem(id, live, feedItem) {
  const site = process.env.SHOP_SITE_URL || "https://magicvibes.ru";
  const price = Math.round(live?.priceRub || 0);
  const item = {
    id,
    name: live?.name || feedItem?.name || id,
    regular_price: price,
    final_price: price,
    vat: YCP_VAT,
    warehouses: [{ id: YCP_WAREHOUSE_ID, available_quantity: ycpAvailable(live) }],
    dimensions: ycpDimensions(live?.volume || feedItem?.volume || extractVolume(live?.name || feedItem?.name || ""), live?.category || feedItem?.category),
    characteristics: [],
    variations: [],
  };
  const img = live?.images?.[0] || feedItem?.images?.[0];
  if (img) item.img = img;
  if (feedItem?.slug) item.url = `${site}/product/${feedItem.slug}`;
  return item;
}

// ── orders ──────────────────────────────────────────────────────────────────
async function ycpFindBy(path, value) {
  const prisma = getPrisma();
  if (!prisma || !value) return null;
  return prisma.shopOrder.findFirst({ where: { delivery: { path: ["ycp", path], equals: String(value) } }, orderBy: { createdAt: "desc" } });
}

async function ycpUpdate(order, patch, ycpPatch, historyStatus) {
  const prisma = getPrisma();
  const delivery = order.delivery && typeof order.delivery === "object" ? order.delivery : {};
  const ycp = { ...(delivery.ycp || {}), ...(ycpPatch || {}) };
  if (historyStatus) {
    const history = Array.isArray(ycp.history) ? ycp.history : [];
    if (!history.some((h) => h.status === historyStatus)) history.push({ status: historyStatus, timestamp: Math.floor(Date.now() / 1000) });
    ycp.history = history;
  }
  return prisma.shopOrder.update({ where: { id: order.id }, data: { ...(patch || {}), delivery: { ...delivery, ycp } } });
}

const ycpRouter = express.Router();
ycpRouter.use(express.json({ limit: "1mb" }));
ycpRouter.use(ycpAuth);

// GET /warehouses — where parcels ship from (Yandex delivery needs the origin address)
ycpRouter.get("/warehouses", async (request, response) => {
  const address = cleanText(process.env.YCP_WAREHOUSE_ADDRESS || "");
  const phone = cleanText(process.env.YCP_WAREHOUSE_PHONE || "");
  const warehouses = address ? [{
    id: YCP_WAREHOUSE_ID,
    title: cleanText(process.env.YCP_WAREHOUSE_TITLE || "Склад Magic Vibes"),
    address,
    phone: phone || "+70000000000",
    self_pickup_options: { enabled: false },
    ycp_delivery_options: { enabled: true },
  }] : [];
  const offset = Math.max(0, Number(request.query.offset) || 0);
  const limit = Math.max(1, Number(request.query.limit) || 100);
  response.json({ warehouses: warehouses.slice(offset, offset + limit), total_count: warehouses.length });
});

// POST /checkout/basket/check — prices and stock, called on every basket change
ycpRouter.post("/checkout/basket/check", async (request, response, next) => {
  try {
    const body = request.body || {};
    const items = Array.isArray(body.items) ? body.items.slice(0, 50) : [];
    if (!items.length) return response.status(400).json({ error: "items is required" });
    const fromMC = body.offers_id_from_merchant_center !== false;
    const out = [];
    for (const it of items) {
      const { feedItem, live, offerId } = await ycpResolve(it.id, fromMC);
      if (!live && !feedItem) continue;
      out.push(ycpBasketItem(live?.offerId || offerId, live, feedItem));
    }
    if (!out.length) return response.status(404).json({ error: "Товары не найдены" });
    response.json({ items: out });
  } catch (error) { next(error); }
});

// POST /checkout — final check, reserve, create the order draft
ycpRouter.post("/checkout", async (request, response, next) => {
  try {
    const prisma = getPrisma();
    if (!prisma) return response.status(500).json({ error: "База данных недоступна" });
    const body = request.body || {};
    const sessionId = cleanText(body.session_id || "");
    const items = Array.isArray(body.items) ? body.items : [];
    const customer = body.customer || {};
    const delivery = body.delivery || {};
    if (!sessionId || !items.length || !customer.phone || !customer.email) return response.status(400).json({ error: "session_id, items, customer are required" });

    // idempotent: a retried request for the same session gets the same order number
    const existing = await ycpFindBy("session_id", sessionId);
    if (existing) return response.status(201).json({ order_number: existing.id });

    const lines = [];
    const actual = [];
    let conflict = false;
    for (const it of items) {
      const { live, offerId } = await ycpResolve(it.id, false);
      const qty = Math.max(1, Math.round(Number(it.quantity) || 1));
      const price = Math.round(live?.priceRub || 0);
      const available = ycpAvailable(live);
      if (!live || available < qty || price !== Math.round(Number(it.final_price) || 0)) conflict = true;
      actual.push({ id: live?.offerId || offerId, regular_price: price, final_price: price, warehouses: [{ id: YCP_WAREHOUSE_ID, available_quantity: available }] });
      if (live) lines.push({ offerId: live.offerId, name: live.name, brand: live.brand || "", image: live.images?.[0] || null, volume: live.volume || null, quantity: qty, priceRub: price });
    }
    if (conflict) return response.status(409).json({ error: "Цены изменились или товары закончились", actual_inventory: { items: actual }, checkout_canceled: false });

    const email = cleanText(customer.email).toLowerCase();
    const buyer = await prisma.shopCustomer.findUnique({ where: { email }, select: { id: true } }).catch(() => null);
    const addr = delivery.address || {};
    const deliveryPrice = Math.round(Number(delivery.price) || 0);
    const orderId = `MV-Y${Date.now().toString(36).toUpperCase()}`;
    const [firstName, ...rest] = cleanText(customer.full_name || "").split(/\s+/);
    await prisma.shopOrder.create({
      data: {
        id: orderId,
        customerId: buyer?.id || null,
        status: YCP_DRAFT,
        items: lines,
        totalRub: lines.reduce((s, l) => s + l.priceRub * l.quantity, 0) + deliveryPrice,
        comment: cleanText(delivery.delivery_notes || "") || null,
        delivery: {
          type: delivery.delivery_method === "courier" ? "courier" : "pickup",
          firstName: firstName || "", lastName: rest.join(" "),
          phone: cleanText(customer.phone), email,
          city: cleanText(addr.locality || ""),
          address: [addr.address, addr.apartment && `кв. ${addr.apartment}`, addr.entrance && `подъезд ${addr.entrance}`, addr.floor && `этаж ${addr.floor}`, addr.intercom && `домофон ${addr.intercom}`].filter(Boolean).join(", "),
          pvzId: cleanText(addr.pickup_point_id || "") || undefined,
          pvzName: cleanText(delivery.service_display_name || "") || undefined,
          ycp: {
            session_id: sessionId,
            warehouse_id: cleanText(body.warehouse_id || YCP_WAREHOUSE_ID),
            delivery_method: delivery.delivery_method, service_type: delivery.service_type,
            service_display_name: delivery.service_display_name || null,
            delivery_price: deliveryPrice, delivery_date_interval: delivery.delivery_date_interval || null,
            ycp_delivery_option_id: delivery.ycp_delivery_option_id || null,
            history: [{ status: "new", timestamp: Math.floor(Date.now() / 1000) }],
          },
        },
      },
    });
    logger.info("ycp checkout session", { orderId, sessionId, items: lines.length });
    response.status(201).json({ order_number: orderId });
  } catch (error) { next(error); }
});

// POST /checkout/placed — the buyer finished the order (paid online or pays on delivery)
ycpRouter.post("/checkout/placed", async (request, response, next) => {
  try {
    const b = { ...(request.query || {}), ...(request.body || {}) };
    const order = await ycpFindBy("session_id", cleanText(b.session_id || ""));
    if (!order) return response.status(404).json({ error: "Заказ не найден" });
    if (order.status === "cancelled") return response.status(409).json({ error: "Заказ уже отменен" });
    const online = b.payment_method === "online";
    const first = order.status === YCP_DRAFT;
    const updated = await ycpUpdate(order, { status: first ? (online ? "paid" : "pending") : order.status }, {
      order_id: cleanText(b.order_id || ""), order_number: b.order_number ?? null,
      payment_method: b.payment_method || null, online_payment_method: b.online_payment_method || null,
      acquiring_id: b.acquiring_id || null, placed_at: new Date().toISOString(),
    }, "processing");
    if (first) {
      const d = updated.delivery || {};
      logger.info("ycp order placed", { orderId: order.id, ycpOrderId: b.order_id, payment: b.payment_method });
      if (d.email && typeof sendOrderSequenceEmail === "function") {
        sendOrderSequenceEmail({ orderId: order.id, email: d.email, firstName: d.firstName || "", items: Array.isArray(order.items) ? order.items : [], totalRub: order.totalRub })
          .catch((err) => logger.warn("ycp order e-mail failed", { orderId: order.id, detail: err?.message }));
      }
    }
    response.json({});
  } catch (error) { next(error); }
});

// POST /checkout/cancel — session abandoned (1 h passed or the buyer went back to the basket)
ycpRouter.post("/checkout/cancel", async (request, response, next) => {
  try {
    const sessionId = cleanText((request.query || {}).session_id || (request.body || {}).session_id || "");
    const order = await ycpFindBy("session_id", sessionId);
    if (!order) return response.status(404).json({ error: "Сессия не найдена" });
    if (order.status === YCP_DRAFT) await ycpUpdate(order, { status: "cancelled" }, { session_cancelled_at: new Date().toISOString() }, "cancelled");
    response.json({});
  } catch (error) { next(error); }
});

// POST /order/cancel — a placed order is cancelled
ycpRouter.post("/order/cancel", async (request, response, next) => {
  try {
    const orderId = cleanText((request.query || {}).order_id || (request.body || {}).order_id || "");
    const order = await ycpFindBy("order_id", orderId);
    if (!order) return response.status(404).json({ error: "Заказ не найден" });
    if (order.status !== "cancelled") {
      await ycpUpdate(order, { status: "cancelled" }, { cancelled_at: new Date().toISOString() }, "cancelled");
      logger.info("ycp order cancelled", { orderId: order.id, ycpOrderId: orderId });
    }
    response.json({});
  } catch (error) { next(error); }
});

// POST /order/delivered — delivery by Yandex finished (maybe partially bought out)
ycpRouter.post("/order/delivered", async (request, response, next) => {
  try {
    const orderId = cleanText((request.query || {}).order_id || (request.body || {}).order_id || "");
    const order = await ycpFindBy("order_id", orderId);
    if (!order) return response.status(404).json({ error: "Заказ не найден" });
    if (order.status === "cancelled") return response.status(409).json({ error: "Заказ отменен" });
    const purchased = Array.isArray((request.body || {}).purchased_items) ? request.body.purchased_items : [];
    await ycpUpdate(order, { status: "delivered" }, { purchased_items: purchased, delivered_at: new Date().toISOString() }, "delivered");
    response.json({});
  } catch (error) { next(error); }
});

// GET /order — delivery status history for the buyer's order page in Yandex
const YCP_STATUS_FROM_SHOP = { paid: "processing", pending: "processing", confirmed: "processing", picking: "processing", shipped: "in_progress", delivered: "delivered", cancelled: "cancelled" };
ycpRouter.get("/order", async (request, response, next) => {
  try {
    const orderId = cleanText(request.query.order_id || "");
    if (!orderId) return response.status(400).json({ error: "order_id is required" });
    const order = await ycpFindBy("order_id", orderId);
    if (!order) return response.status(400).json({ error: "Заказ не найден" });
    const ycp = order.delivery?.ycp || {};
    const history = (Array.isArray(ycp.history) ? ycp.history : [{ status: "new", timestamp: Math.floor(new Date(order.createdAt).getTime() / 1000) }]).slice();
    // statuses changed in the shop admin (shipped, delivered …) show up here too
    const current = YCP_STATUS_FROM_SHOP[order.status];
    if (current && !history.some((h) => h.status === current)) history.push({ status: current, timestamp: Math.floor(Date.now() / 1000) });
    const cancelled = order.status === "cancelled";
    const purchased = Array.isArray(ycp.purchased_items) ? ycp.purchased_items : null;
    const items = (Array.isArray(order.items) ? order.items : []).map((i) => {
      const bought = purchased ? (purchased.find((p) => String(p.id) === String(i.offerId))?.quantity ?? 0) : i.quantity;
      return { id: i.offerId, quantity: i.quantity, refused_count: cancelled ? i.quantity : Math.max(0, i.quantity - bought) };
    });
    const out = { items, delivery_statuses: history };
    if (ycp.tracking_url) out.tracking_url = ycp.tracking_url;
    response.json(out);
  } catch (error) { next(error); }
});

// Delivery is left to Yandex (YCP delivery / Yandex Delivery) — these two answer with
// our flat terms in case the cabinet is switched to merchant-managed delivery.
ycpRouter.get("/checkout/delivery/pickup_points", (_request, response) => {
  response.json({ pickup_points: [], total_count: 0 });
});

ycpRouter.post("/checkout/delivery/options", async (request, response, next) => {
  try {
    const s = await readShopSettings();
    // free delivery from the shop's threshold, like on the site
    let total = 0;
    for (const it of Array.isArray(request.body?.items) ? request.body.items : []) {
      const { live } = await ycpResolve(it.id, false);
      total += Math.round(live?.priceRub || 0) * Math.max(1, Number(it.quantity) || 1);
    }
    const free = Number(s.freeDeliveryFrom) > 0 && total >= Number(s.freeDeliveryFrom);
    const day = (n) => new Date(Date.now() + n * 86400000).toISOString().slice(0, 10);
    response.json({
      delivery_options: [{
        id: "mv-standard",
        cost: free ? 0 : Number(s.deliveryPriceRub || 0),
        delivery_date_interval: { start_interval: { date: day(Number(s.deliveryDaysMin) || 1) }, end_interval: { date: day(Number(s.deliveryDays) || 5) }, time_zone: 3 },
      }],
    });
  } catch (error) { next(error); }
});

ycpRouter.use((error, _request, response, _next) => {
  logger.warn("ycp request failed", { detail: error?.message || String(error) });
  response.status(500).json({ error: "Внутренняя ошибка сервера" });
});

app.use(["/api/ycp/api/v1", "/api/ycp"], ycpRouter);
