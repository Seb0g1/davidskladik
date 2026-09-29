// Доставка магазина: СДЭК, Яндекс Доставка (в другой день), Достависта (курьер день в день).
// Покупатель платит за доставку сам: цена = тариф перевозчика + наценка (по умолчанию 2,5% — покрывает
// эквайринг Ozon Pay с суммы доставки), округление вверх до 10 ₽. Сроки = срок перевозчика + дни сборки.
// Globals: app, shopCors, requireAdmin, getPrisma, cleanText, logger, readShopSettings, readAppSettings,
//          writeAppSettings?, SHOP_SETTINGS_KEY, findShopProductByOfferId, backgroundJobsEnabled
//
// env: CDEK_CLIENT_ID, CDEK_CLIENT_SECRET, CDEK_API_BASE (https://api.cdek.ru)
//      YANDEX_DELIVERY_TOKEN, YANDEX_DELIVERY_BASE (https://b2b-authproxy.taxi.yandex.net)
//      DOSTAVISTA_API_TOKEN, DOSTAVISTA_API_BASE (https://robot.dostavista.ru — боевой; robotapitest… — тест),
//      DOSTAVISTA_CALLBACK_SECRET

const CARRIER_DEFAULTS = {
  sender: {
    name: "Magic Vibes", contactName: "", phone: "", city: "Москва", address: "", comment: "",
    cdekCityCode: 44,            // город отгрузки в СДЭК (44 — Москва)
    cdekShipmentPoint: "",       // код ПВЗ СДЭК, куда сдаём посылки (пусто — по тарифу «склад», город отгрузки)
    yandexStationId: "",         // точка самопривоза / склад Яндекса (platform_station_id) — нужна для создания заявки
    yandexStationName: "",
  },
  cdek: { enabled: true, pvz: true, postamat: true, courier: true },
  yandex: { enabled: true, pvz: true, courier: true },
  dostavista: { enabled: true, regions: ["москва", "московская"], fast: true, slot: true },
  pricing: { extraPct: 2.5, extraRub: 0, roundTo: 10, handlingDays: 1, minRub: 0 },
  autoCreateShipment: false,
};

function carrierSettings(shopSettings) {
  const c = shopSettings?.carriers || {};
  const merge = (k) => ({ ...CARRIER_DEFAULTS[k], ...(c[k] || {}) });
  return { sender: merge("sender"), cdek: merge("cdek"), yandex: merge("yandex"), dostavista: merge("dostavista"), pricing: merge("pricing"), autoCreateShipment: Boolean(c.autoCreateShipment) };
}

const CARRIER_ENV = () => ({
  cdek: { id: process.env.CDEK_CLIENT_ID || "", secret: process.env.CDEK_CLIENT_SECRET || "", base: (process.env.CDEK_API_BASE || "https://api.cdek.ru").replace(/\/+$/, "") },
  yandex: { token: process.env.YANDEX_DELIVERY_TOKEN || "", base: (process.env.YANDEX_DELIVERY_BASE || "https://b2b-authproxy.taxi.yandex.net").replace(/\/+$/, "") },
  dv: { token: process.env.DOSTAVISTA_API_TOKEN || "", base: (process.env.DOSTAVISTA_API_BASE || "https://robot.dostavista.ru").replace(/\/+$/, ""), secret: process.env.DOSTAVISTA_CALLBACK_SECRET || "" },
});

async function carrierFetch(url, opts = {}, timeoutMs = 9000) {
  const res = await fetch(url, { ...opts, signal: AbortSignal.timeout(timeoutMs) });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON */ }
  if (!res.ok) {
    const msg = json?.message || json?.errors?.map?.((e) => e.message || e).join("; ") || json?.code || text.slice(0, 200);
    const err = new Error(`${res.status} ${msg}`);
    err.status = res.status; err.body = json || text;
    throw err;
  }
  return json;
}

// ── Посылка: вес и габариты из объёма флаконов ────────────────────────────────
function carrierParcel(items) {
  let grams = 150; // коробка и наполнитель
  let units = 0;
  const lines = [];
  for (const it of items) {
    const qty = Math.max(1, Number(it.quantity) || 1);
    const m = String(it.volume || it.name || "").match(/(\d+(?:[.,]\d+)?)\s*(мл|ml)(?![a-zа-яё])/i);
    const ml = m ? parseFloat(m[1].replace(",", ".")) : 100;
    const w = Math.round(ml <= 12 ? 40 : 180 + ml * 3.2); // флакон в коробке: 100 мл ≈ 500 г
    grams += w * qty;
    units += qty;
    lines.push({ ...it, qty, unitGrams: w });
  }
  const h = Math.min(40, 10 + Math.max(0, units - 1) * 6);
  return { grams, kg: Math.max(0.1, Math.round(grams / 100) / 10), length: 22, width: 16, height: h, units, lines };
}

function carrierClientPrice(rawRub, cs) {
  const p = cs.pricing;
  const step = Number(p.roundTo) > 0 ? Number(p.roundTo) : 10;
  let v = Number(rawRub) * (1 + (Number(p.extraPct) || 0) / 100) + (Number(p.extraRub) || 0);
  v = Math.ceil(v / step) * step;
  return Math.max(Number(p.minRub) || 0, v);
}

const _carrierCache = new Map();
async function carrierCached(key, ttlMs, fn) {
  const hit = _carrierCache.get(key);
  if (hit && Date.now() - hit.at < ttlMs) return hit.value;
  const value = await fn();
  _carrierCache.set(key, { at: Date.now(), value });
  if (_carrierCache.size > 3000) for (const k of [..._carrierCache.keys()].slice(0, 500)) _carrierCache.delete(k);
  return value;
}
const cityKey = (s) => cleanText(s || "").toLowerCase().replace(/ё/g, "е").replace(/^(г\.?|город)\s+/, "").trim();

// ── СДЭК ──────────────────────────────────────────────────────────────────────
let _cdekToken = null;
async function cdekToken() {
  const e = CARRIER_ENV().cdek;
  if (!e.id || !e.secret) throw new Error("СДЭК не настроен (CDEK_CLIENT_ID/SECRET)");
  if (_cdekToken && _cdekToken.exp > Date.now() + 60000) return _cdekToken.token;
  const j = await carrierFetch(`${e.base}/v2/oauth/token`, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: new URLSearchParams({ grant_type: "client_credentials", client_id: e.id, client_secret: e.secret }) });
  _cdekToken = { token: j.access_token, exp: Date.now() + (Number(j.expires_in) || 3600) * 1000 };
  return _cdekToken.token;
}
async function cdekApi(path, body, method) {
  const e = CARRIER_ENV().cdek;
  return carrierFetch(`${e.base}${path}`, { method: method || (body ? "POST" : "GET"), headers: { Authorization: `Bearer ${await cdekToken()}`, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
}
async function cdekCity(city) {
  const k = cityKey(city);
  if (!k) return null;
  return carrierCached(`cdek:city:${k}`, 24 * 3600e3, async () => {
    const list = await cdekApi(`/v2/location/cities?country_codes=RU&size=10&city=${encodeURIComponent(cleanText(city).replace(/^(г\.?|город)\s+/i, ""))}`);
    const arr = Array.isArray(list) ? list : [];
    const exact = arr.filter((c) => cityKey(c.city) === k);
    // одноимённые города: крупнее тот, у кого есть sub_region «городской округ …» — берём первый точный
    return (exact[0] || arr[0]) ? { code: (exact[0] || arr[0]).code, name: (exact[0] || arr[0]).city, region: (exact[0] || arr[0]).region } : null;
  });
}
const CDEK_TARIFFS = { pvz: 136, postamat: 368, courier: 137 };
async function cdekTariffs(fromCode, toCode, parcel) {
  return carrierCached(`cdek:tl:${fromCode}:${toCode}:${parcel.grams}:${parcel.height}`, 3600e3, async () => {
    const j = await cdekApi("/v2/calculator/tarifflist", { from_location: { code: fromCode }, to_location: { code: toCode }, packages: [{ weight: parcel.grams, length: parcel.length, width: parcel.width, height: parcel.height }] });
    return Array.isArray(j?.tariff_codes) ? j.tariff_codes : [];
  });
}
async function cdekPoints(city) {
  const c = await cdekCity(city);
  if (!c) return [];
  return carrierCached(`cdek:pts:${c.code}`, 6 * 3600e3, async () => {
    const list = await cdekApi(`/v2/deliverypoints?city_code=${c.code}&is_handout=true&size=2000`);
    return (Array.isArray(list) ? list : []).filter((p) => p.location?.latitude).map((p) => ({
      id: p.code, carrier: "cdek", kind: p.type === "POSTAMAT" ? "postamat" : "pvz",
      name: p.type === "POSTAMAT" ? "Постамат СДЭК" : "ПВЗ СДЭК",
      address: cleanText(p.location?.address_full || p.location?.address || p.name || ""),
      city: p.location?.city || c.name, lat: p.location.latitude, lng: p.location.longitude,
      schedule: cleanText(p.work_time || "") || null, type: p.type === "POSTAMAT" ? "Постамат" : "ПВЗ",
    }));
  });
}

// ── Яндекс Доставка (в другой день) ───────────────────────────────────────────
async function yaApi(path, body, method) {
  const e = CARRIER_ENV().yandex;
  if (!e.token) throw new Error("Яндекс Доставка не настроена (YANDEX_DELIVERY_TOKEN)");
  return carrierFetch(`${e.base}/api/b2b/platform${path}`, { method: method || (body ? "POST" : "GET"), headers: { Authorization: `Bearer ${e.token}`, "Accept-Language": "ru", "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined }, 12000);
}
async function yaGeoId(city) {
  const k = cityKey(city);
  if (!k) return null;
  return carrierCached(`ya:geo:${k}`, 24 * 3600e3, async () => (await yaApi("/location/detect", { location: cleanText(city) }))?.variants?.[0]?.geo_id || null);
}
async function yaPoints(city) {
  const geo = await yaGeoId(city);
  if (!geo) return [];
  return carrierCached(`ya:pts:${geo}`, 6 * 3600e3, async () => {
    const j = await yaApi("/pickup-points/list", { geo_id: geo, payment_method: "already_paid", type: "pickup_point" });
    const t = await yaApi("/pickup-points/list", { geo_id: geo, payment_method: "already_paid", type: "terminal" }).catch(() => ({ points: [] }));
    return [...(j?.points || []), ...(t?.points || [])].filter((p) => p.position?.latitude).map((p) => ({
      id: p.id, carrier: "yandex", kind: p.type === "terminal" ? "postamat" : "pvz",
      name: cleanText(p.name || "Пункт Яндекс Доставки"), address: cleanText(p.address?.full_address || ""),
      city: cleanText(p.address?.locality || city), lat: p.position.latitude, lng: p.position.longitude,
      schedule: yaSchedule(p.schedule), type: p.type === "terminal" ? "Постамат" : "ПВЗ",
    }));
  });
}
function yaSchedule(s) {
  const r = s?.restrictions?.[0];
  if (!r) return null;
  const f = (t) => `${String(t?.hours ?? 0).padStart(2, "0")}:${String(t?.minutes ?? 0).padStart(2, "0")}`;
  return `${f(r.time_from)}–${f(r.time_to)}`;
}
function yaSource(cs) {
  const s = cs.sender;
  if (s.yandexStationId) return { platform_station_id: s.yandexStationId };
  return { address: dvSenderAddress(cs) || s.city || "Москва" };
}
async function yaPrice(cs, { tariff, pointId, address }, parcel, goodsRub) {
  const body = {
    source: yaSource(cs),
    destination: tariff === "self_pickup" ? { platform_station_id: pointId } : { address },
    tariff, total_weight: parcel.grams, total_assessed_price: Math.round(goodsRub * 100), client_price: 0,
    payment_method: "already_paid", places: [{ physical_dims: { weight_gross: parcel.grams, dx: parcel.length, dy: parcel.width, dz: parcel.height } }],
  };
  const key = `ya:price:${JSON.stringify(body.source)}:${tariff}:${pointId || cityKey(address)}:${parcel.grams}:${Math.round(goodsRub / 1000)}`;
  return carrierCached(key, 1800e3, async () => {
    const j = await yaApi("/pricing-calculator", body);
    return { rub: parseFloat(String(j?.pricing_total || "0")) || 0, days: Number(j?.delivery_days) || null };
  });
}

// ── Достависта ────────────────────────────────────────────────────────────────
async function dvApi(path, body) {
  const e = CARRIER_ENV().dv;
  if (!e.token) throw new Error("Достависта не настроена (DOSTAVISTA_API_TOKEN)");
  const j = await carrierFetch(`${e.base}/api/business/1.6/${path}`, { method: body ? "POST" : "GET", headers: { "X-DV-Auth-Token": e.token, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  if (j && j.is_successful === false) {
    const err = new Error(`Достависта: ${(j.errors || []).join(", ")} ${j.parameter_errors ? JSON.stringify(j.parameter_errors) : ""}`.trim());
    err.body = j;
    throw err;
  }
  return j;
}
function dvCovers(cs, city, region = "") {
  city = `${city || ""} ${region || ""}`;
  const k = cityKey(city);
  return !!k && (cs.dostavista.regions || []).some((r) => k.includes(cityKey(r)));
}
function dvSenderAddress(cs) {
  const s = cs.sender;
  if (!s.address) return "";
  // адрес уже с городом («Москва, ул. Складочная…») — второй раз город не добавляем
  return s.city && !cityKey(s.address).startsWith(cityKey(s.city)) ? `${s.city}, ${s.address}` : s.address;
}
async function dvCalc(cs, address, parcel, goodsRub, extra) {
  const from = dvSenderAddress(cs);
  const key = `dv:${JSON.stringify(extra)}:${cityKey(from)}:${cityKey(address)}:${parcel.kg}`;
  return carrierCached(key, 900e3, async () => {
    const j = await dvApi("calculate-order", {
      matter: "Парфюмерия", total_weight_kg: Math.ceil(parcel.kg), insurance_amount: String(Math.round(goodsRub)),
      points: [{ address: from }, { address }], ...extra,
    });
    return parseFloat(j?.order?.payment_amount || "0") || 0;
  });
}
// «быстрее»: сразу; пеший курьер или автомобиль — берём дешевле
async function dvPriceFast(cs, address, parcel, goodsRub) {
  const [foot, car] = await Promise.all([6, 7].map((v) => dvCalc(cs, address, parcel, goodsRub, { type: "standard", vehicle_type_id: v }).catch(() => 0)));
  const opts = [[foot, 6], [car, 7]].filter(([r]) => r > 0).sort((a, b) => a[0] - b[0]);
  return opts[0] ? { rub: opts[0][0], vehicleTypeId: opts[0][1] } : { rub: 0 };
}
// интервалы «ко времени» на сегодня и завтра (прошедшие и слишком ранние отбрасываем)
async function dvIntervals() {
  return carrierCached(`dv:intervals:${new Date().toISOString().slice(0, 13)}`, 1800e3, async () => {
    const out = [];
    for (const d of [0, 1]) {
      const date = new Date(Date.now() + d * 864e5 + 3 * 3600e3).toISOString().slice(0, 10); // дата по Москве
      const j = await dvApi(`delivery-intervals?date=${date}`).catch(() => null);
      for (const iv of j?.delivery_intervals || []) {
        if (Date.parse(iv.required_start_datetime) < Date.now() + 2.5 * 3600e3) continue; // успеть собрать заказ
        out.push({ from: iv.required_start_datetime, to: iv.required_finish_datetime });
      }
    }
    return out.slice(0, 12);
  });
}

// ── Варианты доставки для корзины ─────────────────────────────────────────────
const CARRIER_TITLES = {
  cdek_pvz: "СДЭК — пункт выдачи или постамат", cdek_postamat: "СДЭК — постамат", cdek_courier: "СДЭК — курьер до двери",
  yandex_pvz: "Яндекс Доставка — пункт выдачи или постамат", yandex_courier: "Яндекс Доставка — курьер",
  dostavista: "Достависта — быстрее: курьер выедет сразу", dostavista_slot: "Достависта — ко времени: интервал 4 часа",
};

async function carrierLoadItems(rawItems) {
  const items = [];
  for (const it of (Array.isArray(rawItems) ? rawItems : []).slice(0, 50)) {
    const offerId = cleanText(it?.offerId || "");
    if (!offerId) continue;
    const p = await findShopProductByOfferId(offerId, { fast: true }).catch(() => null);
    items.push({ offerId, name: p?.name || cleanText(it?.name || ""), volume: p?.volume || "", quantity: Math.min(10, Math.max(1, Number(it.quantity) || 1)), priceRub: Number(p?.priceRub) || 0 });
  }
  return items;
}

const withTimeout = (p, ms) => Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), ms))]);

/**
 * Все доступные варианты: [{ id, carrier, kind: "pvz"|"postamat"|"courier", title, priceRub, rawRub, daysMin, daysMax,
 *   needs: "point"|"address", pointPrice?: true }]. address/pointId уточняют цену выбранного варианта.
 */
async function shopCarrierOptions({ items, goodsRub, city, region = "", address = "", pointId = "", only = null, courierOnly = false }) {
  const settings = await readShopSettings();
  const cs = carrierSettings(settings);
  const parcel = carrierParcel(items);
  const freeFrom = Math.max(0, Number(settings.freeDeliveryFrom) || 0);
  const free = freeFrom > 0 && goodsRub >= freeFrom;
  const hd = Math.max(0, Number(cs.pricing.handlingDays) || 0);
  const out = [];
  const errors = {};
  const want = (id) => (!only || only === id) && (!courierOnly || /courier|dostavista/.test(id));
  const push = (id, carrier, kind, rawRub, dMin, dMax, needs, extra = {}) => {
    if (!(rawRub > 0)) return;
    out.push({ id, carrier, kind, title: CARRIER_TITLES[id], rawRub: Math.round(rawRub * 100) / 100, priceRub: free ? 0 : carrierClientPrice(rawRub, cs), free, daysMin: dMin != null ? dMin + hd : null, daysMax: dMax != null ? dMax + hd : null, needs, ...extra });
  };
  if (!cityKey(city)) return { options: [], parcel, errors: { city: "no_city" } };
  const env = CARRIER_ENV();
  const tasks = [];

  if (cs.cdek.enabled && env.cdek.id && (want("cdek_pvz") || want("cdek_postamat") || want("cdek_courier"))) {
    tasks.push(withTimeout((async () => {
      const to = await cdekCity(city);
      if (!to) { errors.cdek = "city_not_found"; return; }
      const list = await cdekTariffs(Number(cs.sender.cdekCityCode) || 44, to.code, parcel);
      const t = (code) => list.find((x) => x.tariff_code === code);
      if (cs.cdek.pvz && want("cdek_pvz")) {
        // выбранный постамат → тариф «склад-постамат», иначе «склад-склад»; до выбора — дешевле из двух
        let pointKind = "";
        if (pointId && only === "cdek_pvz") pointKind = (typeof cdekPointById === "function" ? (await cdekPointById(pointId))?.kind : "") || (await cdekPoints(city)).find((p) => p.id === pointId)?.kind || "";
        const pv = t(CDEK_TARIFFS.pvz), pa = cs.cdek.postamat ? t(CDEK_TARIFFS.postamat) : null;
        const x = pointKind === "postamat" ? pa : pointKind === "pvz" ? pv : [pv, pa].filter(Boolean).sort((a, b) => a.delivery_sum - b.delivery_sum)[0];
        if (x) push("cdek_pvz", "cdek", pointKind || "pvz", x.delivery_sum, x.period_min, x.period_max, "point", { tariffCode: x.tariff_code, cityCode: to.code, postamats: !!pa });
      }
      if (cs.cdek.courier && want("cdek_courier") && t(CDEK_TARIFFS.courier)) { const x = t(CDEK_TARIFFS.courier); push("cdek_courier", "cdek", "courier", x.delivery_sum, x.period_min, x.period_max, "address", { tariffCode: x.tariff_code, cityCode: to.code }); }
    })(), 12000).catch((e) => { errors.cdek = e.message; }));
  }

  if (cs.yandex.enabled && env.yandex.token) {
    if (cs.yandex.pvz && want("yandex_pvz")) {
      tasks.push(withTimeout((async () => {
        let pid = pointId && only === "yandex_pvz" ? pointId : "";
        if (!pid) { const pts = await yaPoints(city); pid = pts.find((p) => p.kind === "pvz")?.id || pts[0]?.id; }
        if (!pid) { errors.yandex_pvz = "no_points"; return; }
        const r = await yaPrice(cs, { tariff: "self_pickup", pointId: pid }, parcel, goodsRub);
        push("yandex_pvz", "yandex", "pvz", r.rub, r.days ? Math.max(1, r.days - 2) : null, r.days, "point");
      })(), 14000).catch((e) => { errors.yandex_pvz = e.message; }));
    }
    if (cs.yandex.courier && want("yandex_courier")) {
      tasks.push(withTimeout((async () => {
        const dest = address ? `${cleanText(city)}, ${cleanText(address)}` : cleanText(city);
        const r = await yaPrice(cs, { tariff: "time_interval", address: dest }, parcel, goodsRub);
        push("yandex_courier", "yandex", "courier", r.rub, r.days ? Math.max(1, r.days - 2) : null, r.days, "address");
      })(), 14000).catch((e) => { errors.yandex_courier = e.message; }));
    }
  }

  if (cs.dostavista.enabled && env.dv.token && (want("dostavista") || want("dostavista_slot")) && dvCovers(cs, city, region)) {
    if (!dvSenderAddress(cs)) errors.dostavista = "no_sender_address";
    else {
      const dest = address ? `${cleanText(city)}, ${cleanText(address)}` : cleanText(city);
      if (want("dostavista") && cs.dostavista.fast !== false) tasks.push(withTimeout((async () => {
        const r = await dvPriceFast(cs, dest, parcel, goodsRub);
        push("dostavista", "dostavista", "courier", r.rub, 0, 0, "address", { estimate: !address, sameDay: true, speed: "fast", vehicleTypeId: r.vehicleTypeId });
      })(), 12000).catch((e) => { errors.dostavista = e.message; }));
      if (want("dostavista_slot") && cs.dostavista.slot !== false) tasks.push(withTimeout((async () => {
        const [rub, intervals] = await Promise.all([dvCalc(cs, dest, parcel, goodsRub, { type: "same_day" }), dvIntervals()]);
        if (intervals.length) push("dostavista_slot", "dostavista", "courier", rub, 0, 1, "address", { estimate: !address, speed: "slot", intervals });
      })(), 12000).catch((e) => { errors.dostavista_slot = e.message; }));
    }
  }

  await Promise.all(tasks);
  const order = ["dostavista", "dostavista_slot", "cdek_pvz", "yandex_pvz", "cdek_courier", "yandex_courier"];
  out.sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));
  return { options: out, parcel: { grams: parcel.grams, dims: [parcel.length, parcel.width, parcel.height] }, errors };
}

app.post("/api/shop/checkout/delivery-options", shopCors, async (request, response, next) => {
  try {
    const body = request.body || {};
    const items = await carrierLoadItems(body.items);
    const goodsList = items.reduce((s, i) => s + i.priceRub * i.quantity, 0);
    const goodsRub = Number(body.goodsRub) > 0 ? Math.min(goodsList || Infinity, Number(body.goodsRub)) : goodsList;
    const r = await shopCarrierOptions({
      items, goodsRub, city: cleanText(body.city || "").slice(0, 120), address: cleanText(body.address || "").slice(0, 300),
      pointId: cleanText(body.pointId || "").slice(0, 80), only: body.only ? cleanText(body.only) : null,
    });
    if (Object.keys(r.errors).length) logger.info("carrier options errors", { city: body.city, errors: r.errors });
    response.json({ ok: true, options: r.options, parcel: r.parcel });
  } catch (error) { next(error); }
});

app.get("/api/shop/delivery/points", shopCors, async (request, response, next) => {
  try {
    const carrier = cleanText(request.query.carrier || "");
    const city = cleanText(request.query.city || "").slice(0, 120);
    const kind = cleanText(request.query.kind || "");
    if (!city) return response.status(400).json({ error: "city required" });
    let pts = carrier === "cdek" ? await cdekPoints(city) : carrier === "yandex" ? await yaPoints(city) : null;
    if (!pts) return response.status(400).json({ error: "unknown carrier" });
    if (kind) pts = pts.filter((p) => p.kind === kind);
    if (carrier === "cdek" && !carrierSettings(await readShopSettings()).cdek.postamat) pts = pts.filter((p) => p.kind !== "postamat");
    response.json({ pvz: pts.slice(0, 2500), city, count: pts.length, source: carrier });
  } catch (error) { next(error); }
});

// ── Отправки ─────────────────────────────────────────────────────────────────
function addrDetails(d) {
  const parts = [d.flat && `кв./офис ${d.flat}`, d.entrance && `подъезд ${d.entrance}`, d.floor && `этаж ${d.floor}`, d.intercom && `домофон ${d.intercom}`];
  return { line: parts.filter(Boolean).join(", "), comment: [parts.filter(Boolean).join(", "), cleanText(d.addrComment || "")].filter(Boolean).join(". ") };
}
function shipmentRecipient(d) {
  return { name: [d.firstName, d.lastName].filter(Boolean).join(" ").trim() || "Покупатель", phone: cleanText(d.phone || "").replace(/[^\d+]/g, ""), email: cleanText(d.email || "") };
}

async function cdekCreateShipment(order, d, cs) {
  const parcel = carrierParcel(order.items);
  const rcp = shipmentRecipient(d);
  const s = cs.sender;
  const body = {
    type: 1, number: order.id, tariff_code: Number(d.tariffCode) || CDEK_TARIFFS[d.kind] || 136,
    comment: [`Magic Vibes ${order.id}`, addrDetails(d).comment].filter(Boolean).join(". ").slice(0, 255),
    recipient: { name: rcp.name, phones: [{ number: rcp.phone }], ...(rcp.email ? { email: rcp.email } : {}) },
    ...(s.cdekShipmentPoint ? { shipment_point: s.cdekShipmentPoint } : { from_location: { code: Number(s.cdekCityCode) || 44, address: s.address || s.city } }),
    ...(d.kind === "courier"
      ? { to_location: { code: Number(d.cityCode) || (await cdekCity(d.city))?.code, address: [cleanText(d.address || ""), d.flat && `кв. ${d.flat}`].filter(Boolean).join(", "), ...(d.lat && d.lng ? { latitude: Number(d.lat), longitude: Number(d.lng) } : {}) } }
      : { delivery_point: d.pvzId }),
    packages: [{
      number: "1", weight: parcel.grams, length: parcel.length, width: parcel.width, height: parcel.height,
      items: parcel.lines.map((l) => ({ name: cleanText(l.name || l.offerId).slice(0, 255), ware_key: String(l.offerId).slice(0, 50), payment: { value: 0 }, cost: Math.round(Number(l.priceRub) || 0), amount: l.qty, weight: l.unitGrams })),
    }],
    ...(s.phone ? { sender: { name: s.contactName || s.name, phones: [{ number: s.phone.replace(/[^\d+]/g, "") }] } } : {}),
    seller: { name: s.name || "Magic Vibes" },
  };
  const j = await cdekApi("/v2/orders", body);
  const uuid = j?.entity?.uuid;
  if (!uuid) throw new Error(`СДЭК не вернул заказ: ${JSON.stringify(j?.requests?.[0]?.errors || j).slice(0, 300)}`);
  return { carrier: "cdek", id: uuid, status: "created", createdAt: new Date().toISOString() };
}

async function yandexCreateShipment(order, d, cs) {
  if (!cs.sender.yandexStationId) throw new Error("Укажите точку сдачи Яндекс Доставки в настройках доставки (станция самопривоза)");
  const parcel = carrierParcel(order.items);
  const rcp = shipmentRecipient(d);
  const barcode = `MV-${order.id}`.slice(0, 40);
  const body = {
    info: { operator_request_id: order.id, comment: [`Magic Vibes ${order.id}`, addrDetails(d).comment].filter(Boolean).join(". ").slice(0, 250) },
    source: { platform_station: { platform_id: cs.sender.yandexStationId } },
    destination: d.kind === "courier"
      ? { type: "custom_location", custom_location: { ...(d.lat && d.lng ? { latitude: Number(d.lat), longitude: Number(d.lng) } : {}), details: { full_address: `${cleanText(d.city || "")}, ${cleanText(d.address || "")}`, ...(d.flat ? { room: String(d.flat).slice(0, 20) } : {}) } } }
      : { type: "platform_station", platform_station: { platform_id: d.pvzId } },
    items: parcel.lines.map((l) => ({ count: l.qty, name: cleanText(l.name || l.offerId).slice(0, 200), article: String(l.offerId).slice(0, 50), place_barcode: barcode,
      billing_details: { unit_price: Math.round((Number(l.priceRub) || 0) * 100), assessed_unit_price: Math.round((Number(l.priceRub) || 0) * 100), nds: -1 } })),
    places: [{ barcode, physical_dims: { weight_gross: parcel.grams, dx: parcel.length, dy: parcel.width, dz: parcel.height } }],
    billing_info: { payment_method: "already_paid" },
    recipient_info: { first_name: cleanText(d.firstName || "Покупатель"), ...(d.lastName ? { last_name: cleanText(d.lastName) } : {}), phone: rcp.phone, ...(rcp.email ? { email: rcp.email } : {}) },
    last_mile_policy: d.kind === "courier" ? "time_interval" : "self_pickup",
  };
  const offers = await yaApi("/offers/create", body);
  const list = offers?.offers || [];
  if (!list.length) throw new Error("Яндекс Доставка не предложила вариантов для этого заказа");
  // дешевле, при равной цене — раньше привезут
  const price = (o) => parseFloat(o.offer_details?.pricing_total) || Infinity;
  const when = (o) => Date.parse(o.offer_details?.delivery_interval?.min || "") || Infinity;
  const best = [...list].sort((a, b) => price(a) - price(b) || when(a) - when(b))[0];
  const c = await yaApi("/offers/confirm", { offer_id: best.offer_id });
  if (!c?.request_id) throw new Error("Яндекс Доставка не подтвердила заявку");
  return { carrier: "yandex", id: c.request_id, status: "created", priceRub: parseFloat(best.offer_details?.pricing_total) || null, createdAt: new Date().toISOString() };
}

async function dostavistaCreateShipment(order, d, cs) {
  const from = dvSenderAddress(cs);
  if (!from || !cs.sender.phone) throw new Error("Укажите адрес и телефон отправителя в настройках доставки");
  const parcel = carrierParcel(order.items);
  const rcp = shipmentRecipient(d);
  const goods = (order.items || []).reduce((s, i) => s + (Number(i.priceRub) || 0) * (Number(i.quantity) || 1), 0);
  const slot = d.method === "dostavista_slot" && d.dvFrom && d.dvTo;
  const j = await dvApi("create-order", {
    matter: "Парфюмерия", total_weight_kg: Math.ceil(parcel.kg),
    ...(slot ? { type: "same_day" } : { type: "standard", vehicle_type_id: Number(d.vehicleTypeId) || 6 }),
    insurance_amount: String(Math.round(goods)), is_client_notification_enabled: true, is_contact_person_notification_enabled: true,
    points: [
      { address: from, contact_person: { phone: cs.sender.phone.replace(/[^\d+]/g, ""), name: cs.sender.contactName || cs.sender.name }, note: cleanText(cs.sender.comment || "").slice(0, 300) || undefined, client_order_id: order.id },
      {
        address: `${cleanText(d.city || "")}, ${cleanText(d.address || "")}`, contact_person: { phone: rcp.phone, name: rcp.name }, client_order_id: order.id,
        note: [`Заказ Magic Vibes ${order.id}`, cleanText(d.addrComment || "")].filter(Boolean).join(". ").slice(0, 300),
        ...(d.flat ? { apartment_number: String(d.flat).slice(0, 20) } : {}), ...(d.entrance ? { entrance_number: String(d.entrance).slice(0, 10) } : {}),
        ...(d.floor ? { floor_number: String(d.floor).slice(0, 10) } : {}), ...(d.intercom ? { intercom_code: String(d.intercom).slice(0, 20) } : {}),
        ...(d.lat && d.lng ? { latitude: String(d.lat), longitude: String(d.lng) } : {}),
        ...(slot ? { required_start_datetime: d.dvFrom, required_finish_datetime: d.dvTo } : {}),
      },
    ],
  });
  const o = j?.order;
  if (!o?.order_id) throw new Error("Достависта не создала заказ");
  return { carrier: "dostavista", id: String(o.order_id), status: o.status || "new", priceRub: parseFloat(o.payment_amount) || null, trackingUrl: o.points?.[1]?.tracking_url || null, createdAt: new Date().toISOString() };
}

// статус перевозчика → { label, shop } (shop — статус заказа магазина, если нужно продвинуть)
function carrierStatusMap(carrier, code) {
  const c = String(code || "").toLowerCase();
  if (carrier === "cdek") {
    if (["delivered"].includes(c)) return { label: "Вручён", shop: "delivered" };
    if (["not_delivered", "returned", "returned_to_sender_city_warehouse"].includes(c)) return { label: "Не вручён / возврат", shop: null };
    if (["accepted", "created"].includes(c)) return { label: "Создан в СДЭК", shop: null };
    if (c === "invalid") return { label: "Ошибка в заказе СДЭК", shop: null };
    if (c === "ready_for_pickup" || c === "accepted_at_pick_up_point") return { label: "Ждёт в пункте выдачи", shop: "shipped" };
    return { label: `В пути (${code})`, shop: "shipped" };
  }
  if (carrier === "yandex") {
    if (["delivery_delivered", "delivered", "delivery_transmitted_to_recipient"].includes(c)) return { label: "Вручён", shop: "delivered" };
    if (c.startsWith("cancel")) return { label: "Отменён", shop: null };
    if (["created", "draft", "validating", "created_in_platform"].includes(c)) return { label: "Заявка создана", shop: null };
    if (c.includes("storage") || c.includes("arrived_pickup") || c.includes("arrived_to_pickup")) return { label: "Ждёт в пункте выдачи", shop: "shipped" };
    return { label: `В пути (${code})`, shop: "shipped" };
  }
  if (["completed", "finished"].includes(c)) return { label: "Доставлен", shop: "delivered" };
  if (["canceled", "failed"].includes(c)) return { label: "Отменён", shop: null };
  if (["active", "courier_assigned", "courier_departed", "parcel_picked_up", "courier_arrived"].includes(c)) return { label: "Курьер в пути", shop: "shipped" };
  return { label: code === "new" || code === "available" ? "Ищем курьера" : String(code || "создан"), shop: null };
}

async function carrierRefreshShipment(order) {
  const d = order.delivery || {};
  const sh = d.shipment;
  if (!sh?.id) return null;
  let code = sh.status, number = sh.number || null, trackingUrl = sh.trackingUrl || null;
  if (sh.carrier === "cdek") {
    const j = await cdekApi(`/v2/orders/${sh.id}`);
    const e = j?.entity || {};
    code = e.statuses?.[0]?.code || code;
    number = e.cdek_number || number;
    if (number) trackingUrl = `https://www.cdek.ru/ru/tracking?order_id=${number}`;
    if ((j?.requests || []).some((r) => r.state === "INVALID")) code = "INVALID";
  } else if (sh.carrier === "yandex") {
    const j = await yaApi(`/request/info?request_id=${encodeURIComponent(sh.id)}`);
    code = j?.state?.status || code;
    trackingUrl = j?.sharing_url || trackingUrl;
    number = j?.courier_order_id ? String(j.courier_order_id) : number;
  } else if (sh.carrier === "dostavista") {
    const j = await dvApi(`orders?order_id=${encodeURIComponent(sh.id)}`);
    const o = j?.orders?.[0];
    code = o?.status || code;
    trackingUrl = o?.points?.[1]?.tracking_url || trackingUrl;
  }
  const m = carrierStatusMap(sh.carrier, code);
  return { ...sh, status: code, statusLabel: m.label, number, trackingUrl, updatedAt: new Date().toISOString(), _shop: m.shop };
}

async function carrierSaveShipment(prisma, order, shipment) {
  const { _shop, ...clean } = shipment;
  const delivery = { ...(order.delivery || {}), shipment: clean };
  const data = { delivery };
  const rank = { pending: 0, payment_pending: 0, paid: 1, confirmed: 2, picking: 3, shipped: 4, delivered: 5 };
  if (_shop && (rank[_shop] ?? -1) > (rank[order.status] ?? -1) && order.status !== "cancelled") data.status = _shop;
  return prisma.shopOrder.update({ where: { id: order.id }, data });
}

async function shopCarrierCreateShipment(orderId) {
  const prisma = getPrisma();
  const order = await prisma.shopOrder.findUnique({ where: { id: orderId } });
  if (!order) throw new Error("Заказ не найден");
  const d = order.delivery || {};
  if (!["cdek", "yandex", "dostavista"].includes(d.carrier)) throw new Error("У заказа нет службы доставки СДЭК / Яндекс / Достависта");
  if (d.shipment?.id && !["canceled", "cancelled"].includes(String(d.shipment.status).toLowerCase())) throw new Error("Отправка уже создана");
  const cs = carrierSettings(await readShopSettings());
  const create = d.carrier === "cdek" ? cdekCreateShipment : d.carrier === "yandex" ? yandexCreateShipment : dostavistaCreateShipment;
  const shipment = await create(order, d, cs);
  logger.info("shop shipment created", { orderId, carrier: d.carrier, id: shipment.id });
  let saved = await carrierSaveShipment(prisma, order, shipment);
  // первое обновление статуса (номер СДЭК, ссылка отслеживания)
  try { const fresh = await carrierRefreshShipment(saved); if (fresh) saved = await carrierSaveShipment(prisma, saved, fresh); } catch (_) { /* позже, фоновой синхронизацией */ }
  shopShipmentEmail(saved).catch((e) => logger.warn("shipment email failed", { orderId, detail: e.message }));
  return saved;
}

async function shopCarrierCancelShipment(orderId) {
  const prisma = getPrisma();
  const order = await prisma.shopOrder.findUnique({ where: { id: orderId } });
  const sh = order?.delivery?.shipment;
  if (!sh?.id) throw new Error("Отправки нет");
  if (sh.carrier === "cdek") await cdekApi(`/v2/orders/${sh.id}`, null, "DELETE");
  else if (sh.carrier === "yandex") await yaApi("/request/cancel", { request_id: sh.id });
  else if (sh.carrier === "dostavista") await dvApi("cancel-order", { order_id: Number(sh.id) });
  return carrierSaveShipment(prisma, order, { ...sh, status: "canceled", statusLabel: "Отменена", updatedAt: new Date().toISOString() });
}

// ── Админка ───────────────────────────────────────────────────────────────────
app.get("/api/shop/admin/carriers", requireAdmin, async (_request, response, next) => {
  try {
    const cs = carrierSettings(await readShopSettings());
    const env = CARRIER_ENV();
    const check = async (fn) => { try { await withTimeout(fn(), 10000); return { ok: true }; } catch (e) { return { ok: false, error: e.message }; } };
    const [cdek, yandex, dostavista] = await Promise.all([
      env.cdek.id ? check(() => cdekToken()) : { ok: false, error: "нет ключей" },
      env.yandex.token ? check(() => yaApi("/location/detect", { location: "Москва" })) : { ok: false, error: "нет токена" },
      env.dv.token ? check(() => dvApi("client")) : { ok: false, error: "нет токена" },
    ]);
    response.json({ ok: true, settings: cs, connection: { cdek, yandex, dostavista: { ...dostavista, test: /robotapitest/.test(env.dv.base) } } });
  } catch (error) { next(error); }
});

// точки для настройки отгрузки: ПВЗ СДЭК (сдача посылок) и станции самопривоза Яндекса
app.get("/api/shop/admin/carriers/dropoff", requireAdmin, async (request, response, next) => {
  try {
    const city = cleanText(request.query.city || "Москва");
    const carrier = cleanText(request.query.carrier || "yandex");
    if (carrier === "cdek") {
      const c = await cdekCity(city);
      const list = c ? await cdekApi(`/v2/deliverypoints?city_code=${c.code}&is_reception=true&type=PVZ&size=500`) : [];
      return response.json({ points: (Array.isArray(list) ? list : []).map((p) => ({ id: p.code, address: cleanText(p.location?.address_full || p.location?.address || ""), schedule: p.work_time || "" })) });
    }
    const geo = await yaGeoId(city);
    const j = geo ? await yaApi("/pickup-points/list", { geo_id: geo, available_for_dropoff: true }) : { points: [] };
    response.json({ points: (j.points || []).map((p) => ({ id: p.id, address: cleanText(p.address?.full_address || ""), name: cleanText(p.name || ""), schedule: yaSchedule(p.schedule) || "" })) });
  } catch (error) { next(error); }
});

app.post("/api/shop/admin/carriers/preview", requireAdmin, async (request, response, next) => {
  try {
    const body = request.body || {};
    const cities = (Array.isArray(body.cities) && body.cities.length ? body.cities : ["Москва", "Санкт-Петербург", "Казань", "Екатеринбург", "Новосибирск"]).slice(0, 8);
    const items = [{ offerId: "preview", name: "Парфюм 100 мл", volume: `${Number(body.ml) || 100} мл`, quantity: Math.max(1, Number(body.qty) || 1), priceRub: Number(body.goodsRub) || 8000 }];
    const rows = [];
    for (const city of cities) rows.push({ city, ...(await shopCarrierOptions({ items, goodsRub: Number(body.goodsRub) || 8000, city })) });
    response.json({ ok: true, rows });
  } catch (error) { next(error); }
});

app.post("/api/shop/admin/orders/:id/shipment", requireAdmin, async (request, response, next) => {
  try {
    const action = cleanText(request.body?.action || "create");
    const order = action === "cancel" ? await shopCarrierCancelShipment(request.params.id)
      : action === "refresh" ? await (async () => { const prisma = getPrisma(); const o = await prisma.shopOrder.findUnique({ where: { id: request.params.id } }); const f = await carrierRefreshShipment(o); return f ? carrierSaveShipment(prisma, o, f) : o; })()
      : await shopCarrierCreateShipment(request.params.id);
    response.json({ ok: true, order });
  } catch (error) {
    logger.warn("shop shipment action failed", { orderId: request.params.id, detail: error.message });
    response.status(400).json({ ok: false, error: error.message });
  }
});

// ── Callback Достависты (через magicvibes.ru/callback/dostavista/* → сюда, тело как text/plain) ──
app.post("/api/shop/callback/dostavista", express.text({ type: "*/*", limit: "1mb" }), async (request, response) => {
  try {
    const secret = CARRIER_ENV().dv.secret;
    const raw = typeof request.body === "string" ? request.body : JSON.stringify(request.body || {});
    const sig = String(request.headers["x-dv-signature"] || "");
    const expected = require("crypto").createHmac("sha256", secret).update(raw).digest("hex");
    if (!secret || !sig || sig.length !== expected.length || !require("crypto").timingSafeEqual(Buffer.from(sig), Buffer.from(expected))) {
      logger.warn("dostavista callback: bad signature");
      return response.status(401).json({ ok: false });
    }
    const ev = JSON.parse(raw);
    const dvOrderId = String(ev?.order?.order_id || ev?.delivery?.order_id || "");
    const prisma = getPrisma();
    if (dvOrderId && prisma) {
      const rows = await prisma.$queryRawUnsafe(`SELECT id FROM shop_orders WHERE delivery::jsonb #>> '{shipment,carrier}' = 'dostavista' AND delivery::jsonb #>> '{shipment,id}' = $1 LIMIT 1`, dvOrderId);
      if (rows[0]) {
        const order = await prisma.shopOrder.findUnique({ where: { id: rows[0].id } });
        const fresh = await carrierRefreshShipment(order).catch(() => null);
        if (fresh) await carrierSaveShipment(prisma, order, fresh);
      }
    }
    response.json({ ok: true });
  } catch (error) {
    logger.warn("dostavista callback failed", { detail: error.message });
    response.status(200).json({ ok: false });
  }
});

// ── Фоновая синхронизация статусов (воркер, раз в 30 минут) ───────────────────
async function carrierSyncShipments() {
  const prisma = getPrisma();
  if (!prisma) return;
  const rows = await prisma.$queryRawUnsafe(`SELECT id FROM shop_orders WHERE delivery::jsonb #>> '{shipment,id}' IS NOT NULL AND status NOT IN ('delivered','cancelled') AND created_at > now() - interval '60 days' LIMIT 200`);
  for (const r of rows) {
    try {
      const order = await prisma.shopOrder.findUnique({ where: { id: r.id } });
      const st = String(order?.delivery?.shipment?.status || "").toLowerCase();
      if (["canceled", "cancelled", "delivered", "completed"].includes(st)) continue;
      const fresh = await carrierRefreshShipment(order);
      if (fresh) await carrierSaveShipment(prisma, order, fresh);
    } catch (e) { logger.warn("carrier sync failed", { orderId: r.id, detail: e.message }); }
    await new Promise((res) => setTimeout(res, 400));
  }
}
if (typeof backgroundJobsEnabled !== "undefined" && backgroundJobsEnabled) {
  setTimeout(() => { carrierSyncShipments().catch(() => {}); setInterval(() => carrierSyncShipments().catch(() => {}), 30 * 60 * 1000); }, 90 * 1000);
}

// ── Отслеживание доставки на сайте (/track/<заказ>?k=<ключ>): курьер Достависты на карте в реальном времени ──
function shopTrackKey(orderId) {
  const secret = process.env.APP_SESSION_SECRET || process.env.DOSTAVISTA_CALLBACK_SECRET || "magicvibes-track";
  return require("crypto").createHmac("sha256", secret).update(`track:${orderId}`).digest("hex").slice(0, 20);
}
function shopTrackUrl(orderId) {
  return `${process.env.SHOP_BASE_URL || "https://magicvibes.ru"}/track/${encodeURIComponent(orderId)}?k=${shopTrackKey(orderId)}`;
}

async function dvLive(sh) {
  return carrierCached(`dv:live:${sh.id}`, 12000, async () => {
    const [o, c] = await Promise.all([
      dvApi(`orders?order_id=${encodeURIComponent(sh.id)}`).then((j) => j?.orders?.[0] || null),
      dvApi(`courier?order_id=${encodeURIComponent(sh.id)}`).catch(() => null),
    ]);
    const from = o?.points?.[0] || {}, to = o?.points?.[1] || {};
    const cr = c?.courier || null;
    const status = String(o?.status || sh.status || "").toLowerCase();
    const pickedUp = !!from.courier_visit_datetime;
    const delivered = status === "completed" || !!to.courier_visit_datetime;
    const stage = ["canceled", "failed"].includes(status) ? "canceled" : delivered ? "delivered" : pickedUp ? "on_way" : cr ? "to_pickup" : "search";
    return {
      stage, status,
      origin: from.latitude ? { lat: Number(from.latitude), lng: Number(from.longitude) } : null,
      eta: to.estimated_arrival_datetime || null,
      pickedUpAt: from.courier_visit_datetime || null,
      deliveredAt: to.courier_visit_datetime || null,
      trackingUrl: to.tracking_url || sh.trackingUrl || null,
      courier: cr ? {
        name: [cr.name, cr.surname ? `${String(cr.surname)[0]}.` : ""].filter(Boolean).join(" ") || "Курьер",
        phone: cr.phone ? `+${String(cr.phone).replace(/\D/g, "")}` : null,
        photo: cr.photo_url || null,
        lat: cr.latitude != null ? Number(cr.latitude) : null, lng: cr.longitude != null ? Number(cr.longitude) : null,
        vehicle: c?.courier_vehicle ? cleanText([c.courier_vehicle.vehicle_make, c.courier_vehicle.vehicle_model, c.courier_vehicle.vehicle_color].filter(Boolean).join(" ")) || null : null,
      } : null,
    };
  });
}

app.get("/api/shop/track/:id", shopCors, async (request, response, next) => {
  try {
    const id = cleanText(request.params.id || "").slice(0, 40);
    const k = String(request.query.k || "");
    const want = shopTrackKey(id);
    if (!k || k.length !== want.length || !require("crypto").timingSafeEqual(Buffer.from(k), Buffer.from(want))) return response.status(404).json({ error: "Заказ не найден" });
    const prisma = getPrisma();
    const order = prisma ? await prisma.shopOrder.findUnique({ where: { id } }) : null;
    if (!order) return response.status(404).json({ error: "Заказ не найден" });
    const d = order.delivery || {};
    const sh = d.shipment || null;
    const dest = d.lat && d.lng ? { lat: Number(d.lat), lng: Number(d.lng) } : null;
    const base = {
      ok: true, orderId: id, shopStatus: order.status, createdAt: order.createdAt,
      carrier: d.carrier || (d.type === "ozon_pay" ? "ozon" : null), carrierTitle: d.carrierTitle || null, method: d.method || null,
      kind: d.kind || d.type || null, daysMin: d.daysMin ?? null, daysMax: d.daysMax ?? null,
      destination: { ...(dest || {}), address: [d.city, d.address, d.flat && `кв. ${d.flat}`].filter(Boolean).join(", "), pvzName: d.pvzName || null },
      slot: d.dvFrom && d.dvTo ? { from: d.dvFrom, to: d.dvTo } : null,
      shipment: sh ? { status: sh.status, statusLabel: sh.statusLabel || null, number: sh.number || null, trackingUrl: sh.trackingUrl || null } : null,
      live: null,
    };
    if (sh?.carrier === "dostavista" && !["canceled", "cancelled"].includes(String(sh.status).toLowerCase())) {
      base.live = await dvLive(sh).catch((e) => { logger.warn("track live failed", { id, detail: e.message }); return null; });
      if (base.live?.trackingUrl) base.shipment.trackingUrl = base.live.trackingUrl;
    }
    response.set("Cache-Control", "no-store");
    response.json(base);
  } catch (error) { next(error); }
});

// письмо покупателю, когда заказ передан в службу доставки — со ссылкой на отслеживание
async function shopShipmentEmail(order) {
  const d = order.delivery || {};
  if (!d.email || typeof shopSendEmail !== "function" || typeof mvEmailLayout !== "function") return;
  const express = d.carrier === "dostavista";
  const html = mvEmailLayout({
    preheader: express ? "Курьер скоро заберёт заказ — следите за ним на карте" : "Заказ передан в службу доставки",
    kicker: "Доставка", title: express ? "Курьер уже едет" : "Заказ в пути",
    script: express ? "следите за ним на карте" : "скоро будет у вас",
    intro: express
      ? `Заказ <b>${mvEsc(order.id)}</b> передан курьеру Достависты. На странице отслеживания видно, где сейчас курьер, и примерное время приезда.`
      : `Заказ <b>${mvEsc(order.id)}</b> передан в ${mvEsc(d.carrierTitle || "службу доставки")}. Статус и трек-номер — на странице отслеживания.`,
    cta: { label: express ? "Где курьер" : "Отследить заказ", href: shopTrackUrl(order.id) },
    note: express ? "Курьер позвонит перед приездом. Если планы изменились — ответьте на это письмо." : "Мы пришлём уведомление, когда заказ прибудет.",
  });
  await shopSendEmail({ to: cleanText(d.email), subject: express ? `Курьер везёт заказ ${order.id}` : `Заказ ${order.id} в пути`, html });
}
