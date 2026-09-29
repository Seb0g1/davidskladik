// Общая карта доставки: поиск адреса (Photon / OpenStreetMap), пункты всех служб в видимой области
// с кластерами, цена для выбранного пункта или адреса. Город вводить не нужно — он берётся из пункта или адреса.
// Globals: app, shopCors, cleanText, logger, cdekApi, yaApi, yaSchedule, carrierCached, carrierSettings,
//          readShopSettings, CARRIER_ENV, shopCarrierOptions, carrierLoadItems, withTimeout

// ── Поиск адреса ──────────────────────────────────────────────────────────────
const GEO_UA = "MagicVibesShop/1.0 (noreply@magicvibes.ru)";
function geoFromPhoton(f) {
  const p = f?.properties || {};
  const [lng, lat] = f?.geometry?.coordinates || [];
  // villages: Photon puts the village into `district` (and the municipal okrug into `county`) —
  // carriers look up a settlement, not «…городской округ»
  const city = p.city || p.town || p.village || p.locality || p.district || (p.state === "Москва" || p.state === "Санкт-Петербург" ? p.state : "") || p.county || "";
  let street = p.street || (p.type === "street" ? p.name : "") || "";
  // a village without streets: «Хлопово, 10В», not «Хлопово, Хлопово, 10В»
  if (street && city && street.toLowerCase() === city.toLowerCase()) street = "";
  const house = p.housenumber || "";
  const line = [street, house].filter(Boolean).join(", ") || p.name || "";
  const kind = house ? "house" : p.type === "street" || street ? "street" : "place";
  // house number without a street (village): the settlement is part of the address line itself
  const title = !street && house ? `${city}, ${house}` : line || city;
  const full = !street && house ? `${city}, ${house}` : [city, line].filter(Boolean).join(", ");
  return {
    kind, lat, lng, city: cleanText(city), region: cleanText(p.state || ""), street: cleanText(street), house: cleanText(house),
    postcode: cleanText(p.postcode || ""), name: cleanText(p.name && p.name !== street ? p.name : ""),
    title: cleanText(title), subtitle: cleanText([line ? city : "", p.state && p.state !== city ? p.state : ""].filter(Boolean).join(", ")),
    full: cleanText(full),
  };
}
async function photon(path) {
  return carrierCached(`geo:${path}`, 24 * 3600e3, async () => {
    const r = await fetch(`https://photon.komoot.io${path}`, { headers: { "User-Agent": GEO_UA }, signal: AbortSignal.timeout(6000) });
    if (!r.ok) throw new Error(`geo ${r.status}`);
    return r.json();
  });
}

app.get("/api/shop/geo/suggest", shopCors, async (request, response, next) => {
  try {
    const q = cleanText(request.query.q || "").slice(0, 120);
    if (q.length < 2) return response.json({ items: [] });
    const lat = parseFloat(request.query.lat), lng = parseFloat(request.query.lng);
    const bias = Number.isFinite(lat) && Number.isFinite(lng) ? `&lat=${lat.toFixed(3)}&lon=${lng.toFixed(3)}` : "";
    // только Россия; дома, улицы, населённые пункты, метро и заметные места
    const j = await photon(`/api/?q=${encodeURIComponent(q)}&limit=10&bbox=19.6,41.1,180,82${bias}`);
    const seen = new Set();
    const items = (j.features || []).filter((f) => (f.properties?.countrycode || "RU") === "RU").map(geoFromPhoton)
      .filter((g) => g.lat && (g.city || g.title) && !seen.has(g.full) && seen.add(g.full)).slice(0, 7);
    response.json({ items });
  } catch (error) {
    logger.warn("geo suggest failed", { detail: error.message });
    response.json({ items: [] });
  }
});

app.get("/api/shop/geo/reverse", shopCors, async (request, response, next) => {
  try {
    const lat = parseFloat(request.query.lat), lng = parseFloat(request.query.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return response.status(400).json({ error: "lat/lng" });
    // several nearest objects: the very nearest is often a building without a number, a fence or a shop,
    // while the house with a number is next to it — prefer a numbered house within ~30 m, then a street
    const j = await photon(`/reverse?lat=${lat.toFixed(5)}&lon=${lng.toFixed(5)}&limit=6&radius=0.3`);
    const dist = (f) => {
      const [x, y] = f?.geometry?.coordinates || [];
      const dLat = (y - lat) * 111320, dLng = (x - lng) * 111320 * Math.cos((lat * Math.PI) / 180);
      return Number.isFinite(dLat) && Number.isFinite(dLng) ? Math.hypot(dLat, dLng) : Infinity;
    };
    const feats = (j.features || []).filter((f) => (f.properties?.countrycode || "RU") === "RU");
    const f = feats.find((x) => x.properties?.housenumber && dist(x) <= 30)
      || feats.find((x) => x.properties?.street || x.properties?.type === "street")
      || feats[0];
    const g = f ? geoFromPhoton(f) : null;
    // the pin stays where the buyer put it; a numbered house found nearby lends its address
    response.json({ place: g ? { ...g, lat, lng } : null });
  } catch (error) {
    logger.warn("geo reverse failed", { detail: error.message });
    response.json({ place: null });
  }
});

// ── Пункты: индекс СДЭК по России (раз в 12 часов) и плитки Яндекса ─────────────
let _cdekIndex = null, _cdekIndexAt = 0, _cdekIndexLoading = null;
async function cdekIndex() {
  if (_cdekIndex && Date.now() - _cdekIndexAt < 12 * 3600e3) return _cdekIndex;
  if (!_cdekIndexLoading) {
    _cdekIndexLoading = (async () => {
      const list = await cdekApi("/v2/deliverypoints?country_code=RU&is_handout=true");
      const out = (Array.isArray(list) ? list : []).filter((p) => p.location?.latitude).map((p) => ({
        id: p.code, carrier: "cdek", kind: p.type === "POSTAMAT" ? "postamat" : "pvz",
        name: p.type === "POSTAMAT" ? "Постамат СДЭК" : "Пункт выдачи СДЭК",
        address: cleanText(p.location?.address || p.location?.address_full || ""),
        city: cleanText(p.location?.city || ""), region: cleanText(p.location?.region || ""),
        lat: p.location.latitude, lng: p.location.longitude, schedule: cleanText(p.work_time || "") || null,
        note: cleanText(p.address_comment || "").slice(0, 200) || null,
      }));
      _cdekIndex = out; _cdekIndexAt = Date.now();
      _cdekIndexById = new Map(out.map((p) => [p.id, p]));
      logger.info("cdek points index", { count: out.length });
      return out;
    })().finally(() => { _cdekIndexLoading = null; });
  }
  return _cdekIndex || _cdekIndexLoading;
}
let _cdekIndexById = new Map();
async function cdekPointById(id) {
  if (!_cdekIndexById.size) await cdekIndex().catch(() => null);
  return _cdekIndexById.get(id) || null;
}

const YA_TILE = 0.05; // ≈ 5 км — в плитке не больше 300 пунктов даже в центре Москвы
async function yaTile(ty, tx) {
  return carrierCached(`ya:tile:${ty}:${tx}`, 6 * 3600e3, async () => {
    const j = await yaApi("/pickup-points/list", {
      latitude: { from: ty * YA_TILE, to: (ty + 1) * YA_TILE }, longitude: { from: tx * YA_TILE * 2, to: (tx + 1) * YA_TILE * 2 },
      payment_method: "already_paid",
    });
    return (j?.points || []).filter((p) => p.position?.latitude && ["pickup_point", "terminal"].includes(p.type)).map((p) => ({
      id: p.id, carrier: "yandex", kind: p.type === "terminal" ? "postamat" : "pvz",
      name: cleanText(p.name || "Пункт Яндекс Доставки"), address: cleanText(p.address?.full_address || ""),
      city: cleanText(p.address?.locality || "").replace(/\s+г\.?$/i, ""), region: cleanText(p.address?.region || ""),
      lat: p.position.latitude, lng: p.position.longitude, schedule: yaSchedule(p.schedule),
      note: cleanText(p.instruction || "").slice(0, 200) || null,
    }));
  });
}
const _yaById = new Map();

// сетка кластеров: при большом числе точек отдаём группы с числом, по клику карта приближается
// Cells are a fixed size on screen (~72 px at this zoom), anchored to the globe: a share of the bbox
// made ~15 px cells on a narrow phone map, so 40 px cluster badges piled into one black mass.
function clusterPoints(points, s, w, n, e, zoom) {
  if (zoom >= 15 || points.length <= 350) return { points, clusters: [] };
  const dx = (360 / Math.pow(2, zoom)) * (72 / 256);
  const dy = dx * Math.cos((((s + n) / 2) * Math.PI) / 180); // Web Mercator: a degree of latitude is taller
  const grid = new Map();
  for (const p of points) {
    const k = `${Math.floor(p.lat / dy)}:${Math.floor(p.lng / dx)}`;
    const g = grid.get(k) || { n: 0, lat: 0, lng: 0, carriers: new Set(), sample: p };
    g.n++; g.lat += p.lat; g.lng += p.lng; g.carriers.add(p.carrier);
    grid.set(k, g);
  }
  const out = [], clusters = [];
  for (const g of grid.values()) {
    if (g.n === 1) out.push(g.sample);
    else clusters.push({ lat: g.lat / g.n, lng: g.lng / g.n, count: g.n, carriers: [...g.carriers] });
  }
  return { points: out, clusters };
}

app.get("/api/shop/delivery/map-points", shopCors, async (request, response, next) => {
  try {
    const s = parseFloat(request.query.s), w = parseFloat(request.query.w), n = parseFloat(request.query.n), e = parseFloat(request.query.e);
    const zoom = Math.round(Number(request.query.z) || 12);
    if (![s, w, n, e].every(Number.isFinite) || n <= s || e <= w) return response.status(400).json({ error: "bbox" });
    if (n - s > 1.6 || e - w > 3.2 || zoom < 9) return response.json({ points: [], clusters: [], zoomIn: true });
    const cs = carrierSettings(await readShopSettings());
    const env = CARRIER_ENV();
    const wantCarrier = cleanText(request.query.carrier || "");
    let pts = [];
    if (cs.cdek.enabled && cs.cdek.pvz && env.cdek.id && (!wantCarrier || wantCarrier === "cdek")) {
      const idx = await withTimeout(cdekIndex(), 15000).catch(() => []);
      for (const p of idx) if (p.lat >= s && p.lat <= n && p.lng >= w && p.lng <= e && (cs.cdek.postamat || p.kind !== "postamat")) pts.push(p);
    }
    if (cs.yandex.enabled && cs.yandex.pvz && env.yandex.token && (!wantCarrier || wantCarrier === "yandex")) {
      const tiles = [];
      for (let ty = Math.floor(s / YA_TILE); ty <= Math.floor(n / YA_TILE); ty++) {
        for (let tx = Math.floor(w / (YA_TILE * 2)); tx <= Math.floor(e / (YA_TILE * 2)); tx++) tiles.push([ty, tx]);
      }
      if (tiles.length <= 64) {
        const res = await Promise.all(tiles.map(([ty, tx]) => withTimeout(yaTile(ty, tx), 9000).catch(() => [])));
        for (const list of res) for (const p of list) {
          _yaById.set(p.id, p);
          if (p.lat >= s && p.lat <= n && p.lng >= w && p.lng <= e) pts.push(p);
        }
      }
    }
    const r = clusterPoints(pts, s, w, n, e, zoom);
    response.set("Cache-Control", "public, max-age=300");
    response.json({ ...r, total: pts.length });
  } catch (error) { next(error); }
});

// ── Цена для выбранного пункта или адреса ─────────────────────────────────────
// Body: { items, goodsRub?, target: { type: "point", carrier, id } | { type: "address", city, region, full, lat, lng } }
app.post("/api/shop/checkout/quote", shopCors, async (request, response, next) => {
  try {
    const body = request.body || {};
    const t = body.target || {};
    const items = await carrierLoadItems(body.items);
    const goodsList = items.reduce((s2, i) => s2 + i.priceRub * i.quantity, 0);
    const goodsRub = Number(body.goodsRub) > 0 ? Math.min(goodsList || Infinity, Number(body.goodsRub)) : goodsList;
    if (t.type === "point") {
      const carrier = cleanText(t.carrier), id = cleanText(t.id);
      let point = carrier === "cdek" ? await cdekPointById(id) : _yaById.get(id) || null;
      // пункт Яндекса не из кеша карты (сохранённый адрес после перезапуска) — спрашиваем по id
      if (!point && carrier === "yandex") {
        const j = await yaApi("/pickup-points/list", { pickup_point_ids: [id] }).catch(() => null);
        const p = j?.points?.[0];
        if (p?.position) {
          point = { id: p.id, carrier: "yandex", kind: p.type === "terminal" ? "postamat" : "pvz", name: cleanText(p.name || "Пункт Яндекс Доставки"),
            address: cleanText(p.address?.full_address || ""), city: cleanText(p.address?.locality || "").replace(/\s+г\.?$/i, ""), region: cleanText(p.address?.region || ""),
            lat: p.position.latitude, lng: p.position.longitude, schedule: yaSchedule(p.schedule), note: cleanText(p.instruction || "").slice(0, 200) || null };
          _yaById.set(id, point);
        }
      }
      if (!point) return response.status(404).json({ error: "Пункт не найден — обновите карту" });
      const r = await shopCarrierOptions({ items, goodsRub, city: point.city, region: point.region, pointId: id, only: carrier === "cdek" ? "cdek_pvz" : "yandex_pvz" });
      return response.json({ ok: true, point, options: r.options });
    }
    if (t.type === "address") {
      const city = cleanText(t.city || ""), full = cleanText(t.full || "");
      if (!city || !full) return response.status(400).json({ error: "Укажите адрес на карте" });
      const r = await shopCarrierOptions({ items, goodsRub, city, region: cleanText(t.region || ""), address: full.replace(new RegExp(`^${city.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")},\\s*`, "i"), ""), courierOnly: true });
      return response.json({ ok: true, options: r.options.filter((o) => o.kind === "courier") });
    }
    response.status(400).json({ error: "target" });
  } catch (error) { next(error); }
});

// индекс СДЭК прогреваем на API после старта
if (process.env.SERVER_ROLE !== "worker") setTimeout(() => { cdekIndex().catch((e) => logger.warn("cdek index warmup failed", { detail: e.message })); }, 30000);
