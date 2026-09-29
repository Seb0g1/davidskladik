"use client";
// Единая карта доставки: пункты выдачи всех служб (с логотипами) и курьер до двери с выбором дома на карте.
// Город вводить не нужно: поиск по адресу/метро/городу или «где я», цена считается после выбора пункта или дома.
import "leaflet/dist/leaflet.css";
import "maplibre-gl/dist/maplibre-gl.css";
import "@maplibre/maplibre-gl-leaflet";
import { DELIVERY_MAP_STYLE } from "@/lib/mapStyle";
import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import { ArrowLeft, Search, Navigation, Loader2, MapPin, Truck, X, Clock, Check, Home } from "lucide-react";

const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

export type Carrier = "cdek" | "yandex" | "dostavista";
export type PickOption = {
  id: string; carrier: Carrier; kind: string; title: string; priceRub: number; free?: boolean;
  daysMin: number | null; daysMax: number | null; sameDay?: boolean; speed?: "fast" | "slot";
  intervals?: { from: string; to: string }[]; estimate?: boolean;
};
export type MapPoint = { id: string; carrier: "cdek" | "yandex"; kind: "pvz" | "postamat"; name: string; address: string; city: string; region: string; lat: number; lng: number; schedule: string | null; note?: string | null };
export type Place = { kind: "house" | "street" | "place"; lat: number; lng: number; city: string; region: string; street: string; house: string; postcode: string; title: string; subtitle: string; full: string };
export type AddrDetails = { flat: string; entrance: string; floor: string; intercom: string; comment: string };
export type DeliverySelection =
  | { mode: "point"; option: PickOption; point: MapPoint }
  | { mode: "address"; option: PickOption; place: Place; details: AddrDetails; slot?: { from: string; to: string } };

import { CARRIER_LOGO, MARK, CARRIER_NAME, rubFmt, daysText, slotText } from "@/lib/delivery";
import Logo from "@/components/Logo";
export { CARRIER_LOGO, daysText, slotText };

const CSS = `
.dp{position:fixed;inset:0;z-index:10000;background:#fff;display:flex;flex-direction:column;font-family:inherit;color:var(--ink)}
.dp-top{display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid rgba(var(--ink-rgb),.08);flex-shrink:0}
.dp-back{width:40px;height:40px;border-radius:12px;border:1px solid rgba(var(--ink-rgb),.1);background:#fff;display:grid;place-items:center;cursor:pointer;flex-shrink:0}
.dp-tabs{display:flex;gap:4px;background:rgba(var(--ink-rgb),.06);border-radius:14px;padding:4px;flex:1;max-width:440px}
.dp-tabs button{flex:1;display:flex;align-items:center;justify-content:center;gap:6px;border:0;border-radius:10px;padding:9px 8px;font:inherit;font-size:14px;font-weight:700;background:transparent;color:rgba(var(--ink-rgb),.6);cursor:pointer;white-space:nowrap}
.dp-tabs button.on{background:#fff;color:var(--ink);box-shadow:0 2px 8px rgba(0,0,0,.08)}
.dp-search{position:relative;padding:10px 12px;flex-shrink:0;display:flex;gap:8px}
.dp-search-in{flex:1;position:relative}
.dp-search input{width:100%;box-sizing:border-box;border:1.5px solid rgba(var(--ink-rgb),.14);border-radius:14px;padding:12px 38px 12px 40px;font:inherit;font-size:16px;outline:0;background:#fff}
.dp-search input:focus{border-color:var(--ink)}
.dp-search .ic{position:absolute;left:13px;top:50%;transform:translateY(-50%);color:rgba(var(--ink-rgb),.45)}
.dp-search .clr{position:absolute;right:8px;top:50%;transform:translateY(-50%);border:0;background:none;cursor:pointer;color:rgba(var(--ink-rgb),.45);padding:6px}
.dp-geo{width:48px;border-radius:14px;border:1.5px solid rgba(var(--ink-rgb),.14);background:#fff;display:grid;place-items:center;cursor:pointer;flex-shrink:0;color:#1d4ed8}
.dp-sug{position:absolute;left:12px;right:12px;top:calc(100% - 4px);background:#fff;border-radius:16px;box-shadow:0 18px 44px rgba(0,0,0,.18);z-index:1100;overflow:hidden;max-height:60vh;overflow-y:auto}
.dp-sug button{display:flex;align-items:flex-start;gap:10px;width:100%;text-align:left;border:0;background:#fff;padding:12px 14px;font:inherit;cursor:pointer;border-bottom:1px solid rgba(var(--ink-rgb),.06)}
.dp-sug button:hover,.dp-sug button:focus{background:rgba(var(--accent-rgb),.06)}
.dp-sug b{display:block;font-size:14.5px}.dp-sug span{display:block;font-size:12.5px;color:rgba(var(--ink-rgb),.55)}
.dp-body{flex:1;min-height:0;display:flex;position:relative}
.dp-mapwrap{position:relative;flex:1;min-height:0}
.dp-map{position:absolute;inset:0}
.dp-panel{width:400px;flex-shrink:0;border-left:1px solid rgba(var(--ink-rgb),.08);overflow-y:auto;padding:16px;box-sizing:border-box;background:#fff}
@media (max-width:900px){.dp-body{flex-direction:column}.dp-panel{width:auto;border-left:0;border-top:1px solid rgba(var(--ink-rgb),.08);max-height:52vh;border-radius:22px 22px 0 0;box-shadow:0 -10px 30px rgba(0,0,0,.08);margin-top:-18px;position:relative;z-index:500;padding:16px 16px calc(16px + env(safe-area-inset-bottom))}}
.dp-pin{display:flex;align-items:center;justify-content:center;background:#fff;border-radius:10px;box-shadow:0 3px 10px rgba(0,0,0,.25);border:2px solid #fff;height:26px;box-sizing:border-box;position:relative;transition:transform .15s}
.dp-pin:after{content:"";position:absolute;left:50%;bottom:-7px;transform:translateX(-50%);border:6px solid transparent;border-top-color:#fff;border-bottom:0}
.dp-pin img{display:block;pointer-events:none}
.dp-pin.cdek img{height:9px}.dp-pin.yandex img{height:18px}
.dp-pin.on{background:#121212;border-color:#121212;transform:scale(1.25)}.dp-pin.on:after{border-top-color:#121212}
.dp-pin.on.cdek img{filter:brightness(0) invert(1)}
.dp-pin.pm:before{content:"";position:absolute;right:-4px;top:-4px;width:9px;height:9px;border-radius:50%;background:#ff3d7f;border:2px solid #fff}
.dp-cl{width:40px;height:40px;border-radius:50%;background:#121212;color:#d9f84a;font-weight:800;font-size:13px;display:flex;align-items:center;justify-content:center;border:3px solid #fff;box-shadow:0 3px 12px rgba(0,0,0,.3)}
.dp-center-pin{position:absolute;left:50%;top:50%;z-index:600;transform:translate(-50%,-100%);pointer-events:none;filter:drop-shadow(0 6px 8px rgba(0,0,0,.35))}
.dp-center-dot{position:absolute;left:50%;top:50%;z-index:599;width:10px;height:6px;border-radius:50%;background:rgba(0,0,0,.35);transform:translate(-50%,-50%);pointer-events:none}
.dp-hint{position:absolute;left:50%;top:12px;transform:translateX(-50%);z-index:700;background:#121212;color:#fff;border-radius:999px;padding:8px 14px;font-size:13px;font-weight:600;white-space:nowrap;box-shadow:0 6px 18px rgba(0,0,0,.2)}
.dp-filters{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
.dp-filters button{display:flex;align-items:center;gap:6px;border:1.5px solid rgba(var(--ink-rgb),.12);border-radius:999px;background:#fff;padding:6px 12px;font:inherit;font-size:13px;font-weight:600;cursor:pointer;height:34px}
.dp-filters button.on{border-color:var(--ink);background:rgba(var(--accent-rgb),.05)}
.dp-filters img{height:12px}.dp-filters img.ym{height:18px}
.dp-card{display:flex;flex-direction:column;gap:8px}
.dp-logo{height:18px;align-self:flex-start}
.dp-card h3{margin:0;font-size:17px;line-height:1.3}
.dp-muted{font-size:13.5px;color:rgba(var(--ink-rgb),.6);line-height:1.45}
.dp-price{display:flex;align-items:baseline;justify-content:space-between;gap:10px;background:rgba(var(--ink-rgb),.04);border-radius:14px;padding:12px 14px;margin-top:4px}
.dp-price b{font-size:20px}.dp-price span{font-size:13.5px;color:rgba(var(--ink-rgb),.65)}
.dp-go{width:100%;border:0;border-radius:16px;padding:16px;font:inherit;font-size:16px;font-weight:800;background:#121212;color:#d9f84a;cursor:pointer;margin-top:6px}
.dp-go:disabled{opacity:.4;cursor:not-allowed}
.dp-opts{display:flex;flex-direction:column;gap:8px;margin-top:6px}
.dp-opt{display:flex;align-items:center;gap:12px;border:1.5px solid rgba(var(--ink-rgb),.12);border-radius:16px;background:#fff;padding:12px;font:inherit;text-align:left;cursor:pointer;width:100%;color:var(--ink)}
.dp-opt.on{border-color:var(--ink);box-shadow:inset 0 0 0 1px var(--ink)}
.dp-opt img{height:16px;width:auto;flex-shrink:0;max-width:96px}
.dp-opt .t{flex:1;min-width:0}.dp-opt .t b{display:block;font-size:14px}.dp-opt .t span{display:block;font-size:12.5px;color:rgba(var(--ink-rgb),.6)}
.dp-opt .p{font-weight:800;white-space:nowrap}
.dp-fields{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}
.dp-fields label{display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:700;color:rgba(var(--ink-rgb),.6)}
.dp-fields input,.dp-comment{border:1.5px solid rgba(var(--ink-rgb),.12);border-radius:12px;padding:10px;font:inherit;font-size:15px;width:100%;box-sizing:border-box;outline:0}
.dp-fields input:focus,.dp-comment:focus{border-color:var(--ink)}
.dp-slots{display:flex;flex-wrap:wrap;gap:6px}
.dp-slots button{border:1.5px solid rgba(var(--ink-rgb),.12);border-radius:999px;background:#fff;padding:7px 11px;font:inherit;font-size:12.5px;font-weight:600;cursor:pointer}
.dp-slots button.on{background:#121212;color:#fff;border-color:#121212}
.dp-warn{background:#fff6d6;border-radius:12px;padding:10px 12px;font-size:13px;line-height:1.4}
.dp-err{background:rgba(var(--danger-rgb),.08);color:var(--danger);border-radius:12px;padding:10px 12px;font-size:13px}
.dp-h{font-size:12px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:rgba(var(--ink-rgb),.5);margin:14px 0 6px}
@media (max-width:480px){.dp-fields{grid-template-columns:repeat(2,1fr)}.dp-tabs button{font-size:13px}}
.dp-brand{margin-left:auto;display:flex;align-items:center;flex-shrink:0;padding-left:6px}
.dp-brand .full{display:block}.dp-brand .mark{display:none}
@media (max-width:640px){.dp-brand .full{display:none}.dp-brand .mark{display:block}}
.dp-house{display:flex;gap:8px;align-items:flex-end;margin-top:4px}
.dp-house label{flex:1;display:flex;flex-direction:column;gap:4px;font-size:11.5px;font-weight:700;color:rgba(var(--ink-rgb),.6)}
.dp-house input{border:1.5px solid var(--ink);border-radius:12px;padding:11px 12px;font:inherit;font-size:16px;width:100%;box-sizing:border-box;outline:0}
.dp .leaflet-control-attribution{font-size:10px;background:rgba(255,255,255,.75);border-radius:6px 0 0 0;padding:1px 6px}
.dp .leaflet-control-attribution a{color:#555}
`;

const plural = (n: number, one: string, few: string, many: string) => {
  const m10 = n % 10, m100 = n % 100;
  return m10 === 1 && m100 !== 11 ? one : m10 >= 2 && m10 <= 4 && (m100 < 12 || m100 > 14) ? few : many;
};

const LAST_KEY = "mv_geo_last";
// metres between two points (small distances, equirectangular is enough)
const metres = (a: { lat: number; lng: number }, b: { lat: number; lng: number }) =>
  Math.hypot((a.lat - b.lat) * 111320, (a.lng - b.lng) * 111320 * Math.cos((a.lat * Math.PI) / 180));
const readLast = (): { lat: number; lng: number; z: number } | null => {
  try { const v = JSON.parse(localStorage.getItem(LAST_KEY) || "null"); return v && Number.isFinite(v.lat) ? v : null; } catch { return null; }
};

export default function DeliveryPicker({ open, initialMode = "point", onClose, onSelect, items, goodsRub, browse = false, start }: {
  open: boolean; initialMode?: "point" | "address"; onClose: () => void; onSelect?: (s: DeliverySelection) => void;
  items: { offerId: string; quantity: number }[]; goodsRub: number; browse?: boolean;
  start?: { lat: number; lng: number } | null;
}) {
  const [tab, setTab] = useState<"point" | "address">(initialMode);
  const tabRef = useRef(tab); tabRef.current = tab;
  const [query, setQuery] = useState("");
  const [sug, setSug] = useState<Place[]>([]);
  const [sugOpen, setSugOpen] = useState(false);
  const [zoomIn, setZoomIn] = useState(false);
  const [ptsLoading, setPtsLoading] = useState(false);
  const [filter, setFilter] = useState<"all" | "cdek" | "yandex">("all");
  const filterRef = useRef(filter); filterRef.current = filter;
  const [count, setCount] = useState(0);
  const [active, setActive] = useState<MapPoint | null>(null);
  const [pq, setPq] = useState<{ loading: boolean; option?: PickOption; error?: string }>({ loading: false });
  const [place, setPlace] = useState<Place | null>(null);
  const [placeLoading, setPlaceLoading] = useState(false);
  // private houses and villages often have no number in OpenStreetMap: the buyer types it in
  const [manualHouse, setManualHouse] = useState("");
  const [houseDeb, setHouseDeb] = useState("");
  const [details, setDetails] = useState<AddrDetails>({ flat: "", entrance: "", floor: "", intercom: "", comment: "" });
  const [cq, setCq] = useState<{ loading: boolean; list: PickOption[]; error?: string }>({ loading: false, list: [] });
  const [chosen, setChosen] = useState("");
  const [slot, setSlot] = useState<{ from: string; to: string } | null>(null);

  const mapDiv = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const layerRef = useRef<L.LayerGroup | null>(null);
  const skipReverse = useRef(false);
  const moveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const loadSeq = useRef(0);
  const activeIdRef = useRef<string | null>(null);
  // адрес под меткой: ответ старого запроса не должен перезаписать новый (поиск, сдвиг карты)
  const revSeq = useRef(0);
  // where the address under the pin was last looked up: resizing the map (the panel grows when the
  // courier fields appear, the phone keyboard opens) nudges the centre by a few pixels — not a new place
  const lastRev = useRef<{ lat: number; lng: number } | null>(null);

  // ── карта ───────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!open) return;
    setTab(initialMode);
    const el = mapDiv.current;
    if (!el || mapRef.current) return;
    const last = start ? { ...start, z: 16 } : readLast();
    const map = L.map(el, { center: last ? [last.lat, last.lng] : [55.751, 37.618], zoom: last ? Math.max(12, last.z) : 11, zoomControl: false, attributionControl: false });
    // своя схема: только улицы, дома, вода, парки и названия — без POI, аэродромов, военных и промзон
    (L as unknown as { maplibreGL: (o: { style: unknown; attributionControl?: boolean }) => L.Layer })
      .maplibreGL({ style: DELIVERY_MAP_STYLE, attributionControl: false }).addTo(map);
    L.control.zoom({ position: "topright" }).addTo(map);
    // ODbL: the data source must be credited on the map
    L.control.attribution({ position: "bottomright", prefix: false })
      .addAttribution('<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap</a>')
      .addTo(map);
    layerRef.current = L.layerGroup().addTo(map);
    mapRef.current = map;
    map.on("moveend", () => {
      if (moveTimer.current) clearTimeout(moveTimer.current);
      moveTimer.current = setTimeout(() => {
        const c = map.getCenter();
        try { localStorage.setItem(LAST_KEY, JSON.stringify({ lat: c.lat, lng: c.lng, z: map.getZoom() })); } catch { /* ignore */ }
        if (tabRef.current === "point") void loadPoints();
        else if (skipReverse.current) skipReverse.current = false;
        else if (lastRev.current && metres(lastRev.current, c) < 12) { /* same spot */ }
        else void reverse(c.lat, c.lng);
      }, tabRef.current === "point" ? 250 : 450);
    });
    const ro = new ResizeObserver(() => map.invalidateSize());
    ro.observe(el);
    setTimeout(() => { map.invalidateSize(); if (tabRef.current === "point") void loadPoints(); else void reverse(map.getCenter().lat, map.getCenter().lng); }, 60);
    return () => { ro.disconnect(); map.remove(); mapRef.current = null; layerRef.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // смена вкладки: пункты ↔ курьер
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    layerRef.current?.clearLayers();
    setActive(null); activeIdRef.current = null;
    setQuery(""); setSug([]); setSugOpen(false);
    if (tab === "point") void loadPoints();
    else { if (map.getZoom() < 15) { skipReverse.current = false; map.setZoom(16); } else void reverse(map.getCenter().lat, map.getCenter().lng); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);
  useEffect(() => { if (tab === "point") void loadPoints(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [filter]);

  async function loadPoints() {
    const map = mapRef.current, layer = layerRef.current;
    if (!map || !layer || tabRef.current !== "point") return;
    const b = map.getBounds();
    const seq = ++loadSeq.current;
    setPtsLoading(true);
    try {
      const f = filterRef.current;
      const r = await fetch(`${API}/delivery/map-points?s=${b.getSouth().toFixed(4)}&w=${b.getWest().toFixed(4)}&n=${b.getNorth().toFixed(4)}&e=${b.getEast().toFixed(4)}&z=${map.getZoom()}${f !== "all" ? `&carrier=${f}` : ""}`);
      const d: { points: MapPoint[]; clusters: { lat: number; lng: number; count: number }[]; zoomIn?: boolean; total?: number } = await r.json();
      if (seq !== loadSeq.current || tabRef.current !== "point") return;
      layer.clearLayers();
      setZoomIn(!!d.zoomIn);
      setCount(d.total || 0);
      for (const c of d.clusters || []) {
        const m = L.marker([c.lat, c.lng], { icon: L.divIcon({ className: "", html: `<div class="dp-cl">${c.count > 999 ? "999+" : c.count}</div>`, iconSize: [40, 40], iconAnchor: [20, 20] }) });
        m.on("click", () => map.setView([c.lat, c.lng], Math.min(18, map.getZoom() + 2)));
        m.addTo(layer);
      }
      for (const p of d.points || []) {
        const on = p.id === activeIdRef.current;
        const w = p.carrier === "cdek" ? 46 : 32;
        const m = L.marker([p.lat, p.lng], {
          icon: L.divIcon({ className: "", html: `<div class="dp-pin ${p.carrier}${p.kind === "postamat" ? " pm" : ""}${on ? " on" : ""}" style="width:${w}px"><img src="${MARK[p.carrier]}" alt=""></div>`, iconSize: [w, 26], iconAnchor: [w / 2, 33] }),
          zIndexOffset: on ? 1000 : 0,
        });
        m.on("click", () => { pickPoint(p); void loadPoints(); });
        m.addTo(layer);
      }
    } catch { /* сеть: просто не обновили */ }
    finally { if (seq === loadSeq.current) setPtsLoading(false); }
  }

  async function pickPoint(p: MapPoint) {
    setActive(p); activeIdRef.current = p.id;
    if (browse || !items.length) return;
    setPq({ loading: true });
    try {
      const r = await fetch(`${API}/checkout/quote`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ items, goodsRub, target: { type: "point", carrier: p.carrier, id: p.id } }) });
      const d = await r.json();
      const o: PickOption | undefined = d.options?.[0];
      setPq(o ? { loading: false, option: o } : { loading: false, error: d.error || "В этот пункт доставка сейчас недоступна — выберите соседний" });
    } catch { setPq({ loading: false, error: "Не получилось рассчитать — попробуйте ещё раз" }); }
  }

  async function reverse(lat: number, lng: number) {
    lastRev.current = { lat, lng };
    const seq = ++revSeq.current;
    setPlaceLoading(true);
    try {
      const d = await (await fetch(`${API}/geo/reverse?lat=${lat.toFixed(6)}&lng=${lng.toFixed(6)}`)).json();
      if (seq === revSeq.current && tabRef.current === "address") setPlace(d.place || null);
    } catch { /* ignore */ }
    finally { if (seq === revSeq.current) setPlaceLoading(false); }
  }

  // новый адрес под меткой — номер, введённый для прошлого места, к нему не относится
  const houseAt = useRef<{ lat: number; lng: number } | null>(null);
  useEffect(() => {
    if (!place) return;
    if (houseAt.current && metres(houseAt.current, place) < 25) return; // same house, keep the typed number
    houseAt.current = { lat: place.lat, lng: place.lng };
    setManualHouse(""); setHouseDeb("");
  }, [place?.lat, place?.lng]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { const t = setTimeout(() => setHouseDeb(manualHouse.trim().slice(0, 20)), 600); return () => clearTimeout(t); }, [manualHouse]);
  const needsHouse = !!place && place.kind !== "house" && !!(place.street || place.city);
  const effPlace: Place | null = place && needsHouse && houseDeb
    ? {
      ...place, kind: "house", house: houseDeb,
      title: place.street ? `${place.street}, ${houseDeb}` : `${place.city}, д. ${houseDeb}`,
      full: [place.city, place.street, houseDeb].filter(Boolean).join(", "),
    }
    : place;

  // курьер: варианты для найденного дома
  const placeKey = effPlace ? `${effPlace.lat.toFixed(5)},${effPlace.lng.toFixed(5)}|${effPlace.house}` : "";
  useEffect(() => {
    const place = effPlace;
    if (tab !== "address" || !place || place.kind !== "house" || browse || !items.length) { setCq({ loading: false, list: [] }); return; }
    let alive = true;
    setCq((c) => ({ ...c, loading: true, error: undefined }));
    fetch(`${API}/checkout/quote`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items, goodsRub, target: { type: "address", city: place.city || place.region, region: place.region, full: place.full, lat: place.lat, lng: place.lng } }),
    }).then((r) => r.json()).then((d) => {
      if (!alive) return;
      const list: PickOption[] = d.options || [];
      setCq({ loading: false, list, error: list.length ? undefined : d.error || "Курьеры сюда не возят — выберите пункт выдачи" });
      setChosen((c) => (list.some((o) => o.id === c) ? c : list[0]?.id || ""));
    }).catch(() => { if (alive) setCq({ loading: false, list: [], error: "Не получилось рассчитать — попробуйте ещё раз" }); });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [placeKey, tab]);
  const chosenOpt = cq.list.find((o) => o.id === chosen) || null;
  useEffect(() => { if (chosenOpt?.speed === "slot") setSlot((s) => (s && chosenOpt.intervals?.some((i) => i.from === s.from) ? s : chosenOpt.intervals?.[0] || null)); }, [chosenOpt]);

  // ── поиск ───────────────────────────────────────────────────────────────
  useEffect(() => {
    const q = query.trim();
    if (q.length < 3) { setSug([]); return; }
    const t = setTimeout(async () => {
      const c = mapRef.current?.getCenter();
      try {
        const d = await (await fetch(`${API}/geo/suggest?q=${encodeURIComponent(q)}${c ? `&lat=${c.lat.toFixed(3)}&lng=${c.lng.toFixed(3)}` : ""}`)).json();
        setSug(d.items || []);
      } catch { /* ignore */ }
    }, 300);
    return () => clearTimeout(t);
  }, [query]);
  function goTo(p: Place) {
    setSugOpen(false);
    setQuery(p.kind === "house" ? p.full : p.title);
    const map = mapRef.current;
    if (!map) return;
    if (tabRef.current === "address" && p.kind === "house") { revSeq.current++; setPlaceLoading(false); skipReverse.current = true; lastRev.current = { lat: p.lat, lng: p.lng }; setPlace(p); }
    map.setView([p.lat, p.lng], p.kind === "house" ? 17 : p.kind === "street" ? 16 : tabRef.current === "point" ? 13 : 15);
  }
  function geolocate() {
    navigator.geolocation?.getCurrentPosition(
      (pos) => mapRef.current?.setView([pos.coords.latitude, pos.coords.longitude], tabRef.current === "point" ? 15 : 17),
      () => alert("Не получилось определить местоположение — найдите адрес в поиске"),
      { timeout: 10000, maximumAge: 60000 },
    );
  }

  if (!open) return null;

  const confirmPoint = () => { if (active && pq.option && onSelect) onSelect({ mode: "point", option: pq.option, point: active }); };
  const confirmAddress = () => {
    if (!effPlace || effPlace.kind !== "house" || !chosenOpt || !onSelect) return;
    onSelect({ mode: "address", option: chosenOpt, place: effPlace, details, ...(chosenOpt.speed === "slot" && slot ? { slot } : {}) });
  };
  const setD = (k: keyof AddrDetails) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => setDetails((d) => ({ ...d, [k]: e.target.value }));

  return (
    <div className="dp" role="dialog" aria-label="Выбор доставки">
      <style>{CSS}</style>
      <div className="dp-top">
        <button type="button" className="dp-back" onClick={onClose} aria-label="Назад"><ArrowLeft size={18} /></button>
        <div className="dp-tabs" role="tablist">
          <button type="button" role="tab" aria-selected={tab === "point"} className={tab === "point" ? "on" : ""} onClick={() => setTab("point")}><MapPin size={16} />Пункт выдачи</button>
          <button type="button" role="tab" aria-selected={tab === "address"} className={tab === "address" ? "on" : ""} onClick={() => setTab("address")}><Truck size={16} />Курьер до двери</button>
        </div>
        <div className="dp-brand" aria-label="Magic Vibes">
          <span className="full"><Logo height={28} /></span>
          <span className="mark"><Logo variant="mark" height={30} /></span>
        </div>
      </div>

      <div className="dp-search">
        <div className="dp-search-in">
          <Search size={17} className="ic" />
          <input
            value={query} onChange={(e) => { setQuery(e.target.value); setSugOpen(true); }} onFocus={() => sug.length && query.length >= 3 && setSugOpen(true)}
            onKeyDown={(e) => { if (e.key === "Enter" && sug[0]) { e.preventDefault(); goTo(sug[0]); } if (e.key === "Escape") setSugOpen(false); }}
            placeholder={tab === "point" ? "Город, улица или метро рядом с вами" : "Улица и номер дома, например «Ленинский проспект 30»"}
            aria-label="Поиск адреса" enterKeyHint="search" autoComplete="off"
          />
          {query && <button type="button" className="clr" aria-label="Очистить" onClick={() => { setQuery(""); setSug([]); }}><X size={16} /></button>}
        </div>
        <button type="button" className="dp-geo" onClick={geolocate} title="Где я" aria-label="Определить моё местоположение"><Navigation size={18} /></button>
        {sugOpen && sug.length > 0 && (
          <div className="dp-sug">
            {sug.map((s, i) => (
              <button key={i} type="button" onClick={() => goTo(s)}>
                {s.kind === "house" ? <Home size={16} style={{ marginTop: 2, flexShrink: 0 }} /> : <MapPin size={16} style={{ marginTop: 2, flexShrink: 0 }} />}
                <div><b>{s.title}</b><span>{s.subtitle || s.city}</span></div>
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="dp-body" onClick={() => sugOpen && setSugOpen(false)}>
        <div className="dp-mapwrap">
          <div ref={mapDiv} className="dp-map" />
          {tab === "address" && (<>
            <svg className="dp-center-pin" width="40" height="52" viewBox="0 0 40 52" aria-hidden><path d="M20 1C9.5 1 1 9.5 1 20c0 13.6 19 31 19 31s19-17.4 19-31C39 9.5 30.5 1 20 1z" fill="#121212" /><path d="M22.5 10.5c.8 4.6 2.8 6.6 7.4 7.4-4.6.8-6.6 2.8-7.4 7.4-.8-4.6-2.8-6.6-7.4-7.4 4.6-.8 6.6-2.8 7.4-7.4Z" fill="#d9f84a" /><circle cx="14" cy="26" r="2.3" fill="#ff3d7f" /></svg>
            <span className="dp-center-dot" />
          </>)}
          {tab === "point" && zoomIn && <div className="dp-hint">Приблизьте карту или найдите адрес, чтобы увидеть пункты</div>}
          {tab === "point" && !zoomIn && ptsLoading && <div className="dp-hint"><Loader2 size={13} className="mv-spin" style={{ verticalAlign: -2 }} /> Загружаем пункты…</div>}
        </div>

        <div className="dp-panel">
          {tab === "point" ? (
            active ? (
              <div className="dp-card">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img className="dp-logo" src={CARRIER_LOGO[active.carrier]} alt={CARRIER_NAME[active.carrier]} />
                <h3>{active.kind === "postamat" ? "Постамат" : "Пункт выдачи"} · {active.address}</h3>
                <div className="dp-muted">{[active.city, active.name !== "Пункт выдачи СДЭК" && active.name !== "Постамат СДЭК" ? active.name : ""].filter(Boolean).join(" · ")}</div>
                {active.schedule && <div className="dp-muted"><Clock size={13} style={{ verticalAlign: -2 }} /> {active.schedule}</div>}
                {active.note && <div className="dp-muted">{active.note}</div>}
                {!browse && (pq.loading ? (
                  <div className="dp-price"><span><Loader2 size={14} className="mv-spin" style={{ verticalAlign: -2 }} /> Считаем стоимость…</span></div>
                ) : pq.option ? (
                  <div className="dp-price"><b>{pq.option.priceRub > 0 ? rubFmt(pq.option.priceRub) : "Бесплатно"}</b><span>{daysText(pq.option)}</span></div>
                ) : pq.error ? <div className="dp-err">{pq.error}</div> : null)}
                {!browse && <button type="button" className="dp-go" disabled={!pq.option} onClick={confirmPoint}>Заберу здесь{pq.option ? ` · ${pq.option.priceRub > 0 ? rubFmt(pq.option.priceRub) : "бесплатно"}` : ""}</button>}
                <button type="button" className="mv-co-link" style={{ alignSelf: "center", marginTop: 4 }} onClick={() => { setActive(null); activeIdRef.current = null; void loadPoints(); }}>Выбрать другой пункт</button>
              </div>
            ) : (
              <div>
                <div className="dp-filters" role="group" aria-label="Службы доставки">
                  <button type="button" className={filter === "all" ? "on" : ""} onClick={() => setFilter("all")}>Все</button>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <button type="button" className={filter === "cdek" ? "on" : ""} onClick={() => setFilter("cdek")}><img src={CARRIER_LOGO.cdek} alt="СДЭК" /></button>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <button type="button" className={filter === "yandex" ? "on" : ""} onClick={() => setFilter("yandex")}><img className="ym" src={MARK.yandex} alt="" /> Яндекс</button>
                </div>
                <div className="dp-card">
                  <h3>Выберите пункт на карте</h3>
                  <div className="dp-muted">
                    Нажмите на значок службы — покажем адрес, часы работы, цену и срок доставки.
                    {count > 0 && !zoomIn ? ` На карте ${count} ${plural(count, "пункт", "пункта", "пунктов")}.` : ""} Розовая точка на значке — постамат.
                  </div>
                </div>
              </div>
            )
          ) : (
            <div className="dp-card">
              {placeLoading && !place ? <div className="dp-muted"><Loader2 size={14} className="mv-spin" /> Определяем адрес…</div> : place ? (
                <>
                  <h3>{effPlace?.title || place.title || "Адрес на карте"}</h3>
                  <div className="dp-muted">{place.subtitle || place.city}</div>
                  {needsHouse && !browse ? (
                    <div className="dp-house">
                      <label>Номер дома{place.street ? "" : " (в посёлке или СНТ — номер участка)"}
                        <input value={manualHouse} onChange={(e) => setManualHouse(e.target.value)} placeholder="например, 12 или 5к2" inputMode="text" autoComplete="off" maxLength={20} />
                      </label>
                    </div>
                  ) : place.kind !== "house" ? <div className="dp-warn">Передвиньте карту так, чтобы метка стояла на вашем доме, или найдите адрес с номером дома.</div> : null}
                  {needsHouse && !browse && !manualHouse.trim() && <div className="dp-muted">У этого дома на карте нет номера — впишите его, и мы посчитаем доставку.</div>}
                </>
              ) : <div className="dp-muted">Передвиньте карту — метка в центре покажет адрес.</div>}

              {!browse && effPlace?.kind === "house" && (<>
                <div className="dp-h">Квартира и как пройти</div>
                <div className="dp-fields">
                  <label>Кв./офис<input value={details.flat} onChange={setD("flat")} inputMode="numeric" /></label>
                  <label>Подъезд<input value={details.entrance} onChange={setD("entrance")} inputMode="numeric" /></label>
                  <label>Этаж<input value={details.floor} onChange={setD("floor")} inputMode="numeric" /></label>
                  <label>Домофон<input value={details.intercom} onChange={setD("intercom")} /></label>
                </div>
                <textarea className="dp-comment" rows={2} placeholder="Комментарий курьеру (необязательно)" value={details.comment} onChange={setD("comment")} style={{ marginTop: 8, resize: "none" }} />

                <div className="dp-h">Кто привезёт</div>
                {cq.loading ? <div className="dp-muted"><Loader2 size={14} className="mv-spin" /> Считаем стоимость…</div>
                  : cq.error ? <div className="dp-err">{cq.error}</div> : (
                    <div className="dp-opts" role="radiogroup">
                      {cq.list.map((o) => (
                        <button key={o.id} type="button" role="radio" aria-checked={chosen === o.id} className={`dp-opt${chosen === o.id ? " on" : ""}`} onClick={() => setChosen(o.id)}>
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img src={CARRIER_LOGO[o.carrier]} alt={CARRIER_NAME[o.carrier]} />
                          <span className="t"><b>{o.speed === "fast" ? "Быстрее — сразу" : o.speed === "slot" ? "Ко времени — интервал 4 часа" : "Курьер до двери"}</b><span>{daysText(o)}</span></span>
                          <span className="p">{o.priceRub > 0 ? rubFmt(o.priceRub) : "0 ₽"}</span>
                        </button>
                      ))}
                    </div>
                  )}
                {chosenOpt?.speed === "slot" && chosenOpt.intervals?.length ? (<>
                  <div className="dp-h">Когда привезти</div>
                  <div className="dp-slots">
                    {chosenOpt.intervals.map((iv) => <button key={iv.from} type="button" className={slot?.from === iv.from ? "on" : ""} onClick={() => setSlot(iv)}>{slotText(iv)}</button>)}
                  </div>
                </>) : null}
                <button type="button" className="dp-go" disabled={!chosenOpt || (chosenOpt.speed === "slot" && !slot)} onClick={confirmAddress}>
                  Привезти сюда{chosenOpt ? ` · ${chosenOpt.priceRub > 0 ? rubFmt(chosenOpt.priceRub) : "бесплатно"}` : ""}
                </button>
                <div className="dp-muted" style={{ textAlign: "center" }}><Check size={12} style={{ verticalAlign: -1 }} /> Курьер позвонит перед приездом</div>
              </>)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
