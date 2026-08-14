import { useState, useEffect, useRef } from "react";
import L from "leaflet";
import { X, Search, MapPin, Loader2, Navigation, Clock, CheckCircle, Building2, ArrowLeft } from "lucide-react";
import clsx from "clsx";

export interface PvzPoint {
  id: string;
  name: string;
  address: string;
  lat: number;
  lng: number;
  schedule: string | null;
  type?: string;
  city?: string;
}

interface Props {
  open: boolean;
  onClose: () => void;
  onSelect: (pvz: PvzPoint) => void;
  defaultCity?: string;
}

const API_BASE = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop";

/* ── Custom SVG marker icons ─────────────────────────────────────── */
const mkIcon = (color: string, size = 32, shadow = false) =>
  L.divIcon({
    className: "",
    html: `<div style="position:relative;width:${size}px;height:${size}px">
      ${shadow ? `<div style="position:absolute;bottom:-4px;left:50%;transform:translateX(-50%);width:${size * 0.6}px;height:8px;background:rgba(0,0,0,0.2);border-radius:50%;filter:blur(4px)"></div>` : ""}
      <svg viewBox="0 0 32 40" fill="none" xmlns="http://www.w3.org/2000/svg" style="width:${size}px;height:${size * 1.25}px;filter:drop-shadow(0 3px 8px rgba(0,0,0,0.3))">
        <path d="M16 0C8.268 0 2 6.268 2 14c0 9.333 12 26 14 26s14-16.667 14-26C30 6.268 23.732 0 16 0z" fill="${color}"/>
        <circle cx="16" cy="14" r="6" fill="white" opacity="0.9"/>
        <circle cx="16" cy="14" r="3" fill="${color}"/>
      </svg>
    </div>`,
    iconSize: [size, size * 1.25],
    iconAnchor: [size / 2, size * 1.25],
    popupAnchor: [0, -size * 1.25 - 4],
  });

const mkUserIcon = () =>
  L.divIcon({
    className: "",
    html: `<div style="position:relative">
      <div style="width:20px;height:20px;background:#3b82f6;border-radius:50%;border:3px solid #fff;box-shadow:0 2px 8px rgba(59,130,246,0.5)"></div>
      <div style="position:absolute;inset:-6px;border-radius:50%;background:rgba(59,130,246,0.2);animation:none"></div>
    </div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10],
  });

/* ── PVZ List Item ────────────────────────────────────────────────── */
function PvzItem({ pvz, selected, onClick }: { pvz: PvzPoint; selected: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      data-id={pvz.id}
      onClick={onClick}
      className={clsx("pvz-item w-full text-left px-5 py-4 transition-all group", selected && "selected")}
      style={{
        borderBottom: "1px solid rgba(0,0,0,0.04)",
        background: selected ? "linear-gradient(135deg,#f5f3ff,#ede9fe)" : undefined,
      }}
    >
      <div className="flex items-start gap-3">
        <div className={clsx(
          "w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 mt-0.5 transition-all",
          selected ? "bg-violet-100" : "bg-gray-100 group-hover:bg-violet-50"
        )}>
          <Building2 size={16} className={selected ? "text-violet-600" : "text-gray-400 group-hover:text-violet-400"} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-0.5">
            <span className="text-[13px] font-semibold text-gray-900 leading-tight">{pvz.name}</span>
            {pvz.type && pvz.type !== "ПВЗ" && (
              <span className="text-[10px] bg-orange-100 text-orange-600 px-1.5 py-0.5 rounded-md font-semibold leading-tight">{pvz.type}</span>
            )}
          </div>
          <p className="text-[12px] text-gray-400 leading-snug line-clamp-2">{pvz.address}</p>
          {pvz.schedule && (
            <div className="flex items-center gap-1.5 mt-1.5">
              <Clock size={10} className="text-emerald-500 flex-shrink-0" />
              <span className="text-[11px] text-emerald-600 font-medium">Сегодня {pvz.schedule}</span>
            </div>
          )}
        </div>
        {selected && (
          <div className="flex-shrink-0">
            <CheckCircle size={18} className="text-violet-600" />
          </div>
        )}
      </div>

      {/* Select button when selected */}
      {selected && (
        <div className="mt-3 pl-12">
          <button
            type="button"
            onClick={(e) => e.stopPropagation()}
            className="w-full py-2.5 rounded-xl text-[13px] font-semibold text-white transition-all duration-200 hover:shadow-lg"
            style={{
              background: "linear-gradient(135deg, #7c3aed, #9333ea)",
              boxShadow: "0 4px 16px rgba(124,58,237,0.3)",
            }}
            onMouseEnter={e => { (e.currentTarget as HTMLButtonElement).style.transform = "translateY(-1px)"; }}
            onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.transform = ""; }}
          >
            Выбрать этот пункт
          </button>
        </div>
      )}
    </button>
  );
}

export default function OzonPickupMap({ open, onClose, onSelect, defaultCity = "" }: Props) {
  const [cityInput, setCityInput] = useState(defaultCity);
  const [pvzList, setPvzList] = useState<PvzPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [geoLoading, setGeoLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [mobileView, setMobileView] = useState<"map" | "list">("map");

  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const markersRef = useRef<Map<string, L.Marker>>(new Map());
  const userMarkerRef = useRef<L.Marker | null>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const pvzListRef = useRef<PvzPoint[]>([]);
  pvzListRef.current = pvzList;
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open) return;

    setCityInput(defaultCity);
    setPvzList([]);
    setSearched(false);
    setSelectedId(null);
    markersRef.current.clear();
    userMarkerRef.current = null;

    const container = mapDivRef.current;
    if (container && !mapRef.current) {
      const map = L.map(container, {
        center: [55.75, 37.62],
        zoom: 11,
        zoomControl: false,
      });
      // CartoDB Positron — clean minimal look
      L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
        attribution: "© CartoDB © OSM",
        maxZoom: 19,
        subdomains: "abcd",
      }).addTo(map);
      // Custom zoom position
      L.control.zoom({ position: "topright" }).addTo(map);
      mapRef.current = map;
    } else if (mapRef.current) {
      mapRef.current.invalidateSize();
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (window as any)["__pvzPick"] = (id: string) => {
      const pvz = pvzListRef.current.find((p) => p.id === id);
      if (pvz) { onSelectRef.current(pvz); onCloseRef.current(); }
    };

    if (navigator.geolocation) {
      setGeoLoading(true);
      navigator.geolocation.getCurrentPosition(
        (pos) => { setGeoLoading(false); void doLoadByCoords(pos.coords.latitude, pos.coords.longitude); },
        () => { setGeoLoading(false); void doLoadByCity(defaultCity || "Москва"); },
        { timeout: 5000, maximumAge: 60000 }
      );
    } else {
      void doLoadByCity(defaultCity || "Москва");
    }

    return () => {
      if (mapRef.current) { mapRef.current.remove(); mapRef.current = null; }
      markersRef.current.clear();
      userMarkerRef.current = null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (window as any)["__pvzPick"];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  function buildPopupHtml(pvz: PvzPoint) {
    const sched = pvz.schedule
      ? `<div style="display:flex;align-items:center;gap:5px;margin-top:4px">
          <span style="width:6px;height:6px;border-radius:50%;background:#059669;flex-shrink:0"></span>
          <span style="color:#059669;font-size:11px;font-weight:500">Сегодня ${pvz.schedule}</span>
         </div>`
      : "";
    const badge = pvz.type && pvz.type !== "ПВЗ"
      ? `<span style="display:inline-block;background:#fff7ed;color:#c2410c;font-size:10px;padding:2px 6px;border-radius:5px;font-weight:600;margin-left:4px">${pvz.type}</span>`
      : "";
    return `<div style="font-family:Inter,system-ui,sans-serif;min-width:210px;max-width:240px;padding:4px 2px">
      <div style="display:flex;align-items:flex-start;gap:6px;margin-bottom:6px">
        <div style="width:8px;height:8px;border-radius:50%;background:#7c3aed;flex-shrink:0;margin-top:4px"></div>
        <div>
          <span style="font-weight:700;font-size:13px;color:#1d1d1f;line-height:1.3">${pvz.name}</span>${badge}
        </div>
      </div>
      <p style="color:#6b7280;font-size:11px;line-height:1.5;margin-bottom:4px">${pvz.address}</p>
      ${sched}
      <button onclick="window.__pvzPick('${pvz.id}')"
        style="margin-top:10px;width:100%;background:linear-gradient(135deg,#7c3aed,#9333ea);color:#fff;border:none;border-radius:10px;padding:9px;font-size:12px;font-weight:700;cursor:pointer;letter-spacing:0.01em;box-shadow:0 4px 12px rgba(124,58,237,0.35)">
        Выбрать пункт
      </button>
    </div>`;
  }

  function placeMarkers(points: PvzPoint[]) {
    const map = mapRef.current;
    if (!map) return;
    markersRef.current.forEach((m) => m.remove());
    markersRef.current.clear();

    const defaultIcon = mkIcon("#ff6a00");
    points.forEach((pvz) => {
      if (!pvz.lat || !pvz.lng) return;
      const marker = L.marker([pvz.lat, pvz.lng], { icon: defaultIcon }).addTo(map);
      marker.bindPopup(buildPopupHtml(pvz), {
        maxWidth: 260,
        className: "pvz-leaflet-popup",
        closeButton: true,
      });
      marker.on("click", () => highlightPvz(pvz, false));
      markersRef.current.set(pvz.id, marker);
    });

    const valid = points.filter((p) => p.lat && p.lng);
    if (valid.length > 0) {
      map.fitBounds(
        L.latLngBounds(valid.map((p) => [p.lat, p.lng] as [number, number])),
        { padding: [40, 40], maxZoom: 14 }
      );
    }
  }

  function highlightPvz(pvz: PvzPoint, panMap = true) {
    setSelectedId(pvz.id);
    const selectedIcon = mkIcon("#7c3aed", 38, true);
    const defaultIcon = mkIcon("#ff6a00");
    markersRef.current.forEach((marker, id) => marker.setIcon(id === pvz.id ? selectedIcon : defaultIcon));
    if (panMap && mapRef.current && pvz.lat && pvz.lng) {
      mapRef.current.setView([pvz.lat, pvz.lng], 16, { animate: true });
      setTimeout(() => markersRef.current.get(pvz.id)?.openPopup(), 300);
    }
    const el = listRef.current?.querySelector<HTMLElement>(`[data-id="${pvz.id}"]`);
    el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    setMobileView("list");
  }

  async function doLoadByCity(city: string) {
    if (!city.trim()) return;
    setLoading(true); setSearched(false); setPvzList([]); setSelectedId(null);
    try {
      const res = await fetch(`${API_BASE}/delivery/pvz?city=${encodeURIComponent(city.trim())}`);
      const data: { pvz?: PvzPoint[]; city?: string } = await res.json();
      const list = Array.isArray(data.pvz) ? data.pvz : [];
      setPvzList(list); pvzListRef.current = list; setSearched(true);
      placeMarkers(list);
    } catch { setSearched(true); }
    finally { setLoading(false); }
  }

  async function doLoadByCoords(lat: number, lng: number) {
    setLoading(true); setSearched(false); setPvzList([]); setSelectedId(null);
    const map = mapRef.current;
    if (map) {
      userMarkerRef.current?.remove();
      userMarkerRef.current = L.marker([lat, lng], { icon: mkUserIcon() }).addTo(map);
      map.setView([lat, lng], 13, { animate: true });
    }
    try {
      const res = await fetch(`${API_BASE}/delivery/pvz?lat=${lat}&lng=${lng}`);
      const data: { pvz?: PvzPoint[]; city?: string } = await res.json();
      const list = Array.isArray(data.pvz) ? data.pvz : [];
      if (data.city) setCityInput(data.city);
      setPvzList(list); pvzListRef.current = list; setSearched(true);
      placeMarkers(list);
    } catch { setSearched(true); }
    finally { setLoading(false); }
  }

  function handleSearch(e: React.FormEvent) { e.preventDefault(); void doLoadByCity(cityInput); }
  function handleGeo() {
    if (!navigator.geolocation) return;
    setGeoLoading(true);
    navigator.geolocation.getCurrentPosition(
      (pos) => { setGeoLoading(false); void doLoadByCoords(pos.coords.latitude, pos.coords.longitude); },
      () => setGeoLoading(false),
      { timeout: 10000, maximumAge: 60000 }
    );
  }

  if (!open) return null;

  const selectedPvz = pvzList.find((p) => p.id === selectedId);

  return (
    <div className="pvz-overlay fixed inset-0 z-[200] flex flex-col" style={{ background: "#f8f7ff" }}>

      {/* ── TOP HEADER ── */}
      <div className="flex items-center gap-3 px-4 py-3 flex-shrink-0" style={{
        background: "rgba(255,255,255,0.95)",
        backdropFilter: "blur(20px)",
        borderBottom: "1px solid rgba(0,0,0,0.07)",
        boxShadow: "0 1px 12px rgba(0,0,0,0.06)",
      }}>
        <button
          onClick={onClose}
          className="p-2 rounded-xl hover:bg-gray-100 transition-colors text-gray-500 flex items-center gap-1.5"
        >
          <ArrowLeft size={18} />
        </button>
        <div className="flex items-center gap-2.5 flex-1">
          <div
            className="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
            style={{ background: "linear-gradient(135deg,#ff6a00,#ee0979)" }}
          >
            <MapPin size={14} className="text-white" />
          </div>
          <div>
            <div className="font-bold text-[14px] text-gray-900 leading-tight">Пункты выдачи Ozon</div>
            {pvzList.length > 0 && (
              <div className="text-[11px] text-gray-400">{pvzList.length} точк{pvzList.length === 1 ? "а" : pvzList.length < 5 ? "и" : ""} рядом</div>
            )}
          </div>
        </div>

        {/* Mobile toggle */}
        <div className="flex lg:hidden gap-1 p-1 rounded-xl" style={{ background: "#f3f4f6" }}>
          <button
            type="button"
            onClick={() => setMobileView("map")}
            className={clsx("px-3 py-1.5 rounded-lg text-[12px] font-semibold transition-all", mobileView === "map" ? "bg-white text-violet-700 shadow-sm" : "text-gray-500")}
          >Карта</button>
          <button
            type="button"
            onClick={() => setMobileView("list")}
            className={clsx("px-3 py-1.5 rounded-lg text-[12px] font-semibold transition-all", mobileView === "list" ? "bg-white text-violet-700 shadow-sm" : "text-gray-500")}
          >
            Список {pvzList.length > 0 && <span className="ml-1 bg-violet-100 text-violet-700 px-1.5 py-0.5 rounded-md text-[10px]">{pvzList.length}</span>}
          </button>
        </div>
      </div>

      {/* ── SEARCH BAR ── */}
      <form onSubmit={handleSearch} className="flex items-center gap-2 px-4 py-3 flex-shrink-0" style={{
        background: "rgba(255,255,255,0.9)",
        backdropFilter: "blur(12px)",
        borderBottom: "1px solid rgba(0,0,0,0.05)",
      }}>
        <div className="relative flex-1">
          <Search size={14} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
          <input
            type="text"
            value={cityInput}
            onChange={(e) => setCityInput(e.target.value)}
            placeholder="Введите ваш город..."
            className="w-full pl-10 pr-4 py-2.5 text-sm font-medium focus:outline-none transition-all rounded-xl"
            style={{
              background: "#f8f8fc",
              border: "1.5px solid #e5e7eb",
              color: "#1d1d1f",
            }}
            onFocus={e => { (e.target as HTMLInputElement).style.borderColor = "#7c3aed"; (e.target as HTMLInputElement).style.background = "#fff"; }}
            onBlur={e => { (e.target as HTMLInputElement).style.borderColor = "#e5e7eb"; (e.target as HTMLInputElement).style.background = "#f8f8fc"; }}
            autoFocus={!defaultCity}
          />
        </div>
        <button
          type="submit"
          disabled={!cityInput.trim() || loading}
          className="px-4 py-2.5 text-white text-sm font-semibold rounded-xl transition-all disabled:opacity-40 flex items-center gap-1.5 flex-shrink-0"
          style={{ background: "linear-gradient(135deg,#7c3aed,#9333ea)", boxShadow: "0 2px 8px rgba(124,58,237,0.3)" }}
        >
          {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
          Найти
        </button>
        <button
          type="button"
          onClick={handleGeo}
          disabled={geoLoading || loading}
          title="Моё местоположение"
          className="p-2.5 rounded-xl transition-all disabled:opacity-40 flex-shrink-0"
          style={{
            background: "#eff6ff",
            border: "1.5px solid #bfdbfe",
            color: "#3b82f6",
          }}
          onMouseEnter={e => { (e.currentTarget as HTMLButtonElement).style.background = "#dbeafe"; }}
          onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.background = "#eff6ff"; }}
        >
          {geoLoading ? <Loader2 size={16} className="animate-spin" /> : <Navigation size={16} />}
        </button>
      </form>

      {/* ── MAIN CONTENT ── */}
      <div className="flex flex-1 min-h-0 overflow-hidden">

        {/* ── MAP (left/main on desktop, toggle on mobile) ── */}
        <div
          className={clsx("relative flex-1", mobileView !== "map" ? "hidden lg:block" : "block")}
          style={{ minWidth: 0 }}
        >
          <div
            ref={mapDivRef}
            style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
          />

          {/* Loading overlay on map */}
          {loading && (
            <div className="absolute inset-0 flex items-center justify-center z-20" style={{ background: "rgba(255,255,255,0.7)", backdropFilter: "blur(8px)" }}>
              <div className="flex items-center gap-3 px-6 py-4 rounded-2xl" style={{ background: "#fff", boxShadow: "0 8px 32px rgba(0,0,0,0.12)" }}>
                <Loader2 size={20} className="animate-spin text-violet-600" />
                <span className="text-[14px] font-medium text-gray-700">Ищем пункты выдачи…</span>
              </div>
            </div>
          )}

          {/* Selected PVZ floating card on map (desktop) */}
          {selectedPvz && (
            <div
              className="absolute bottom-6 left-6 z-20 hidden lg:block"
              style={{
                background: "rgba(255,255,255,0.97)",
                backdropFilter: "blur(20px)",
                borderRadius: 20,
                padding: "20px 24px",
                boxShadow: "0 16px 48px rgba(0,0,0,0.15)",
                maxWidth: 320,
                border: "1px solid rgba(124,58,237,0.2)",
              }}
            >
              <div className="flex items-start gap-3 mb-4">
                <div className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0" style={{ background: "#f5f3ff" }}>
                  <Building2 size={18} style={{ color: "#7c3aed" }} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="font-bold text-[14px] text-gray-900 leading-tight">{selectedPvz.name}</div>
                  <div className="text-[12px] text-gray-400 mt-0.5 leading-snug">{selectedPvz.address}</div>
                  {selectedPvz.schedule && (
                    <div className="flex items-center gap-1.5 mt-1.5">
                      <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                      <span className="text-[11px] text-emerald-600 font-medium">Сегодня {selectedPvz.schedule}</span>
                    </div>
                  )}
                </div>
              </div>
              <button
                type="button"
                onClick={() => { onSelect(selectedPvz); onClose(); }}
                className="w-full py-3 rounded-xl text-[13px] font-bold text-white transition-all hover:-translate-y-0.5"
                style={{ background: "linear-gradient(135deg,#7c3aed,#9333ea)", boxShadow: "0 6px 20px rgba(124,58,237,0.4)" }}
              >
                Выбрать этот пункт
              </button>
            </div>
          )}
        </div>

        {/* ── SIDEBAR / LIST ── */}
        <div
          className={clsx(
            "flex flex-col overflow-hidden",
            mobileView !== "list" ? "hidden lg:flex" : "flex w-full",
            "lg:w-[380px] lg:flex-shrink-0"
          )}
          style={{ borderLeft: "1px solid rgba(0,0,0,0.06)", background: "#fff" }}
        >
          {/* Empty states */}
          {!loading && !searched && (
            <div className="flex flex-col items-center justify-center py-20 px-8 text-center flex-1">
              <div className="w-16 h-16 rounded-3xl flex items-center justify-center mb-5" style={{ background: "linear-gradient(135deg,#f5f3ff,#ede9fe)" }}>
                <Navigation size={28} style={{ color: "#7c3aed" }} />
              </div>
              <p className="text-[15px] font-semibold text-gray-900 mb-2">Найдите ближайший пункт</p>
              <p className="text-[13px] text-gray-400 leading-relaxed">Введите город или нажмите кнопку геолокации</p>
            </div>
          )}

          {loading && (
            <div className="flex flex-col items-center justify-center py-20 gap-4 flex-1">
              <Loader2 size={28} className="animate-spin" style={{ color: "#7c3aed" }} />
              <p className="text-[13px] font-medium text-gray-500">Ищем пункты рядом с вами…</p>
            </div>
          )}

          {!loading && searched && pvzList.length === 0 && (
            <div className="flex flex-col items-center justify-center py-20 px-8 text-center flex-1">
              <div className="w-16 h-16 rounded-3xl flex items-center justify-center mb-5" style={{ background: "#fff7f0" }}>
                <MapPin size={28} style={{ color: "#f97316" }} />
              </div>
              <p className="text-[15px] font-semibold text-gray-900 mb-2">Пункты не найдены</p>
              <p className="text-[13px] text-gray-400">Попробуйте другой город или используйте геолокацию</p>
            </div>
          )}

          {!loading && pvzList.length > 0 && (
            <>
              {/* Count header */}
              <div className="px-5 py-3 flex-shrink-0 flex items-center justify-between" style={{ background: "#fafafa", borderBottom: "1px solid rgba(0,0,0,0.05)" }}>
                <span className="text-[12px] font-bold text-gray-500 uppercase tracking-wider">
                  {pvzList.length} пункт{pvzList.length === 1 ? "" : pvzList.length < 5 ? "а" : "ов"} выдачи
                </span>
                {selectedId && (
                  <button
                    type="button"
                    onClick={() => { const pvz = pvzList.find(p => p.id === selectedId); if (pvz) { onSelect(pvz); onClose(); } }}
                    className="text-[12px] font-bold text-violet-600 hover:text-violet-800 transition-colors"
                  >
                    Подтвердить →
                  </button>
                )}
              </div>

              {/* List */}
              <div ref={listRef} className="flex-1 overflow-y-auto" style={{ scrollbarWidth: "thin", scrollbarColor: "#e5e7eb transparent" }}>
                {pvzList.map((pvz) => (
                  <PvzItem
                    key={pvz.id}
                    pvz={pvz}
                    selected={selectedId === pvz.id}
                    onClick={() => {
                      highlightPvz(pvz);
                      if (selectedId === pvz.id) { onSelect(pvz); onClose(); }
                    }}
                  />
                ))}
                <div style={{ height: 40 }} />
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
