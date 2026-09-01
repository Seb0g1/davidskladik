import { useState, useEffect, useRef } from "react";
import L from "leaflet";
import { X, Search, MapPin, Loader2, Navigation, Clock, CheckCircle, Building2, ArrowLeft } from "lucide-react";

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

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent3: "#EDD9B0",
};

// MV-branded map pin: dark body + gold MV text; selected = gold body + dark text
const mkIcon = (selected = false) =>
  L.divIcon({
    className: "",
    html: `<svg width="34" height="46" viewBox="0 0 34 46" fill="none" xmlns="http://www.w3.org/2000/svg"
             style="filter:drop-shadow(0 4px 14px rgba(0,0,0,0.75));display:block">
      <path d="M17 1C8.163 1 1 8.163 1 17c0 11.5 14.5 27.5 16 27.5S33 28.5 33 17C33 8.163 25.837 1 17 1z"
            fill="${selected ? "#C9A96E" : "#141210"}"
            stroke="${selected ? "rgba(237,217,176,0.7)" : "rgba(201,169,110,0.55)"}"
            stroke-width="1.5"/>
      <rect x="8.5" y="7" width="17" height="17" rx="4.5"
            fill="${selected ? "rgba(14,13,11,0.18)" : "rgba(201,169,110,0.09)"}"
            stroke="${selected ? "rgba(14,13,11,0.15)" : "rgba(201,169,110,0.2)"}" stroke-width="0.75"/>
      <text x="17" y="19.5" text-anchor="middle" dominant-baseline="middle"
            font-family="Georgia,serif" font-size="10.5" font-weight="700" font-style="italic"
            fill="${selected ? "#0E0D0B" : "#C9A96E"}" letter-spacing="0.3">MV</text>
    </svg>`,
    iconSize: [34, 46],
    iconAnchor: [17, 46],
    popupAnchor: [0, -50],
  });

const mkUserIcon = () =>
  L.divIcon({
    className: "",
    html: `<div style="position:relative">
      <div style="width:18px;height:18px;background:#3b82f6;border-radius:50%;border:3px solid #fff;box-shadow:0 2px 8px rgba(59,130,246,0.7)"></div>
      <div style="position:absolute;inset:-6px;border-radius:50%;background:rgba(59,130,246,0.18)"></div>
    </div>`,
    iconSize: [18, 18],
    iconAnchor: [9, 9],
  });

const MAP_CSS = `
.pvz-map-wrap .leaflet-control-zoom a {
  background: rgba(20,18,16,0.95) !important;
  color: rgba(244,239,230,0.75) !important;
  border-color: rgba(255,252,245,0.1) !important;
  font-size: 16px !important;
}
.pvz-map-wrap .leaflet-control-zoom a:hover {
  background: rgba(201,169,110,0.12) !important;
  color: #C9A96E !important;
}
.pvz-map-wrap .leaflet-control-attribution { display: none !important; }
.pvz-dark-popup .leaflet-popup-content-wrapper {
  background: rgba(22,21,18,0.97) !important;
  border: 1px solid rgba(201,169,110,0.15) !important;
  border-radius: 16px !important;
  box-shadow: 0 12px 40px rgba(0,0,0,0.75) !important;
  padding: 0 !important;
  backdrop-filter: blur(20px);
}
.pvz-dark-popup .leaflet-popup-content {
  margin: 14px 16px !important;
  color: #F4EFE6 !important;
}
.pvz-dark-popup .leaflet-popup-tip-container { display: none; }
.pvz-dark-popup .leaflet-popup-close-button {
  color: rgba(244,239,230,0.35) !important;
  font-size: 18px !important;
  top: 8px !important; right: 10px !important;
}
.pvz-dark-popup .leaflet-popup-close-button:hover { color: #F4EFE6 !important; }
`;

function PvzItem({ pvz, selected, onClick }: { pvz: PvzPoint; selected: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      data-id={pvz.id}
      onClick={onClick}
      style={{
        width: "100%", textAlign: "left", padding: "14px 16px",
        background: selected ? "rgba(201,169,110,0.1)" : "transparent",
        borderBottom: `1px solid ${S.border}`,
        border: "none", borderBottomColor: S.border,
        cursor: "pointer", transition: "background 0.15s ease", display: "block",
      }}
      onMouseEnter={e => { if (!selected) (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.04)"; }}
      onMouseLeave={e => { if (!selected) (e.currentTarget as HTMLElement).style.background = "transparent"; }}
    >
      <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
        <div style={{
          width: 36, height: 36, borderRadius: 10, flexShrink: 0, marginTop: 1,
          display: "flex", alignItems: "center", justifyContent: "center",
          background: selected ? "rgba(201,169,110,0.15)" : "rgba(255,255,255,0.06)",
          border: `1px solid ${selected ? "rgba(201,169,110,0.25)" : S.border}`,
          transition: "all 0.15s ease",
        }}>
          <Building2 size={16} style={{ color: selected ? S.accent3 : S.muted }} />
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", marginBottom: 2 }}>
            <span style={{ fontSize: 13, fontWeight: 600, color: S.text, lineHeight: 1.3 }}>{pvz.name}</span>
            {pvz.type && pvz.type !== "ПВЗ" && (
              <span style={{ fontSize: 10, background: "rgba(249,115,22,0.15)", color: "#fb923c", padding: "2px 7px", borderRadius: 6, fontWeight: 600 }}>{pvz.type}</span>
            )}
          </div>
          <p style={{ fontSize: 12, color: S.muted, lineHeight: 1.5, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{pvz.address}</p>
          {pvz.schedule && (
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6 }}>
              <Clock size={10} style={{ color: "#4ade80", flexShrink: 0 }} />
              <span style={{ fontSize: 11, color: "#4ade80", fontWeight: 500 }}>Сегодня {pvz.schedule}</span>
            </div>
          )}
        </div>
        {selected && <CheckCircle size={18} style={{ color: S.accent3, flexShrink: 0 }} />}
      </div>

      {selected && (
        <div style={{ marginTop: 12, paddingLeft: 48 }}>
          <button
            type="button"
            onClick={e => e.stopPropagation()}
            style={{
              width: "100%", padding: "10px", borderRadius: 12, fontSize: 13, fontWeight: 700,
              color: "#0E0D0B", border: "none", cursor: "pointer",
              background: S.accent, boxShadow: "0 4px 16px rgba(201,169,110,0.25)",
            }}
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
  const [winW, setWinW] = useState(() => (typeof window !== "undefined" ? window.innerWidth : 1024));

  useEffect(() => {
    const onResize = () => setWinW(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const isMobile = winW < 1024;

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
  const isAutoMoveRef = useRef(false);
  const moveLoadTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!open) return;

    setCityInput(defaultCity);
    setPvzList([]);
    setSearched(false);
    setSelectedId(null);
    markersRef.current.clear();
    userMarkerRef.current = null;

    const container = mapDivRef.current;
    let resizeObserver: ResizeObserver | null = null;
    if (container && !mapRef.current) {
      const map = L.map(container, {
        center: [55.75, 37.62],
        zoom: 11,
        zoomControl: false,
        attributionControl: false,
      });
      // Stadia Alidade Smooth Dark — clean dark tiles, no API key for low traffic
      L.tileLayer("https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png", {
        maxZoom: 20,
        attribution: "",
      }).addTo(map);
      L.control.zoom({ position: "topright" }).addTo(map);
      mapRef.current = map;
      map.on("moveend", () => {
        if (isAutoMoveRef.current) { isAutoMoveRef.current = false; return; }
        if (moveLoadTimerRef.current) clearTimeout(moveLoadTimerRef.current);
        moveLoadTimerRef.current = setTimeout(() => {
          if (!mapRef.current) return;
          const c = mapRef.current.getCenter();
          void doLoadByCoords(c.lat, c.lng, true);
        }, 900);
      });
      setTimeout(() => map.invalidateSize(), 0);
      setTimeout(() => map.invalidateSize(), 150);
      setTimeout(() => map.invalidateSize(), 400);
      if (typeof ResizeObserver !== "undefined") {
        resizeObserver = new ResizeObserver(() => map.invalidateSize());
        resizeObserver.observe(container);
      }
    } else if (mapRef.current) {
      setTimeout(() => mapRef.current?.invalidateSize(), 0);
      setTimeout(() => mapRef.current?.invalidateSize(), 150);
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
        () => { setGeoLoading(false); void doLoadByCity("Москва"); },
        { timeout: 5000, maximumAge: 60000 }
      );
    } else {
      void doLoadByCity("Москва");
    }

    return () => {
      if (moveLoadTimerRef.current) clearTimeout(moveLoadTimerRef.current);
      resizeObserver?.disconnect();
      if (mapRef.current) { mapRef.current.remove(); mapRef.current = null; }
      markersRef.current.clear();
      userMarkerRef.current = null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (window as any)["__pvzPick"];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  useEffect(() => {
    if (mobileView === "map" && mapRef.current) {
      requestAnimationFrame(() => {
        mapRef.current?.invalidateSize();
        setTimeout(() => mapRef.current?.invalidateSize(), 150);
      });
    }
  }, [mobileView]);

  function buildPopupHtml(pvz: PvzPoint) {
    const sched = pvz.schedule
      ? `<div style="display:flex;align-items:center;gap:5px;margin-top:6px">
          <span style="width:6px;height:6px;border-radius:50%;background:#4ade80;flex-shrink:0"></span>
          <span style="color:#4ade80;font-size:11px;font-weight:500">Сегодня ${pvz.schedule}</span>
         </div>`
      : "";
    const badge = pvz.type && pvz.type !== "ПВЗ"
      ? `<span style="display:inline-block;background:rgba(249,115,22,0.15);color:#fb923c;font-size:10px;padding:2px 7px;border-radius:6px;font-weight:600;margin-left:4px">${pvz.type}</span>`
      : "";
    return `<div style="font-family:Inter,system-ui,sans-serif;min-width:200px;max-width:230px;padding:2px 0">
      <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:6px">
        <div style="width:7px;height:7px;border-radius:50%;background:#C9A96E;flex-shrink:0;margin-top:5px"></div>
        <div>
          <span style="font-weight:700;font-size:13px;color:#F4EFE6;line-height:1.3">${pvz.name}</span>${badge}
        </div>
      </div>
      <p style="color:rgba(244,239,230,0.48);font-size:11px;line-height:1.5;margin-bottom:4px">${pvz.address}</p>
      ${sched}
      <button onclick="window.__pvzPick('${pvz.id}')"
        style="margin-top:12px;width:100%;background:#C9A96E;color:#0E0D0B;border:none;border-radius:10px;padding:9px;font-size:12px;font-weight:700;cursor:pointer">
        Выбрать пункт
      </button>
    </div>`;
  }

  function placeMarkers(points: PvzPoint[], fitMap = true) {
    const map = mapRef.current;
    if (!map) return;
    markersRef.current.forEach((m) => m.remove());
    markersRef.current.clear();

    points.forEach((pvz) => {
      if (!pvz.lat || !pvz.lng) return;
      const marker = L.marker([pvz.lat, pvz.lng], { icon: mkIcon(false) }).addTo(map);
      marker.bindPopup(buildPopupHtml(pvz), {
        maxWidth: 250,
        className: "pvz-dark-popup",
        closeButton: true,
      });
      marker.on("click", () => highlightPvz(pvz, false));
      markersRef.current.set(pvz.id, marker);
    });

    if (fitMap) {
      const valid = points.filter((p) => p.lat && p.lng);
      if (valid.length > 0) {
        isAutoMoveRef.current = true;
        map.fitBounds(
          L.latLngBounds(valid.map((p) => [p.lat, p.lng] as [number, number])),
          { padding: [40, 40], maxZoom: 14 }
        );
      }
    }
  }

  function highlightPvz(pvz: PvzPoint, panMap = true) {
    setSelectedId(pvz.id);
    markersRef.current.forEach((marker, id) =>
      marker.setIcon(id === pvz.id ? mkIcon(true) : mkIcon(false))
    );
    if (panMap && mapRef.current && pvz.lat && pvz.lng) {
      isAutoMoveRef.current = true; // prevent moveend → doLoadByCoords on this pan
      mapRef.current.setView([pvz.lat, pvz.lng], 16, { animate: true });
      setTimeout(() => markersRef.current.get(pvz.id)?.openPopup(), 320);
    }
    const el = listRef.current?.querySelector<HTMLElement>(`[data-id="${pvz.id}"]`);
    el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    setMobileView("list");
  }

  async function doLoadByCity(city: string) {
    if (!city.trim()) return;
    setCityInput(city);
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

  async function doLoadByCoords(lat: number, lng: number, fromMap = false) {
    setLoading(true); setSearched(false); setPvzList([]); setSelectedId(null);
    const map = mapRef.current;
    if (map && !fromMap) {
      userMarkerRef.current?.remove();
      userMarkerRef.current = L.marker([lat, lng], { icon: mkUserIcon() }).addTo(map);
      isAutoMoveRef.current = true;
      map.setView([lat, lng], 13, { animate: true });
    }
    try {
      const res = await fetch(`${API_BASE}/delivery/pvz?lat=${lat}&lng=${lng}`);
      const data: { pvz?: PvzPoint[]; city?: string } = await res.json();
      const list = Array.isArray(data.pvz) ? data.pvz : [];
      if (data.city && !fromMap) setCityInput(data.city);
      setPvzList(list); pvzListRef.current = list; setSearched(true);
      placeMarkers(list, !fromMap);
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
    <div className="pvz-map-wrap" style={{ position: "fixed", inset: 0, zIndex: 200, display: "flex", flexDirection: "column", background: S.bg }}>
      <style>{MAP_CSS}</style>

      {/* ── HEADER ── */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12, padding: "12px 16px", flexShrink: 0,
        background: "rgba(14,13,11,0.97)", backdropFilter: "blur(20px)",
        borderBottom: `1px solid ${S.border}`, boxShadow: "0 1px 24px rgba(0,0,0,0.4)",
      }}>
        <button onClick={onClose} style={{
          padding: "8px", borderRadius: 12, background: "rgba(255,255,255,0.05)", border: `1px solid ${S.border}`,
          color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", transition: "all 0.15s",
        }}
          onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.1)"; (e.currentTarget as HTMLElement).style.color = S.text; }}
          onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.05)"; (e.currentTarget as HTMLElement).style.color = S.muted; }}
        >
          <ArrowLeft size={18} />
        </button>

        {/* MV logo */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1 }}>
          <div style={{
            width: 36, height: 36, borderRadius: 10, flexShrink: 0,
            background: "#0E0D0B", border: "1px solid rgba(201,169,110,0.3)",
            display: "flex", alignItems: "center", justifyContent: "center",
            boxShadow: "0 0 0 1px rgba(201,169,110,0.08)",
          }}>
            <svg viewBox="0 0 32 32" width="26" height="26">
              <rect width="32" height="32" rx="6" fill="#0E0D0B"/>
              <text x="16" y="22" fontFamily="Georgia,serif" fontSize="15" fontWeight="700" fontStyle="italic" fill="#C9A96E" textAnchor="middle">MV</text>
            </svg>
          </div>
          <div>
            <div style={{ fontWeight: 700, fontSize: 14, color: S.text }}>Пункты выдачи</div>
            <div style={{ fontSize: 11, color: S.muted }}>
              {pvzList.length > 0
                ? `${pvzList.length} пункт${pvzList.length === 1 ? "" : pvzList.length < 5 ? "а" : "ов"} рядом`
                : "Magic Vibes"}
            </div>
          </div>
        </div>

        {isMobile && (
          <div style={{ display: "flex", gap: 4, padding: 4, borderRadius: 12, background: "rgba(255,255,255,0.05)", flexShrink: 0 }}>
            {(["map", "list"] as const).map((v) => (
              <button key={v} type="button" onClick={() => setMobileView(v)} style={{
                padding: "6px 12px", borderRadius: 8, fontSize: 12, fontWeight: 600, cursor: "pointer", border: "none",
                background: mobileView === v ? S.surface2 : "transparent",
                color: mobileView === v ? S.accent3 : S.muted,
                transition: "all 0.15s ease",
              }}>
                {v === "map" ? "Карта" : <>Список {pvzList.length > 0 && <span style={{ marginLeft: 4, background: "rgba(201,169,110,0.14)", color: S.accent3, padding: "1px 6px", borderRadius: 6, fontSize: 10 }}>{pvzList.length}</span>}</>}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* ── SEARCH BAR ── */}
      <form onSubmit={handleSearch} style={{
        display: "flex", alignItems: "center", gap: 8, padding: "10px 16px", flexShrink: 0,
        background: "rgba(14,13,11,0.95)", backdropFilter: "blur(12px)",
        borderBottom: `1px solid ${S.border}`,
      }}>
        <div style={{ position: "relative", flex: 1 }}>
          <Search size={14} style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: S.subtle, pointerEvents: "none" }} />
          <input
            type="text"
            value={cityInput}
            onChange={(e) => setCityInput(e.target.value)}
            placeholder="Введите ваш город..."
            style={{
              width: "100%", paddingLeft: 38, paddingRight: 14, paddingTop: 10, paddingBottom: 10,
              fontSize: 13, fontWeight: 500, fontFamily: "inherit",
              background: "rgba(255,255,255,0.05)", border: `1.5px solid ${S.border}`,
              borderRadius: 12, color: S.text, outline: "none", transition: "all 0.15s ease",
            }}
            onFocus={e => { (e.target as HTMLInputElement).style.borderColor = "rgba(201,169,110,0.4)"; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.08)"; }}
            onBlur={e => { (e.target as HTMLInputElement).style.borderColor = S.border; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.05)"; }}
          />
        </div>
        <button type="submit" disabled={!cityInput.trim() || loading} style={{
          padding: "10px 16px", fontSize: 13, fontWeight: 600, borderRadius: 12, border: "none", cursor: "pointer",
          background: "#C9A96E", color: "#0E0D0B",
          display: "flex", alignItems: "center", gap: 6, flexShrink: 0,
          opacity: (!cityInput.trim() || loading) ? 0.5 : 1,
        }}>
          {loading ? <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} /> : <Search size={14} />}
          Найти
        </button>
        <button type="button" onClick={handleGeo} disabled={geoLoading || loading} title="Моё местоположение" style={{
          padding: 10, borderRadius: 12, border: `1.5px solid rgba(59,130,246,0.22)`, cursor: "pointer",
          background: "rgba(59,130,246,0.08)", color: "#60a5fa",
          display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
          opacity: (geoLoading || loading) ? 0.5 : 1, transition: "all 0.15s",
        }}
          onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(59,130,246,0.18)"; }}
          onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "rgba(59,130,246,0.08)"; }}
        >
          {geoLoading ? <Loader2 size={16} style={{ animation: "spin 1s linear infinite" }} /> : <Navigation size={16} />}
        </button>
      </form>

      {/* ── MAIN CONTENT ── */}
      <div style={{ display: "flex", flex: 1, minHeight: 0, overflow: "hidden" }}>

        {/* ── MAP ── */}
        <div style={{
          position: "relative", flex: 1, minWidth: 0,
          display: isMobile && mobileView !== "map" ? "none" : "flex",
          flexDirection: "column",
        }}>
          <div ref={mapDivRef} style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }} />

          {/* Loading overlay */}
          {loading && (
            <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", zIndex: 20, background: "rgba(14,13,11,0.72)", backdropFilter: "blur(8px)" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 12, padding: "16px 24px", borderRadius: 16, background: S.surface, border: `1px solid ${S.borderMd}`, boxShadow: "0 8px 32px rgba(0,0,0,0.5)" }}>
                <Loader2 size={20} style={{ color: S.accent3, animation: "spin 1s linear infinite" }} />
                <span style={{ fontSize: 14, fontWeight: 500, color: S.muted }}>Ищем пункты выдачи…</span>
              </div>
            </div>
          )}

          {/* Selected PVZ floating card (desktop only) */}
          {selectedPvz && !isMobile && (
            <div style={{
              position: "absolute", bottom: 24, left: 24, zIndex: 20,
              background: "rgba(14,13,11,0.97)", backdropFilter: "blur(20px)",
              borderRadius: 20, padding: "20px 24px", boxShadow: "0 16px 48px rgba(0,0,0,0.7)",
              maxWidth: 320, border: `1px solid rgba(201,169,110,0.15)`,
            }}>
              <div style={{ display: "flex", alignItems: "flex-start", gap: 12, marginBottom: 16 }}>
                <div style={{ width: 36, height: 36, borderRadius: 10, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, background: "rgba(201,169,110,0.12)", border: "1px solid rgba(201,169,110,0.22)" }}>
                  <Building2 size={18} style={{ color: S.accent3 }} />
                </div>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 700, fontSize: 14, color: S.text }}>{selectedPvz.name}</div>
                  <div style={{ fontSize: 12, color: S.muted, marginTop: 2 }}>{selectedPvz.address}</div>
                  {selectedPvz.schedule && (
                    <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 6 }}>
                      <div style={{ width: 6, height: 6, borderRadius: "50%", background: "#4ade80" }} />
                      <span style={{ fontSize: 11, color: "#4ade80", fontWeight: 500 }}>Сегодня {selectedPvz.schedule}</span>
                    </div>
                  )}
                </div>
              </div>
              <button type="button" onClick={() => { onSelect(selectedPvz); onClose(); }} style={{
                width: "100%", padding: "12px", borderRadius: 12, fontSize: 13, fontWeight: 700, color: "#0E0D0B",
                background: S.accent, border: "none", cursor: "pointer",
                boxShadow: "0 6px 20px rgba(201,169,110,0.28)", transition: "transform 0.15s ease",
              }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.transform = "translateY(-1px)"; }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.transform = ""; }}
              >
                Выбрать этот пункт
              </button>
            </div>
          )}
        </div>

        {/* ── SIDEBAR LIST ── */}
        <div style={{
          display: isMobile && mobileView !== "list" ? "none" : "flex",
          flexDirection: "column", overflow: "hidden",
          borderLeft: `1px solid ${S.border}`, background: S.surface,
          width: isMobile ? "100%" : 380, flexShrink: 0,
        }}>
          {!loading && !searched && (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "80px 32px", textAlign: "center", flex: 1 }}>
              <div style={{ width: 64, height: 64, borderRadius: 24, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 20, background: "rgba(201,169,110,0.08)", border: "1px solid rgba(201,169,110,0.15)" }}>
                <Navigation size={28} style={{ color: S.accent3 }} />
              </div>
              <p style={{ fontSize: 15, fontWeight: 600, color: S.text, marginBottom: 8 }}>Найдите ближайший пункт</p>
              <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.6 }}>Введите город или нажмите кнопку геолокации</p>
            </div>
          )}

          {loading && (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "80px 20px", gap: 16, flex: 1 }}>
              <Loader2 size={28} style={{ color: S.accent3, animation: "spin 1s linear infinite" }} />
              <p style={{ fontSize: 13, fontWeight: 500, color: S.muted }}>Ищем пункты рядом…</p>
            </div>
          )}

          {!loading && searched && pvzList.length === 0 && (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "80px 32px", textAlign: "center", flex: 1 }}>
              <div style={{ width: 64, height: 64, borderRadius: 24, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 20, background: "rgba(249,115,22,0.08)", border: "1px solid rgba(249,115,22,0.18)" }}>
                <MapPin size={28} style={{ color: "#fb923c" }} />
              </div>
              <p style={{ fontSize: 15, fontWeight: 600, color: S.text, marginBottom: 8 }}>Пункты не найдены</p>
              <p style={{ fontSize: 13, color: S.muted }}>Попробуйте другой город или используйте геолокацию</p>
            </div>
          )}

          {!loading && pvzList.length > 0 && (
            <>
              <div style={{ padding: "10px 16px", flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "space-between", background: "rgba(255,255,255,0.02)", borderBottom: `1px solid ${S.border}` }}>
                <span style={{ fontSize: 11, fontWeight: 700, color: S.subtle, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                  {pvzList.length} пункт{pvzList.length === 1 ? "" : pvzList.length < 5 ? "а" : "ов"} выдачи
                </span>
                {selectedId && (
                  <button type="button" onClick={() => { const pvz = pvzList.find(p => p.id === selectedId); if (pvz) { onSelect(pvz); onClose(); } }}
                    style={{ fontSize: 12, fontWeight: 700, color: S.accent3, background: "none", border: "none", cursor: "pointer" }}>
                    Подтвердить →
                  </button>
                )}
              </div>
              <div ref={listRef} style={{ flex: 1, overflowY: "auto", scrollbarWidth: "thin", scrollbarColor: `${S.border} transparent` }}>
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
