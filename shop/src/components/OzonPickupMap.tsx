import { useState, useEffect, useRef } from "react";
import L from "leaflet";
import { X, Search, MapPin, Loader2, ChevronRight, Clock, Navigation } from "lucide-react";
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

const mkOrangeIcon = () =>
  L.divIcon({
    className: "",
    html: `<div style="width:24px;height:24px;background:#f90;border-radius:50% 50% 50% 0;transform:rotate(-45deg);border:2px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.35)"></div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 24],
    popupAnchor: [0, -28],
  });

const mkSelectedIcon = () =>
  L.divIcon({
    className: "",
    html: `<div style="width:30px;height:30px;background:#7c3aed;border-radius:50% 50% 50% 0;transform:rotate(-45deg);border:2px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.4)"></div>`,
    iconSize: [30, 30],
    iconAnchor: [15, 30],
    popupAnchor: [0, -34],
  });

const mkUserIcon = () =>
  L.divIcon({
    className: "",
    html: `<div style="width:16px;height:16px;background:#3b82f6;border-radius:50%;border:3px solid #fff;box-shadow:0 0 0 3px rgba(59,130,246,.3)"></div>`,
    iconSize: [16, 16],
    iconAnchor: [8, 8],
  });

export default function OzonPickupMap({ open, onClose, onSelect, defaultCity = "" }: Props) {
  const [cityInput, setCityInput] = useState(defaultCity);
  const [pvzList, setPvzList] = useState<PvzPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [geoLoading, setGeoLoading] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const mapDivRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const markersRef = useRef<Map<string, L.Marker>>(new Map());
  const userMarkerRef = useRef<L.Marker | null>(null);
  const listRef = useRef<HTMLDivElement>(null);
  // keep latest pvzList/callbacks available inside global popup handler
  const pvzListRef = useRef<PvzPoint[]>([]);
  pvzListRef.current = pvzList;
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open) return;

    // Reset UI state
    setCityInput(defaultCity);
    setPvzList([]);
    setSearched(false);
    setSelectedId(null);
    markersRef.current.clear();
    userMarkerRef.current = null;

    // Init Leaflet map on the already-rendered div
    const container = mapDivRef.current;
    if (container && !mapRef.current) {
      const map = L.map(container, { center: [55.75, 37.62], zoom: 11, zoomControl: true });
      L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
        attribution: "© CartoDB",
        maxZoom: 19,
        subdomains: "abcd",
      }).addTo(map);
      mapRef.current = map;
    } else if (mapRef.current) {
      mapRef.current.invalidateSize();
    }

    // Автозагрузка: сначала геолокация, при отказе — дефолтный город или Москва
    if (navigator.geolocation) {
      setGeoLoading(true);
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          setGeoLoading(false);
          void doLoadByCoords(pos.coords.latitude, pos.coords.longitude);
        },
        () => {
          setGeoLoading(false);
          void doLoadByCity(defaultCity || "Москва");
        },
        { timeout: 5000, maximumAge: 60000 }
      );
    } else {
      void doLoadByCity(defaultCity || "Москва");
    }

    // Expose popup button handler globally
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (window as any)["__pvzPick"] = (id: string) => {
      const pvz = pvzListRef.current.find((p) => p.id === id);
      if (pvz) { onSelectRef.current(pvz); onCloseRef.current(); }
    };

    return () => {
      // Destroy map on close so next open starts clean
      if (mapRef.current) {
        mapRef.current.remove();
        mapRef.current = null;
      }
      markersRef.current.clear();
      userMarkerRef.current = null;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (window as any)["__pvzPick"];
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  function placeMarkers(points: PvzPoint[]) {
    const map = mapRef.current;
    if (!map) return;
    markersRef.current.forEach((m) => m.remove());
    markersRef.current.clear();

    const orangeIcon = mkOrangeIcon();
    points.forEach((pvz) => {
      if (!pvz.lat || !pvz.lng) return;
      const marker = L.marker([pvz.lat, pvz.lng], { icon: orangeIcon }).addTo(map);
      marker.bindPopup(buildPopup(pvz), { maxWidth: 240 });
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

  function buildPopup(pvz: PvzPoint) {
    const sched = pvz.schedule ? `<div style="color:#059669;font-size:11px;margin-top:3px">🕐 Сегодня ${pvz.schedule}</div>` : "";
    const badge = pvz.type && pvz.type !== "ПВЗ" ? ` <span style="background:#fff3cd;color:#92400e;font-size:10px;padding:1px 5px;border-radius:4px">${pvz.type}</span>` : "";
    return `<div style="font-family:system-ui,sans-serif;min-width:190px">
      <div style="font-weight:700;font-size:13px;line-height:1.3">${pvz.name}${badge}</div>
      <div style="color:#555;font-size:11px;margin-top:3px;line-height:1.4">${pvz.address}</div>
      ${sched}
      <button onclick="window.__pvzPick('${pvz.id}')" style="margin-top:8px;width:100%;background:#7c3aed;color:#fff;border:none;border-radius:8px;padding:7px;font-size:12px;font-weight:600;cursor:pointer">Выбрать</button>
    </div>`;
  }

  function highlightPvz(pvz: PvzPoint, panMap = true) {
    setSelectedId(pvz.id);
    const selectedIcon = mkSelectedIcon();
    const orangeIcon = mkOrangeIcon();
    markersRef.current.forEach((marker, id) => {
      marker.setIcon(id === pvz.id ? selectedIcon : orangeIcon);
    });
    if (panMap && mapRef.current && pvz.lat && pvz.lng) {
      mapRef.current.setView([pvz.lat, pvz.lng], 15, { animate: true });
      markersRef.current.get(pvz.id)?.openPopup();
    }
    const el = listRef.current?.querySelector(`[data-id="${pvz.id}"]`);
    el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  async function doLoadByCity(city: string) {
    if (!city.trim()) return;
    setLoading(true);
    setSearched(false);
    setPvzList([]);
    setSelectedId(null);
    try {
      const res = await fetch(`${API_BASE}/delivery/pvz?city=${encodeURIComponent(city.trim())}`);
      const data: { pvz?: PvzPoint[]; city?: string } = await res.json();
      const list = Array.isArray(data.pvz) ? data.pvz : [];
      setPvzList(list);
      pvzListRef.current = list;
      setSearched(true);
      placeMarkers(list);
    } catch {
      setSearched(true);
    } finally {
      setLoading(false);
    }
  }

  async function doLoadByCoords(lat: number, lng: number) {
    setLoading(true);
    setSearched(false);
    setPvzList([]);
    setSelectedId(null);
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
      setPvzList(list);
      pvzListRef.current = list;
      setSearched(true);
      placeMarkers(list);
    } catch {
      setSearched(true);
    } finally {
      setLoading(false);
    }
  }

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    void doLoadByCity(cityInput);
  }

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

  return (
    <div className="fixed inset-0 z-[200] flex flex-col bg-white">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-100 bg-white shadow-sm flex-shrink-0">
        <button onClick={onClose} className="p-2 rounded-xl hover:bg-gray-100 transition-colors text-gray-500">
          <X size={18} />
        </button>
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-lg flex items-center justify-center" style={{ background: "#f90" }}>
            <MapPin size={14} className="text-white" />
          </div>
          <span className="font-semibold text-[15px] text-gray-900">Пункты выдачи Ozon</span>
        </div>
      </div>

      {/* Search + geo */}
      <form onSubmit={handleSearch} className="flex items-center gap-2 px-4 py-3 border-b border-gray-100 bg-gray-50 flex-shrink-0">
        <div className="relative flex-1">
          <Search size={14} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
          <input
            type="text"
            value={cityInput}
            onChange={(e) => setCityInput(e.target.value)}
            placeholder="Введите город..."
            className="w-full pl-10 pr-4 py-2.5 bg-white border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 transition-all"
            autoFocus={!defaultCity}
          />
        </div>
        <button
          type="submit"
          disabled={!cityInput.trim() || loading}
          className="px-4 py-2.5 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white text-sm font-medium rounded-xl transition-colors flex items-center gap-1.5"
        >
          {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
          Найти
        </button>
        <button
          type="button"
          onClick={handleGeo}
          disabled={geoLoading || loading}
          title="Моё местоположение"
          className="p-2.5 bg-white border border-gray-200 rounded-xl hover:bg-blue-50 hover:border-blue-300 disabled:opacity-40 transition-colors"
        >
          {geoLoading ? <Loader2 size={16} className="animate-spin text-blue-500" /> : <Navigation size={16} className="text-blue-500" />}
        </button>
      </form>

      {/* Leaflet map — always rendered while open */}
      <div
        ref={mapDivRef}
        className="flex-shrink-0 border-b border-gray-100"
        style={{ height: "45vh", minHeight: 200 }}
      />

      {/* PVZ list */}
      <div ref={listRef} className="flex-1 min-h-0 overflow-y-auto">
        {loading && (
          <div className="flex items-center justify-center py-10 gap-2 text-gray-500 text-sm">
            <Loader2 size={18} className="animate-spin" /> Ищем пункты выдачи…
          </div>
        )}

        {!loading && !searched && (
          <div className="flex flex-col items-center justify-center py-12 px-6 text-center">
            <Navigation size={36} className="text-gray-200 mb-3" />
            <p className="text-sm text-gray-400">Введите город или нажмите кнопку геолокации</p>
          </div>
        )}

        {!loading && searched && pvzList.length > 0 && (
          <div className="divide-y divide-gray-50">
            <p className="px-4 py-2 text-[11px] font-semibold text-gray-400 uppercase tracking-wider bg-gray-50">
              {pvzList.length} пункт{pvzList.length === 1 ? "" : pvzList.length < 5 ? "а" : "ов"} найдено
            </p>
            {pvzList.map((pvz) => (
              <button
                key={pvz.id}
                data-id={pvz.id}
                type="button"
                onClick={() => highlightPvz(pvz)}
                className={clsx(
                  "w-full text-left px-4 py-3 transition-colors flex items-start gap-3 group",
                  selectedId === pvz.id ? "bg-violet-50" : "hover:bg-violet-50"
                )}
              >
                <div className={clsx(
                  "w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5 transition-colors",
                  selectedId === pvz.id ? "bg-violet-100" : "bg-orange-100 group-hover:bg-orange-200"
                )}>
                  <MapPin size={15} className={selectedId === pvz.id ? "text-violet-600" : "text-orange-500"} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1.5 mb-0.5 flex-wrap">
                    <p className="text-[13px] font-semibold text-gray-900 leading-tight">{pvz.name}</p>
                    {pvz.type && pvz.type !== "ПВЗ" && (
                      <span className="text-[10px] bg-orange-100 text-orange-600 px-1.5 py-0.5 rounded-md font-medium">{pvz.type}</span>
                    )}
                  </div>
                  <p className="text-[12px] text-gray-400 leading-snug">{pvz.address}</p>
                  {pvz.schedule && (
                    <div className="flex items-center gap-1 mt-1">
                      <Clock size={10} className="text-emerald-500 flex-shrink-0" />
                      <span className="text-[11px] text-emerald-600">Сегодня {pvz.schedule}</span>
                    </div>
                  )}
                </div>
                {selectedId === pvz.id ? (
                  <button
                    type="button"
                    onClick={(e) => { e.stopPropagation(); onSelect(pvz); onClose(); }}
                    className="flex-shrink-0 px-3 py-1.5 bg-violet-600 text-white text-[12px] font-semibold rounded-lg hover:bg-violet-700 transition-colors"
                  >
                    Выбрать
                  </button>
                ) : (
                  <ChevronRight size={15} className="text-gray-300 group-hover:text-violet-400 transition-colors flex-shrink-0 mt-1" />
                )}
              </button>
            ))}
          </div>
        )}

        {!loading && searched && pvzList.length === 0 && (
          <div className="flex flex-col items-center justify-center py-12 px-6 text-center">
            <MapPin size={36} className="text-gray-200 mb-3" />
            <p className="text-[13px] font-semibold text-gray-900 mb-1">Пункты не найдены</p>
            <p className="text-[12px] text-gray-400">Попробуйте другой город или используйте геолокацию</p>
          </div>
        )}
      </div>
    </div>
  );
}
