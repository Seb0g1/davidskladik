import { useEffect, useRef, useState, useCallback } from "react";
import { X, Search, MapPin, Loader2, Clock, ChevronRight } from "lucide-react";
import clsx from "clsx";

export interface PvzPoint {
  id: string;
  name: string;
  address: string;
  lat: number;
  lng: number;
  schedule: string | null;
}

interface Props {
  open: boolean;
  onClose: () => void;
  onSelect: (pvz: PvzPoint) => void;
  defaultCity?: string;
}

const OZON_ORANGE = "#f90";
const OZON_SELECTED = "#005bff";

function markerSvg(selected: boolean) {
  const bg = selected ? OZON_SELECTED : OZON_ORANGE;
  return `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="38" viewBox="0 0 32 38">
    <path d="M16 0C7.163 0 0 7.163 0 16c0 10 16 22 16 22s16-12 16-22C32 7.163 24.837 0 16 0z" fill="${bg}" stroke="white" stroke-width="2"/>
    <circle cx="16" cy="16" r="6" fill="white"/>
  </svg>`;
}

export default function OzonPickupMap({ open, onClose, onSelect, defaultCity = "" }: Props) {
  const mapRef = useRef<HTMLDivElement>(null);
  const leafletMap = useRef<any>(null);
  const markersLayer = useRef<any>(null);
  const leafletLoaded = useRef(false);

  const [city, setCity] = useState(defaultCity);
  const [cityInput, setCityInput] = useState(defaultCity);
  const [pvzList, setPvzList] = useState<PvzPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<PvzPoint | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notConfigured, setNotConfigured] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  // Load Leaflet CSS dynamically
  useEffect(() => {
    if (document.querySelector("#leaflet-css")) return;
    const link = document.createElement("link");
    link.id = "leaflet-css";
    link.rel = "stylesheet";
    link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
    document.head.appendChild(link);
  }, []);

  // Initialize map when modal opens
  useEffect(() => {
    if (!open || !mapRef.current) return;
    if (leafletMap.current) return;

    async function initMap() {
      const L = (await import("leaflet")).default;
      leafletLoaded.current = true;

      const map = L.map(mapRef.current!, {
        center: [55.75, 37.62],
        zoom: 11,
        zoomControl: true,
      });

      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "© OpenStreetMap",
        maxZoom: 19,
      }).addTo(map);

      markersLayer.current = L.layerGroup().addTo(map);
      leafletMap.current = map;
      setMapReady(true);
    }

    initMap();

    return () => {
      if (leafletMap.current) {
        leafletMap.current.remove();
        leafletMap.current = null;
        markersLayer.current = null;
        setMapReady(false);
      }
    };
  }, [open]);

  // Re-render markers when list or selection changes
  useEffect(() => {
    if (!mapReady || !leafletMap.current || !markersLayer.current) return;

    import("leaflet").then(({ default: L }) => {
      markersLayer.current.clearLayers();

      pvzList.forEach(pvz => {
        const isSelected = selected?.id === pvz.id;
        const icon = L.divIcon({
          className: "",
          html: markerSvg(isSelected),
          iconSize: [32, 38],
          iconAnchor: [16, 38],
          popupAnchor: [0, -38],
        });

        const marker = L.marker([pvz.lat, pvz.lng], { icon })
          .addTo(markersLayer.current)
          .on("click", () => handleSelect(pvz));

        marker.bindTooltip(pvz.address, {
          direction: "top",
          offset: [0, -36],
          className: "pvz-tooltip",
        });
      });

      if (pvzList.length > 0 && !selected) {
        const group = L.featureGroup(markersLayer.current.getLayers());
        leafletMap.current.fitBounds(group.getBounds().pad(0.2));
      } else if (selected) {
        leafletMap.current.setView([selected.lat, selected.lng], 15, { animate: true });
      }
    });
  }, [pvzList, selected, mapReady]);

  const fetchPvz = useCallback(async (cityName: string) => {
    if (!cityName.trim()) return;
    setLoading(true);
    setError(null);
    setPvzList([]);
    setSelected(null);
    setCity(cityName.trim());
    try {
      const res = await fetch(`/api/shop/delivery/pvz?city=${encodeURIComponent(cityName.trim())}`);
      const data = await res.json();
      if (data.configured === false) {
        setNotConfigured(true);
      } else {
        setNotConfigured(false);
        setPvzList(data.pvz || []);
        if ((data.pvz || []).length === 0) {
          setError(`Пункты Ozon не найдены в городе «${cityName.trim()}». Попробуйте другой город.`);
        }
      }
    } catch {
      setError("Ошибка загрузки. Проверьте подключение.");
    } finally {
      setLoading(false);
    }
  }, []);

  // Auto-load if defaultCity provided
  useEffect(() => {
    if (open && defaultCity && mapReady) {
      fetchPvz(defaultCity);
    }
  }, [open, defaultCity, mapReady, fetchPvz]);

  function handleSelect(pvz: PvzPoint) {
    setSelected(pvz);
  }

  function handleConfirm() {
    if (selected) {
      onSelect(selected);
      onClose();
    }
  }

  function handleSearchSubmit(e: React.FormEvent) {
    e.preventDefault();
    fetchPvz(cityInput);
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[200] flex flex-col bg-white animate-fade-in">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-100 bg-white shadow-sm">
        <button
          onClick={onClose}
          className="p-2 rounded-xl hover:bg-gray-100 transition-colors text-apple-gray"
        >
          <X size={18} />
        </button>
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-lg flex items-center justify-center" style={{ background: OZON_ORANGE }}>
            <MapPin size={14} className="text-white" />
          </div>
          <span className="font-semibold text-[15px] text-apple-black">Пункты выдачи Ozon</span>
        </div>
      </div>

      {/* City search */}
      <form onSubmit={handleSearchSubmit} className="flex items-center gap-2 px-4 py-3 border-b border-gray-100 bg-gray-50">
        <div className="relative flex-1">
          <Search size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
          <input
            type="text"
            value={cityInput}
            onChange={e => setCityInput(e.target.value)}
            placeholder="Введите город..."
            className="w-full pl-10 pr-4 py-2.5 bg-white border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 transition-all"
            autoFocus={!defaultCity}
          />
        </div>
        <button
          type="submit"
          disabled={!cityInput.trim() || loading}
          className="px-4 py-2.5 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white text-sm font-medium rounded-xl transition-colors"
        >
          {loading ? <Loader2 size={14} className="animate-spin" /> : "Найти"}
        </button>
      </form>

      {/* Content */}
      <div className="flex flex-1 min-h-0">
        {/* Left panel — PVZ list */}
        <div className="w-full md:w-80 flex-shrink-0 flex flex-col border-r border-gray-100 overflow-hidden">
          {notConfigured ? (
            <div className="flex-1 flex flex-col items-center justify-center p-6 text-center gap-3">
              <div className="w-14 h-14 rounded-2xl bg-orange-50 flex items-center justify-center">
                <MapPin size={24} className="text-orange-400" />
              </div>
              <p className="text-sm font-semibold text-apple-black">Сервис не настроен</p>
              <p className="text-xs text-apple-gray leading-relaxed">
                Для отображения ПВЗ нужен API-ключ 2GIS.<br />
                Зарегистрируйтесь на <span className="font-medium text-violet-600">dev.2gis.ru</span> и добавьте<br />
                <code className="bg-gray-100 px-1 rounded text-[11px]">DGIS_API_KEY=ваш_ключ</code><br />
                в файл .env на сервере.
              </p>
            </div>
          ) : loading ? (
            <div className="flex-1 flex flex-col gap-3 p-4">
              {[...Array(5)].map((_, i) => (
                <div key={i} className="h-16 rounded-xl bg-gray-100 animate-pulse" />
              ))}
            </div>
          ) : error ? (
            <div className="flex-1 flex items-center justify-center p-6 text-center">
              <p className="text-sm text-apple-gray">{error}</p>
            </div>
          ) : pvzList.length === 0 ? (
            <div className="flex-1 flex flex-col items-center justify-center p-6 text-center gap-3">
              <MapPin size={32} className="text-gray-300" />
              <p className="text-sm text-apple-gray">Введите город для поиска ПВЗ</p>
            </div>
          ) : (
            <div className="flex-1 overflow-y-auto divide-y divide-gray-50">
              {pvzList.map(pvz => {
                const isSelected = selected?.id === pvz.id;
                return (
                  <button
                    key={pvz.id}
                    type="button"
                    onClick={() => handleSelect(pvz)}
                    className={clsx(
                      "w-full text-left px-4 py-3.5 flex items-start gap-3 transition-colors",
                      isSelected ? "bg-orange-50" : "hover:bg-gray-50"
                    )}
                  >
                    <div
                      className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5 transition-colors"
                      style={{ background: isSelected ? OZON_SELECTED : OZON_ORANGE }}
                    >
                      <MapPin size={14} className="text-white" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className={clsx(
                        "text-[13px] font-medium leading-snug truncate",
                        isSelected ? "text-blue-700" : "text-apple-black"
                      )}>
                        {pvz.address}
                      </p>
                      {pvz.schedule && (
                        <p className="text-[11px] text-apple-gray flex items-center gap-1 mt-0.5">
                          <Clock size={10} />
                          {pvz.schedule}
                        </p>
                      )}
                    </div>
                    {isSelected && <ChevronRight size={14} className="text-blue-500 flex-shrink-0 mt-1" />}
                  </button>
                );
              })}
            </div>
          )}

          {/* Confirm button */}
          {selected && (
            <div className="p-3 border-t border-gray-100 bg-white">
              <button
                type="button"
                onClick={handleConfirm}
                className="w-full py-3 bg-violet-600 hover:bg-violet-700 text-white text-sm font-semibold rounded-xl transition-colors"
              >
                Выбрать этот пункт
              </button>
              <p className="text-[11px] text-apple-gray text-center mt-1.5 px-2 leading-tight truncate">
                {selected.address}
              </p>
            </div>
          )}
        </div>

        {/* Map */}
        <div className="flex-1 relative hidden md:block">
          <div ref={mapRef} className="absolute inset-0" />
          {!mapReady && (
            <div className="absolute inset-0 bg-gray-100 flex items-center justify-center">
              <Loader2 size={24} className="text-gray-400 animate-spin" />
            </div>
          )}
        </div>
      </div>

      {/* Mobile: confirm bottom bar */}
      <div className="md:hidden">
        {selected && (
          <div className="border-t border-gray-100 p-3 bg-white">
            <button
              type="button"
              onClick={handleConfirm}
              className="w-full py-3.5 bg-violet-600 hover:bg-violet-700 text-white text-sm font-semibold rounded-xl transition-colors"
            >
              Выбрать · {selected.address.split(",")[0]}
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
