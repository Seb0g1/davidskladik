import { useEffect, useRef, useState, useCallback } from "react";
import { X, Search, MapPin, Loader2, ChevronRight, AlertCircle } from "lucide-react";
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

// Build Yandex Maps iframe URL showing Ozon pickup points in a city
function ymapsUrl(city: string) {
  const q = `пункт выдачи ozon ${city}`;
  return `https://yandex.ru/map-widget/v1/?text=${encodeURIComponent(q)}&z=12&l=map&pt=&source=serp_navig`;
}

export default function OzonPickupMap({ open, onClose, onSelect, defaultCity = "" }: Props) {
  const [city, setCity] = useState(defaultCity);
  const [cityInput, setCityInput] = useState(defaultCity);
  const [mapCity, setMapCity] = useState(defaultCity); // the city currently shown in iframe
  const [address, setAddress] = useState("");
  const [addressError, setAddressError] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setCity(defaultCity);
      setCityInput(defaultCity);
      setMapCity(defaultCity);
      setAddress("");
      setAddressError(false);
    }
  }, [open, defaultCity]);

  function handleCitySearch(e: React.FormEvent) {
    e.preventDefault();
    const c = cityInput.trim();
    if (!c) return;
    setCity(c);
    setMapCity(c);
    setAddress("");
  }

  function handleConfirm() {
    if (!address.trim()) {
      setAddressError(true);
      inputRef.current?.focus();
      return;
    }
    const pvz: PvzPoint = {
      id: `manual-${Date.now()}`,
      name: "Ozon ПВЗ",
      address: `${city ? city + ", " : ""}${address.trim()}`,
      lat: 0,
      lng: 0,
      schedule: null,
    };
    onSelect(pvz);
    onClose();
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[200] flex flex-col bg-white">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-100 bg-white shadow-sm flex-shrink-0">
        <button onClick={onClose} className="p-2 rounded-xl hover:bg-gray-100 transition-colors text-apple-gray">
          <X size={18} />
        </button>
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-lg flex items-center justify-center bg-[#f90]">
            <MapPin size={14} className="text-white" />
          </div>
          <span className="font-semibold text-[15px] text-apple-black">Пункты выдачи Ozon</span>
        </div>
      </div>

      {/* City search */}
      <form onSubmit={handleCitySearch} className="flex items-center gap-2 px-4 py-2.5 border-b border-gray-100 bg-gray-50 flex-shrink-0">
        <div className="relative flex-1">
          <Search size={14} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
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
          disabled={!cityInput.trim()}
          className="px-4 py-2.5 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white text-sm font-medium rounded-xl transition-colors"
        >
          Найти
        </button>
      </form>

      {/* Map */}
      <div className="flex-1 min-h-0 relative">
        {mapCity ? (
          <iframe
            key={mapCity}
            src={ymapsUrl(mapCity)}
            className="absolute inset-0 w-full h-full border-0"
            title="Ozon ПВЗ на карте"
            allow="geolocation"
          />
        ) : (
          <div className="absolute inset-0 bg-gray-100 flex flex-col items-center justify-center gap-3 text-center px-6">
            <MapPin size={40} className="text-gray-300" />
            <p className="text-sm text-apple-gray">Введите город для поиска пунктов выдачи</p>
          </div>
        )}
      </div>

      {/* Bottom panel — address input */}
      <div className="flex-shrink-0 border-t border-gray-100 bg-white p-4 space-y-3">
        {mapCity && (
          <div className="flex items-start gap-2 bg-blue-50 border border-blue-100 rounded-xl px-3 py-2.5">
            <AlertCircle size={14} className="text-blue-500 flex-shrink-0 mt-0.5" />
            <p className="text-[12px] text-blue-700 leading-snug">
              Найдите удобный ПВЗ на карте и введите его адрес ниже.
              На карте показаны реальные Ozon ПВЗ в городе.
            </p>
          </div>
        )}
        <div>
          <label className="text-xs font-semibold text-apple-gray uppercase tracking-wider block mb-1.5">
            Адрес пункта выдачи *
          </label>
          <input
            ref={inputRef}
            type="text"
            value={address}
            onChange={e => { setAddress(e.target.value); setAddressError(false); }}
            placeholder="ул. Ленина, д. 5 (как показано на карте)"
            className={clsx(
              "w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 transition-all",
              addressError
                ? "ring-2 ring-red-400 bg-red-50"
                : "focus:ring-violet-400"
            )}
          />
          {addressError && (
            <p className="text-[11px] text-red-500 mt-1">Введите адрес пункта выдачи</p>
          )}
        </div>
        <button
          type="button"
          onClick={handleConfirm}
          disabled={!mapCity}
          className="w-full flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white py-3.5 rounded-2xl font-semibold text-[15px] transition-all"
        >
          <ChevronRight size={16} />
          Выбрать этот пункт
        </button>
      </div>
    </div>
  );
}
