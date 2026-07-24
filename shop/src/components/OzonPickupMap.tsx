import { useState, useEffect } from "react";
import { X, Search, MapPin, Loader2, ChevronRight, Clock } from "lucide-react";
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

const API_BASE = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop";

export default function OzonPickupMap({ open, onClose, onSelect, defaultCity = "" }: Props) {
  const [cityInput, setCityInput] = useState(defaultCity);
  const [pvzList, setPvzList] = useState<PvzPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [manualAddress, setManualAddress] = useState("");
  const [showManual, setShowManual] = useState(false);

  useEffect(() => {
    if (open) {
      setCityInput(defaultCity);
      setPvzList([]);
      setSearched(false);
      setManualAddress("");
      setShowManual(false);
      if (defaultCity) search(defaultCity);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, defaultCity]);

  async function search(city: string) {
    if (!city.trim()) return;
    setLoading(true);
    setSearched(false);
    setPvzList([]);
    setShowManual(false);
    try {
      const res = await fetch(`${API_BASE}/delivery/pvz?city=${encodeURIComponent(city.trim())}`);
      const data = await res.json();
      const list: PvzPoint[] = Array.isArray(data.pvz) ? data.pvz : [];
      setPvzList(list);
      setSearched(true);
      if (!list.length) setShowManual(true);
    } catch {
      setSearched(true);
      setShowManual(true);
    } finally {
      setLoading(false);
    }
  }

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    search(cityInput);
  }

  function handleSelectPvz(pvz: PvzPoint) {
    onSelect(pvz);
    onClose();
  }

  function handleManualConfirm() {
    if (!manualAddress.trim()) return;
    onSelect({
      id: `manual-${Date.now()}`,
      name: "Ozon ПВЗ",
      address: [cityInput.trim(), manualAddress.trim()].filter(Boolean).join(", "),
      lat: 0, lng: 0, schedule: null,
    });
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
      <form onSubmit={handleSearch} className="flex items-center gap-2 px-4 py-3 border-b border-gray-100 bg-gray-50 flex-shrink-0">
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
          disabled={!cityInput.trim() || loading}
          className="px-4 py-2.5 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white text-sm font-medium rounded-xl transition-colors flex items-center gap-1.5"
        >
          {loading ? <Loader2 size={14} className="animate-spin" /> : <Search size={14} />}
          Найти
        </button>
      </form>

      {/* Results */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        {loading && (
          <div className="flex items-center justify-center py-16 gap-2 text-apple-gray text-sm">
            <Loader2 size={18} className="animate-spin" />
            Ищем пункты выдачи…
          </div>
        )}

        {!loading && !searched && (
          <div className="flex flex-col items-center justify-center py-16 px-6 text-center">
            <MapPin size={40} className="text-gray-200 mb-3" />
            <p className="text-sm text-apple-gray">Введите город для поиска пунктов выдачи Ozon</p>
          </div>
        )}

        {!loading && searched && pvzList.length > 0 && (
          <div className="divide-y divide-gray-50">
            <p className="px-4 py-2.5 text-[11px] font-semibold text-apple-gray uppercase tracking-wider bg-gray-50">
              {pvzList.length} пункт{pvzList.length === 1 ? "" : pvzList.length < 5 ? "а" : "ов"} в {cityInput}
            </p>
            {pvzList.map(pvz => (
              <button
                key={pvz.id}
                type="button"
                onClick={() => handleSelectPvz(pvz)}
                className="w-full text-left px-4 py-3.5 hover:bg-violet-50 transition-colors flex items-start gap-3 group"
              >
                <div className="w-8 h-8 rounded-lg bg-orange-100 flex items-center justify-center flex-shrink-0 mt-0.5 group-hover:bg-orange-200 transition-colors">
                  <MapPin size={15} className="text-orange-500" />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-[13px] font-semibold text-apple-black leading-tight mb-0.5">{pvz.name}</p>
                  <p className="text-[12px] text-apple-gray leading-snug">{pvz.address}</p>
                  {pvz.schedule && (
                    <div className="flex items-center gap-1 mt-1">
                      <Clock size={10} className="text-emerald-500 flex-shrink-0" />
                      <span className="text-[11px] text-emerald-600">{pvz.schedule}</span>
                    </div>
                  )}
                </div>
                <ChevronRight size={15} className="text-gray-300 group-hover:text-violet-400 transition-colors flex-shrink-0 mt-1" />
              </button>
            ))}
          </div>
        )}

        {!loading && searched && pvzList.length === 0 && (
          <div className="px-4 py-8 text-center">
            <MapPin size={36} className="text-gray-200 mx-auto mb-3" />
            <p className="text-sm font-semibold text-apple-black mb-1">Пункты не найдены автоматически</p>
            <p className="text-[12px] text-apple-gray mb-5 leading-relaxed">
              Найдите ПВЗ на{" "}
              <a
                href={`https://www.ozon.ru/my/pointlist?city=${encodeURIComponent(cityInput)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-violet-600 underline"
              >
                сайте Ozon
              </a>{" "}
              и введите адрес вручную ниже.
            </p>
          </div>
        )}
      </div>

      {/* Manual input — shown as fallback when no results */}
      {showManual && !loading && searched && (
        <div className="flex-shrink-0 border-t border-gray-100 bg-white p-4 space-y-3">
          <label className="text-xs font-semibold text-apple-gray uppercase tracking-wider block">
            Адрес пункта выдачи *
          </label>
          <input
            type="text"
            value={manualAddress}
            onChange={e => setManualAddress(e.target.value)}
            placeholder="ул. Ленина, д. 5"
            className="w-full px-4 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
          />
          <button
            type="button"
            onClick={handleManualConfirm}
            disabled={!manualAddress.trim()}
            className="w-full flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 disabled:opacity-40 text-white py-3.5 rounded-2xl font-semibold text-[15px] transition-all"
          >
            <ChevronRight size={16} />
            Выбрать этот адрес
          </button>
        </div>
      )}
    </div>
  );
}
