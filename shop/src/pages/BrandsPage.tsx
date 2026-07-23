import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Search, ArrowRight, X } from "lucide-react";
import { api } from "../api";

function initial(name: string) {
  return name.trim()[0]?.toUpperCase() ?? "?";
}

const PALETTE = [
  "#f5f3ff", "#fdf2f8", "#eff6ff", "#f0fdf4", "#fffbeb",
  "#fff1f2", "#f0f9ff", "#fafaf9", "#f7fee7",
];
function bgForName(name: string) {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return PALETTE[h % PALETTE.length];
}

export default function BrandsPage() {
  const [q, setQ] = useState("");

  const { data: brands = [], isLoading } = useQuery({
    queryKey: ["shop-brands"],
    queryFn: () => api.brands(),
    staleTime: 5 * 60_000,
  });

  const filtered = q.trim()
    ? brands.filter((b) => b.name.toLowerCase().includes(q.toLowerCase()))
    : brands;

  // Group alphabetically
  const grouped: Record<string, typeof brands> = {};
  for (const b of filtered) {
    const letter = b.name[0]?.toUpperCase() ?? "#";
    if (!grouped[letter]) grouped[letter] = [];
    grouped[letter].push(b);
  }
  const letters = Object.keys(grouped).sort();

  return (
    <div className="bg-white min-h-screen">
      {/* Hero */}
      <div className="border-b border-gray-100">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-10">
          <h1 className="text-3xl font-bold text-apple-black tracking-tight mb-1">Бренды</h1>
          <p className="text-sm text-apple-gray mb-6">{brands.length} брендов в наличии</p>

          {/* Search */}
          <div className="relative max-w-sm">
            <Search size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
            <input
              type="text"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Найти бренд..."
              className="w-full pl-10 pr-9 py-3 bg-gray-50 border border-gray-200 rounded-2xl text-sm focus:outline-none focus:border-violet-400 focus:bg-white transition-all"
            />
            {q && (
              <button onClick={() => setQ("")} className="absolute right-3 top-1/2 -translate-y-1/2 text-apple-gray hover:text-apple-black transition-colors">
                <X size={14} />
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="max-w-5xl mx-auto px-4 sm:px-6 py-8">
        {isLoading ? (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
            {Array.from({ length: 24 }).map((_, i) => (
              <div key={i} className="h-20 skeleton rounded-2xl" />
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div className="text-center py-16">
            <p className="text-apple-gray text-sm">Бренды не найдены</p>
          </div>
        ) : q ? (
          // Flat grid when searching
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
            {filtered.map((b) => (
              <BrandCard key={b.name} name={b.name} count={b.count} />
            ))}
          </div>
        ) : (
          // Alphabetical groups
          <div className="space-y-8">
            {letters.map((letter) => (
              <section key={letter}>
                <div className="flex items-center gap-3 mb-3">
                  <span className="text-[13px] font-bold text-apple-gray uppercase tracking-widest">{letter}</span>
                  <div className="flex-1 h-px bg-gray-100" />
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
                  {grouped[letter].map((b) => (
                    <BrandCard key={b.name} name={b.name} count={b.count} />
                  ))}
                </div>
              </section>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function BrandCard({ name, count }: { name: string; count: number }) {
  const bg = bgForName(name);
  return (
    <Link
      to={`/catalog?brand=${encodeURIComponent(name)}`}
      className="group flex items-center gap-3 p-3.5 bg-white rounded-2xl transition-all duration-200 hover:-translate-y-0.5"
      style={{ border: "1px solid #efefef", boxShadow: "0 1px 4px rgba(0,0,0,0.03)" }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLElement).style.borderColor = "#7c3aed";
        (e.currentTarget as HTMLElement).style.boxShadow = "0 6px 20px rgba(124,58,237,0.1)";
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLElement).style.borderColor = "#efefef";
        (e.currentTarget as HTMLElement).style.boxShadow = "0 1px 4px rgba(0,0,0,0.03)";
      }}
    >
      {/* Avatar */}
      <div
        className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 font-bold text-[15px] transition-all duration-200"
        style={{ background: bg, color: "#7c3aed" }}
      >
        {initial(name)}
      </div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="text-[13px] font-semibold text-apple-black truncate leading-tight">{name}</div>
        <div className="text-[11px] text-apple-gray mt-0.5">{count} товаров</div>
      </div>

      <ArrowRight
        size={13}
        className="text-apple-gray opacity-0 group-hover:opacity-100 group-hover:text-violet-600 transition-all duration-200 flex-shrink-0"
      />
    </Link>
  );
}
