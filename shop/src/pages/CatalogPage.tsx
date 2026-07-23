import { useState, useEffect, useRef } from "react";
import { useSearchParams, useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { SlidersHorizontal, X, ChevronDown, ChevronUp } from "lucide-react";
import { api } from "../api";
import ProductCard from "../components/ProductCard";
import clsx from "clsx";

const CAT_ICONS: Record<string, string> = {
  parfum: "🌹", edp: "🫧", edt: "💧", edc: "🌿",
  deo: "✨", home: "🕯️", sets: "🎁", body: "🌸",
};

const SORT_OPTIONS = [
  { value: "name",       label: "По названию" },
  { value: "price_asc",  label: "Дешевле" },
  { value: "price_desc", label: "Дороже" },
];

const PAGE_SIZE = 24;

function Skeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden" style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04)" }}>
      <div className="aspect-square skeleton" />
      <div className="p-3.5 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3.5 skeleton rounded" />
        <div className="h-4 skeleton rounded w-1/2 mt-1" />
      </div>
    </div>
  );
}

export default function CatalogPage() {
  const { category: urlCategory } = useParams<{ category?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const category = urlCategory || (searchParams.get("category") ?? "");
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [brandsExpanded, setBrandsExpanded] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);

  const q      = searchParams.get("q") ?? "";
  const brand  = searchParams.get("brand") ?? "";
  const sort   = (searchParams.get("sort") ?? "name") as "name" | "price_asc" | "price_desc";
  const page   = Number(searchParams.get("page") ?? 1);
  const inStock = searchParams.get("inStock") === "true";

  const { data, isLoading } = useQuery({
    queryKey: ["shop-catalog", { category, q, brand, sort, page, inStock }],
    queryFn: () => api.catalog({ category, q, brand, sort, page, pageSize: PAGE_SIZE, inStock }),
  });

  const { data: autoCategories } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  useEffect(() => { if (filtersOpen) setFiltersOpen(false); }, [location.pathname]);

  function setParam(key: string, value: string | null) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (!value) next.delete(key); else next.set(key, value);
      if (key !== "page") next.delete("page");
      return next;
    });
  }

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const pageTitle = category
    ? category.charAt(0).toUpperCase() + category.slice(1)
    : q ? `«${q}»` : "Весь каталог";

  const activeFilters = [brand && `Бренд: ${brand}`, inStock && "В наличии"].filter(Boolean);
  const visibleBrands = brandsExpanded ? (data?.brands ?? []) : (data?.brands ?? []).slice(0, 10);

  const Sidebar = (
    <div className="space-y-6">
      {/* Search */}
      <div>
        <label className="text-[12px] font-semibold text-apple-gray uppercase tracking-wider block mb-2">Поиск</label>
        <div className="relative">
          <input
            type="text"
            value={q}
            onChange={(e) => setParam("q", e.target.value)}
            placeholder="Название, бренд..."
            className="w-full px-3.5 py-2.5 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all pr-8"
          />
          {q && <button onClick={() => setParam("q", null)} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-apple-gray hover:text-apple-black"><X size={14} /></button>}
        </div>
      </div>

      {/* Sort */}
      <div>
        <label className="text-[12px] font-semibold text-apple-gray uppercase tracking-wider block mb-2">Сортировка</label>
        <div className="space-y-1">
          {SORT_OPTIONS.map((o) => (
            <button
              key={o.value}
              onClick={() => setParam("sort", o.value)}
              className={clsx(
                "w-full text-left px-3 py-2.5 rounded-xl text-sm font-medium transition-colors",
                sort === o.value ? "bg-violet-600 text-white" : "text-apple-black hover:bg-apple-gray-bg"
              )}
            >
              {o.label}
            </button>
          ))}
        </div>
      </div>

      {/* In stock */}
      <div>
        <label className="text-[12px] font-semibold text-apple-gray uppercase tracking-wider block mb-2">Наличие</label>
        <label className="flex items-center gap-2.5 cursor-pointer py-2">
          <input
            type="checkbox"
            checked={inStock}
            onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)}
            className="w-4 h-4 accent-violet-600 rounded"
          />
          <span className="text-sm text-apple-black font-medium">Только в наличии</span>
        </label>
      </div>

      {/* Brands */}
      {(data?.brands?.length ?? 0) > 0 && (
        <div>
          <label className="text-[12px] font-semibold text-apple-gray uppercase tracking-wider block mb-2">Бренды</label>
          <div className="space-y-0.5 max-h-64 overflow-y-auto pr-1">
            <label className="flex items-center gap-2 cursor-pointer py-1.5 px-1 rounded-lg hover:bg-apple-gray-bg transition-colors">
              <input type="radio" name="brand" checked={!brand} onChange={() => setParam("brand", null)} className="accent-violet-600" />
              <span className="text-sm text-apple-black">Все бренды</span>
            </label>
            {visibleBrands.map((b) => (
              <label key={b} className="flex items-center gap-2 cursor-pointer py-1.5 px-1 rounded-lg hover:bg-apple-gray-bg transition-colors">
                <input type="radio" name="brand" checked={brand === b} onChange={() => setParam("brand", b)} className="accent-violet-600" />
                <span className="text-sm text-apple-black truncate">{b}</span>
              </label>
            ))}
          </div>
          {(data?.brands?.length ?? 0) > 10 && (
            <button onClick={() => setBrandsExpanded(!brandsExpanded)} className="flex items-center gap-1 text-xs text-violet-600 hover:text-violet-800 mt-2 font-medium">
              {brandsExpanded ? <><ChevronUp size={13} />Скрыть</> : <><ChevronDown size={13} />Ещё {(data?.brands?.length ?? 0) - 10} брендов</>}
            </button>
          )}
        </div>
      )}

      {/* Reset */}
      {(brand || q || inStock) && (
        <button onClick={() => setSearchParams({})} className="w-full text-sm text-red-500 hover:text-red-700 font-medium py-2 rounded-xl hover:bg-red-50 transition-colors">
          Сбросить фильтры
        </button>
      )}
    </div>
  );

  return (
    <div className="bg-apple-gray-bg min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
        {/* Header */}
        <div className="flex items-center justify-between mb-6 gap-4 flex-wrap">
          <div>
            <h1 className="text-2xl font-bold text-apple-black tracking-tight">{pageTitle}</h1>
            {data && <p className="text-sm text-apple-gray mt-0.5">{data.total.toLocaleString("ru-RU")} товаров</p>}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setFiltersOpen(true)}
              className="lg:hidden flex items-center gap-2 px-4 py-2.5 bg-white rounded-xl text-sm font-medium border border-gray-200 hover:border-violet-300 transition-colors"
            >
              <SlidersHorizontal size={15} /> Фильтры
              {activeFilters.length > 0 && (
                <span className="w-5 h-5 bg-violet-600 text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                  {activeFilters.length}
                </span>
              )}
            </button>
          </div>
        </div>

        {/* Category tabs */}
        {autoCategories && autoCategories.length > 0 && (
          <div className="flex gap-2 overflow-x-auto pb-2 mb-4" style={{ scrollbarWidth: "none" }}>
            <Link
              to="/catalog"
              className={clsx(
                "flex-shrink-0 inline-flex items-center gap-1.5 px-4 py-2 rounded-xl text-sm font-medium transition-colors whitespace-nowrap",
                !category ? "bg-violet-600 text-white" : "bg-white border border-gray-200 text-apple-black hover:border-violet-300"
              )}
            >
              Все
            </Link>
            {autoCategories.map((cat) => (
              <Link
                key={cat.slug}
                to={`/catalog?category=${cat.slug}`}
                className={clsx(
                  "flex-shrink-0 inline-flex items-center gap-1.5 px-4 py-2 rounded-xl text-sm font-medium transition-colors whitespace-nowrap",
                  category === cat.slug ? "bg-violet-600 text-white" : "bg-white border border-gray-200 text-apple-black hover:border-violet-300"
                )}
              >
                <span>{CAT_ICONS[cat.slug] || "🌸"}</span>
                {cat.label}
                <span className={clsx("text-[11px]", category === cat.slug ? "text-violet-200" : "text-apple-gray")}>
                  {cat.count}
                </span>
              </Link>
            ))}
          </div>
        )}

        {/* Active filter chips */}
        {activeFilters.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-4">
            {activeFilters.map((f, idx) => (
              <span key={idx} className="inline-flex items-center gap-1.5 bg-violet-100 text-violet-700 text-xs font-medium px-3 py-1.5 rounded-full">
                {f}
                <button onClick={() => {
                  if (typeof f === "string" && f.startsWith("Бренд:")) setParam("brand", null);
                  if (f === "В наличии") setParam("inStock", null);
                }}><X size={11} /></button>
              </span>
            ))}
          </div>
        )}

        <div className="flex gap-6">
          {/* Desktop sidebar */}
          <aside className="hidden lg:block w-56 flex-shrink-0">
            <div className="bg-white rounded-2xl p-5 sticky top-20" style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04)" }}>
              {Sidebar}
            </div>
          </aside>

          {/* Grid */}
          <div className="flex-1 min-w-0">
            {isLoading ? (
              <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-4 gap-3">
                {Array.from({ length: PAGE_SIZE }).map((_, i) => <Skeleton key={i} />)}
              </div>
            ) : data?.products.length ? (
              <>
                <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-4 gap-3">
                  {data.products.map((p) => <ProductCard key={p.offerId} product={p} />)}
                </div>

                {totalPages > 1 && (
                  <div className="flex justify-center items-center gap-2 mt-10">
                    {page > 1 && (
                      <button onClick={() => setParam("page", String(page - 1))} className="w-10 h-10 flex items-center justify-center rounded-xl bg-white border border-gray-200 hover:border-violet-400 text-sm font-medium transition-colors">
                        ‹
                      </button>
                    )}
                    {Array.from({ length: Math.min(totalPages, 7) }, (_, i) => {
                      const p2 = i + 1;
                      return (
                        <button
                          key={p2}
                          onClick={() => setParam("page", String(p2))}
                          className={clsx(
                            "w-10 h-10 rounded-xl text-sm font-medium transition-colors",
                            p2 === page
                              ? "bg-violet-600 text-white"
                              : "bg-white border border-gray-200 text-apple-black hover:border-violet-400"
                          )}
                        >
                          {p2}
                        </button>
                      );
                    })}
                    {page < totalPages && (
                      <button onClick={() => setParam("page", String(page + 1))} className="w-10 h-10 flex items-center justify-center rounded-xl bg-white border border-gray-200 hover:border-violet-400 text-sm font-medium transition-colors">
                        ›
                      </button>
                    )}
                  </div>
                )}
              </>
            ) : (
              <div className="text-center py-24 bg-white rounded-2xl">
                <div className="text-5xl mb-4">🔍</div>
                <p className="text-lg font-semibold text-apple-black mb-2">Товары не найдены</p>
                <p className="text-sm text-apple-gray mb-6">Попробуйте изменить фильтры</p>
                <button onClick={() => setSearchParams({})} className="text-sm text-violet-600 hover:text-violet-800 font-medium">
                  Сбросить фильтры
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Mobile filter drawer */}
      {filtersOpen && (
        <div className="fixed inset-0 z-50 flex">
          <div className="absolute inset-0 bg-black/40 backdrop-blur-sm modal-overlay" onClick={() => setFiltersOpen(false)} />
          <div ref={drawerRef} className="relative ml-auto w-full max-w-xs h-full bg-white overflow-y-auto modal-content p-6 shadow-xl">
            <div className="flex items-center justify-between mb-6">
              <h3 className="font-bold text-apple-black text-lg">Фильтры</h3>
              <button onClick={() => setFiltersOpen(false)} className="p-2 rounded-xl hover:bg-gray-100 transition-colors"><X size={18} /></button>
            </div>
            {Sidebar}
          </div>
        </div>
      )}
    </div>
  );
}
