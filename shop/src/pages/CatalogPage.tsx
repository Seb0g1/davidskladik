import { useState, useRef, useCallback } from "react";
import { useSearchParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Search, SlidersHorizontal, X, ChevronDown, ArrowRight } from "lucide-react";
import clsx from "clsx";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

const CAT_LABELS: Record<string, string> = {
  testers: "Тестеры и отливанты",
  parfum:  "Духи",
  edp:     "Парфюмерная вода",
  edt:     "Туалетная вода",
  edc:     "Одеколон",
  deo:     "Дезодоранты",
  home:    "Ароматы для дома",
  sets:    "Подарочные наборы",
  body:    "Уход за телом",
};

const PAGE_SIZE = 24;

function CardSkeleton() {
  return (
    <div className="product-card">
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div className="p-3 space-y-2">
        <div className="h-2 skeleton rounded w-1/2" />
        <div className="h-3 skeleton rounded" />
        <div className="h-4 skeleton rounded w-2/3 mt-2" />
      </div>
    </div>
  );
}

function CategoryCarouselRow({ cat, index }: { cat: AutoCategory; index: number }) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [canRight, setCanRight] = useState(true);

  const { data, isLoading } = useQuery({
    queryKey: ["shop-cat-carousel", cat.slug],
    queryFn: () => api.catalog({ category: cat.slug, pageSize: 12, sort: "name" }),
    staleTime: 5 * 60_000,
  });

  const syncArrows = useCallback(() => {
    const el = trackRef.current;
    if (!el) return;
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 8);
  }, []);

  return (
    <section className="anim-slide-up" style={{ animationDelay: `${index * 0.07}s` }}>
      <div className="section-header px-4 pt-5 pb-3">
        <div>
          <h2 className="text-[16px] font-bold text-[#111] tracking-tight">
            {CAT_LABELS[cat.slug] || cat.label}
          </h2>
          <p className="text-[11px] text-[#ABABAB] mt-0.5">{cat.count.toLocaleString("ru-RU")} товаров</p>
        </div>
        <Link to={`/catalog?category=${cat.slug}`}
          className="text-[13px] font-semibold text-violet-600 flex items-center gap-1 whitespace-nowrap hover:text-violet-800 transition-colors">
          Все <ArrowRight size={13} strokeWidth={2.5} />
        </Link>
      </div>

      <div className="relative">
        <div ref={trackRef} className="scroll-x flex gap-3 px-4 pb-4" onScroll={syncArrows}>
          {isLoading
            ? Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="flex-shrink-0" style={{ width: 152 }}><CardSkeleton /></div>
              ))
            : data?.products.map((p) => (
                <div key={p.offerId} className="flex-shrink-0" style={{ width: 152 }}>
                  <ProductCard product={p} />
                </div>
              ))}
        </div>
        {canRight && (
          <div className="absolute right-0 top-0 bottom-4 w-10 bg-gradient-to-l from-white to-transparent pointer-events-none" />
        )}
      </div>

      <div className="mx-4 border-b border-[#F0F0F0]" />
    </section>
  );
}

function Pagination({ page, total, onPage }: { page: number; total: number; onPage: (p: number) => void }) {
  const pages: (number | "…")[] = [];
  if (total <= 7) {
    for (let i = 1; i <= total; i++) pages.push(i);
  } else {
    pages.push(1);
    if (page > 3) pages.push("…");
    for (let i = Math.max(2, page - 1); i <= Math.min(total - 1, page + 1); i++) pages.push(i);
    if (page < total - 2) pages.push("…");
    pages.push(total);
  }
  return (
    <div className="flex items-center gap-1.5 justify-center mt-8 pb-2">
      <button disabled={page === 1} onClick={() => onPage(page - 1)} className="pg-btn">‹</button>
      {pages.map((p, i) =>
        p === "…"
          ? <span key={`d${i}`} className="w-9 h-9 flex items-center justify-center text-[#888] text-sm">…</span>
          : <button key={p} onClick={() => onPage(p as number)} className={clsx("pg-btn", p === page && "current")}>{p}</button>
      )}
      <button disabled={page === total} onClick={() => onPage(page + 1)} className="pg-btn">›</button>
    </div>
  );
}

export default function CatalogPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [filtersOpen, setFiltersOpen] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);

  const category = searchParams.get("category") ?? "";
  const q        = searchParams.get("q") ?? "";
  const brand    = searchParams.get("brand") ?? "";
  const sort     = (searchParams.get("sort") ?? "name") as "name" | "price_asc" | "price_desc";
  const page     = Number(searchParams.get("page") ?? 1);
  const inStock  = searchParams.get("inStock") === "true";

  const showGrid = !!(category || q || brand || inStock || sort !== "name");

  const { data, isLoading } = useQuery({
    queryKey: ["shop-catalog", { category, q, brand, sort, page, inStock }],
    queryFn: () => api.catalog({ category, q, brand, sort, page, pageSize: PAGE_SIZE, inStock }),
    enabled: showGrid,
  });

  const { data: autoCategories } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  function setParam(key: string, value: string | null) {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (!value) next.delete(key); else next.set(key, value);
      if (key !== "page") next.delete("page");
      return next;
    });
  }

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const hasActiveFilters = !!(brand || q || inStock);
  const activeFilterCount = [brand, q, inStock].filter(Boolean).length;

  const pageTitle = autoCategories?.find((c) => c.slug === category)
    ? (CAT_LABELS[category] || category)
    : q ? `«${q}»`
    : brand ? brand
    : "Весь каталог";

  return (
    <div className="bg-white min-h-screen">

      {/* ── Sticky top strip ── */}
      <div className="sticky top-14 z-30 bg-white/95 backdrop-blur-sm border-b border-[#EBEBEB]">

        {/* Search + filter row */}
        <div className="flex items-center gap-2 px-3 py-2.5">
          <div className="relative flex-1">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-[#ABABAB] pointer-events-none" />
            <input
              type="text"
              value={q}
              onChange={(e) => setParam("q", e.target.value || null)}
              placeholder="Поиск по каталогу..."
              className="input-base pl-8 pr-7 py-2.5 text-[13px]"
              style={{ padding: "9px 30px 9px 32px" }}
            />
            {q && (
              <button onClick={() => setParam("q", null)} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[#ABABAB] hover:text-[#333]">
                <X size={14} />
              </button>
            )}
          </div>
          <button
            onClick={() => setFiltersOpen(true)}
            className={clsx(
              "flex-shrink-0 flex items-center gap-1.5 px-3.5 py-[9px] rounded-xl text-[13px] font-semibold transition-all",
              activeFilterCount
                ? "bg-violet-600 text-white"
                : "bg-[#F5F5F3] text-[#555] hover:text-[#111]"
            )}
          >
            <SlidersHorizontal size={14} strokeWidth={2} />
            {activeFilterCount > 0 ? activeFilterCount : ""}
          </button>
        </div>

        {/* Category chips */}
        <div className="scroll-x flex gap-2 px-3 pb-2.5">
          <Link
            to="/catalog"
            className={clsx("chip flex-shrink-0 py-1.5 text-[12.5px]", !category && !showGrid && "active")}
            style={{ padding: "5px 13px" }}
          >
            Все
          </Link>
          {autoCategories?.map((cat) => (
            <Link
              key={cat.slug}
              to={`/catalog?category=${cat.slug}${brand ? `&brand=${encodeURIComponent(brand)}` : ""}${inStock ? "&inStock=true" : ""}`}
              className={clsx("chip flex-shrink-0 py-1.5 text-[12.5px]", category === cat.slug && "active")}
              style={{ padding: "5px 13px" }}
            >
              {CAT_LABELS[cat.slug] || cat.label}
            </Link>
          ))}
        </div>
      </div>

      {/* ══════ CAROUSEL MODE ══════ */}
      {!showGrid ? (
        <div className="pb-6">
          {autoCategories && autoCategories.length > 0 ? (
            autoCategories.map((cat, i) => <CategoryCarouselRow key={cat.slug} cat={cat} index={i} />)
          ) : (
            <div className="px-4 py-8 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
              {Array.from({ length: 12 }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          )}
        </div>

      ) : (
        /* ══════ GRID MODE ══════ */
        <div className="px-3 py-3">
          {/* Header */}
          <div className="flex items-center justify-between mb-3 anim-slide-up">
            <div>
              <h1 className="text-[16px] font-bold text-[#111] tracking-tight">{pageTitle}</h1>
              {data && (
                <p className="text-[11px] text-[#888] mt-0.5">{data.total.toLocaleString("ru-RU")} товаров</p>
              )}
            </div>
            {/* Desktop sort */}
            <div className="relative hidden md:block">
              <select value={sort} onChange={(e) => setParam("sort", e.target.value)}
                className="appearance-none pl-3 pr-7 py-2 bg-[#F5F5F3] border border-transparent rounded-xl text-[13px] font-medium text-[#333] focus:outline-none focus:border-violet-400 cursor-pointer">
                <option value="name">По названию</option>
                <option value="price_asc">Сначала дешевле</option>
                <option value="price_desc">Сначала дороже</option>
              </select>
              <ChevronDown size={12} className="absolute right-2 top-1/2 -translate-y-1/2 text-[#888] pointer-events-none" />
            </div>
          </div>

          {/* Active filter chips */}
          {hasActiveFilters && (
            <div className="flex flex-wrap gap-1.5 mb-3 anim-fade-in">
              {brand && (
                <span className="inline-flex items-center gap-1 bg-violet-50 text-violet-700 border border-violet-200 text-[11px] font-semibold px-2.5 py-1 rounded-full">
                  {brand}<button onClick={() => setParam("brand", null)} className="ml-0.5"><X size={10} /></button>
                </span>
              )}
              {q && (
                <span className="inline-flex items-center gap-1 bg-violet-50 text-violet-700 border border-violet-200 text-[11px] font-semibold px-2.5 py-1 rounded-full">
                  «{q}»<button onClick={() => setParam("q", null)} className="ml-0.5"><X size={10} /></button>
                </span>
              )}
              {inStock && (
                <span className="inline-flex items-center gap-1 bg-emerald-50 text-emerald-700 border border-emerald-200 text-[11px] font-semibold px-2.5 py-1 rounded-full">
                  В наличии<button onClick={() => setParam("inStock", null)} className="ml-0.5"><X size={10} /></button>
                </span>
              )}
              <button
                onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
                className="text-[11px] text-[#888] hover:text-red-500 transition-colors px-1"
              >
                Сбросить
              </button>
            </div>
          )}

          {/* Grid */}
          {isLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 xl:grid-cols-5 gap-3">
              {Array.from({ length: PAGE_SIZE }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          ) : data?.products.length ? (
            <>
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 xl:grid-cols-5 gap-3">
                {data.products.map((p, i) => (
                  <div key={p.offerId} className="anim-slide-up" style={{ animationDelay: `${Math.min(i, 12) * 0.03}s` }}>
                    <ProductCard product={p} />
                  </div>
                ))}
              </div>
              {totalPages > 1 && (
                <Pagination page={page} total={totalPages} onPage={(p) => { setParam("page", String(p)); window.scrollTo({ top: 0, behavior: "smooth" }); }} />
              )}
            </>
          ) : (
            <div className="flex flex-col items-center justify-center py-20 text-center anim-fade-in">
              <div className="w-16 h-16 rounded-3xl mb-5 flex items-center justify-center text-2xl bg-violet-50">🔍</div>
              <p className="text-[16px] font-bold text-[#111] mb-1">Товары не найдены</p>
              <p className="text-[13px] text-[#888] mb-6 max-w-xs leading-relaxed">Попробуйте изменить фильтры или выбрать другую категорию</p>
              <button
                onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
                className="btn-primary px-6 py-3 text-[13px]"
              >
                Сбросить фильтры
              </button>
            </div>
          )}
        </div>
      )}

      {/* ── Filter sheet (mobile) ── */}
      {filtersOpen && (
        <div className="fixed inset-0 z-50 flex flex-col justify-end">
          <div className="absolute inset-0 bg-black/40 modal-overlay" onClick={() => setFiltersOpen(false)} />
          <div ref={drawerRef} className="relative bg-white rounded-t-3xl sheet-content max-h-[85vh] overflow-y-auto">
            <div className="sticky top-0 bg-white rounded-t-3xl border-b border-[#F0F0F0] px-5 py-4 flex items-center justify-between">
              <div>
                <h3 className="font-bold text-[#111] text-base">Фильтры</h3>
                {activeFilterCount > 0 && <p className="text-[11px] text-violet-600 mt-0.5">{activeFilterCount} выбрано</p>}
              </div>
              <button onClick={() => setFiltersOpen(false)}
                className="w-8 h-8 flex items-center justify-center rounded-full bg-[#F5F5F3] hover:bg-[#EBEBEB] transition-colors">
                <X size={16} />
              </button>
            </div>

            <div className="px-5 py-5 space-y-5 pb-8">
              {/* Brand */}
              {(data?.brands?.length ?? 0) > 0 && (
                <div>
                  <p className="text-[11px] font-bold uppercase tracking-wider text-[#888] mb-2">Бренд</p>
                  <div className="relative">
                    <select value={brand} onChange={(e) => setParam("brand", e.target.value || null)}
                      className="input-base appearance-none pr-8">
                      <option value="">Все бренды</option>
                      {data?.brands.map((b) => <option key={b} value={b}>{b}</option>)}
                    </select>
                    <ChevronDown size={13} className="absolute right-3 top-1/2 -translate-y-1/2 text-[#ABABAB] pointer-events-none" />
                  </div>
                </div>
              )}

              {/* Sort */}
              <div>
                <p className="text-[11px] font-bold uppercase tracking-wider text-[#888] mb-2">Сортировка</p>
                <div className="flex flex-col gap-2">
                  {[
                    { value: "name",       label: "По названию" },
                    { value: "price_asc",  label: "Сначала дешевле" },
                    { value: "price_desc", label: "Сначала дороже" },
                  ].map((opt) => (
                    <button
                      key={opt.value}
                      onClick={() => setParam("sort", opt.value)}
                      className={clsx(
                        "w-full text-left px-4 py-3 rounded-xl text-[13px] font-medium transition-all border",
                        sort === opt.value
                          ? "bg-violet-50 border-violet-300 text-violet-700"
                          : "bg-[#F5F5F3] border-transparent text-[#333] hover:border-[#E0E0E0]"
                      )}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* In stock toggle */}
              <label className="flex items-center justify-between py-1 cursor-pointer">
                <span className="text-[14px] font-medium text-[#111]">Только в наличии</span>
                <div className="relative flex-shrink-0">
                  <input type="checkbox" checked={inStock} onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)} className="sr-only" />
                  <div className={clsx("w-11 h-6 rounded-full transition-colors duration-200", inStock ? "bg-violet-600" : "bg-[#E0E0E0]")}>
                    <div className={clsx("absolute top-1 w-4 h-4 bg-white rounded-full shadow transition-transform duration-200", inStock ? "translate-x-6" : "translate-x-1")} />
                  </div>
                </div>
              </label>

              {/* Apply */}
              <button onClick={() => setFiltersOpen(false)} className="btn-primary w-full py-3.5">
                Применить
              </button>

              {hasActiveFilters && (
                <button
                  onClick={() => { setSearchParams(new URLSearchParams(category ? { category } : {})); setFiltersOpen(false); }}
                  className="w-full py-3 text-[13px] font-semibold text-red-500 hover:text-red-600 transition-colors"
                >
                  Сбросить всё
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
