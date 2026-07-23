import { useState, useRef, useCallback } from "react";
import { useSearchParams, useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Search, SlidersHorizontal, X, ChevronDown, ChevronLeft, ChevronRight, ArrowRight } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";
import clsx from "clsx";

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
    <div className="bg-white rounded-2xl overflow-hidden shadow-card">
      <div className="aspect-square skeleton" />
      <div className="p-3.5 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3.5 skeleton rounded" />
        <div className="h-4 skeleton rounded w-1/2 mt-1" />
      </div>
    </div>
  );
}

/* ── Horizontal carousel for one category ── */
function CategoryCarousel({ cat, index }: { cat: AutoCategory; index: number }) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [canLeft, setCanLeft]   = useState(false);
  const [canRight, setCanRight] = useState(true);

  const { data, isLoading } = useQuery({
    queryKey: ["shop-cat-carousel", cat.slug],
    queryFn: () => api.catalog({ category: cat.slug, pageSize: 14, sort: "name" }),
    staleTime: 5 * 60_000,
  });

  const syncArrows = useCallback(() => {
    const el = trackRef.current;
    if (!el) return;
    setCanLeft(el.scrollLeft > 8);
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 8);
  }, []);

  function scroll(dir: "left" | "right") {
    const el = trackRef.current;
    if (!el) return;
    el.scrollBy({ left: dir === "right" ? el.clientWidth * 0.75 : -el.clientWidth * 0.75, behavior: "smooth" });
  }

  const items = isLoading
    ? Array.from({ length: 8 })
    : (data?.products ?? []);

  return (
    <section
      className="anim-slide-up"
      style={{ animationDelay: `${index * 0.08}s` }}
    >
      {/* Section header */}
      <div className="flex items-end justify-between mb-4">
        <div>
          <h2 className="text-xl font-bold text-apple-black tracking-tight">
            {CAT_LABELS[cat.slug] || cat.label}
          </h2>
          <p className="text-[12px] text-apple-gray mt-0.5">{cat.count.toLocaleString("ru-RU")} товаров</p>
        </div>
        <Link
          to={`/catalog?category=${cat.slug}`}
          className="inline-flex items-center gap-1 text-[13px] font-semibold text-violet-600 hover:text-violet-800 transition-colors whitespace-nowrap"
        >
          Смотреть все <ArrowRight size={13} />
        </Link>
      </div>

      {/* Carousel */}
      <div className="relative carousel-wrapper">
        {/* Left arrow */}
        {canLeft && (
          <button className="carousel-btn left hidden md:flex" onClick={() => scroll("left")}>
            <ChevronLeft size={16} />
          </button>
        )}

        {/* Track */}
        <div
          ref={trackRef}
          className="carousel-track"
          onScroll={syncArrows}
        >
          {isLoading
            ? Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="carousel-item w-40 sm:w-44">
                  <CardSkeleton />
                </div>
              ))
            : data?.products.map((p, i) => (
                <div
                  key={p.offerId}
                  className="carousel-item w-40 sm:w-44 anim-fade-in"
                  style={{ animationDelay: `${index * 0.08 + i * 0.04}s` }}
                >
                  <ProductCard product={p} />
                </div>
              ))}
        </div>

        {/* Right arrow */}
        {canRight && items.length > 0 && (
          <button className="carousel-btn right hidden md:flex" onClick={() => scroll("right")}>
            <ChevronRight size={16} />
          </button>
        )}

        {/* Right fade overlay */}
        {canRight && (
          <div className="absolute right-0 top-0 bottom-0 w-12 bg-gradient-to-l from-white/80 to-transparent pointer-events-none" />
        )}
      </div>

      {/* Section divider */}
      <div className="mt-8 border-b border-gray-100" />
    </section>
  );
}

/* ── Smart pagination ── */
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
    <div className="flex items-center gap-1.5 justify-center mt-10">
      <button disabled={page === 1} onClick={() => onPage(page - 1)}
        className="w-9 h-9 flex items-center justify-center rounded-xl border border-gray-200 text-sm font-medium hover:border-violet-400 hover:text-violet-600 disabled:opacity-30 disabled:pointer-events-none transition-colors">
        ‹
      </button>
      {pages.map((p, i) => p === "…"
        ? <span key={`d${i}`} className="w-9 h-9 flex items-center justify-center text-apple-gray text-sm">…</span>
        : <button key={p} onClick={() => onPage(p as number)}
            className={clsx("w-9 h-9 rounded-xl text-sm font-medium transition-colors",
              p === page ? "bg-violet-600 text-white shadow-sm" : "border border-gray-200 text-apple-black hover:border-violet-400 hover:text-violet-600")}>
            {p}
          </button>
      )}
      <button disabled={page === total} onClick={() => onPage(page + 1)}
        className="w-9 h-9 flex items-center justify-center rounded-xl border border-gray-200 text-sm font-medium hover:border-violet-400 hover:text-violet-600 disabled:opacity-30 disabled:pointer-events-none transition-colors">
        ›
      </button>
    </div>
  );
}

/* ════════════════════════════════════════════════════ */
export default function CatalogPage() {
  const { category: urlCategory } = useParams<{ category?: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const category = urlCategory || (searchParams.get("category") ?? "");
  const [filtersOpen, setFiltersOpen] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);

  const q       = searchParams.get("q") ?? "";
  const brand   = searchParams.get("brand") ?? "";
  const sort    = (searchParams.get("sort") ?? "name") as "name" | "price_asc" | "price_desc";
  const page    = Number(searchParams.get("page") ?? 1);
  const inStock = searchParams.get("inStock") === "true";

  // Show grid when any filter active or non-default sort
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
  const activeCategory = autoCategories?.find((c) => c.slug === category);
  const pageTitle = activeCategory
    ? (CAT_LABELS[activeCategory.slug] || activeCategory.label)
    : q ? `«${q}»`
    : brand ? brand
    : "Весь каталог";

  const hasActiveFilters = !!(brand || q || inStock);

  /* ── Filter panel (used in drawer + desktop bar) ── */
  const FilterBar = (
    <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2 flex-wrap">
      {/* Search */}
      <div className="relative flex-1 min-w-44">
        <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
        <input
          type="text"
          value={q}
          onChange={(e) => setParam("q", e.target.value || null)}
          placeholder="Поиск..."
          className="w-full pl-8 pr-7 py-2 bg-gray-50 border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 focus:bg-white transition-all"
        />
        {q && <button onClick={() => setParam("q", null)} className="absolute right-2.5 top-1/2 -translate-y-1/2 text-apple-gray hover:text-apple-black"><X size={12} /></button>}
      </div>

      {/* Brand */}
      {(data?.brands?.length ?? 0) > 0 && (
        <div className="relative">
          <select value={brand} onChange={(e) => setParam("brand", e.target.value || null)}
            className="appearance-none pl-3 pr-7 py-2 bg-gray-50 border border-gray-200 rounded-xl text-sm text-apple-black focus:outline-none focus:border-violet-400 transition-colors cursor-pointer max-w-[160px]">
            <option value="">Все бренды</option>
            {data?.brands.map((b) => <option key={b} value={b}>{b}</option>)}
          </select>
          <ChevronDown size={12} className="absolute right-2 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
        </div>
      )}

      {/* Sort */}
      <div className="relative">
        <select value={sort} onChange={(e) => setParam("sort", e.target.value)}
          className="appearance-none pl-3 pr-7 py-2 bg-gray-50 border border-gray-200 rounded-xl text-sm text-apple-black focus:outline-none focus:border-violet-400 transition-colors cursor-pointer">
          <option value="name">По названию</option>
          <option value="price_asc">Дешевле</option>
          <option value="price_desc">Дороже</option>
        </select>
        <ChevronDown size={12} className="absolute right-2 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
      </div>

      {/* In-stock */}
      <button
        onClick={() => setParam("inStock", inStock ? null : "true")}
        className={clsx("px-4 py-2 rounded-xl text-sm font-medium transition-colors border",
          inStock ? "bg-violet-50 border-violet-300 text-violet-700" : "bg-gray-50 border-gray-200 text-apple-gray hover:border-violet-300 hover:text-apple-black")}
      >
        В наличии
      </button>

      {hasActiveFilters && (
        <button onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
          className="text-sm text-apple-gray hover:text-red-500 transition-colors p-2">
          <X size={15} />
        </button>
      )}
    </div>
  );

  /* ─────────────────────────────────────── RENDER ── */
  return (
    <div className="bg-white min-h-screen">

      {/* ── Sticky category tabs ── */}
      <div className="sticky top-0 z-20 bg-white/95 backdrop-blur-sm border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex gap-1 overflow-x-auto py-3" style={{ scrollbarWidth: "none" }}>
            <Link to="/catalog"
              className={clsx("flex-shrink-0 px-4 py-1.5 rounded-full text-[13px] font-semibold whitespace-nowrap transition-all duration-150",
                !category ? "bg-violet-600 text-white shadow-sm" : "text-apple-gray hover:text-apple-black hover:bg-gray-100")}>
              Все товары
            </Link>
            {autoCategories?.map((cat) => (
              <Link key={cat.slug}
                to={`/catalog?category=${cat.slug}${brand ? `&brand=${encodeURIComponent(brand)}` : ""}${inStock ? "&inStock=true" : ""}`}
                className={clsx("flex-shrink-0 px-4 py-1.5 rounded-full text-[13px] font-semibold whitespace-nowrap transition-all duration-150",
                  category === cat.slug ? "bg-violet-600 text-white shadow-sm" : "text-apple-gray hover:text-apple-black hover:bg-gray-100")}>
                {CAT_LABELS[cat.slug] || cat.label}
              </Link>
            ))}
          </div>
        </div>
      </div>

      {/* ══════════ CAROUSEL MODE (no filter) ══════════ */}
      {!showGrid ? (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 pt-8 pb-12 space-y-0">
          {/* Quick search over carousels */}
          <div className="mb-8 anim-slide-up" style={{ animationDelay: "0s" }}>
            <div className="relative max-w-sm">
              <Search size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
              <input type="text" value={q} onChange={(e) => setParam("q", e.target.value || null)}
                placeholder="Поиск по каталогу..."
                className="w-full pl-10 pr-4 py-3 bg-gray-50 border border-gray-200 rounded-2xl text-sm focus:outline-none focus:border-violet-400 focus:bg-white transition-all shadow-sm"
              />
            </div>
          </div>

          {autoCategories && autoCategories.length > 0 ? (
            autoCategories.map((cat, i) => (
              <CategoryCarousel key={cat.slug} cat={cat} index={i} />
            ))
          ) : (
            /* loading state */
            <div className="space-y-12">
              {Array.from({ length: 3 }).map((_, i) => (
                <div key={i}>
                  <div className="h-6 skeleton rounded w-48 mb-4" />
                  <div className="flex gap-3">
                    {Array.from({ length: 7 }).map((_, j) => (
                      <div key={j} className="flex-shrink-0 w-40"><CardSkeleton /></div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

      ) : (
        /* ══════════ GRID MODE (category / search / brand) ══════════ */
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
          {/* Header row */}
          <div className="flex flex-col sm:flex-row sm:items-center gap-4 mb-5 anim-slide-up">
            <div className="flex-1">
              <h1 className="text-xl font-bold text-apple-black tracking-tight">{pageTitle}</h1>
              {data && <p className="text-sm text-apple-gray mt-0.5">{data.total.toLocaleString("ru-RU")} товаров</p>}
            </div>
            {/* Desktop filter bar */}
            <div className="hidden lg:flex">{FilterBar}</div>
            {/* Mobile filter btn */}
            <button onClick={() => setFiltersOpen(true)}
              className="lg:hidden flex items-center gap-2 px-4 py-2 bg-gray-50 border border-gray-200 rounded-xl text-sm font-medium hover:border-violet-300 transition-colors self-start">
              <SlidersHorizontal size={15} /> Фильтры
              {hasActiveFilters && (
                <span className="w-5 h-5 bg-violet-600 text-white text-[10px] font-bold rounded-full flex items-center justify-center">
                  {[brand, q, inStock].filter(Boolean).length}
                </span>
              )}
            </button>
          </div>

          {/* Active chips */}
          {hasActiveFilters && (
            <div className="flex flex-wrap gap-2 mb-5 anim-fade-in">
              {brand && (
                <span className="inline-flex items-center gap-1.5 bg-violet-50 text-violet-700 border border-violet-200 text-xs font-semibold px-3 py-1.5 rounded-full">
                  {brand}<button onClick={() => setParam("brand", null)}><X size={11} /></button>
                </span>
              )}
              {q && (
                <span className="inline-flex items-center gap-1.5 bg-violet-50 text-violet-700 border border-violet-200 text-xs font-semibold px-3 py-1.5 rounded-full">
                  «{q}»<button onClick={() => setParam("q", null)}><X size={11} /></button>
                </span>
              )}
              {inStock && (
                <span className="inline-flex items-center gap-1.5 bg-emerald-50 text-emerald-700 border border-emerald-200 text-xs font-semibold px-3 py-1.5 rounded-full">
                  В наличии<button onClick={() => setParam("inStock", null)}><X size={11} /></button>
                </span>
              )}
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
                  <div key={p.offerId} className="anim-slide-up" style={{ animationDelay: `${i * 0.03}s` }}>
                    <ProductCard product={p} />
                  </div>
                ))}
              </div>
              {totalPages > 1 && (
                <Pagination page={page} total={totalPages} onPage={(p) => setParam("page", String(p))} />
              )}
            </>
          ) : (
            <div className="flex flex-col items-center justify-center py-28 text-center anim-fade-in">
              <div className="w-20 h-20 rounded-3xl mb-6 flex items-center justify-center text-3xl"
                style={{ background: "linear-gradient(135deg, #ede9fe, #ddd6fe)" }}>
                🔍
              </div>
              <p className="text-lg font-bold text-apple-black mb-1">Товары не найдены</p>
              <p className="text-sm text-apple-gray mb-6 max-w-xs">Попробуйте изменить фильтры или выбрать другую категорию</p>
              <button onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
                className="px-6 py-2.5 bg-violet-600 hover:bg-violet-700 text-white text-sm font-semibold rounded-xl transition-colors">
                Сбросить фильтры
              </button>
            </div>
          )}
        </div>
      )}

      {/* ── Mobile drawer ── */}
      {filtersOpen && (
        <div className="fixed inset-0 z-50 flex">
          <div className="absolute inset-0 bg-black/40 backdrop-blur-sm modal-overlay" onClick={() => setFiltersOpen(false)} />
          <div ref={drawerRef} className="relative ml-auto w-full max-w-sm h-full bg-white overflow-y-auto modal-content shadow-2xl">
            <div className="sticky top-0 bg-white border-b border-gray-100 px-6 py-4 flex items-center justify-between">
              <h3 className="font-bold text-apple-black text-base">Фильтры</h3>
              <button onClick={() => setFiltersOpen(false)}
                className="w-8 h-8 flex items-center justify-center rounded-full bg-gray-100 hover:bg-gray-200 transition-colors">
                <X size={16} />
              </button>
            </div>
            <div className="px-6 py-5 space-y-5">
              {/* Search in drawer */}
              <div className="relative">
                <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
                <input type="text" value={q} onChange={(e) => setParam("q", e.target.value || null)}
                  placeholder="Поиск..." className="w-full pl-8 pr-7 py-2.5 bg-gray-50 border border-gray-200 rounded-xl text-sm focus:outline-none focus:border-violet-400 transition-all" />
                {q && <button onClick={() => setParam("q", null)} className="absolute right-3 top-1/2 -translate-y-1/2 text-apple-gray"><X size={13} /></button>}
              </div>

              {(data?.brands?.length ?? 0) > 0 && (
                <div>
                  <div className="text-[11px] font-bold uppercase tracking-wider text-apple-gray mb-2">Бренд</div>
                  <div className="relative">
                    <select value={brand} onChange={(e) => setParam("brand", e.target.value || null)}
                      className="w-full appearance-none pl-3.5 pr-8 py-2.5 bg-gray-50 border border-gray-200 rounded-xl text-sm text-apple-black focus:outline-none focus:border-violet-400 cursor-pointer">
                      <option value="">Все бренды</option>
                      {data?.brands.map((b) => <option key={b} value={b}>{b}</option>)}
                    </select>
                    <ChevronDown size={13} className="absolute right-3 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
                  </div>
                </div>
              )}

              <div>
                <div className="text-[11px] font-bold uppercase tracking-wider text-apple-gray mb-2">Сортировка</div>
                <div className="relative">
                  <select value={sort} onChange={(e) => setParam("sort", e.target.value)}
                    className="w-full appearance-none pl-3.5 pr-8 py-2.5 bg-gray-50 border border-gray-200 rounded-xl text-sm text-apple-black focus:outline-none focus:border-violet-400 cursor-pointer">
                    <option value="name">По названию</option>
                    <option value="price_asc">Сначала дешевле</option>
                    <option value="price_desc">Сначала дороже</option>
                  </select>
                  <ChevronDown size={13} className="absolute right-3 top-1/2 -translate-y-1/2 text-apple-gray pointer-events-none" />
                </div>
              </div>

              <label className="flex items-center gap-3 cursor-pointer group">
                <div className="relative flex-shrink-0">
                  <input type="checkbox" checked={inStock} onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)} className="sr-only" />
                  <div className={clsx("w-10 h-6 rounded-full transition-colors", inStock ? "bg-violet-600" : "bg-gray-200 group-hover:bg-gray-300")}>
                    <div className={clsx("absolute top-1 w-4 h-4 bg-white rounded-full shadow transition-transform", inStock ? "translate-x-5" : "translate-x-1")} />
                  </div>
                </div>
                <span className="text-sm font-medium text-apple-black">Только в наличии</span>
              </label>

              {hasActiveFilters && (
                <button onClick={() => { setSearchParams(new URLSearchParams(category ? { category } : {})); setFiltersOpen(false); }}
                  className="w-full py-2.5 text-sm font-semibold text-red-500 border border-red-100 rounded-xl hover:bg-red-50 transition-colors">
                  Сбросить фильтры
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
