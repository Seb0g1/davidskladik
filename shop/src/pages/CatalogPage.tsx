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
    <div style={{ background: "var(--surface)", borderRadius: 16, border: "1px solid var(--border)", overflow: "hidden" }}>
      <div style={{ aspectRatio: "4/5", background: "rgba(255,255,255,0.03)" }} className="skeleton" />
      <div style={{ padding: "10px 12px 12px", display: "flex", flexDirection: "column", gap: 6 }}>
        <div className="skeleton" style={{ height: 8, width: "45%", borderRadius: 6 }} />
        <div className="skeleton" style={{ height: 12, width: "80%", borderRadius: 6 }} />
        <div className="skeleton" style={{ height: 14, width: "55%", borderRadius: 6, marginTop: 4 }} />
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
    <section className="anim-slide-up" style={{ animationDelay: `${index * 0.06}s` }}>
      {/* Category header */}
      <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", padding: "20px 16px 12px" }}>
        <div>
          <h2 style={{ fontSize: 17, fontWeight: 700, color: "var(--text)", letterSpacing: "-0.025em" }}>
            {CAT_LABELS[cat.slug] || cat.label}
          </h2>
          <p style={{ fontSize: 11, color: "var(--subtle)", marginTop: 2 }}>{cat.count.toLocaleString("ru-RU")} товаров</p>
        </div>
        <Link to={`/catalog?category=${cat.slug}`}
          style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12.5, fontWeight: 600, color: "var(--accent2)", textDecoration: "none", whiteSpace: "nowrap", transition: "color 0.15s ease" }}
          onMouseEnter={e => (e.currentTarget.style.color = "var(--accent3)")}
          onMouseLeave={e => (e.currentTarget.style.color = "var(--accent2)")}
        >
          Все <ArrowRight size={13} strokeWidth={2.5} />
        </Link>
      </div>

      <div style={{ position: "relative" }}>
        <div ref={trackRef} className="scroll-x" style={{ display: "flex", gap: 12, padding: "0 16px 16px" }} onScroll={syncArrows}>
          {isLoading
            ? Array.from({ length: 6 }).map((_, i) => (
                <div key={i} style={{ flexShrink: 0, width: 140 }}><CardSkeleton /></div>
              ))
            : data?.products.map((p) => (
                <div key={p.offerId} style={{ flexShrink: 0, width: 140 }}>
                  <ProductCard product={p} />
                </div>
              ))}
        </div>
        {canRight && (
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 16, width: 48, background: "linear-gradient(to left, var(--bg) 20%, transparent)", pointerEvents: "none" }} />
        )}
      </div>

      <div style={{ margin: "0 16px", borderBottom: "1px solid var(--border)" }} />
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
    <div style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "center", marginTop: 32, paddingBottom: 8 }}>
      <button disabled={page === 1} onClick={() => onPage(page - 1)} className="pg-btn">‹</button>
      {pages.map((p, i) =>
        p === "…"
          ? <span key={`d${i}`} style={{ width: 36, height: 36, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--subtle)", fontSize: 13 }}>…</span>
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
    <div style={{ background: "var(--bg)", minHeight: "100vh" }}>

      {/* ── Sticky top strip ── */}
      <div style={{ position: "sticky", top: 60, zIndex: 30, background: "rgba(9,6,15,0.92)", backdropFilter: "blur(20px)", WebkitBackdropFilter: "blur(20px)", borderBottom: "1px solid var(--border)" }}>

        {/* Search + filter row */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 12px" }}>
          <div style={{ position: "relative", flex: 1 }}>
            <Search size={14} style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: "var(--subtle)", pointerEvents: "none" }} />
            <input
              type="text"
              value={q}
              onChange={(e) => setParam("q", e.target.value || null)}
              placeholder="Поиск по каталогу..."
              className="input-base"
              style={{ paddingLeft: 36, paddingRight: q ? 36 : 14, paddingTop: 10, paddingBottom: 10, fontSize: 13 }}
            />
            {q && (
              <button onClick={() => setParam("q", null)} style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", cursor: "pointer", color: "var(--subtle)", display: "flex" }}>
                <X size={14} />
              </button>
            )}
          </div>
          <button
            onClick={() => setFiltersOpen(true)}
            style={{
              flexShrink: 0, display: "flex", alignItems: "center", gap: 6,
              padding: "10px 14px", borderRadius: 12, fontSize: 13, fontWeight: 600, border: "none", cursor: "pointer",
              background: activeFilterCount ? "var(--accent)" : "var(--surface2)",
              color: activeFilterCount ? "#fff" : "var(--muted)",
              boxShadow: activeFilterCount ? "0 0 16px rgba(201,169,110,0.35)" : "none",
              transition: "all 0.15s ease",
            }}
          >
            <SlidersHorizontal size={14} strokeWidth={2} />
            {activeFilterCount > 0 && activeFilterCount}
          </button>
        </div>

        {/* Category chips */}
        <div className="scroll-x" style={{ display: "flex", gap: 8, padding: "0 12px 10px" }}>
          <Link
            to="/catalog"
            className={clsx("chip", !category && !showGrid && "active")}
            style={{ flexShrink: 0 }}
          >
            Все
          </Link>
          {autoCategories?.map((cat) => (
            <Link
              key={cat.slug}
              to={`/catalog?category=${cat.slug}${brand ? `&brand=${encodeURIComponent(brand)}` : ""}${inStock ? "&inStock=true" : ""}`}
              className={clsx("chip", category === cat.slug && "active")}
              style={{ flexShrink: 0 }}
            >
              {CAT_LABELS[cat.slug] || cat.label}
            </Link>
          ))}
        </div>
      </div>

      {/* ══════ CAROUSEL MODE ══════ */}
      {!showGrid ? (
        <div style={{ paddingBottom: 24 }}>
          {autoCategories && autoCategories.length > 0 ? (
            autoCategories.map((cat, i) => <CategoryCarouselRow key={cat.slug} cat={cat} index={i} />)
          ) : (
            <div style={{ padding: "16px 12px", display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12 }}>
              {Array.from({ length: 12 }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          )}
        </div>

      ) : (
        /* ══════ GRID MODE ══════ */
        <div style={{ padding: "14px 12px" }}>
          {/* Header */}
          <div className="anim-slide-up" style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", marginBottom: 14 }}>
            <div>
              <h1 style={{ fontSize: 17, fontWeight: 700, color: "var(--text)", letterSpacing: "-0.025em" }}>{pageTitle}</h1>
              {data && (
                <p style={{ fontSize: 11, color: "var(--subtle)", marginTop: 2 }}>{data.total.toLocaleString("ru-RU")} товаров</p>
              )}
            </div>
            {/* Desktop sort */}
            <div className="hidden md:block" style={{ position: "relative" }}>
              <select value={sort} onChange={(e) => setParam("sort", e.target.value)}
                style={{ appearance: "none", padding: "8px 28px 8px 12px", background: "var(--surface2)", border: "1px solid var(--border)", borderRadius: 10, fontSize: 12.5, fontWeight: 500, color: "var(--muted)", cursor: "pointer" }}>
                <option value="name">По названию</option>
                <option value="price_asc">Сначала дешевле</option>
                <option value="price_desc">Сначала дороже</option>
              </select>
              <ChevronDown size={12} style={{ position: "absolute", right: 8, top: "50%", transform: "translateY(-50%)", color: "var(--subtle)", pointerEvents: "none" }} />
            </div>
          </div>

          {/* Active filter chips */}
          {hasActiveFilters && (
            <div className="anim-fade-in" style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 14 }}>
              {brand && (
                <span style={{ display: "inline-flex", alignItems: "center", gap: 4, background: "rgba(201,169,110,0.1)", color: "var(--accent3)", border: "1px solid rgba(124,58,237,0.2)", fontSize: 11, fontWeight: 600, padding: "4px 10px", borderRadius: 100 }}>
                  {brand}<button onClick={() => setParam("brand", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex" }}><X size={10} /></button>
                </span>
              )}
              {q && (
                <span style={{ display: "inline-flex", alignItems: "center", gap: 4, background: "rgba(201,169,110,0.1)", color: "var(--accent3)", border: "1px solid rgba(124,58,237,0.2)", fontSize: 11, fontWeight: 600, padding: "4px 10px", borderRadius: 100 }}>
                  «{q}»<button onClick={() => setParam("q", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex" }}><X size={10} /></button>
                </span>
              )}
              {inStock && (
                <span style={{ display: "inline-flex", alignItems: "center", gap: 4, background: "rgba(16,185,129,0.1)", color: "#6EE7B7", border: "1px solid rgba(16,185,129,0.2)", fontSize: 11, fontWeight: 600, padding: "4px 10px", borderRadius: 100 }}>
                  В наличии<button onClick={() => setParam("inStock", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex" }}><X size={10} /></button>
                </span>
              )}
              <button
                onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
                style={{ fontSize: 11, color: "var(--subtle)", background: "none", border: "none", cursor: "pointer", padding: "4px 6px", transition: "color 0.15s ease" }}
                onMouseEnter={e => (e.currentTarget.style.color = "#F87171")}
                onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
              >
                Сбросить
              </button>
            </div>
          )}

          {/* Grid */}
          {isLoading ? (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(148px, 1fr))", gap: 10 }}>
              {Array.from({ length: PAGE_SIZE }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          ) : data?.products.length ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(148px, 1fr))", gap: 10 }}>
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
            <div className="anim-fade-in" style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "80px 20px", textAlign: "center" }}>
              <div style={{ width: 64, height: 64, borderRadius: 20, marginBottom: 20, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 28, background: "var(--surface2)", border: "1px solid var(--border)" }}>🔍</div>
              <p style={{ fontSize: 17, fontWeight: 700, color: "var(--text)", marginBottom: 6 }}>Товары не найдены</p>
              <p style={{ fontSize: 13, color: "var(--muted)", marginBottom: 24, maxWidth: 280, lineHeight: 1.6 }}>Попробуйте изменить фильтры или выбрать другую категорию</p>
              <button
                onClick={() => setSearchParams(new URLSearchParams(category ? { category } : {}))}
                className="btn-primary"
                style={{ padding: "12px 24px", fontSize: 13 }}
              >
                Сбросить фильтры
              </button>
            </div>
          )}
        </div>
      )}

      {/* ── Filter sheet (mobile) ── */}
      {filtersOpen && (
        <div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
          <div className="modal-overlay" style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.7)" }} onClick={() => setFiltersOpen(false)} />
          <div ref={drawerRef} className="sheet-content" style={{ position: "relative", background: "var(--surface)", borderRadius: "24px 24px 0 0", maxHeight: "88vh", overflowY: "auto", border: "1px solid var(--border-md)", borderBottom: "none" }}>
            <div style={{ position: "sticky", top: 0, background: "var(--surface)", borderRadius: "24px 24px 0 0", borderBottom: "1px solid var(--border)", padding: "18px 20px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div>
                <h3 style={{ fontWeight: 700, color: "var(--text)", fontSize: 16 }}>Фильтры</h3>
                {activeFilterCount > 0 && <p style={{ fontSize: 11, color: "var(--accent2)", marginTop: 2 }}>{activeFilterCount} выбрано</p>}
              </div>
              <button onClick={() => setFiltersOpen(false)}
                style={{ width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center", borderRadius: "50%", background: "var(--surface2)", border: "1px solid var(--border)", cursor: "pointer", color: "var(--muted)" }}>
                <X size={15} />
              </button>
            </div>

            <div style={{ padding: "20px 20px 32px", display: "flex", flexDirection: "column", gap: 22 }}>
              {/* Brand */}
              {(data?.brands?.length ?? 0) > 0 && (
                <div>
                  <p style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--subtle)", marginBottom: 8 }}>Бренд</p>
                  <div style={{ position: "relative" }}>
                    <select value={brand} onChange={(e) => setParam("brand", e.target.value || null)}
                      className="input-base" style={{ appearance: "none", paddingRight: 32 }}>
                      <option value="">Все бренды</option>
                      {data?.brands.map((b) => <option key={b} value={b}>{b}</option>)}
                    </select>
                    <ChevronDown size={13} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", color: "var(--subtle)", pointerEvents: "none" }} />
                  </div>
                </div>
              )}

              {/* Sort */}
              <div>
                <p style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--subtle)", marginBottom: 8 }}>Сортировка</p>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {[
                    { value: "name",       label: "По названию" },
                    { value: "price_asc",  label: "Сначала дешевле" },
                    { value: "price_desc", label: "Сначала дороже" },
                  ].map((opt) => (
                    <button
                      key={opt.value}
                      onClick={() => setParam("sort", opt.value)}
                      style={{
                        width: "100%", textAlign: "left", padding: "12px 16px", borderRadius: 12, fontSize: 13.5, fontWeight: 500,
                        background: sort === opt.value ? "rgba(201,169,110,0.1)" : "var(--surface2)",
                        border: `1px solid ${sort === opt.value ? "rgba(201,169,110,0.3)" : "var(--border)"}`,
                        color: sort === opt.value ? "var(--accent3)" : "var(--muted)",
                        cursor: "pointer", transition: "all 0.15s ease",
                      }}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* In stock toggle */}
              <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}>
                <span style={{ fontSize: 14, fontWeight: 500, color: "var(--text)" }}>Только в наличии</span>
                <div style={{ position: "relative", flexShrink: 0 }}>
                  <input type="checkbox" checked={inStock} onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)} className="sr-only" />
                  <div style={{ width: 44, height: 24, borderRadius: 100, background: inStock ? "var(--accent)" : "var(--surface2)", border: `1px solid ${inStock ? "transparent" : "var(--border)"}`, transition: "background 0.2s ease", boxShadow: inStock ? "0 0 12px rgba(201,169,110,0.25)" : "none" }}>
                    <div style={{ position: "absolute", top: 4, left: inStock ? 24 : 4, width: 16, height: 16, background: inStock ? "#fff" : "var(--muted)", borderRadius: "50%", transition: "left 0.2s ease, background 0.2s ease" }} />
                  </div>
                </div>
              </label>

              {/* Apply */}
              <button onClick={() => setFiltersOpen(false)} className="btn-primary" style={{ width: "100%", padding: "14px", fontSize: 14 }}>
                Применить
              </button>

              {hasActiveFilters && (
                <button
                  onClick={() => { setSearchParams(new URLSearchParams(category ? { category } : {})); setFiltersOpen(false); }}
                  style={{ width: "100%", padding: "12px", fontSize: 13, fontWeight: 600, color: "#F87171", background: "none", border: "none", cursor: "pointer", transition: "color 0.15s ease" }}
                  onMouseEnter={e => (e.currentTarget.style.color = "#FCA5A5")}
                  onMouseLeave={e => (e.currentTarget.style.color = "#F87171")}
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
