"use client";
import { useState, useRef, useCallback } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { Search, X, ChevronDown, ArrowRight, SlidersHorizontal } from "lucide-react";
import clsx from "clsx";
import ProductCard from "@/components/ProductCard";
import { catalogFetch, fetchAutoCategoriesClient } from "@/lib/client";
import type { ShopProduct, AutoCategory } from "@/lib/types";

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

const QUICK_CHIPS = [
  { label: "Женская",  q: "женская",  cat: "" },
  { label: "Мужская",  q: "мужская",  cat: "" },
  { label: "Унисекс",  q: "унисекс",  cat: "" },
  { label: "Нишевая",  q: "нишевая",  cat: "" },
  { label: "Элитная",  q: "элитная",  cat: "" },
  { label: "Арабская", q: "арабская", cat: "" },
  { label: "Пробники", q: "",         cat: "testers" },
  { label: "Наборы",   q: "",         cat: "sets" },
];

const CONCENTRATION_CHIPS = [
  { label: "Парфюмерная вода", cat: "edp" },
  { label: "Туалетная вода",   cat: "edt" },
  { label: "Духи",             cat: "parfum" },
  { label: "Одеколон",         cat: "edc" },
  { label: "Дезодоранты",      cat: "deo" },
];

const GROUP_CHIPS = [
  { label: "Цветочные",  q: "цветочный" },
  { label: "Древесные",  q: "древесный" },
  { label: "Цитрусовые", q: "цитрусовый" },
  { label: "Мускусные",  q: "мускусный" },
  { label: "Восточные",  q: "восточный" },
  { label: "Свежие",     q: "свежий" },
  { label: "Фужерные",   q: "фужерный" },
  { label: "Шипровые",   q: "шипровый" },
];

const PAGE_SIZE = 24;

function ruPlural(n: number, one: string, few: string, many: string) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return few;
  return many;
}

function CardSkeleton() {
  return (
    <div className="product-card" style={{ pointerEvents: "none" }}>
      <div style={{ aspectRatio: "4/5" }} className="skeleton" />
      <div style={{ padding: "14px 14px 16px", display: "flex", flexDirection: "column", gap: 8 }}>
        <div className="skeleton" style={{ height: 8, width: "38%" }} />
        <div className="skeleton" style={{ height: 12, width: "85%" }} />
        <div className="skeleton" style={{ height: 12, width: "60%" }} />
        <div className="skeleton" style={{ height: 13, width: "45%", marginTop: 4 }} />
      </div>
    </div>
  );
}

function CategoryCarouselRow({ cat, index }: { cat: AutoCategory; index: number }) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [canRight, setCanRight] = useState(true);

  const { data, isLoading } = useQuery({
    queryKey: ["cat-carousel", cat.slug],
    queryFn: () => catalogFetch({ category: cat.slug, pageSize: 12, sort: "name" }),
    staleTime: 5 * 60_000,
  });

  const syncArrows = useCallback(() => {
    const el = trackRef.current;
    if (!el) return;
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 8);
  }, []);

  return (
    <section style={{ marginBottom: 40, animationDelay: `${index * 0.06}s` }} className="anim-slide-up">
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", padding: "0 clamp(18px,4vw,32px)", marginBottom: 18, gap: 16 }}>
        <div>
          <h2 className="serif" style={{ margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(22px,2.4vw,30px)", color: "#f5f4f0" }}>
            {CAT_LABELS[cat.slug] || cat.label}
          </h2>
          <p style={{ margin: "4px 0 0", fontSize: 11.5, color: "#7d7a73", letterSpacing: "0.04em" }}>
            {cat.count.toLocaleString("ru-RU")} {ruPlural(cat.count, "аромат", "аромата", "ароматов")}
          </p>
        </div>
        <Link
          href={`/catalog?category=${cat.slug}`}
          style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11.5, letterSpacing: "0.12em", textTransform: "uppercase", color: "#c9a25e", textDecoration: "none", whiteSpace: "nowrap", flexShrink: 0, transition: "color 0.2s" }}
          onMouseEnter={e => (e.currentTarget.style.color = "#e9d2a0")}
          onMouseLeave={e => (e.currentTarget.style.color = "#c9a25e")}
        >
          Все <ArrowRight size={12} strokeWidth={2} />
        </Link>
      </div>

      <div style={{ position: "relative" }}>
        <div ref={trackRef} className="scroll-x" style={{ display: "flex", gap: 14, padding: "0 clamp(18px,4vw,32px) 8px" }} onScroll={syncArrows}>
          {isLoading
            ? Array.from({ length: 6 }).map((_, i) => (
                <div key={i} style={{ flexShrink: 0, width: 200 }}><CardSkeleton /></div>
              ))
            : data?.products.map((p) => (
                <div key={p.offerId} style={{ flexShrink: 0, width: 200 }}>
                  <ProductCard product={p} />
                </div>
              ))}
        </div>
        {canRight && (
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 8, width: 64, background: "linear-gradient(to left, #0b0b0b 20%, transparent)", pointerEvents: "none" }} />
        )}
      </div>

      <div style={{ margin: "16px clamp(18px,4vw,32px) 0", borderBottom: "1px solid rgba(255,255,255,0.06)" }} />
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
    <div style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "center", marginTop: 40, paddingBottom: 8 }}>
      <button disabled={page === 1} onClick={() => onPage(page - 1)} className="pg-btn">‹</button>
      {pages.map((p, i) =>
        p === "…"
          ? <span key={`d${i}`} style={{ width: 36, textAlign: "center", color: "rgba(245,244,240,0.28)", fontSize: 13 }}>…</span>
          : <button key={p} onClick={() => onPage(p as number)} className={clsx("pg-btn", p === page && "current")}>{p}</button>
      )}
      <button disabled={page === total} onClick={() => onPage(page + 1)} className="pg-btn">›</button>
    </div>
  );
}

function FilterSidebar({
  sort, inStock, brand, brands, category, setParam, resetAll,
}: {
  sort: string; inStock: boolean; brand: string; brands: string[];
  category: string; setParam: (k: string, v: string | null) => void;
  resetAll: () => void;
}) {
  return (
    <aside className="cat-sidebar" style={{ background: "#0d0d0d", border: "1px solid rgba(255,255,255,0.06)", borderRadius: 3, overflow: "hidden" }}>
      <div className="sidebar-section">
        <p style={{ margin: "0 0 12px", fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66" }}>Сортировка</p>
        {[
          { value: "name",       label: "По названию" },
          { value: "price_asc",  label: "Сначала дешевле" },
          { value: "price_desc", label: "Сначала дороже" },
        ].map((opt) => (
          <button key={opt.value} onClick={() => setParam("sort", opt.value)} className={clsx("sort-opt", sort === opt.value && "active")}>
            <span className="dot" />
            {opt.label}
          </button>
        ))}
      </div>

      <div className="sidebar-section">
        <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}>
          <span style={{ fontSize: 13, color: inStock ? "#e9d2a0" : "rgba(245,244,240,0.52)", letterSpacing: "0.03em", transition: "color 0.2s" }}>Только в наличии</span>
          <div style={{ position: "relative", flexShrink: 0 }}>
            <input type="checkbox" checked={inStock} onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)} style={{ position: "absolute", opacity: 0, pointerEvents: "none" }} />
            <div style={{ width: 40, height: 22, borderRadius: 100, background: inStock ? "#c9a25e" : "rgba(255,255,255,0.08)", border: `1px solid ${inStock ? "transparent" : "rgba(255,255,255,0.12)"}`, transition: "background 0.2s", position: "relative" }}>
              <div style={{ position: "absolute", top: 3, left: inStock ? 21 : 3, width: 14, height: 14, background: inStock ? "#14120f" : "rgba(245,244,240,0.3)", borderRadius: "50%", transition: "left 0.2s, background 0.2s" }} />
            </div>
          </div>
        </label>
      </div>

      {brands.length > 0 && (
        <div className="sidebar-section">
          <p style={{ margin: "0 0 8px", fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66" }}>Бренд</p>
          {brand && (
            <button onClick={() => setParam("brand", null)} style={{ marginBottom: 8, fontSize: 11, color: "#c9a25e", background: "none", border: "none", cursor: "pointer", padding: 0, letterSpacing: "0.06em", fontFamily: "inherit" }}>
              × Сбросить
            </button>
          )}
          <div style={{ maxHeight: 220, overflowY: "auto", scrollbarWidth: "thin", scrollbarColor: "rgba(201,162,94,0.2) transparent" }}>
            <button onClick={() => setParam("brand", null)} className={clsx("dd-chip", !brand && "active")}>
              <span className="dot" />Все бренды
            </button>
            {brands.map((b) => (
              <button key={b} onClick={() => setParam("brand", b === brand ? null : b)} className={clsx("dd-chip", brand === b && "active")}>
                <span className="dot" />{b}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="sidebar-section">
        <p style={{ margin: "0 0 12px", fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66" }}>Концентрация</p>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {CONCENTRATION_CHIPS.map(c => (
            <button key={c.cat} onClick={() => setParam("category", category === c.cat ? null : c.cat)} className={clsx("dd-chip", category === c.cat && "active")}>
              <span className="dot" />{c.label}
            </button>
          ))}
        </div>
      </div>

      {(brand || inStock || sort !== "name") && (
        <div className="sidebar-section">
          <button
            onClick={resetAll}
            style={{ width: "100%", padding: "9px 12px", border: "1px solid rgba(239,68,68,0.25)", borderRadius: 2, background: "rgba(239,68,68,0.05)", color: "#f87171", fontSize: 12, letterSpacing: "0.08em", cursor: "pointer", fontFamily: "inherit", transition: "background 0.2s" }}
            onMouseEnter={e => (e.currentTarget.style.background = "rgba(239,68,68,0.1)")}
            onMouseLeave={e => (e.currentTarget.style.background = "rgba(239,68,68,0.05)")}
          >
            Сбросить фильтры
          </button>
        </div>
      )}
    </aside>
  );
}

function FilterPill({ label, active, children, onClear }: {
  label: string; active?: boolean; children: React.ReactNode; onClear?: () => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const handleOutside = useCallback((e: MouseEvent) => {
    if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
  }, []);

  const toggle = () => {
    setOpen(s => {
      if (!s) document.addEventListener("mousedown", handleOutside);
      else document.removeEventListener("mousedown", handleOutside);
      return !s;
    });
  };

  return (
    <div ref={ref} style={{ position: "relative", flexShrink: 0 }}>
      <button onClick={toggle} className={clsx("filter-pill", active && "active")}>
        {label}
        {active && onClear && (
          <span onClick={(e) => { e.stopPropagation(); onClear(); setOpen(false); }} style={{ display: "inline-flex", alignItems: "center", marginLeft: 2, opacity: 0.7 }}>
            <X size={10} />
          </span>
        )}
        {!active && <ChevronDown size={10} style={{ transition: "transform 0.2s", transform: open ? "rotate(180deg)" : "none" }} />}
      </button>
      {open && <div className="filter-dropdown">{children}</div>}
    </div>
  );
}

interface Props {
  initialProducts: ShopProduct[];
  initialTotal: number;
  initialBrands: string[];
  autoCategories: AutoCategory[];
}

function CatalogInner({ initialProducts, initialTotal, initialBrands, autoCategories: serverAutoCategories }: Props) {
  const sp = useSearchParams();
  const router = useRouter();
  const [filtersOpen, setFiltersOpen] = useState(false);
  const drawerRef = useRef<HTMLDivElement>(null);

  const category = sp.get("category") ?? "";
  const q        = sp.get("q") ?? "";
  const brand    = sp.get("brand") ?? "";
  const sort     = (sp.get("sort") ?? "name") as "name" | "price_asc" | "price_desc";
  const page     = Number(sp.get("page") ?? 1);
  const inStock  = sp.get("inStock") === "true";

  const showGrid = !!(category || q || brand || inStock || sort !== "name") || sp.get("view") === "grid";

  // Client-side auto-categories (used in mobile chips and carousel mode)
  const { data: clientAutoCats } = useQuery({
    queryKey: ["auto-categories"],
    queryFn: fetchAutoCategoriesClient,
    staleTime: 5 * 60_000,
    initialData: serverAutoCategories.length > 0 ? serverAutoCategories : undefined,
  });
  const autoCategories = clientAutoCats ?? serverAutoCategories;

  const brands = showGrid ? initialBrands : [];
  const products = initialProducts;
  const total = initialTotal;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const hasActiveFilters = !!(brand || q || inStock);
  const activeFilterCount = [brand, q, inStock].filter(Boolean).length;

  const pageTitle = autoCategories?.find((c) => c.slug === category)
    ? (CAT_LABELS[category] || category)
    : q ? `«${q}»`
    : brand ? brand
    : "Весь каталог";

  const activeQuickChip = QUICK_CHIPS.find(c =>
    (c.q && q === c.q && !category) || (c.cat && category === c.cat && !q)
  );
  const activeConc  = CONCENTRATION_CHIPS.find(c => c.cat === category);
  const activeGroup = GROUP_CHIPS.find(c => c.q === q);

  function setParam(key: string, value: string | null) {
    const next = new URLSearchParams(sp.toString());
    if (!value) next.delete(key); else next.set(key, value);
    if (key !== "page") next.delete("page");
    const str = next.toString();
    router.push(`/catalog${str ? "?" + str : ""}`);
  }

  function applyQuickChip(chip: typeof QUICK_CHIPS[0]) {
    const next = new URLSearchParams();
    if (chip.q) next.set("q", chip.q);
    if (chip.cat) next.set("category", chip.cat);
    router.push(`/catalog${next.toString() ? "?" + next.toString() : ""}`);
  }

  function resetAll() {
    router.push("/catalog");
  }

  function resetAllKeepCat() {
    const next = new URLSearchParams();
    if (category) next.set("category", category);
    router.push(`/catalog${next.toString() ? "?" + next.toString() : ""}`);
  }

  return (
    <div style={{ background: "#0b0b0b", minHeight: "100vh" }}>
      {/* ── Sticky top bar ── */}
      <div style={{ position: "sticky", top: 60, zIndex: 30, background: "rgba(11,11,11,0.95)", backdropFilter: "blur(16px)", WebkitBackdropFilter: "blur(16px)", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>

        {/* Search row */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px clamp(18px,4vw,32px)" }}>
          <div style={{ position: "relative", flex: 1 }}>
            <Search size={14} style={{ position: "absolute", left: 13, top: "50%", transform: "translateY(-50%)", color: "rgba(245,244,240,0.28)", pointerEvents: "none" }} />
            <input
              type="text"
              defaultValue={q}
              key={q}
              onKeyDown={(e) => { if (e.key === "Enter") setParam("q", (e.target as HTMLInputElement).value || null); }}
              onBlur={(e) => { if (e.target.value !== q) setParam("q", e.target.value || null); }}
              placeholder="Поиск по каталогу..."
              className="input-base"
              style={{ paddingLeft: 38, paddingRight: q ? 36 : 14, fontSize: 13 }}
            />
            {q && (
              <button onClick={() => setParam("q", null)} style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", cursor: "pointer", color: "rgba(245,244,240,0.28)", display: "flex" }}>
                <X size={14} />
              </button>
            )}
          </div>

          <button
            className="cat-filter-btn"
            onClick={() => setFiltersOpen(true)}
            style={{
              alignItems: "center", gap: 6,
              padding: "9px 14px", borderRadius: 2, fontSize: 12, letterSpacing: "0.1em",
              textTransform: "uppercase", border: "none", cursor: "pointer",
              background: activeFilterCount ? "rgba(201,162,94,0.12)" : "rgba(255,255,255,0.06)",
              color: activeFilterCount ? "#e9d2a0" : "rgba(245,244,240,0.52)",
              transition: "all 0.2s",
            }}
          >
            <SlidersHorizontal size={13} strokeWidth={1.8} />
            {activeFilterCount > 0 ? `Фильтры · ${activeFilterCount}` : "Фильтры"}
          </button>
        </div>

        {/* Quick chips: Gender / Type */}
        <div className="scroll-x" style={{ display: "flex", gap: 6, padding: "0 clamp(18px,4vw,32px) 8px", alignItems: "center" }}>
          <span style={{ fontSize: 10.5, letterSpacing: "0.18em", textTransform: "uppercase", color: "#5d5a54", flexShrink: 0, marginRight: 4 }}>Тип:</span>
          <Link href="/catalog" className={clsx("cat-chip", !showGrid && "active")} style={{ flexShrink: 0 }}>Все</Link>
          {QUICK_CHIPS.map(chip => (
            <button
              key={chip.label}
              onClick={() => activeQuickChip?.label === chip.label ? resetAll() : applyQuickChip(chip)}
              className={clsx("cat-chip", activeQuickChip?.label === chip.label && "active")}
              style={{ flexShrink: 0 }}
            >
              {chip.label}
            </button>
          ))}
        </div>

        {/* Extended filter pills row (desktop) */}
        <div className="hidden md:flex scroll-x" style={{ gap: 6, padding: "0 clamp(18px,4vw,32px) 10px", alignItems: "center", flexWrap: "nowrap", overflow: "visible" }}>
          <span style={{ fontSize: 10.5, letterSpacing: "0.18em", textTransform: "uppercase", color: "#5d5a54", flexShrink: 0, marginRight: 4 }}>Фильтр:</span>

          {brands.length > 0 && (
            <FilterPill label={brand ? `Бренд: ${brand}` : "Бренд"} active={!!brand} onClear={() => setParam("brand", null)}>
              <p style={{ margin: "0 0 10px", fontSize: 9.5, letterSpacing: "0.22em", textTransform: "uppercase", color: "#6f6c66" }}>Бренд</p>
              <div style={{ maxHeight: 280, overflowY: "auto", scrollbarWidth: "thin" }}>
                {brands.map(b => (
                  <button key={b} onClick={() => setParam("brand", b === brand ? null : b)} className={clsx("dd-chip", brand === b && "active")}>
                    <span className="dot" />{b}
                  </button>
                ))}
              </div>
            </FilterPill>
          )}

          <FilterPill label={activeConc ? `Концентрация: ${activeConc.label}` : "Концентрация"} active={!!activeConc} onClear={() => setParam("category", null)}>
            <p style={{ margin: "0 0 10px", fontSize: 9.5, letterSpacing: "0.22em", textTransform: "uppercase", color: "#6f6c66" }}>Концентрация</p>
            {CONCENTRATION_CHIPS.map(c => (
              <button key={c.cat} onClick={() => setParam("category", category === c.cat ? null : c.cat)} className={clsx("dd-chip", category === c.cat && "active")}>
                <span className="dot" />{c.label}
              </button>
            ))}
          </FilterPill>

          <FilterPill label={activeGroup ? `Группа: ${activeGroup.label}` : "Группы"} active={!!activeGroup} onClear={() => setParam("q", null)}>
            <p style={{ margin: "0 0 10px", fontSize: 9.5, letterSpacing: "0.22em", textTransform: "uppercase", color: "#6f6c66" }}>Группы аромата</p>
            {GROUP_CHIPS.map(c => (
              <button key={c.q} onClick={() => setParam("q", q === c.q ? null : c.q)} className={clsx("dd-chip", q === c.q && "active")}>
                <span className="dot" />{c.label}
              </button>
            ))}
          </FilterPill>

          <button onClick={() => setParam("inStock", inStock ? null : "true")} className={clsx("filter-pill", inStock && "active")}>
            В наличии
            {inStock && <X size={10} style={{ marginLeft: 2 }} onClick={(e) => { e.stopPropagation(); setParam("inStock", null); }} />}
          </button>

          <FilterPill
            label={sort === "name" ? "Сортировка" : sort === "price_asc" ? "↑ Цена" : "↓ Цена"}
            active={sort !== "name"}
            onClear={() => setParam("sort", "name")}
          >
            <p style={{ margin: "0 0 10px", fontSize: 9.5, letterSpacing: "0.22em", textTransform: "uppercase", color: "#6f6c66" }}>Сортировка</p>
            {[
              { value: "name",       label: "По названию" },
              { value: "price_asc",  label: "Сначала дешевле" },
              { value: "price_desc", label: "Сначала дороже" },
            ].map(o => (
              <button key={o.value} onClick={() => setParam("sort", o.value)} className={clsx("dd-chip", sort === o.value && "active")}>
                <span className="dot" />{o.label}
              </button>
            ))}
          </FilterPill>

          {(hasActiveFilters || sort !== "name" || activeConc) && (
            <button
              onClick={resetAll}
              style={{ flexShrink: 0, fontSize: 11.5, letterSpacing: "0.06em", color: "rgba(245,244,240,0.28)", background: "none", border: "none", cursor: "pointer", padding: "7px 4px", fontFamily: "inherit", transition: "color 0.2s" }}
              onMouseEnter={e => (e.currentTarget.style.color = "#f87171")}
              onMouseLeave={e => (e.currentTarget.style.color = "rgba(245,244,240,0.28)")}
            >
              × Сбросить всё
            </button>
          )}
        </div>

        {/* Mobile: category chips */}
        <div className="flex md:hidden scroll-x" style={{ gap: 8, padding: "0 clamp(18px,4vw,32px) 10px" }}>
          <Link href="/catalog" className={clsx("cat-chip", !category && !showGrid && "active")} style={{ flexShrink: 0 }}>Все</Link>
          {autoCategories.map((cat) => (
            <Link
              key={cat.slug}
              href={`/catalog?category=${cat.slug}${brand ? `&brand=${encodeURIComponent(brand)}` : ""}${inStock ? "&inStock=true" : ""}`}
              className={clsx("cat-chip", category === cat.slug && "active")}
              style={{ flexShrink: 0 }}
            >
              {CAT_LABELS[cat.slug] || cat.label}
            </Link>
          ))}
        </div>
      </div>

      {/* ── Main content ── */}
      <div className={showGrid ? "cat-layout" : undefined} style={{ maxWidth: 1400, margin: "0 auto" }}>

        {showGrid && (
          <FilterSidebar
            sort={sort} inStock={inStock} brand={brand} brands={brands}
            category={category} setParam={setParam} resetAll={resetAllKeepCat}
          />
        )}

        <div style={{ padding: showGrid ? "clamp(20px,3vw,32px) clamp(18px,4vw,32px)" : 0 }}>

          {/* ══ CAROUSEL MODE ══ */}
          {!showGrid ? (
            <div style={{ paddingTop: 32, paddingBottom: 40 }}>
              {autoCategories.length > 0
                ? autoCategories.map((cat, i) => <CategoryCarouselRow key={cat.slug} cat={cat} index={i} />)
                : (
                  <div style={{ padding: "0 clamp(18px,4vw,32px)", display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 14 }}>
                    {Array.from({ length: 12 }).map((_, i) => <CardSkeleton key={i} />)}
                  </div>
                )}
            </div>
          ) : (
            /* ══ GRID MODE ══ */
            <>
              <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", marginBottom: 20, gap: 16, flexWrap: "wrap" }}>
                <div>
                  <h1 className="serif" style={{ margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(26px,3vw,38px)", lineHeight: 1, color: "#f5f4f0" }}>{pageTitle}</h1>
                  {total > 0 && <p style={{ margin: "6px 0 0", fontSize: 12, color: "#7d7a73", letterSpacing: "0.04em" }}>{total.toLocaleString("ru-RU")} {ruPlural(total, "аромат", "аромата", "ароматов")}</p>}
                </div>
              </div>

              {hasActiveFilters && (
                <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 20 }}>
                  {brand && (
                    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", borderRadius: 2, background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.28)", fontSize: 11.5, letterSpacing: "0.06em", color: "#e9d2a0" }}>
                      {brand}<button onClick={() => setParam("brand", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex", padding: 0 }}><X size={10} /></button>
                    </span>
                  )}
                  {q && (
                    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", borderRadius: 2, background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.28)", fontSize: 11.5, letterSpacing: "0.06em", color: "#e9d2a0" }}>
                      «{q}»<button onClick={() => setParam("q", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex", padding: 0 }}><X size={10} /></button>
                    </span>
                  )}
                  {inStock && (
                    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 12px", borderRadius: 2, background: "rgba(74,222,128,0.07)", border: "1px solid rgba(74,222,128,0.2)", fontSize: 11.5, letterSpacing: "0.06em", color: "#6EE7B7" }}>
                      В наличии<button onClick={() => setParam("inStock", null)} style={{ background: "none", border: "none", cursor: "pointer", color: "inherit", display: "flex", padding: 0 }}><X size={10} /></button>
                    </span>
                  )}
                  <button
                    onClick={resetAllKeepCat}
                    style={{ fontSize: 11.5, letterSpacing: "0.06em", color: "rgba(245,244,240,0.28)", background: "none", border: "none", cursor: "pointer", padding: "5px 4px", transition: "color 0.2s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "#f87171")}
                    onMouseLeave={e => (e.currentTarget.style.color = "rgba(245,244,240,0.28)")}
                  >
                    Сбросить всё
                  </button>
                </div>
              )}

              {products.length > 0 ? (
                <>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 14 }}>
                    {products.map((p, i) => (
                      <div key={p.offerId} className="anim-slide-up" style={{ animationDelay: `${Math.min(i, 12) * 0.03}s` }}>
                        <ProductCard product={p} />
                      </div>
                    ))}
                  </div>
                  {totalPages > 1 && (
                    <Pagination
                      page={page}
                      total={totalPages}
                      onPage={(p) => {
                        setParam("page", String(p));
                        window.scrollTo({ top: 0, behavior: "smooth" });
                      }}
                    />
                  )}
                </>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "80px 20px", textAlign: "center" }}>
                  <div style={{ fontSize: 44, marginBottom: 20 }}>🔍</div>
                  <h2 className="serif" style={{ margin: "0 0 10px", fontStyle: "italic", fontWeight: 400, fontSize: 28, color: "#f5f4f0" }}>Ничего не найдено</h2>
                  <p style={{ margin: "0 0 28px", fontSize: 14, color: "#7d7a73", lineHeight: 1.6 }}>Попробуйте изменить фильтры или поисковый запрос</p>
                  <button onClick={resetAllKeepCat} className="btn-primary">Сбросить фильтры</button>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* ── Mobile filter drawer ── */}
      {filtersOpen && (
        <div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
          <div className="modal-overlay" style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.7)", backdropFilter: "blur(4px)" }} onClick={() => setFiltersOpen(false)} />
          <div ref={drawerRef} style={{ position: "relative", background: "#141414", borderRadius: "12px 12px 0 0", maxHeight: "88vh", overflowY: "auto", border: "1px solid rgba(255,255,255,0.1)", borderBottom: "none" }}>
            <div style={{ position: "sticky", top: 0, background: "#141414", borderRadius: "12px 12px 0 0", borderBottom: "1px solid rgba(255,255,255,0.06)", padding: "18px 20px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <div>
                <h3 style={{ fontWeight: 400, color: "#f5f4f0", fontSize: 16, letterSpacing: "0.02em" }}>Фильтры</h3>
                {activeFilterCount > 0 && <p style={{ fontSize: 11, color: "#c9a25e", marginTop: 2 }}>{activeFilterCount} активно</p>}
              </div>
              <button onClick={() => setFiltersOpen(false)} style={{ width: 32, height: 32, display: "flex", alignItems: "center", justifyContent: "center", borderRadius: 2, background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.09)", cursor: "pointer", color: "rgba(245,244,240,0.52)" }}>
                <X size={14} />
              </button>
            </div>

            <div style={{ padding: "20px 20px 32px", display: "flex", flexDirection: "column", gap: 24 }}>
              <div>
                <p style={{ fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 10 }}>Сортировка</p>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {[
                    { value: "name",       label: "По названию" },
                    { value: "price_asc",  label: "Сначала дешевле" },
                    { value: "price_desc", label: "Сначала дороже" },
                  ].map((opt) => (
                    <button key={opt.value} onClick={() => setParam("sort", opt.value)}
                      style={{ textAlign: "left", padding: "11px 14px", borderRadius: 2, fontSize: 13, letterSpacing: "0.04em", background: sort === opt.value ? "rgba(201,162,94,0.08)" : "rgba(255,255,255,0.03)", border: `1px solid ${sort === opt.value ? "rgba(201,162,94,0.35)" : "rgba(255,255,255,0.09)"}`, color: sort === opt.value ? "#e9d2a0" : "rgba(245,244,240,0.52)", cursor: "pointer", fontFamily: "inherit", transition: "all 0.15s" }}>
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>

              <div>
                <p style={{ fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 10 }}>Концентрация</p>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {CONCENTRATION_CHIPS.map(c => (
                    <button key={c.cat} onClick={() => setParam("category", category === c.cat ? null : c.cat)}
                      style={{ padding: "8px 14px", borderRadius: 20, fontSize: 12.5, fontFamily: "inherit", cursor: "pointer", background: category === c.cat ? "rgba(201,162,94,0.1)" : "rgba(255,255,255,0.04)", border: `1px solid ${category === c.cat ? "rgba(201,162,94,0.5)" : "rgba(255,255,255,0.09)"}`, color: category === c.cat ? "#e9d2a0" : "rgba(245,244,240,0.52)", transition: "all 0.15s" }}>
                      {c.label}
                    </button>
                  ))}
                </div>
              </div>

              {brands.length > 0 && (
                <div>
                  <p style={{ fontSize: 9.5, letterSpacing: "0.28em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 10 }}>Бренд</p>
                  <div style={{ maxHeight: 180, overflowY: "auto", scrollbarWidth: "thin", scrollbarColor: "rgba(201,162,94,0.2) transparent", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, padding: "4px 0" }}>
                    <button onClick={() => setParam("brand", null)} className={clsx("dd-chip", !brand && "active")}>
                      <span className="dot" />Все бренды
                    </button>
                    {brands.map((b) => (
                      <button key={b} onClick={() => setParam("brand", b === brand ? null : b)} className={clsx("dd-chip", brand === b && "active")}>
                        <span className="dot" />{b}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}>
                <span style={{ fontSize: 14, color: "#f5f4f0", letterSpacing: "0.02em" }}>Только в наличии</span>
                <div style={{ position: "relative" }}>
                  <input type="checkbox" checked={inStock} onChange={(e) => setParam("inStock", e.target.checked ? "true" : null)} style={{ position: "absolute", opacity: 0, pointerEvents: "none" }} />
                  <div style={{ width: 44, height: 24, borderRadius: 100, background: inStock ? "#c9a25e" : "rgba(255,255,255,0.08)", border: `1px solid ${inStock ? "transparent" : "rgba(255,255,255,0.12)"}`, transition: "background 0.2s", position: "relative" }}>
                    <div style={{ position: "absolute", top: 4, left: inStock ? 24 : 4, width: 16, height: 16, background: inStock ? "#14120f" : "rgba(245,244,240,0.3)", borderRadius: "50%", transition: "left 0.2s, background 0.2s" }} />
                  </div>
                </div>
              </label>

              <button onClick={() => setFiltersOpen(false)} className="btn-primary" style={{ width: "100%" }}>
                Применить
              </button>

              {hasActiveFilters && (
                <button
                  onClick={() => { resetAllKeepCat(); setFiltersOpen(false); }}
                  style={{ fontSize: 12, letterSpacing: "0.08em", color: "#f87171", background: "none", border: "none", cursor: "pointer", padding: "8px", fontFamily: "inherit", textTransform: "uppercase" }}
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

export default function CatalogClient(props: Props) {
  return <CatalogInner {...props} />;
}
