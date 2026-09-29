"use client";
import { useState, useRef, useCallback, useEffect } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { Search, X, ArrowRight, SlidersHorizontal, ChevronDown } from "lucide-react";
import clsx from "clsx";
import ProductCard from "@/components/ProductCard";
import { catalogFetch, fetchAutoCategoriesClient } from "@/lib/client";
import type { ShopProduct, AutoCategory, CatalogFacets } from "@/lib/types";
import CategoryScene from "@/components/CategoryScene";

const CAT_LABELS: Record<string, string> = {
  testers: "Пробники и отливанты",
  parfum:  "Духи",
  edp:     "Парфюмерная вода",
  edt:     "Туалетная вода",
  edc:     "Одеколон",
  deo:     "Дезодоранты",
  home:    "Ароматы для дома",
  sets:    "Подарочные наборы",
  body:    "Уход за телом",
};

// Each filter group has its own URL param, so they combine instead of overwriting each other
// (the old «Группы» and the search box both wrote to ?q=).
const GENDERS = [
  { v: "женская", label: "Женская" },
  { v: "мужская", label: "Мужская" },
  { v: "унисекс", label: "Унисекс" },
];
const LINES = [
  { v: "нишевая",  label: "Нишевая" },
  { v: "элитная",  label: "Элитная" },
  { v: "арабская", label: "Арабская" },
  { v: "миниатюр", label: "Миниатюры" },
];
const GROUPS = [
  { v: "цветочный",  label: "Цветочные" },
  { v: "древесный",  label: "Древесные" },
  { v: "цитрусовый", label: "Цитрусовые" },
  { v: "свежий",     label: "Свежие" },
  { v: "восточный",  label: "Восточные" },
  { v: "сладкий",    label: "Сладкие" },
  { v: "мускусный",  label: "Мускусные" },
  { v: "фужерный",   label: "Фужерные" },
  { v: "шипровый",   label: "Шипровые" },
];
const TYPES = ["edp", "edt", "parfum", "edc", "testers", "sets", "deo", "home", "body"];
const SORTS = [
  { v: "",           label: "Популярные" },
  { v: "price_asc",  label: "Сначала дешевле" },
  { v: "price_desc", label: "Сначала дороже" },
  { v: "new",        label: "Новинки" },
  { v: "name",       label: "По названию" },
];
// old links (?q=женская, ?q=цветочный) keep working: a collection word in q is shown as its chip
const COLLECTION_WORDS = new Set([...GENDERS, ...LINES, ...GROUPS].map((x) => x.v));
const FILTER_KEYS = ["q", "gender", "line", "group", "category", "brand", "priceMin", "priceMax", "volume", "inStock", "sort"] as const;

const PAGE_SIZE = 24;
const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

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
    queryFn: () => catalogFetch({ category: cat.slug, pageSize: 12, sort: "popular" }),
    staleTime: 5 * 60_000,
  });

  const syncArrows = useCallback(() => {
    const el = trackRef.current;
    if (!el) return;
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 8);
  }, []);

  return (
    <section style={{ marginBottom: 40, animationDelay: `${index * 0.06}s` }} className="anim-slide-up">
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", padding: "0 clamp(16px,4vw,32px)", marginBottom: 18, gap: 16 }}>
        <div>
          <h2 className="serif" style={{ margin: 0, fontWeight: 400, fontSize: "clamp(22px,2.4vw,30px)", color: "var(--ink)" }}>
            {CAT_LABELS[cat.slug] || cat.label}
          </h2>
          <p style={{ margin: "4px 0 0", fontSize: 11.5, color: "rgba(var(--ink-rgb),0.6)", letterSpacing: "0.04em" }}>
            {cat.count.toLocaleString("ru-RU")} {ruPlural(cat.count, "аромат", "аромата", "ароматов")}
          </p>
        </div>
        <Link href={`/catalog?category=${cat.slug}`} className="mv-cat-all">
          Все <ArrowRight size={12} strokeWidth={2} />
        </Link>
      </div>

      <div style={{ position: "relative" }}>
        <div ref={trackRef} className="scroll-x" style={{ display: "flex", gap: 14, padding: "0 clamp(16px,4vw,32px) 8px" }} onScroll={syncArrows}>
          {isLoading
            ? Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="mv-cat-slide"><CardSkeleton /></div>
              ))
            : data?.products.map((p) => (
                <div key={p.offerId} className="mv-cat-slide">
                  <ProductCard product={p} />
                </div>
              ))}
        </div>
        {canRight && (
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 8, width: 64, background: "linear-gradient(to left, var(--paper) 20%, transparent)", pointerEvents: "none" }} />
        )}
      </div>
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
    <nav aria-label="Страницы" style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "center", marginTop: 40, paddingBottom: 8, flexWrap: "wrap" }}>
      <button disabled={page === 1} onClick={() => onPage(page - 1)} className="pg-btn" aria-label="Предыдущая">‹</button>
      {pages.map((p, i) =>
        p === "…"
          ? <span key={`d${i}`} style={{ width: 28, textAlign: "center", color: "rgba(var(--ink-rgb),0.45)", fontSize: 13 }}>…</span>
          : <button key={p} onClick={() => onPage(p as number)} className={clsx("pg-btn", p === page && "current")}>{p}</button>
      )}
      <button disabled={page === total} onClick={() => onPage(page + 1)} className="pg-btn" aria-label="Следующая">›</button>
    </nav>
  );
}

/* ─────────────── filter panel (desktop sidebar + mobile sheet) ─────────────── */
type Params = Record<(typeof FILTER_KEYS)[number], string>;

function Section({ title, children, defaultOpen = true, badge }: { title: string; children: React.ReactNode; defaultOpen?: boolean; badge?: string }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="mv-fs">
      <button type="button" className="mv-fs-h" onClick={() => setOpen((v) => !v)} aria-expanded={open}>
        <span>{title}{badge && <em>{badge}</em>}</span>
        <ChevronDown size={16} style={{ transform: open ? "rotate(180deg)" : "none", transition: "transform .2s" }} />
      </button>
      {open && <div className="mv-fs-b">{children}</div>}
    </div>
  );
}

function Chips<T extends { v: string; label: string }>({ items, value, onPick }: { items: T[]; value: string; onPick: (v: string | null) => void }) {
  return (
    <div className="mv-fchips">
      {items.map((it) => (
        <button key={it.v} type="button" className={clsx("mv-fchip", value === it.v && "on")} onClick={() => onPick(value === it.v ? null : it.v)}>
          {it.label}
        </button>
      ))}
    </div>
  );
}

function PriceRange({ min, max, facet, onApply }: { min: string; max: string; facet?: { min: number; max: number }; onApply: (min: string, max: string) => void }) {
  const [a, setA] = useState(min);
  const [b, setB] = useState(max);
  useEffect(() => { setA(min); setB(max); }, [min, max]);
  const apply = () => { if (a !== min || b !== max) onApply(a.replace(/\D/g, ""), b.replace(/\D/g, "")); };
  const presets: [string, string, string][] = [["", "3000", "до 3 000"], ["3000", "7000", "3–7 тыс."], ["7000", "15000", "7–15 тыс."], ["15000", "", "от 15 000"]];
  return (
    <>
      <div className="mv-price">
        <label><span>от</span><input inputMode="numeric" value={a} placeholder={facet ? String(facet.min) : "0"} onChange={(e) => setA(e.target.value)} onBlur={apply} onKeyDown={(e) => e.key === "Enter" && apply()} aria-label="Цена от" /></label>
        <label><span>до</span><input inputMode="numeric" value={b} placeholder={facet ? String(facet.max) : ""} onChange={(e) => setB(e.target.value)} onBlur={apply} onKeyDown={(e) => e.key === "Enter" && apply()} aria-label="Цена до" /></label>
      </div>
      <div className="mv-fchips" style={{ marginTop: 8 }}>
        {presets.map(([pa, pb, l]) => (
          <button key={l} type="button" className={clsx("mv-fchip", min === pa && max === pb && "on")} onClick={() => onApply(pa, pb)}>{l}</button>
        ))}
      </div>
    </>
  );
}

function BrandList({ facet, value, onPick }: { facet: { name: string; count: number }[]; value: string; onPick: (v: string | null) => void }) {
  const [term, setTerm] = useState("");
  const [all, setAll] = useState(false);
  const t = term.trim().toLowerCase();
  const list = t ? facet.filter((b) => b.name.toLowerCase().includes(t)) : facet;
  const shown = all || t ? list.slice(0, 200) : list.slice(0, 10);
  return (
    <>
      <input className="mv-fsearch" value={term} onChange={(e) => setTerm(e.target.value)} placeholder="Найти бренд" aria-label="Найти бренд" />
      <div className={clsx("mv-fbrands", (all || t) && "scroll")}>
        {value && !facet.some((b) => b.name.toLowerCase() === value.toLowerCase()) && (
          <button type="button" className="mv-fcheck on" onClick={() => onPick(null)}><i />{value}</button>
        )}
        {shown.map((b) => {
          const on = value.toLowerCase() === b.name.toLowerCase();
          return (
            <button key={b.name} type="button" className={clsx("mv-fcheck", on && "on")} onClick={() => onPick(on ? null : b.name)}>
              <i />{b.name}<em>{b.count}</em>
            </button>
          );
        })}
        {!shown.length && <p className="mv-fnone">Не нашли такой бренд</p>}
      </div>
      {!t && list.length > 10 && (
        <button type="button" className="mv-flink" onClick={() => setAll((v) => !v)}>{all ? "Свернуть" : `Показать все (${list.length})`}</button>
      )}
    </>
  );
}

function FilterPanel({ p, facets, set }: { p: Params; facets?: CatalogFacets; set: (patch: Partial<Record<keyof Params, string | null>>) => void }) {
  const gender = p.gender || (GENDERS.some((g) => g.v === p.q) ? p.q : "");
  const line = p.line || (LINES.some((g) => g.v === p.q) ? p.q : "");
  const group = p.group || (GROUPS.some((g) => g.v === p.q) ? p.q : "");
  // picking a chip that was carried in ?q= moves it to its own param
  const pick = (key: "gender" | "line" | "group", v: string | null) => set({ [key]: v, ...(COLLECTION_WORDS.has(p.q) ? { q: null } : {}) });
  return (
    <div className="mv-fpanel">
      <Section title="Сортировка">
        <div className="mv-fchips">
          {SORTS.map((s) => (
            <button key={s.v} type="button" className={clsx("mv-fchip", (p.sort || "") === s.v && "on")} onClick={() => set({ sort: s.v || null })}>{s.label}</button>
          ))}
        </div>
      </Section>
      <Section title="Цена, ₽" badge={p.priceMin || p.priceMax ? "•" : undefined}>
        <PriceRange min={p.priceMin} max={p.priceMax} facet={facets?.price} onApply={(a, b) => set({ priceMin: a || null, priceMax: b || null })} />
      </Section>
      <Section title="Для кого" badge={gender ? "•" : undefined}>
        <Chips items={GENDERS} value={gender} onPick={(v) => pick("gender", v)} />
      </Section>
      <Section title="Тип" badge={p.category ? "•" : undefined}>
        <Chips items={TYPES.map((v) => ({ v, label: CAT_LABELS[v] }))} value={p.category} onPick={(v) => set({ category: v })} />
      </Section>
      {facets?.volumes && facets.volumes.length > 0 && (
        <Section title="Объём, мл" badge={p.volume ? "•" : undefined}>
          <div className="mv-fchips">
            {facets.volumes.map((v) => (
              <button key={v.ml} type="button" className={clsx("mv-fchip", Number(p.volume) === v.ml && "on")} onClick={() => set({ volume: Number(p.volume) === v.ml ? null : String(v.ml) })}>
                {String(v.ml).replace(".", ",")} мл <em>{v.count}</em>
              </button>
            ))}
          </div>
        </Section>
      )}
      <Section title="Бренд" badge={p.brand ? "•" : undefined}>
        {facets?.brands?.length
          ? <BrandList facet={facets.brands} value={p.brand} onPick={(v) => set({ brand: v })} />
          : p.brand ? <button type="button" className="mv-fcheck on" onClick={() => set({ brand: null })}><i />{p.brand}</button> : <p className="mv-fnone">—</p>}
      </Section>
      <Section title="Группа аромата" badge={group ? "•" : undefined} defaultOpen={false}>
        <Chips items={GROUPS} value={group} onPick={(v) => pick("group", v)} />
      </Section>
      <Section title="Коллекции" badge={line ? "•" : undefined} defaultOpen={false}>
        <Chips items={LINES} value={line} onPick={(v) => pick("line", v)} />
      </Section>
      <div className="mv-fs">
        <label className="mv-fswitch">
          <span>Только в наличии</span>
          <input type="checkbox" checked={p.inStock === "true"} onChange={(e) => set({ inStock: e.target.checked ? "true" : null })} />
          <i aria-hidden />
        </label>
      </div>
    </div>
  );
}

interface Props {
  initialProducts: ShopProduct[];
  initialTotal: number;
  initialBrands: string[];
  initialFacets?: CatalogFacets;
  autoCategories: AutoCategory[];
}

function CatalogInner({ initialProducts, initialTotal, initialFacets, autoCategories: serverAutoCategories }: Props) {
  const sp = useSearchParams();
  const router = useRouter();
  const [filtersOpen, setFiltersOpen] = useState(false);

  const p = Object.fromEntries(FILTER_KEYS.map((k) => [k, sp.get(k) ?? ""])) as Params;
  const page = Math.max(1, Number(sp.get("page") ?? 1) || 1);
  const showGrid = FILTER_KEYS.some((k) => p[k]) || sp.get("view") === "grid";

  const { data: clientAutoCats } = useQuery({
    queryKey: ["auto-categories"],
    queryFn: fetchAutoCategoriesClient,
    staleTime: 5 * 60_000,
    initialData: serverAutoCategories.length > 0 ? serverAutoCategories : undefined,
  });
  const autoCategories = clientAutoCats ?? serverAutoCategories;

  const products = initialProducts;
  const total = initialTotal;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  // lock page scroll under the mobile sheet
  useEffect(() => {
    if (!filtersOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = prev; };
  }, [filtersOpen]);

  function go(next: URLSearchParams) {
    const str = next.toString();
    router.push(`/catalog${str ? "?" + str : ""}`, { scroll: false });
  }
  function set(patch: Partial<Record<keyof Params | "page", string | null>>) {
    const next = new URLSearchParams(sp.toString());
    for (const [k, v] of Object.entries(patch)) { if (!v) next.delete(k); else next.set(k, v); }
    if (!("page" in patch)) next.delete("page");
    next.delete("view");
    if (![...next.keys()].some((k) => (FILTER_KEYS as readonly string[]).includes(k))) next.set("view", "grid");
    go(next);
  }
  const resetAll = () => go(new URLSearchParams("view=grid"));

  // active filters as removable chips
  const collectionLabel = (v: string) => [...GENDERS, ...LINES, ...GROUPS].find((x) => x.v === v)?.label || v;
  const active: { key: string; label: string; clear: () => void }[] = [];
  if (p.q) active.push({ key: "q", label: COLLECTION_WORDS.has(p.q) ? collectionLabel(p.q) : `«${p.q}»`, clear: () => set({ q: null }) });
  if (p.gender) active.push({ key: "gender", label: collectionLabel(p.gender), clear: () => set({ gender: null }) });
  if (p.category) active.push({ key: "category", label: CAT_LABELS[p.category] || p.category, clear: () => set({ category: null }) });
  if (p.brand) active.push({ key: "brand", label: p.brand, clear: () => set({ brand: null }) });
  if (p.priceMin || p.priceMax) active.push({ key: "price", label: p.priceMin && p.priceMax ? `${rub(+p.priceMin)} – ${rub(+p.priceMax)}` : p.priceMin ? `от ${rub(+p.priceMin)}` : `до ${rub(+p.priceMax)}`, clear: () => set({ priceMin: null, priceMax: null }) });
  if (p.volume) active.push({ key: "volume", label: `${p.volume.replace(".", ",")} мл`, clear: () => set({ volume: null }) });
  if (p.group) active.push({ key: "group", label: collectionLabel(p.group), clear: () => set({ group: null }) });
  if (p.line) active.push({ key: "line", label: collectionLabel(p.line), clear: () => set({ line: null }) });
  if (p.inStock) active.push({ key: "inStock", label: "В наличии", clear: () => set({ inStock: null }) });
  const filterCount = active.length;

  const title = !showGrid ? "Каталог"
    : p.brand && active.length === 1 ? p.brand
    : p.category && active.length === 1 ? CAT_LABELS[p.category] || "Каталог"
    : p.q && active.length === 1 ? (COLLECTION_WORDS.has(p.q) ? collectionLabel(p.q) : `«${p.q}»`)
    : active.length ? "Подборка" : "Весь каталог";

  const quick = [...GENDERS.map((g) => ({ ...g, key: "gender" as const })), ...LINES.slice(0, 3).map((g) => ({ ...g, key: "line" as const }))];

  return (
    <div style={{ background: "var(--paper)", minHeight: "100vh" }}>
      <CategoryScene q={p.q} category={p.category} title={title} count={showGrid ? total : undefined} />

      {/* ── Sticky top bar ── */}
      <div className="cat-sticky mv-catbar">
        <div className="mv-catbar-row">
          <form className="mv-catsearch" onSubmit={(e) => { e.preventDefault(); const v = new FormData(e.currentTarget).get("q"); set({ q: String(v || "").trim() || null }); }}>
            <Search size={15} aria-hidden />
            <input name="q" type="search" defaultValue={COLLECTION_WORDS.has(p.q) ? "" : p.q} key={p.q} placeholder="Бренд, аромат или нота" enterKeyHint="search" aria-label="Поиск по каталогу" />
            {p.q && !COLLECTION_WORDS.has(p.q) && <button type="button" aria-label="Очистить поиск" onClick={() => set({ q: null })}><X size={15} /></button>}
          </form>
          <button type="button" className="cat-filter-btn mv-filterbtn" onClick={() => setFiltersOpen(true)}>
            <SlidersHorizontal size={15} /> Фильтры{filterCount > 0 && <b>{filterCount}</b>}
          </button>
        </div>

        <div className="scroll-x mv-catquick">
          {quick.map((c) => {
            const on = p[c.key] === c.v || p.q === c.v;
            return (
              <button key={c.v} type="button" className={clsx("cat-chip", on && "active")}
                onClick={() => set({ [c.key]: on ? null : c.v, ...(COLLECTION_WORDS.has(p.q) ? { q: null } : {}) })}>
                {c.label}
              </button>
            );
          })}
          {["testers", "sets"].map((c) => (
            <button key={c} type="button" className={clsx("cat-chip", p.category === c && "active")} onClick={() => set({ category: p.category === c ? null : c })}>
              {c === "testers" ? "Пробники" : "Наборы"}
            </button>
          ))}
        </div>
      </div>

      {/* ── Main content ── */}
      <div className={showGrid ? "cat-layout" : undefined} style={{ maxWidth: 1400, margin: "0 auto" }}>
        {showGrid && (
          <aside className="cat-sidebar">
            <FilterPanel p={p} facets={initialFacets} set={set} />
            {filterCount > 0 && <button type="button" className="mv-freset" onClick={resetAll}>Сбросить все фильтры</button>}
          </aside>
        )}

        <div style={{ padding: showGrid ? "clamp(16px,3vw,28px) clamp(16px,4vw,32px)" : 0, minWidth: 0 }}>
          {!showGrid ? (
            <div style={{ paddingTop: 28, paddingBottom: 40 }}>
              {autoCategories.length > 0
                ? autoCategories.map((cat, i) => <CategoryCarouselRow key={cat.slug} cat={cat} index={i} />)
                : (
                  <div style={{ padding: "0 clamp(16px,4vw,32px)" }} className="product-grid">
                    {Array.from({ length: 12 }).map((_, i) => <CardSkeleton key={i} />)}
                  </div>
                )}
            </div>
          ) : (
            <>
              <div className="mv-cathead">
                <span>{total.toLocaleString("ru-RU")} {ruPlural(total, "товар", "товара", "товаров")}</span>
                <label className="mv-sortsel">
                  <select value={p.sort} onChange={(e) => set({ sort: e.target.value || null })} aria-label="Сортировка">
                    {SORTS.map((s) => <option key={s.v} value={s.v}>{s.label}</option>)}
                  </select>
                  <ChevronDown size={14} aria-hidden />
                </label>
              </div>

              {active.length > 0 && (
                <div className="mv-factive">
                  {active.map((a) => (
                    <button key={a.key} type="button" onClick={a.clear}>{a.label}<X size={13} /></button>
                  ))}
                  <button type="button" className="reset" onClick={resetAll}>Сбросить всё</button>
                </div>
              )}

              {products.length > 0 ? (
                <>
                  <div className="product-grid">
                    {products.map((prod, i) => (
                      <div key={prod.offerId} className="anim-slide-up" style={{ animationDelay: `${Math.min(i, 12) * 0.03}s` }}>
                        <ProductCard product={prod} />
                      </div>
                    ))}
                  </div>
                  {totalPages > 1 && (
                    <Pagination
                      page={page}
                      total={totalPages}
                      onPage={(n) => {
                        set({ page: n > 1 ? String(n) : null });
                        window.scrollTo({ top: 0, behavior: "smooth" });
                      }}
                    />
                  )}
                </>
              ) : (
                <div className="mv-catempty">
                  <div style={{ fontSize: 44 }}>🔍</div>
                  <h2 className="serif">Ничего не найдено</h2>
                  <p>Попробуйте убрать часть фильтров или изменить запрос</p>
                  <button onClick={resetAll} className="btn-primary">Сбросить фильтры</button>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* ── Mobile filter sheet ── */}
      {filtersOpen && (
        <div className="mv-fsheet" role="dialog" aria-modal="true" aria-label="Фильтры">
          <div className="mv-fsheet-bg" onClick={() => setFiltersOpen(false)} />
          <div className="mv-fsheet-card">
            <div className="mv-fsheet-h">
              <b>Фильтры</b>
              {filterCount > 0 && <button type="button" className="mv-flink" onClick={resetAll}>Сбросить</button>}
              <button type="button" className="mv-fsheet-x" aria-label="Закрыть" onClick={() => setFiltersOpen(false)}><X size={18} /></button>
            </div>
            <div className="mv-fsheet-body">
              <FilterPanel p={p} facets={initialFacets} set={set} />
            </div>
            <div className="mv-fsheet-f">
              <button type="button" className="btn-primary" onClick={() => setFiltersOpen(false)}>
                Показать {total.toLocaleString("ru-RU")} {ruPlural(total, "товар", "товара", "товаров")}
              </button>
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
