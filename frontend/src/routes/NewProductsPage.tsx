import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { z } from "zod";
import { ArrowUpDown, ExternalLink, Loader2, Package, RefreshCw, Search, Sparkles, Tag, Truck } from "lucide-react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { compactDate, errorMessage, money } from "../lib/common";

const NewProductRowSchema = z.object({
  article: z.coerce.string(),
  name: z.coerce.string(),
  supplierName: z.coerce.string().optional().default(""),
  partnerId: z.coerce.string().optional().default(""),
  price: z.number().optional().default(0),
  currency: z.coerce.string().optional().default("USD"),
  priceRub: z.number().optional().default(0),
  docDate: z.coerce.string().optional().nullable(),
}).passthrough();

const BySupplierSchema = z.object({
  supplierName: z.coerce.string(),
  partnerId: z.coerce.string().optional().default(""),
  count: z.number(),
}).passthrough();

const NewProductsResponseSchema = z.object({
  ok: z.boolean().optional(),
  total: z.number().optional().default(0),
  shown: z.number().optional().default(0),
  usdRate: z.number().optional().default(95),
  priceMin: z.number().optional().default(0),
  priceMax: z.number().optional().default(0),
  rows: z.array(NewProductRowSchema).optional().default([]),
  bySupplier: z.array(BySupplierSchema).optional().default([]),
}).passthrough();

type NewProductRow = z.infer<typeof NewProductRowSchema>;

function priceLabel(row: NewProductRow): string {
  if (!row.price || row.price <= 0) return "-";
  if (row.currency === "USD") {
    return `${row.price.toFixed(0)} USD ≈ ${money(row.priceRub)}`;
  }
  return money(row.priceRub);
}

function ProductCard({ row }: { row: NewProductRow }) {
  const warehouseUrl = `/app/warehouse?q=${encodeURIComponent(row.article)}`;
  return (
    <article className="new-product-card">
      <div className="new-product-main">
        <div className="new-product-name-row">
          <strong className="new-product-name">{row.name}</strong>
          <span className="new-product-price">{priceLabel(row)}</span>
        </div>
        <div className="new-product-meta">
          <span className="new-product-article"><Tag size={11} /> {row.article}</span>
          {row.supplierName ? <span className="new-product-supplier"><Truck size={11} /> {row.supplierName}</span> : null}
          {row.docDate ? <span className="new-product-date">{compactDate(row.docDate)}</span> : null}
        </div>
      </div>
      <a
        className="secondary-action new-product-link"
        href={warehouseUrl}
        onClick={(e) => {
          e.preventDefault();
          window.history.pushState(null, "", warehouseUrl);
          window.dispatchEvent(new PopStateEvent("popstate"));
        }}
      >
        <ExternalLink size={13} /> Найти на складе
      </a>
    </article>
  );
}

export function NewProductsPage() {
  const [q, setQ] = useState("");
  const [draftQ, setDraftQ] = useState("");
  const [supplierFilter, setSupplierFilter] = useState("");
  const [sort, setSort] = useState("date");

  const params = useMemo(() => {
    const p = new URLSearchParams({ limit: "500", sort });
    if (q) p.set("q", q);
    if (supplierFilter) p.set("supplier", supplierFilter);
    return p.toString();
  }, [q, supplierFilter, sort]);

  const query = useQuery({
    queryKey: ["new-products", params],
    queryFn: () => fetchJson(`/api/new-products?${params}`, NewProductsResponseSchema),
    staleTime: 2 * 60_000,
  });

  const data = query.data;
  const rows = data?.rows || [];
  const bySupplier = data?.bySupplier || [];
  const usdRate = data?.usdRate || 95;

  const supplierOptions = useMemo(() => [
    { value: "", label: "Все поставщики" },
    ...bySupplier.map((s) => ({ value: s.supplierName, label: `${s.supplierName} (${s.count})` })),
  ], [bySupplier]);

  const submitSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setQ(draftQ.trim());
  };

  const priceRangeLabel = data && data.priceMax > 0
    ? `${money(data.priceMin)} – ${money(data.priceMax)}`
    : "-";

  return (
    <section className="page-section new-products-page">
      <PageHeader
        title="Новые товары"
        subtitle="Товары в прайсе поставщиков, которых ещё нет на наших маркетплейсах. Без тестеров и отливантов."
        action={
          <button className="secondary-action" type="button" onClick={() => query.refetch()} disabled={query.isFetching}>
            {query.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
          </button>
        }
      />

      <section className="dashboard-metrics">
        <Stat
          label="Новых товаров"
          value={query.isLoading ? "…" : (data?.total ?? 0)}
          tone={data?.total ? "accent" : ""}
          icon={<Sparkles size={18} />}
        />
        <Stat
          label="Поставщиков"
          value={bySupplier.length}
          tone="accent"
          icon={<Truck size={18} />}
        />
        <Stat
          label="Диапазон цен"
          value={query.isLoading ? "…" : priceRangeLabel}
          tone=""
          icon={<Package size={18} />}
        />
        <Stat
          label="Курс USD"
          value={`${usdRate} ₽`}
          tone=""
          icon={<Tag size={18} />}
        />
      </section>

      <div className="new-products-layout">
        <aside className="new-products-sidebar">
          <div className="section-title compact-title">
            <div><span>Поставщики</span><h3>{bySupplier.length}</h3></div>
          </div>
          <div className="new-products-supplier-list">
            <button
              className={`new-products-supplier-item${!supplierFilter ? " active" : ""}`}
              type="button"
              onClick={() => setSupplierFilter("")}
            >
              <span>Все поставщики</span>
              <strong>{data?.total ?? 0}</strong>
            </button>
            {bySupplier.map((s) => (
              <button
                key={s.partnerId || s.supplierName}
                className={`new-products-supplier-item${supplierFilter === s.supplierName ? " active" : ""}`}
                type="button"
                onClick={() => setSupplierFilter(supplierFilter === s.supplierName ? "" : s.supplierName)}
              >
                <span>{s.supplierName || "Без поставщика"}</span>
                <strong>{s.count}</strong>
              </button>
            ))}
          </div>
        </aside>

        <div className="new-products-main">
          <form className="new-products-search-bar" onSubmit={submitSearch}>
            <div className="new-products-search-input">
              <Search size={15} />
              <input
                value={draftQ}
                onChange={(e) => setDraftQ(e.target.value)}
                placeholder="Поиск по названию или артикулу…"
              />
              {draftQ ? (
                <button type="button" className="new-products-clear" onClick={() => { setDraftQ(""); setQ(""); }}>✕</button>
              ) : null}
            </div>
            <SelectField
              ariaLabel="Сортировка"
              value={sort}
              onChange={setSort}
              options={[
                { value: "date", label: "Сначала новые" },
                { value: "name", label: "По названию" },
                { value: "price", label: "Дешевле сначала" },
                { value: "price_desc", label: "Дороже сначала" },
              ]}
            />
            <button className="primary-action" type="submit">
              <Search size={14} /> Найти
            </button>
          </form>

          {supplierFilter ? (
            <div className="active-filter-strip">
              <Truck size={13} /> Фильтр: {supplierFilter}
              <button type="button" onClick={() => setSupplierFilter("")}>✕</button>
            </div>
          ) : null}

          {query.error ? <div className="inline-error">{errorMessage(query.error)}</div> : null}

          {query.isLoading ? (
            <div className="soft-empty"><Loader2 className="spin" size={18} /> Ищу новые товары у поставщиков…</div>
          ) : null}

          {!query.isLoading && !rows.length ? (
            <div className="empty-state">
              {q || supplierFilter
                ? "Ничего не найдено. Попробуйте изменить фильтр."
                : "Нет новых товаров: все позиции поставщиков уже есть на маркетплейсах."}
            </div>
          ) : null}

          {rows.length ? (
            <>
              <div className="new-products-count-strip">
                <ArrowUpDown size={13} />
                {data?.shown === data?.total
                  ? `${rows.length} товаров`
                  : `Показано ${rows.length} из ${data?.total}`}
                {q ? ` по запросу «${q}»` : ""}
              </div>
              <div className="new-products-list">
                {rows.map((row) => (
                  <ProductCard key={`${row.article}|${row.partnerId}`} row={row} />
                ))}
              </div>
              {(data?.shown ?? 0) < (data?.total ?? 0) ? (
                <div className="soft-empty compact">
                  Показано {data?.shown} из {data?.total}. Уточните поиск для более точного результата.
                </div>
              ) : null}
            </>
          ) : null}
        </div>
      </div>
    </section>
  );
}
