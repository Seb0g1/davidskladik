import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BarChart2, Download, Loader2, RefreshCw, Tag } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

type BrandEntry = { brand: string; count: number; sample?: string[] };
type TnvedEntry = { code: string; fullValue: string; count: number; sample?: string[] };
type CatEntry = { catId: string; count: number; catName?: string };

type OzonSummary = { total: number; withBrand: number; missingBrand: number; withTnved: number; missingTnved: number };
type YandexSummary = { total: number; withVendor: number; missingVendor: number; withTnved: number; missingTnved: number };

type BrandTnvedEntry = {
  brand: string;
  totalCount: number;
  topTypes: string[];
  tnvedCodes: { code: string; fullValue: string; count: number }[];
};

type OzonReport =
  | { building: true; cachedAt: null }
  | { noData: true; building: false }
  | { summary: OzonSummary; brands: BrandEntry[]; tnveds: TnvedEntry[]; brandTnveds?: BrandTnvedEntry[]; cachedAt: string; fromCache: boolean; stale: boolean };

type BrandCategoryEntry = { catId: string; catName: string; count: number };
type BrandCategoriesEntry = { brand: string; totalCount: number; categories: BrandCategoryEntry[] };

type YandexReport = {
  summary: YandexSummary;
  brands: BrandEntry[];
  categories: CatEntry[];
  brandCategories?: BrandCategoriesEntry[];
  cachedAt: string;
  fromCache: boolean;
  stale: boolean;
};

function pct(n: number, total: number) {
  if (!total) return "0%";
  return `${Math.round((n / total) * 100)}%`;
}

type Tab = "ozon" | "yandex";

const TAB_LABELS: Record<Tab, string> = { ozon: "Ozon", yandex: "Яндекс" };

function TabBar({ active, onChange }: { active: Tab; onChange: (t: Tab) => void }) {
  return (
    <div className="bt-tabs">
      {(["ozon", "yandex"] as Tab[]).map((t) => (
        <button
          key={t}
          onClick={() => onChange(t)}
          className={`bt-tab${active === t ? " is-active" : ""}`}
        >
          {TAB_LABELS[t]}
        </button>
      ))}
    </div>
  );
}

function BrandsTable({ brands, total, label }: { brands: BrandEntry[]; total: number; label: string }) {
  const [search, setSearch] = useState("");
  const filtered = search ? brands.filter((b) => b.brand.toLowerCase().includes(search.toLowerCase())) : brands;
  return (
    <section className="table-panel">
      <div className="bt-panel-head">
        <BarChart2 size={15} className="bt-panel-icon" />
        <span className="bt-panel-title">{label} ({brands.length})</span>
      </div>
      <div className="bt-panel-search">
        <input
          className="pm-chip-input"
          placeholder="Поиск бренда…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>
      <div className="bt-panel-scroll bt-panel-scroll--md">
        {filtered.length === 0 ? (
          <div className="soft-empty bt-soft-empty-pad">Ничего не найдено</div>
        ) : (
          <table className="bt-data-table">
            <thead>
              <tr>
                <th>Бренд</th>
                <th className="right">SKU</th>
                <th className="right">%</th>
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 200).map((b) => (
                <tr key={b.brand}>
                  <td>{b.brand}</td>
                  <td className="bt-td-right bt-td-num">{b.count.toLocaleString("ru")}</td>
                  <td className="bt-td-right bt-td-num bt-td-muted">{pct(b.count, total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

function BrandTnvedTypesTable({ entries }: { entries: BrandTnvedEntry[] }) {
  const [search, setSearch] = useState("");
  const filtered = search
    ? entries.filter(
        (e) =>
          e.brand.toLowerCase().includes(search.toLowerCase()) ||
          (e.topTypes || []).some((t) => t.toLowerCase().includes(search.toLowerCase())),
      )
    : entries;

  const hasTypes = entries.some((e) => e.topTypes && e.topTypes.length > 0);

  return (
    <section className="table-panel bt-brand-section">
      <div className="bt-panel-head">
        <Tag size={15} className="bt-panel-icon" />
        <span className="bt-panel-title">Бренд · ТН ВЭД · Тип товара ({entries.length})</span>
        {!hasTypes && (
          <span className="bt-panel-hint">
            Типы появятся после следующего обновления данных Ozon
          </span>
        )}
      </div>
      <div className="bt-panel-search">
        <input
          className="pm-chip-input"
          placeholder="Поиск бренда или типа товара…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>
      <div className="bt-panel-scroll bt-panel-scroll--lg">
        <table className="bt-data-table">
          <thead>
            <tr>
              <th className="bt-col-brand">Бренд</th>
              <th className="bt-col-tnved">Код ТН ВЭД</th>
              <th>Тип товара</th>
              <th className="right bt-col-sku">SKU</th>
            </tr>
          </thead>
          <tbody>
            {filtered.slice(0, 300).map((entry) =>
              entry.tnvedCodes.map((tc, i) => (
                <tr key={`${entry.brand}:${tc.code}`}>
                  <td className={`bt-td-dense${i !== 0 ? " bt-td-ghost" : ""}${i === 0 ? " bt-td-bold" : ""}`}>
                    {i === 0 ? entry.brand : ""}
                  </td>
                  <td className="bt-td-dense bt-td-mono bt-td-muted">{tc.code}</td>
                  <td className="bt-td-dense bt-td-muted bt-td-sm">
                    {i === 0 && entry.topTypes && entry.topTypes.length > 0
                      ? entry.topTypes.map((t) => (
                          <span key={t} className="bt-type-chip">{t}</span>
                        ))
                      : null}
                  </td>
                  <td className="bt-td-dense bt-td-right bt-td-num">{tc.count.toLocaleString("ru")}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function OzonTab() {
  const qc = useQueryClient();
  const [tnvedSearch, setTnvedSearch] = useState("");

  const query = useQuery<OzonReport>({
    queryKey: ["brands-tnved-ozon"],
    queryFn: async () => {
      const r = await fetch("/api/catalog/brands-tnved", { credentials: "same-origin" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    },
    refetchInterval: (q) => {
      const d = q.state.data;
      if (d && "building" in d && d.building) return 5000;
      return false;
    },
    staleTime: 60_000,
  });

  const refresh = useMutation({
    mutationFn: async () => {
      const r = await fetch("/api/catalog/brands-tnved/refresh", { method: "POST", credentials: "same-origin" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["brands-tnved-ozon"] }),
  });

  const data = query.data;
  const isBuilding = (data && "building" in data && data.building) || false;
  const hasNoData = data && "noData" in data && data.noData;
  const hasReport = data && "summary" in data;

  const filteredTnved = hasReport
    ? data.tnveds.filter((t) => !tnvedSearch || t.code.includes(tnvedSearch) || t.fullValue.toLowerCase().includes(tnvedSearch.toLowerCase()))
    : [];

  return (
    <>
      <div className="bt-actions-row">
        <a
          href="/api/catalog/brands-tnved/export-excel"
          className="secondary-action bt-download-link"
          title="Скачать Excel: бренд → коды ТН ВЭД"
        >
          <Download size={15} /> Скачать Excel
        </a>
        <button
          className="secondary-action"
          onClick={() => refresh.mutate()}
          disabled={refresh.isPending || isBuilding}
        >
          {(refresh.isPending || isBuilding) ? <Loader2 size={15} className="spin" /> : <RefreshCw size={15} />}
          {isBuilding ? "Загружаю…" : "Обновить данные Ozon"}
        </button>
      </div>

      {(query.isLoading || (isBuilding && !hasReport)) ? (
        <div className="soft-empty">
          <Loader2 size={24} className="spin" />
          {isBuilding ? "Загружаю данные из Ozon… это может занять 1–2 минуты" : "Загружаю…"}
        </div>
      ) : null}

      {hasNoData && !isBuilding ? (
        <div className="soft-empty">
          <BarChart2 size={24} />
          <div>Данные ещё не загружены.</div>
          <button className="primary-action compact" onClick={() => refresh.mutate()} disabled={refresh.isPending}>
            <RefreshCw size={14} /> Загрузить из Ozon
          </button>
        </div>
      ) : null}

      {hasReport ? (
        <>
          <section className="dashboard-metrics">
            <Stat label="Всего товаров" value={data.summary.total.toLocaleString("ru")} icon={<BarChart2 size={18} />} tone="accent" />
            <Stat label="С брендом" value={`${data.summary.withBrand.toLocaleString("ru")} (${pct(data.summary.withBrand, data.summary.total)})`} icon={<Tag size={18} />} tone="success" />
            <Stat label="Без бренда" value={`${data.summary.missingBrand.toLocaleString("ru")} (${pct(data.summary.missingBrand, data.summary.total)})`} icon={<Tag size={18} />} tone={data.summary.missingBrand > 0 ? "warn" : "success"} />
            <Stat label="С ТН ВЭД" value={`${data.summary.withTnved.toLocaleString("ru")} (${pct(data.summary.withTnved, data.summary.total)})`} icon={<Tag size={18} />} tone="success" />
            <Stat label="Без ТН ВЭД" value={`${data.summary.missingTnved.toLocaleString("ru")} (${pct(data.summary.missingTnved, data.summary.total)})`} icon={<Tag size={18} />} tone={data.summary.missingTnved > 0 ? "warn" : "success"} />
          </section>

          <div className="bt-report-grid">
            <BrandsTable brands={data.brands} total={data.summary.total} label="Бренды" />

            <section className="table-panel">
              <div className="bt-panel-head">
                <Tag size={15} className="bt-panel-icon" />
                <span className="bt-panel-title">Коды ТН ВЭД ({data.tnveds.length})</span>
              </div>
              <div className="bt-panel-search">
                <input
                  className="pm-chip-input"
                  placeholder="Поиск кода или названия…"
                  value={tnvedSearch}
                  onChange={(e) => setTnvedSearch(e.target.value)}
                />
              </div>
              <div className="bt-panel-scroll bt-panel-scroll--md">
                {filteredTnved.length === 0 ? (
                  <div className="soft-empty bt-soft-empty-pad">Ничего не найдено</div>
                ) : (
                  <table className="bt-data-table">
                    <thead>
                      <tr>
                        <th>Код</th>
                        <th>Категория</th>
                        <th className="right">SKU</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTnved.slice(0, 200).map((t) => (
                        <tr key={t.code}>
                          <td className="bt-td-mono">{t.code}</td>
                          <td className="bt-td-muted bt-td-ellipsis" title={t.fullValue}>
                            {t.fullValue.replace(/^\d+\s*[-–]\s*/, "")}
                          </td>
                          <td className="bt-td-right bt-td-num">{t.count.toLocaleString("ru")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </div>

          {data.brandTnveds && data.brandTnveds.length > 0 && (
            <BrandTnvedTypesTable entries={data.brandTnveds} />
          )}

          <div className="bt-data-note">
            Данные от {new Date(data.cachedAt).toLocaleString("ru")}{data.stale ? " · устарели" : ""}
          </div>
        </>
      ) : null}
    </>
  );
}

function YandexTab() {
  const qc = useQueryClient();
  const [catSearch, setCatSearch] = useState("");

  const query = useQuery<YandexReport>({
    queryKey: ["brands-tnved-yandex"],
    queryFn: async () => {
      const r = await fetch("/api/catalog/brands-tnved/yandex", { credentials: "same-origin" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    },
    staleTime: 60_000,
  });

  const refresh = useMutation({
    mutationFn: async () => {
      const r = await fetch("/api/catalog/brands-tnved/yandex/refresh", { method: "POST", credentials: "same-origin" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return r.json();
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["brands-tnved-yandex"] }),
  });

  const data = query.data;

  const filteredCats = data
    ? catSearch
      ? data.categories.filter(
          (c) => c.catId.includes(catSearch) || c.catName?.toLowerCase().includes(catSearch.toLowerCase()),
        )
      : data.categories
    : [];

  return (
    <>
      <div className="bt-actions-row">
        <a
          href="/api/catalog/brands-tnved/combined/export-excel"
          className="secondary-action bt-download-link"
          title="Объединённый Excel: Ozon + Яндекс по брендам с кодами ТН ВЭД"
        >
          <Download size={15} /> Объединённый Excel
        </a>
        <a
          href="/api/catalog/brands-tnved/yandex/export-excel"
          className="secondary-action bt-download-link"
          title="Скачать Excel: бренды Яндекс по категориям"
        >
          <Download size={15} /> Скачать Excel
        </a>
        <button
          className="secondary-action"
          onClick={() => refresh.mutate()}
          disabled={refresh.isPending || query.isFetching}
        >
          {(refresh.isPending || query.isFetching) ? <Loader2 size={15} className="spin" /> : <RefreshCw size={15} />}
          Обновить данные Яндекс
        </button>
      </div>

      {query.isLoading ? (
        <div className="soft-empty">
          <Loader2 size={24} className="spin" />
          Загружаю…
        </div>
      ) : null}

      {data ? (
        <>
          <section className="dashboard-metrics">
            <Stat label="Всего товаров ЯМ" value={data.summary.total.toLocaleString("ru")} icon={<BarChart2 size={18} />} tone="accent" />
            <Stat label="С брендом" value={`${data.summary.withVendor.toLocaleString("ru")} (${pct(data.summary.withVendor, data.summary.total)})`} icon={<Tag size={18} />} tone="success" />
            <Stat label="Без бренда" value={`${data.summary.missingVendor.toLocaleString("ru")} (${pct(data.summary.missingVendor, data.summary.total)})`} icon={<Tag size={18} />} tone={data.summary.missingVendor > 0 ? "warn" : "success"} />
            {data.summary.withTnved != null && (
              <Stat label="С ТН ВЭД" value={`${data.summary.withTnved.toLocaleString("ru")} (${pct(data.summary.withTnved, data.summary.total)})`} icon={<Tag size={18} />} tone="success" />
            )}
            {data.summary.missingTnved != null && (
              <Stat label="Без ТН ВЭД" value={`${data.summary.missingTnved.toLocaleString("ru")} (${pct(data.summary.missingTnved, data.summary.total)})`} icon={<Tag size={18} />} tone={data.summary.missingTnved > 0 ? "warn" : "success"} />
            )}
          </section>

          <div className="bt-report-grid">
            <BrandsTable brands={data.brands} total={data.summary.total} label="Бренды" />

            <section className="table-panel">
              <div className="bt-panel-head">
                <BarChart2 size={15} className="bt-panel-icon" />
                <span className="bt-panel-title">Категории ЯМ ({data.categories.length})</span>
              </div>
              <div className="bt-panel-search">
                <input
                  className="pm-chip-input"
                  placeholder="ID или название категории…"
                  value={catSearch}
                  onChange={(e) => setCatSearch(e.target.value)}
                />
              </div>
              <div className="bt-panel-scroll bt-panel-scroll--md">
                {filteredCats.length === 0 ? (
                  <div className="soft-empty bt-soft-empty-pad">Ничего не найдено</div>
                ) : (
                  <table className="bt-data-table">
                    <thead>
                      <tr>
                        <th>Категория</th>
                        <th className="right">SKU</th>
                        <th className="right">%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredCats.slice(0, 200).map((c) => (
                        <tr key={c.catId}>
                          <td>
                            {c.catName ? (
                              <>
                                <span>{c.catName}</span>
                                <span className="bt-cat-id">{c.catId}</span>
                              </>
                            ) : (
                              <span className="bt-td-mono">{c.catId}</span>
                            )}
                          </td>
                          <td className="bt-td-right bt-td-num">{c.count.toLocaleString("ru")}</td>
                          <td className="bt-td-right bt-td-num bt-td-muted">{pct(c.count, data.summary.total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </div>
          <div className="bt-data-note">
            Данные от {new Date(data.cachedAt).toLocaleString("ru")}{data.stale ? " · устарели" : ""}
          </div>
        </>
      ) : null}
    </>
  );
}

export function BrandsTnvedPage() {
  const [tab, setTab] = useState<Tab>("ozon");

  return (
    <section className="page-section">
      <PageHeader
        title="Бренды / ТН ВЭД"
        subtitle="Агрегация брендов и кодов ТН ВЭД по товарам маркетплейсов"
      />
      <TabBar active={tab} onChange={setTab} />
      {tab === "ozon" ? <OzonTab /> : <YandexTab />}
    </section>
  );
}
