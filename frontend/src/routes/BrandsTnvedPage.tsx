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

type OzonReport =
  | { building: true; cachedAt: null }
  | { noData: true; building: false }
  | { summary: OzonSummary; brands: BrandEntry[]; tnveds: TnvedEntry[]; cachedAt: string; fromCache: boolean; stale: boolean };

type YandexReport = {
  summary: YandexSummary;
  brands: BrandEntry[];
  categories: CatEntry[];
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
    <div style={{ display: "flex", gap: 2, marginBottom: 18, borderBottom: "1px solid var(--line)", paddingBottom: 0 }}>
      {(["ozon", "yandex"] as Tab[]).map((t) => (
        <button
          key={t}
          onClick={() => onChange(t)}
          style={{
            padding: "7px 18px",
            fontSize: 13,
            fontWeight: active === t ? 600 : 400,
            background: "none",
            border: "none",
            borderBottom: active === t ? "2px solid var(--accent)" : "2px solid transparent",
            color: active === t ? "var(--text)" : "var(--muted)",
            cursor: "pointer",
            marginBottom: -1,
          }}
        >
          {TAB_LABELS[t]}
        </button>
      ))}
    </div>
  );
}

function BrandsTable({
  brands,
  total,
  label,
}: {
  brands: BrandEntry[];
  total: number;
  label: string;
}) {
  const [search, setSearch] = useState("");
  const filtered = search ? brands.filter((b) => b.brand.toLowerCase().includes(search.toLowerCase())) : brands;
  return (
    <section className="table-panel">
      <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 16px 6px", borderBottom: "1px solid var(--line)" }}>
        <BarChart2 size={15} style={{ opacity: 0.6 }} />
        <span style={{ fontWeight: 600, fontSize: 14 }}>{label} ({brands.length})</span>
      </div>
      <div style={{ padding: "8px 16px 4px" }}>
        <input
          className="pm-chip-input"
          style={{ width: "100%" }}
          placeholder="Поиск бренда…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>
      <div style={{ overflowY: "auto", maxHeight: 480 }}>
        {filtered.length === 0 ? (
          <div className="soft-empty" style={{ padding: "16px" }}>Ничего не найдено</div>
        ) : (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--line)", background: "var(--panel-2)" }}>
                <th style={{ textAlign: "left", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>Бренд</th>
                <th style={{ textAlign: "right", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>SKU</th>
                <th style={{ textAlign: "right", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>%</th>
              </tr>
            </thead>
            <tbody>
              {filtered.slice(0, 200).map((b) => (
                <tr key={b.brand} style={{ borderBottom: "1px solid var(--line)" }}>
                  <td style={{ padding: "6px 16px" }}>{b.brand}</td>
                  <td style={{ padding: "6px 16px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{b.count.toLocaleString("ru")}</td>
                  <td style={{ padding: "6px 16px", textAlign: "right", color: "var(--muted)", fontVariantNumeric: "tabular-nums" }}>{pct(b.count, total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
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
      <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginBottom: 12 }}>
        <a
          href="/api/catalog/brands-tnved/export-excel"
          className="secondary-action"
          style={{ textDecoration: "none", display: "inline-flex", alignItems: "center", gap: 6 }}
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

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, alignItems: "start" }}>
            <BrandsTable brands={data.brands} total={data.summary.total} label="Бренды" />

            <section className="table-panel">
              <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 16px 6px", borderBottom: "1px solid var(--line)" }}>
                <Tag size={15} style={{ opacity: 0.6 }} />
                <span style={{ fontWeight: 600, fontSize: 14 }}>Коды ТН ВЭД ({data.tnveds.length})</span>
              </div>
              <div style={{ padding: "8px 16px 4px" }}>
                <input
                  className="pm-chip-input"
                  style={{ width: "100%" }}
                  placeholder="Поиск кода или названия…"
                  value={tnvedSearch}
                  onChange={(e) => setTnvedSearch(e.target.value)}
                />
              </div>
              <div style={{ overflowY: "auto", maxHeight: 480 }}>
                {filteredTnved.length === 0 ? (
                  <div className="soft-empty" style={{ padding: "16px" }}>Ничего не найдено</div>
                ) : (
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr style={{ borderBottom: "1px solid var(--line)", background: "var(--panel-2)" }}>
                        <th style={{ textAlign: "left", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>Код</th>
                        <th style={{ textAlign: "left", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>Категория</th>
                        <th style={{ textAlign: "right", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>SKU</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTnved.slice(0, 200).map((t) => (
                        <tr key={t.code} style={{ borderBottom: "1px solid var(--line)" }}>
                          <td style={{ padding: "6px 16px", fontFamily: "monospace", whiteSpace: "nowrap" }}>{t.code}</td>
                          <td style={{ padding: "6px 16px", color: "var(--muted)", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={t.fullValue}>
                            {t.fullValue.replace(/^\d+\s*[-–]\s*/, "")}
                          </td>
                          <td style={{ padding: "6px 16px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{t.count.toLocaleString("ru")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </div>
          <div style={{ fontSize: 12, color: "var(--muted)", marginTop: 8 }}>
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
      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 12 }}>
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

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, alignItems: "start" }}>
            <BrandsTable brands={data.brands} total={data.summary.total} label="Бренды" />

            <section className="table-panel">
              <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "10px 16px 6px", borderBottom: "1px solid var(--line)" }}>
                <BarChart2 size={15} style={{ opacity: 0.6 }} />
                <span style={{ fontWeight: 600, fontSize: 14 }}>Категории ЯМ ({data.categories.length})</span>
              </div>
              <div style={{ padding: "8px 16px 4px" }}>
                <input
                  className="pm-chip-input"
                  style={{ width: "100%" }}
                  placeholder="ID или название категории…"
                  value={catSearch}
                  onChange={(e) => setCatSearch(e.target.value)}
                />
              </div>
              <div style={{ overflowY: "auto", maxHeight: 480 }}>
                {filteredCats.length === 0 ? (
                  <div className="soft-empty" style={{ padding: "16px" }}>Ничего не найдено</div>
                ) : (
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                    <thead>
                      <tr style={{ borderBottom: "1px solid var(--line)", background: "var(--panel-2)" }}>
                        <th style={{ textAlign: "left", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>Категория</th>
                        <th style={{ textAlign: "right", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>SKU</th>
                        <th style={{ textAlign: "right", padding: "6px 16px", fontWeight: 600, fontSize: 12, color: "var(--muted)" }}>%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredCats.slice(0, 200).map((c) => (
                        <tr key={c.catId} style={{ borderBottom: "1px solid var(--line)" }}>
                          <td style={{ padding: "6px 16px" }}>
                            {c.catName ? (
                              <>
                                <span>{c.catName}</span>
                                <span style={{ marginLeft: 6, fontSize: 11, color: "var(--muted)", fontFamily: "monospace" }}>{c.catId}</span>
                              </>
                            ) : (
                              <span style={{ fontFamily: "monospace" }}>{c.catId}</span>
                            )}
                          </td>
                          <td style={{ padding: "6px 16px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{c.count.toLocaleString("ru")}</td>
                          <td style={{ padding: "6px 16px", textAlign: "right", color: "var(--muted)", fontVariantNumeric: "tabular-nums" }}>{pct(c.count, data.summary.total)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </section>
          </div>
          <div style={{ fontSize: 12, color: "var(--muted)", marginTop: 8 }}>
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
