import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Download, Loader2, PackageSearch, RefreshCw } from "lucide-react";
import { useState } from "react";
import { fetchJson } from "../api";
import { NoSupplierSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

function exportCsv(rows: Record<string, unknown>[], filename: string, headers: string[], fields: string[]) {
  const lines = [headers, ...rows.map(r => fields.map(f => `"${String(r[f] ?? "").replace(/"/g, '""')}"`))]
    .map(r => r.join(",")).join("\n");
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob(["﻿" + lines], { type: "text/csv;charset=utf-8" }));
  a.download = filename; a.click();
}

export function NoSupplierPage() {
  const [mpFilter, setMpFilter] = useState("");
  const query = useQuery({ queryKey: ["no-supplier"], queryFn: () => fetchJson("/api/warehouse/no-supplier", NoSupplierSchema) });
  const data = query.data;
  const alerts = (data?.alerts || []).filter(a => !mpFilter || (a as Record<string, unknown>).marketplace === mpFilter);
  return (
    <section className="page-section no-supplier-page">
      <PageHeader title="Ошибки наличия" subtitle="Товары с привязками, у которых сейчас нет активного поставщика или есть риск некорректного остатка." action={(
        <div className="row-actions">
          <button className="secondary-action" onClick={() => query.refetch()}><RefreshCw size={16} /> Обновить</button>
          {alerts.length > 0 && <button className="secondary-action" onClick={() => exportCsv(alerts as Record<string, unknown>[], "no-supplier.csv", ["offerId", "Название", "Маркетплейс", "Привязок", "Активных"], ["offerId", "name", "marketplace", "supplierCount", "availableSupplierCount"])}><Download size={16} /> CSV</button>}
        </div>
      )} />
      <section className="dashboard-metrics">
        <Stat label="Всего товаров" value={data?.total || 0} tone="accent" icon={<PackageSearch size={18} />} />
        <Stat label="Без поставщика" value={data?.withoutSupplier || 0} tone={data?.withoutSupplier ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="В списке" value={alerts.length} tone="accent" icon={<PackageSearch size={18} />} />
      </section>
      {(() => {
        const cachedAt = (data as Record<string, unknown>)?.cachedAt;
        return cachedAt ? (
          <div className="muted" style={{fontSize:12, marginBottom:8}}>
            Данные из кэша: {new Date(String(cachedAt)).toLocaleTimeString("ru")}
          </div>
        ) : null;
      })()}
      <section className="table-panel">
        {query.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю ошибки...</div>}
        <select value={mpFilter} onChange={e => setMpFilter(e.target.value)} style={{marginBottom:8}}>
          <option value="">Все маркетплейсы</option>
          <option value="ozon">Ozon</option>
          <option value="yandex">Яндекс</option>
          <option value="wb">WB</option>
        </select>
        {alerts.map((item) => (
          <article className="job-row" key={String(item.id || item.offerId)}>
            <div>
              <strong>{String(item.offerId || item.name || item.id)}</strong>
              <span>{String(item.name || "Без названия")} · {String(item.marketplace || "-")} · привязок {String(item.supplierCount || 0)} · активных {String(item.availableSupplierCount || 0)}</span>
            </div>
            <button className="secondary-action" type="button" onClick={() => { const url = `/app/warehouse/${encodeURIComponent(`offer:${String(item.offerId || "").toLowerCase()}`)}?q=${encodeURIComponent(String(item.offerId || ""))}`; window.history.pushState(null, "", url); window.dispatchEvent(new PopStateEvent("popstate")); }}>Открыть</button>
          </article>
        ))}
        {!query.isLoading && !alerts.length && <div className="soft-empty">Ошибок наличия нет.</div>}
      </section>
    </section>
  );
}
