import { useQuery } from "@tanstack/react-query";
import { Loader2, RefreshCw } from "lucide-react";
import { fetchJson } from "../api";
import { NoSupplierSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

export function NoSupplierPage() {
  const query = useQuery({ queryKey: ["no-supplier"], queryFn: () => fetchJson("/api/warehouse/no-supplier", NoSupplierSchema) });
  const alerts = query.data?.alerts || [];
  return (
    <>
      <PageHeader title="Ошибки наличия" subtitle="Товары с привязками, у которых сейчас нет активного поставщика или есть риск некорректного остатка." action={<button className="secondary-action" onClick={() => query.refetch()}><RefreshCw size={16} /> Обновить</button>} />
      <section className="summary-grid three">
        <Stat label="Всего товаров" value={query.data?.total || 0} />
        <Stat label="Без поставщика" value={query.data?.withoutSupplier || 0} />
        <Stat label="В списке" value={alerts.length} />
      </section>
      <section className="table-panel">
        {query.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю ошибки...</div>}
        {alerts.map((item) => (
          <article className="job-row" key={String(item.id || item.offerId)}>
            <div>
              <strong>{String(item.offerId || item.name || item.id)}</strong>
              <span>{String(item.name || "Без названия")} · {String(item.marketplace || "-")} · привязок {String(item.supplierCount || 0)} · активных {String(item.availableSupplierCount || 0)}</span>
            </div>
            <a className="secondary-action" href={`/app/warehouse/${encodeURIComponent(`offer:${String(item.offerId || "").toLowerCase()}`)}?q=${encodeURIComponent(String(item.offerId || ""))}`}>Открыть</a>
          </article>
        ))}
        {!query.isLoading && !alerts.length && <div className="soft-empty">Ошибок наличия нет.</div>}
      </section>
    </>
  );
}
