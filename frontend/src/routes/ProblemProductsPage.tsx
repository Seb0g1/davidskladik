import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Loader2, RefreshCcw, Wrench } from "lucide-react";
import { useMemo, useState } from "react";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { ProblemProductsSchema, ProductRepairSchema } from "../types";

const text = (value: unknown) => String(value ?? "").trim();

const label = (value: unknown) => {
  const key = text(value);
  const labels: Record<string, string> = {
    no_supplier: "нет поставщика",
    no_price: "нет цены",
    api_error: "ошибка API",
    price_retry: "retry цен",
    ozon_autoarchive: "Ozon autoarchive",
    stock_only_manual_price_missing: "склад без ручной цены",
    brand_missing: "бренд не определен",
    image_missing: "нет фото",
    image_placeholder: "placeholder фото",
  };
  return labels[key] || key || "-";
};

export function ProblemProductsPage() {
  const [category, setCategory] = useState("all");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: ["problem-products", category],
    queryFn: () => {
      const params = new URLSearchParams({ category, limit: "500" });
      return fetchJson(`/api/problem-products?${params.toString()}`, ProblemProductsSchema);
    },
  });
  const repair = useMutation({
    mutationFn: () => fetchJson("/api/problem-products/repair", ProductRepairSchema, mutationBody({ productIds: Array.from(selected) })),
    onSuccess: () => {
      setSelected(new Set());
      void queryClient.invalidateQueries({ queryKey: ["problem-products"] });
      void queryClient.invalidateQueries({ queryKey: ["sales-automation"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });

  const items = query.data?.items || [];
  const summary = query.data?.summary || {};
  const categories = useMemo(() => Object.entries(summary).sort((a, b) => Number(b[1]) - Number(a[1])), [summary]);
  const toggle = (productId: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(productId)) next.delete(productId);
      else next.add(productId);
      return next;
    });
  };

  return (
    <section className="page-section problem-products-page">
      <PageHeader
        title="Проблемные товары"
        subtitle="Единая диагностика SKU: отсутствие поставщика, цены, фото, бренда и ошибки автоматизации."
        action={(
          <button className="secondary-action" type="button" onClick={() => query.refetch()} disabled={query.isFetching}>
            {query.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Обновить
          </button>
        )}
      />

      <section className="dashboard-metrics">
        <Stat label="Всего проблем" value={query.data?.total ?? 0} tone={query.data?.total ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        {categories.slice(0, 3).map(([key, count]) => (
          <Stat key={key} label={label(key)} value={Number(count)} tone="accent" icon={<AlertTriangle size={18} />} />
        ))}
      </section>

      <div className="control-grid">
        <label>
          Категория
          <SelectField
            ariaLabel="Категория проблемы"
            value={category}
            onChange={setCategory}
            options={[
              { value: "all", label: "Все проблемы" },
              ...categories.map(([key, count]) => ({ value: String(key), label: `${label(key)} · ${Number(count)}` })),
            ]}
          />
        </label>
        <button className="primary-action" type="button" disabled={!selected.size || repair.isPending} onClick={() => repair.mutate()}>
          {repair.isPending ? <Loader2 className="spin" size={16} /> : <Wrench size={16} />} Починить выбранные ({selected.size})
        </button>
      </div>

      {query.error ? <div className="inline-error">{String((query.error as Error).message || query.error)}</div> : null}
      {repair.error ? <div className="inline-error">{String((repair.error as Error).message || repair.error)}</div> : null}
      {repair.data ? (
        <div className="success-strip">
          {repair.data.accepted
            ? `Repair поставлен в очередь: ${text((repair.data.job as Record<string, unknown> | undefined)?.id || "") || "background job"}.`
            : `Repair завершен. Успешно: ${Number(repair.data.repaired || 0)}, ошибок: ${Number(repair.data.failed || 0)}`}
        </div>
      ) : null}

      <div className="table-panel price-table">
        <div className="table-head">
          <span></span><span>Маркет</span><span>Артикул</span><span>Категория</span><span>Причина</span><span>Ошибка</span>
        </div>
        {items.map((item) => {
          const productId = text(item.productId || item.id);
          return (
            <div className="table-row" key={`${productId}-${text(item.marketplace)}-${text(item.offerId)}`}>
              <span data-label="">
                <input type="checkbox" checked={selected.has(productId)} disabled={!productId} onChange={() => productId && toggle(productId)} />
              </span>
              <span data-label="Маркет">{text(item.marketplace)}</span>
              <span data-label="Артикул">{text(item.offerId)}</span>
              <span data-label="Категория"><AlertTriangle size={14} /> {label(item.category)}</span>
              <span data-label="Причина">{label(item.reason)}</span>
              <span data-label="Ошибка">{text(item.lastError) || "-"}</span>
            </div>
          );
        })}
        {!items.length && !query.isLoading ? <div className="empty-state">Проблем по выбранной категории нет.</div> : null}
      </div>
    </section>
  );
}
