import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, CalendarClock, Clock3, Database, ListChecks, Loader2, Package, RefreshCw, Repeat2, RotateCcw, Trash2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { SupplierAltPicker } from "../components/SupplierAltPicker";
import { SupplierPickingListSchema, SupplierPickingRowSchema, SupplierPickingUpdateSchema, SupplierReplaceResponseSchema } from "../types";
import { SupplierCartPanel } from "./OperationsPage";
import { compactDate, errorMessage } from "../lib/common";

type PickingRow = z.infer<typeof SupplierPickingRowSchema>;

const SupplierCartScheduleSchema = z.object({
  ok: z.boolean().optional(),
  settings: z.record(z.string(), z.unknown()).optional().default({}),
  autoRunning: z.boolean().optional().default(false),
  lastAutoRunAt: z.coerce.string().optional().nullable(),
  nextAutoRunAt: z.coerce.string().optional().nullable(),
  lastAutoResult: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

const RollbackSummarySchema = z.object({
  cartProcessed: z.number().optional().default(0),
  draftRows: z.number().optional().default(0),
  pickingRows: z.number().optional().default(0),
  supplierBlocks: z.number().optional().default(0),
  jsonHistory: z.number().optional().default(0),
  pm: z.record(z.string(), z.unknown()).optional().default({}),
  postgres: z.record(z.string(), z.unknown()).optional().nullable(),
}).passthrough();

const SupplierCartRollbackSchema = z.object({
  ok: z.boolean().optional(),
  dryRun: z.boolean().optional().default(true),
  before: RollbackSummarySchema.optional(),
  after: RollbackSummarySchema.optional(),
  pm: z.record(z.string(), z.unknown()).optional().default({}),
}).passthrough();

const PriceMasterStatusSchema = z.object({
  ok: z.boolean().optional().default(false),
  config: z.record(z.string(), z.unknown()).optional().default({}),
  db: z.coerce.string().optional().default(""),
  tables: z.record(z.string(), z.unknown()).optional().default({}),
  davidskladDocs: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  latestDocs: z.array(z.record(z.string(), z.unknown())).optional().default([]),
  latestRows: z.array(z.record(z.string(), z.unknown())).optional().default([]),
}).passthrough();

const text = (value: unknown) => String(value ?? "").trim();
const formatDate = (value: unknown) => {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
};

const countArray = (value: unknown) => Array.isArray(value) ? value.length : 0;
const rollbackCount = (summary: z.infer<typeof RollbackSummarySchema> | undefined) => {
  if (!summary) return 0;
  const pm = summary.pm || {};
  return Number(summary.cartProcessed || 0)
    + Number(summary.draftRows || 0)
    + Number(summary.pickingRows || 0)
    + Number(summary.supplierBlocks || 0)
    + countArray(pm.rowIds)
    + countArray(pm.docIds);
};

function ReadyToShipPanel() {
  const [q, setQ] = useState("");
  const [replaceKey, setReplaceKey] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const sessionQuery = useQuery({
    queryKey: ["session"],
    queryFn: () => fetchJson("/api/session", z.object({ role: z.coerce.string().optional().nullable() }).passthrough()),
    staleTime: 60_000,
  });
  const isAdmin = sessionQuery.data?.role === "admin";

  const listQuery = useQuery({
    queryKey: ["supplier-picking-list", "picked", "ready-to-ship"],
    queryFn: () => fetchJson("/api/supplier-picking-list?status=picked&limit=500", SupplierPickingListSchema),
    refetchInterval: 15_000,
  });

  const revertMutation = useMutation({
    mutationFn: (key: string) =>
      fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: "open" })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });

  // Revert to "open" then immediately replace supplier — both steps are needed because
  // replace-supplier rejects rows that aren't in "open" or "missing" status.
  const revertAndReplaceMutation = useMutation({
    mutationFn: async ({ key, partnerId, rowId }: { key: string; partnerId: string; rowId: string }) => {
      await fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}`, SupplierPickingUpdateSchema, patchBody({ status: "open" }));
      return fetchJson(`/api/supplier-picking-list/${encodeURIComponent(key)}/replace-supplier`, SupplierReplaceResponseSchema, mutationBody({ partnerId, rowId }));
    },
    onSuccess: () => {
      setReplaceKey(null);
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      void queryClient.invalidateQueries({ queryKey: ["suppliers"] });
    },
  });

  const rows = listQuery.data?.rows || [];
  const filteredRows = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return needle
      ? rows.filter((row: PickingRow) =>
          [row.productName, row.offerId, row.orderId, row.postingNumber, row.supplierName]
            .join(" ").toLowerCase().includes(needle))
      : rows;
  }, [q, rows]);

  return (
    <section className="table-panel supplier-cart-panel">
      <div className="section-title">
        <div>
          <span>Готовы к отгрузке</span>
          <h3>Позиции со статусом «собрано» — {rows.length} шт.</h3>
        </div>
        <button className="secondary-action" type="button" onClick={() => listQuery.refetch()} disabled={listQuery.isFetching}>
          {listQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
        </button>
      </div>
      <div className="control-grid compact-controls">
        <label>Поиск
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="SKU, товар, заказ, поставщик" />
        </label>
      </div>
      {!isAdmin ? (
        <div className="soft-empty compact">Замена поставщика и возврат к сборке доступны только администратору.</div>
      ) : null}
      {listQuery.error ? <div className="inline-error">{errorMessage(listQuery.error)}</div> : null}
      {revertMutation.error ? <div className="inline-error">{errorMessage(revertMutation.error)}</div> : null}
      {revertAndReplaceMutation.error ? <div className="inline-error">Замена поставщика: {errorMessage(revertAndReplaceMutation.error)}</div> : null}
      {revertAndReplaceMutation.data ? (
        <div className="success-strip">
          Перезаказано у «{revertAndReplaceMutation.data.supplierName || "нового поставщика"}»: заявка в PriceMaster создана (doc {revertAndReplaceMutation.data.docIds?.join(", ") || "-"}).
        </div>
      ) : null}
      <div className="supplier-cart-list">
        {listQuery.isLoading ? <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю...</div> : null}
        {filteredRows.map((row: PickingRow) => (
          <article className="supplier-cart-row ready" key={row.key}>
            <span className="checkline">
              <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
            </span>
            <strong>{row.productName || row.offerId}</strong>
            <div className="meta-grid">
              <span>Кол-во: {row.quantity}</span>
              <span>Поставщик: {row.supplierName || "-"}</span>
              <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
              <span>Собрал: {row.pickedBy || "-"} · {compactDate(row.pickedAt)}</span>
              <span>Doc/Row: {row.requestDocId || "-"}/{row.requestRowId || "-"}</span>
            </div>
            {isAdmin ? (
              <div className="supplier-cart-actions">
                <button
                  className="secondary-action"
                  type="button"
                  disabled={revertAndReplaceMutation.isPending || revertMutation.isPending}
                  onClick={() => setReplaceKey(replaceKey === row.key ? null : row.key)}
                >
                  <Repeat2 size={14} /> Заменить поставщика и заказать в PM
                </button>
                <button
                  className="secondary-action"
                  type="button"
                  disabled={revertMutation.isPending || revertAndReplaceMutation.isPending}
                  onClick={() => revertMutation.mutate(row.key)}
                >
                  <RotateCcw size={14} /> Вернуть к сборке
                </button>
              </div>
            ) : null}
            {replaceKey === row.key ? (
              <SupplierAltPicker
                offerId={row.offerId}
                currentPartnerId={row.partnerId}
                busy={revertAndReplaceMutation.isPending}
                actionLabel="Вернуть к сборке и заказать у него"
                onPick={(option) => revertAndReplaceMutation.mutate({ key: row.key, partnerId: option.partnerId, rowId: option.rowId })}
                onClose={() => setReplaceKey(null)}
              />
            ) : null}
          </article>
        ))}
        {!filteredRows.length && !listQuery.isLoading ? <div className="soft-empty">Позиций со статусом «собрано» нет.</div> : null}
      </div>
    </section>
  );
}

export function SupplierCartPage() {
  const [tab, setTab] = useState<"cart" | "ready">("cart");
  const queryClient = useQueryClient();
  const schedule = useQuery({
    queryKey: ["supplier-cart", "schedule"],
    queryFn: () => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema),
    refetchInterval: 30_000,
  });
  const toggle = useMutation({
    mutationFn: (autoEnabled: boolean) => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema, patchBody({ autoEnabled })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart", "schedule"] });
    },
  });
  const pmStatus = useQuery({
    queryKey: ["supplier-cart", "pricemaster-status"],
    queryFn: () => fetchJson("/api/supplier-cart/pricemaster/status", PriceMasterStatusSchema),
    refetchInterval: 60_000,
  });
  const rollbackDryRun = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/rollback-all", SupplierCartRollbackSchema, mutationBody({
      confirm: "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART",
      dryRun: true,
    })),
  });
  const rollbackApply = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/rollback-all", SupplierCartRollbackSchema, mutationBody({
      confirm: "ROLLBACK_DAVIDSKLAD_SUPPLIER_CART",
      dryRun: false,
    })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });
  const settings = schedule.data?.settings || {};
  const times = Array.isArray(settings.scheduleTimes) ? settings.scheduleTimes.map(text).filter(Boolean) : ["09:30", "12:00", "15:00"];
  const last = schedule.data?.lastAutoResult;
  const dryRun = rollbackDryRun.data?.before;
  const pm = dryRun?.pm || {};
  return (
    <section className="page-section supplier-cart-page">
      <PageHeader
        title="Автокорзина"
        subtitle="Заказы Ozon/Yandex автоматически собираются и отправляются в корзину PriceMaster по расписанию."
        action={<a className="secondary-action" href="/app/operations"><RefreshCw size={16} /> Операции</a>}
      />
      <section className="dashboard-metrics">
        <Stat label="Авторежим" value={settings.autoEnabled !== false ? "включено" : "выключено"} tone={settings.autoEnabled !== false ? "success" : "warn"} icon={<Clock3 size={18} />} />
        <Stat label="Следующий запуск" value={formatDate(schedule.data?.nextAutoRunAt)} tone="accent" icon={<CalendarClock size={18} />} />
        <Stat label="Документы ДавидСклад" value={pmStatus.data?.davidskladDocs?.length || 0} tone="accent" icon={<Database size={18} />} />
        <Stat label="Строки PM" value={pmStatus.data?.latestRows?.length || 0} tone="accent" icon={<ListChecks size={18} />} />
      </section>
      <section className="table-panel supplier-cart-schedule">
        <div className="section-title">
          <div>
            <span>Расписание</span>
            <h3>Автогенерация корзины</h3>
          </div>
          <button className="secondary-action" type="button" disabled={toggle.isPending} onClick={() => toggle.mutate(!(settings.autoEnabled !== false))}>
            {toggle.isPending ? <Loader2 className="spin" size={16} /> : <Clock3 size={16} />} {settings.autoEnabled !== false ? "Выключить авто" : "Включить авто"}
          </button>
        </div>
        <div className="summary-grid">
          <div><span>Статус</span><strong>{settings.autoEnabled !== false ? "включено" : "выключено"}</strong></div>
          <div><span>Время</span><strong>{times.join(" · ")}</strong></div>
          <div><span>Следующий запуск</span><strong>{formatDate(schedule.data?.nextAutoRunAt)}</strong></div>
          <div><span>Последний запуск</span><strong>{formatDate(schedule.data?.lastAutoRunAt)}</strong></div>
        </div>
        {last ? (
          <div className="success-strip">
            Последний запуск: всего {Number(last.total || 0)}, готово {Number(last.ready || 0)}, добавлено в PriceMaster {Number(last.inserted || 0)}, проверено в PM {Number(last.verifiedRows || 0)}, база {String(last.priceMasterDb || "-")}, создано строк сборки {Number(last.pickingCreated || 0)}, пропущено {Number(last.skipped || 0)}.
          </div>
        ) : null}
      </section>
      <section className="table-panel supplier-cart-schedule">
        <div className="section-title">
          <div>
            <span>PriceMaster</span>
            <h3>Диагностика корзины и полный откат</h3>
          </div>
          <button className="secondary-action danger-action" type="button" disabled={rollbackDryRun.isPending} onClick={() => rollbackDryRun.mutate()}>
            {rollbackDryRun.isPending ? <Loader2 className="spin" size={16} /> : <AlertTriangle size={16} />} Проверить откат
          </button>
        </div>
        <div className="summary-grid">
          <div><span>PM база</span><strong>{pmStatus.data?.db || String(pmStatus.data?.config?.database || "-")}</strong></div>
          <div><span>RequestDocs</span><strong>{pmStatus.data?.tables?.requestDocs ? "ok" : "нет"}</strong></div>
          <div><span>Документы ДавидСклад</span><strong>{pmStatus.data?.davidskladDocs?.length || 0}</strong></div>
          <div><span>Последние строки PM</span><strong>{pmStatus.data?.latestRows?.length || 0}</strong></div>
        </div>
        {pmStatus.error ? <div className="inline-error">{String(pmStatus.error)}</div> : null}
        {dryRun ? (
          <div className="inline-warning">
            Будет очищено: processed {dryRun.cartProcessed}, черновик {dryRun.draftRows}, сборка {dryRun.pickingRows}, блокировки {dryRun.supplierBlocks}, PM rows {countArray(pm.rowIds)}, PM docs {countArray(pm.docIds)}.
            <button className="secondary-action danger-action" type="button" disabled={rollbackApply.isPending || rollbackCount(dryRun) === 0} onClick={() => rollbackApply.mutate()}>
              {rollbackApply.isPending ? <Loader2 className="spin" size={16} /> : <Trash2 size={16} />} Откатить автокорзину и сборку
            </button>
          </div>
        ) : null}
        {rollbackApply.data ? <div className="success-strip">Откат выполнен. Осталось строк сборки: {rollbackApply.data.after?.pickingRows || 0}, PM rows: {countArray(rollbackApply.data.after?.pm?.rowIds)}.</div> : null}
        {rollbackDryRun.error ? <div className="inline-error">{String(rollbackDryRun.error)}</div> : null}
        {rollbackApply.error ? <div className="inline-error">{String(rollbackApply.error)}</div> : null}
      </section>
      <div className="page-tabs">
        <button className={`page-tab-btn${tab === "cart" ? " active" : ""}`} type="button" onClick={() => setTab("cart")}>
          <ListChecks size={15} /> Корзина
        </button>
        <button className={`page-tab-btn${tab === "ready" ? " active" : ""}`} type="button" onClick={() => setTab("ready")}>
          <Package size={15} /> Готовы к отгрузке
        </button>
      </div>
      {tab === "cart" ? <SupplierCartPanel /> : <ReadyToShipPanel />}
    </section>
  );
}
