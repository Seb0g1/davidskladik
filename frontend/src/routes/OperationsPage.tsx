import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Clock3, Copy, ListChecks, Loader2, Plus, RefreshCw, Repeat2, Search, Zap } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody, patchBody } from "../api";
import { OperationCreateSchema, OperationDetailSchema, OperationsSchema, SupplierAlternativesSchema, SupplierCartCommitSchema, SupplierCartHistorySchema, SupplierCartOverrideSchema, SupplierCartPreviewSchema, SupplierCartScheduleSchema } from "../types";
import { SupplierAltPicker } from "../components/SupplierAltPicker";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { asRecord, compactDate, copyPlainText, errorMessage, money, numberValue, useDebounced } from "../lib/common";

function jobStatusLabel(status: unknown): string {
  return ({ queued: "Ожидает", running: "В работе", completed: "Готово", failed: "Ошибка" } as Record<string, string>)[String(status || "")] || String(status || "-");
}

function jobSummary(job: Record<string, unknown>): string {
  const result = asRecord(job.result);
  const parts = [
    result.partial ? "частично" : "",
    Number.isFinite(Number(result.candidates)) ? `привязанных ${result.candidates}` : "",
    Number.isFinite(Number(result.recovered)) ? `восстановлено ${result.recovered}` : "",
    Number.isFinite(Number(result.sellableRecovered)) ? `готовы ${result.sellableRecovered}` : "",
    Number.isFinite(Number(result.unarchived)) ? `разархивировано ${result.unarchived}` : "",
    Number.isFinite(Number(result.queuedByDailyLimit)) && Number(result.queuedByDailyLimit) > 0 ? `очередь Ozon ${result.queuedByDailyLimit}` : "",
    Number.isFinite(Number(result.stockFailed)) && Number(result.stockFailed) > 0 ? `ошибки остатков ${result.stockFailed}` : "",
    Number.isFinite(Number(result.unarchiveFailed)) && Number(result.unarchiveFailed) > 0 ? `ошибки архива ${result.unarchiveFailed}` : "",
    Number.isFinite(Number(result.qualityLoaded)) ? `quality ${result.qualityLoaded}` : "",
    Number.isFinite(Number(result.lowQuality)) ? `ниже порога ${result.lowQuality}` : "",
    Number.isFinite(Number(result.draftsCreated)) ? `AI drafts ${result.draftsCreated}` : "",
    Number.isFinite(Number(result.imageDraftsCreated)) ? `AI фото ${result.imageDraftsCreated}` : "",
    String(result.imageGenerationStoppedReason || ""),
    String(job.error || ""),
  ].filter(Boolean);
  return parts.join(" · ") || String(job.summary || "");
}

function operationIssueType(value: unknown): "pending" | "expected marketplace block" | "queue error" | "hard error" {
  const text = JSON.stringify(value || {}).toLowerCase();
  // Use includes('"pending"') to avoid matching "payment_pending", "price_pending" etc.
  if (text.includes('"pending"') || text.includes("not_visible_after_api") || text.includes("accepted")) return "pending";
  if (text.includes("fbo") || text.includes("forbidden") || text.includes("marketplace block") || text.includes("already")) return "expected marketplace block";
  if (text.includes("queue") || text.includes("stalled") || text.includes("bullmq") || text.includes("redis")) return "queue error";
  return "hard error";
}

function operationIssues(job: Record<string, unknown>) {
  const result = asRecord(job.result);
  const directErrors = Array.isArray(result.errors) ? result.errors : [];
  const resultRows = Array.isArray(result.results) ? result.results : [];
  const warnings = Array.isArray(result.warnings) ? result.warnings : [];
  const failedRows = resultRows.filter((item) => {
    const row = asRecord(item);
    return row.ok === false || row.error || row.status === "failed";
  });
  const items = [...directErrors, ...failedRows, ...warnings].slice(0, 80);
  if (job.error) items.unshift({ error: job.error });
  return items.map((item, index) => {
    const row = asRecord(item);
    const offerId = row.offerId || row.sku || row.article || row.id || row.productId || "";
    const detail = row.error || row.detail || row.message || row.reason || row.code || JSON.stringify(item);
    const type = operationIssueType(item);
    return {
      key: `${offerId || "issue"}-${index}`,
      type,
      offerId: String(offerId || "-"),
      detail: String(detail || "-"),
    };
  });
}

function operationStatItems(job: Record<string, unknown>) {
  const result = asRecord(job.result);
  const keys = [
    ["processed", "Обработано"],
    ["products", "Товаров"],
    ["candidates", "Кандидаты"],
    ["recovered", "Восстановлено"],
    ["sellableRecovered", "Готовы"],
    ["restoredStocks", "Остатки"],
    ["unarchived", "Разархив"],
    ["unarchivePending", "Ожидают"],
    ["queuedByDailyLimit", "Очередь Ozon"],
    ["queueSize", "Всего в очереди"],
    ["stockFailed", "Ошибки остатков"],
    ["unarchiveFailed", "Ошибки архива"],
    ["failed", "Ошибки"],
    ["ozonSent", "Ozon цены"],
    ["ozonFailed", "Ozon ошибки"],
    ["ozonSkipped", "Ozon пропуск"],
    ["yandexSent", "Yandex цены"],
    ["yandexFailed", "Yandex ошибки"],
    ["yandexSkipped", "Yandex пропуск"],
    ["repairedGroups", "PM группы"],
    ["changedProducts", "Изменено товаров"],
    ["draftsCreated", "AI drafts"],
    ["imageDraftsCreated", "AI фото"],
  ];
  return keys
    .map(([key, label]) => ({ key, label, value: result[key] }))
    .filter((item) => Number.isFinite(Number(item.value)));
}

function OperationDetailPanel({ jobId }: { jobId: string }) {
  const [copied, setCopied] = useState(false);
  const detailQuery = useQuery({
    queryKey: ["operation", jobId],
    queryFn: () => fetchJson(`/api/operations/${encodeURIComponent(jobId)}`, OperationDetailSchema),
    enabled: Boolean(jobId),
    refetchInterval: (query) => {
      if (!query.state.data) return 3000;
      const status = String(asRecord(query.state.data?.job).status || "");
      return status === "queued" || status === "running" ? 3000 : false;
    },
  });
  const job = asRecord(detailQuery.data?.job);
  const result = asRecord(job.result);
  const issues = operationIssues(job);
  const stats = operationStatItems(job);
  const copyIssues = async () => {
    const text = issues.map((item) => `${item.type}: ${item.offerId} - ${item.detail}`).join("\n");
    const ok = await copyPlainText(text || jobSummary(job));
    setCopied(ok);
    window.setTimeout(() => setCopied(false), 1400);
  };

  if (!jobId) return null;

  return (
    <section className="operation-detail">
      {detailQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю детали операции...</div>}
      {!detailQuery.isLoading && (
        <>
          <div className="section-title">
            <div>
              <span>Детали операции</span>
              <h3>{String(job.title || job.type || "Операция")}</h3>
            </div>
            <button className="secondary-action" type="button" onClick={() => detailQuery.refetch()}><RefreshCw size={16} /> Обновить</button>
          </div>
          <div className="operation-meta">
            <span>{jobStatusLabel(job.status)} · {Math.round(numberValue(job.progress, 0))}%</span>
            <span>{String(job.user || "system")} · {compactDate(String(job.createdAt || ""))}</span>
            {job.finishedAt ? <span>Финиш: {compactDate(String(job.finishedAt))}</span> : null}
          </div>
          <div className="operation-progress"><span style={{ width: `${Math.max(0, Math.min(100, numberValue(job.progress, 0)))}%` }} /></div>
          <p className="operation-summary">{jobSummary(job) || String(result.summary || "")}</p>
          {stats.length ? (
            <div className="operation-stats">
              {stats.map((item) => <DiagnosticValue key={item.key} label={item.label} value={item.value} tone={Number(item.value) > 0 && String(item.key).toLowerCase().includes("failed") ? "danger" : ""} />)}
            </div>
          ) : null}
          {issues.length ? (
            <div className="operation-issues">
              <div className="section-title compact-title">
                <div><span>Ошибки и предупреждения</span><h3>{issues.length}</h3></div>
                <button className="secondary-action" type="button" onClick={copyIssues}><Copy size={16} /> {copied ? "Скопировано" : "Скопировать"}</button>
              </div>
              {issues.slice(0, 80).map((item) => (
                <div className={`operation-issue ${item.type.replaceAll(" ", "-")}`} key={item.key}>
                  <strong>{item.type}</strong>
                  <span>{item.offerId}</span>
                  <small>{item.detail}</small>
                </div>
              ))}
              {issues.length > 80 && <div className="soft-empty compact">Показаны первые 80 записей. Полный список можно скопировать кнопкой выше.</div>}
            </div>
          ) : <div className="success-strip">Критичных ошибок в результате операции не найдено.</div>}
          {issues.length ? <div className="soft-empty compact">Повтор только ошибок будет доступен после того, как backend начнет сохранять точный список SKU для безопасного повторного запуска.</div> : null}
        </>
      )}
      {detailQuery.error && <div className="inline-error">{errorMessage(detailQuery.error)}</div>}
    </section>
  );
}

const ManualOrderResultSchema = z.object({
  ok: z.boolean().optional(),
  inserted: z.number().optional().default(0),
  docIds: z.array(z.union([z.string(), z.number()])).optional().default([]),
  pickingCreated: z.number().optional().default(0),
}).passthrough();

export function SupplierCartPanel() {
  const [marketplace, setMarketplace] = useState("all");
  const [limit, setLimit] = useState(500);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [altKey, setAltKey] = useState<string | null>(null);
  const [q, setQ] = useState("");
  const [showManualOrder, setShowManualOrder] = useState(false);
  const [manualOfferId, setManualOfferId] = useState("");
  const [manualQty, setManualQty] = useState(1);
  const [manualMp, setManualMp] = useState("ozon");
  const [manualNote, setManualNote] = useState("");
  const [manualPartnerId, setManualPartnerId] = useState("");
  const [manualRowId, setManualRowId] = useState("");
  const debouncedManualOfferId = useDebounced(manualOfferId.trim(), 400);
  const queryClient = useQueryClient();
  const [isGenerating, setIsGenerating] = useState(false);
  const draftQuery = useQuery({
    queryKey: ["supplier-cart-draft"],
    queryFn: () => fetchJson("/api/supplier-cart/draft", SupplierCartPreviewSchema),
    refetchInterval: isGenerating ? 3000 : 5 * 60 * 1000,
  });
  const generatingQuery = useQuery({
    queryKey: ["supplier-cart-generating"],
    queryFn: () => fetchJson("/api/supplier-cart/generating", z.object({ generating: z.boolean(), startedAt: z.string().nullable().optional(), error: z.string().optional().nullable() }).passthrough()),
    refetchInterval: isGenerating ? 3000 : false,
    enabled: isGenerating,
  });
  const generateMutation = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/generate", SupplierCartPreviewSchema, mutationBody({ marketplace, limit })),
    onSuccess: () => {
      setIsGenerating(true);
      // Immediately overwrite stale cache (which might hold { generating: false } from the
      // last run) so the render-below useEffect doesn't prematurely clear isGenerating
      // before the first real poll from the worker arrives.
      queryClient.setQueryData(["supplier-cart-generating"], { generating: true, startedAt: new Date().toISOString() });
    },
  });
  const generateMutationRef = useRef(generateMutation);
  generateMutationRef.current = generateMutation;
  // Stop polling once server reports generation done (useEffect avoids render-body setState).
  useEffect(() => {
    if (isGenerating && generatingQuery.data?.generating === false) {
      setIsGenerating(false);
      generateMutationRef.current.reset(); // clear pre-generation draft so fresh draftQuery data is shown
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
    }
  }, [isGenerating, generatingQuery.data?.generating, queryClient]);
  const commitMutation = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/commit", SupplierCartCommitSchema, mutationBody({
      rows: (generateMutation.data || draftQuery.data)?.rows || [],
      keys: Array.from(selected),
    })),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["supplier-cart-history"] });
      queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
    },
  });
  const reconfirmMutation = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/reconfirm-marketplace", z.object({ ok: z.boolean(), confirmed: z.number(), skipped: z.number().optional() }).passthrough(), mutationBody({})),
  });
  const overrideMutation = useMutation({
    mutationFn: ({ key, partnerId, rowId }: { key: string; partnerId: string; rowId: string }) =>
      fetchJson(`/api/supplier-cart/draft/${encodeURIComponent(key)}/supplier`, SupplierCartOverrideSchema, mutationBody({ partnerId, rowId })),
    onSuccess: () => {
      setAltKey(null);
      // Черновик обновился на сервере — сбрасываем локальный результат generate,
      // иначе он перекроет свежие данные draft-запроса.
      generateMutation.reset();
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
    },
  });
  const manualSuppliersQuery = useQuery({
    queryKey: ["manual-order-suppliers", debouncedManualOfferId],
    queryFn: () => fetchJson(`/api/supplier-cart/alternatives?offerId=${encodeURIComponent(debouncedManualOfferId)}`, SupplierAlternativesSchema),
    enabled: debouncedManualOfferId.length >= 2 && showManualOrder,
    staleTime: 30_000,
  });
  const manualOptions = manualSuppliersQuery.data?.options ?? [];

  const manualOrderMutation = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/manual-order", ManualOrderResultSchema, mutationBody({
      offerId: manualOfferId.trim(),
      quantity: manualQty,
      marketplace: manualMp,
      note: manualNote.trim(),
      ...(manualPartnerId ? { partnerId: manualPartnerId, rowId: manualRowId } : {}),
    })),
    onSuccess: () => {
      setManualOfferId("");
      setManualQty(1);
      setManualNote("");
      setManualPartnerId("");
      setManualRowId("");
      setShowManualOrder(false);
      void queryClient.invalidateQueries({ queryKey: ["supplier-picking-list"] });
    },
  });
  const historyQuery = useQuery({
    queryKey: ["supplier-cart-history"],
    queryFn: () => fetchJson("/api/supplier-cart/history", SupplierCartHistorySchema),
  });
  const scheduleQuery = useQuery({
    queryKey: ["supplier-cart-schedule"],
    queryFn: () => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema),
  });
  const scheduleMutation = useMutation({
    mutationFn: (mode: string) => fetchJson("/api/supplier-cart/schedule", SupplierCartScheduleSchema, patchBody({ mode })),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["supplier-cart-schedule"] }),
  });
  const clearProcessedMutation = useMutation({
    mutationFn: (keys: string[]) => fetchJson("/api/supplier-cart/processed", z.object({ ok: z.boolean(), cleared: z.number() }).passthrough(), { method: "DELETE", body: JSON.stringify({ keys }) }),
    onSuccess: () => {
      generateMutation.reset();
      void queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
    },
  });
  const cartMode = scheduleQuery.data?.settings?.mode === "auto" ? "auto" : "draft";
  const previewData = generateMutation.data || draftQuery.data;
  const rows = previewData?.rows || [];
  const filteredRows = useMemo(() => {
    const words = q.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return rows;
    return rows.filter((row) => {
      const text = [row.productName, row.offerId, row.orderId, row.postingNumber, row.supplierName]
        .join(" ").toLowerCase();
      return words.every((w) => text.includes(w));
    });
  }, [q, rows]);
  const toggleRow = (key: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };
  const selectedReady = rows.filter((row) => selected.has(row.key) && row.ready && !row.alreadyCommitted);

  return (
    <section className="table-panel supplier-cart-panel">
      <div className="section-title">
        <div>
          <span>Автокорзина PriceMaster</span>
          <h3>Заказы Ozon/Yandex &rarr; заявки поставщикам</h3>
        </div>
        <div className="supplier-cart-actions">
          <button className="secondary-action" type="button" onClick={() => setShowManualOrder((v) => !v)}>
            <Plus size={16} /> Ручной заказ
          </button>
          <button className="secondary-action" type="button" onClick={() => generateMutation.mutate()} disabled={generateMutation.isPending || isGenerating}>
            {(generateMutation.isPending || isGenerating) ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />}
            {isGenerating ? "Обновляется…" : "Обновить корзину"}
          </button>
        </div>
      </div>
      <div className="control-grid compact-controls">
        <label>Маркетплейс
          <SelectField
            ariaLabel="Маркетплейс"
            value={marketplace}
            onChange={setMarketplace}
            options={[
              { value: "all", label: "Ozon + Yandex + WB" },
              { value: "ozon", label: "Ozon" },
              { value: "yandex", label: "Yandex" },
              { value: "wb", label: "Wildberries" },
            ]}
          />
        </label>
        <label>Лимит строк
          <input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 500))} />
        </label>
        <label>Отправка в PriceMaster
          <SelectField
            ariaLabel="Режим автокорзины"
            value={cartMode}
            onChange={(mode) => scheduleMutation.mutate(mode)}
            options={[
              { value: "draft", label: "Вручную: только список, в PM — кнопкой" },
              { value: "auto", label: "Автоматически: сразу в PM по расписанию" },
            ]}
          />
        </label>
        <label>Поиск
          <input value={q} onChange={(event) => setQ(event.target.value)} placeholder="SKU, товар, заказ, поставщик" />
        </label>
      </div>
      {showManualOrder ? (
        <div className="table-panel supplier-cart-manual-form">
          <div className="section-title compact-title">
            <div><span>Ручной заказ</span><h3>Добавить товар без заказа маркетплейса</h3></div>
            <button className="secondary-action" type="button" onClick={() => { setShowManualOrder(false); setManualOfferId(""); setManualPartnerId(""); setManualRowId(""); }}>Закрыть</button>
          </div>
          <div className="control-grid compact-controls">
            <label>SKU (артикул)
              <input value={manualOfferId} onChange={(e) => { setManualOfferId(e.target.value); setManualPartnerId(""); setManualRowId(""); }} placeholder="Артикул товара" />
            </label>
            <label>Количество
              <input type="number" min="1" value={manualQty} onChange={(e) => setManualQty(Math.max(1, Number(e.target.value) || 1))} />
            </label>
            <label>Маркетплейс (для поиска товара)
              <SelectField
                ariaLabel="Маркетплейс товара"
                value={manualMp}
                onChange={setManualMp}
                options={[
                  { value: "ozon", label: "Ozon" },
                  { value: "yandex", label: "Yandex" },
                  { value: "wb", label: "Wildberries" },
                ]}
              />
            </label>
            <label>Заметка (необязательно)
              <input value={manualNote} onChange={(e) => setManualNote(e.target.value)} placeholder="Комментарий к заказу" />
            </label>
          </div>
          {debouncedManualOfferId.length >= 2 && (
            <label>
              Поставщик{manualSuppliersQuery.isFetching ? <Loader2 className="spin" size={13} style={{ marginLeft: 6 }} /> : null}
              <select
                value={manualPartnerId ? `${manualPartnerId}|${manualRowId}` : ""}
                onChange={(e) => {
                  const val = e.target.value;
                  if (!val) { setManualPartnerId(""); setManualRowId(""); }
                  else { const [pid, ...rest] = val.split("|"); setManualPartnerId(pid); setManualRowId(rest.join("|") || ""); }
                }}
              >
                <option value="">Авто (по приоритету)</option>
                {manualOptions.map((opt) => {
                  const star = /сорин|инна/i.test(opt.supplierName) ? "★ " : "";
                  const price = opt.price > 0 ? ` — ${opt.price} ${opt.priceCurrency}` : "";
                  const status = opt.blocked ? " [блок]" : opt.cutoffPassed ? " [поздно]" : !opt.available ? " [нет]" : "";
                  return (
                    <option key={`${opt.partnerId}|${opt.rowId}`} value={`${opt.partnerId}|${opt.rowId}`} disabled={opt.blocked}>
                      {star}{opt.supplierName || opt.partnerId}{price}{status}
                    </option>
                  );
                })}
              </select>
              {manualOptions.length === 0 && !manualSuppliersQuery.isFetching && (
                <span className="muted-hint" style={{ fontSize: 12 }}>{manualSuppliersQuery.data?.skipReason || "Нет поставщиков для этого артикула"}</span>
              )}
            </label>
          )}
          <div className="supplier-cart-actions">
            <button
              className="primary-action"
              type="button"
              disabled={!manualOfferId.trim() || manualOrderMutation.isPending}
              onClick={() => manualOrderMutation.mutate()}
            >
              {manualOrderMutation.isPending ? <Loader2 className="spin" size={16} /> : <Plus size={16} />}
              {manualPartnerId
                ? `Заказать у ${manualOptions.find((o) => o.partnerId === manualPartnerId)?.supplierName || manualPartnerId}`
                : "Заказать у поставщика (авто)"}
            </button>
          </div>
          {manualOrderMutation.data ? (
            <div className="success-strip">
              Добавлено в PriceMaster: {manualOrderMutation.data.inserted}. Строк сборки: {manualOrderMutation.data.pickingCreated}. Документы: {manualOrderMutation.data.docIds.join(", ") || "-"}.
            </div>
          ) : null}
          {manualOrderMutation.error ? <div className="inline-error">{errorMessage(manualOrderMutation.error)}</div> : null}
        </div>
      ) : null}
      {cartMode === "draft" ? (
        <div className="soft-empty compact">
          Планировщик ({(scheduleQuery.data?.settings?.scheduleTimes || []).join(", ") || "09:30, 12:00, 15:00"} МСК) только формирует список.
          В PriceMaster строки попадают после нажатия «Добавить выбранное в PriceMaster» — там они создают неотправленные документы
          «Запросы клиентов» (заявка поставщику НЕ отсылается автоматически).
        </div>
      ) : (
        <div className="soft-empty compact">
          Планировщик сам добавляет готовые строки в PriceMaster (документы «Запросы клиентов», без рассылки поставщикам).
        </div>
      )}
      {scheduleMutation.error ? <div className="inline-error">{errorMessage(scheduleMutation.error)}</div> : null}
      {generatingQuery.data?.error ? <div className="inline-error">{String(generatingQuery.data.error)}</div> : null}
      {previewData ? (
        <div className="operation-stats">
          <DiagnosticValue label="Найдено" value={previewData.total} />
          <DiagnosticValue label="Готово" value={previewData.ready} tone={previewData.ready ? "success" : ""} />
          <DiagnosticValue label="Уже в PM" value={previewData.alreadyCommitted} />
          <DiagnosticValue label="Пропущено" value={previewData.skipped} tone={previewData.skipped ? "warning" : ""} />
          <DiagnosticValue label="Черновик" value={previewData.generatedAt ? compactDate(previewData.generatedAt) : "-"} />
          {previewData.generatedBy ? <DiagnosticValue label="Кем" value={previewData.generatedBy} /> : null}
        </div>
      ) : null}
      {previewData?.warnings?.length ? (
        <div className="inline-error">
          {previewData.warnings.map((warning) => `${String(warning.marketplace || "api")}: ${String(warning.error || "error")}`).join(" · ")}
        </div>
      ) : null}
      {rows.length ? (
        <>
          <div className="supplier-cart-actions">
            <button className="primary-action" type="button" disabled={!selectedReady.length || commitMutation.isPending} onClick={() => commitMutation.mutate()}>
              {commitMutation.isPending ? <Loader2 className="spin" size={16} /> : null}
              Добавить выбранное в PriceMaster ({selectedReady.length})
            </button>
            <button className="secondary-action" type="button" onClick={() => setSelected(new Set(rows.filter((row) => row.ready && !row.alreadyCommitted).map((row) => row.key)))}>
              Выбрать готовые
            </button>
            <button className="secondary-action" type="button" onClick={() => setSelected(new Set())}>Снять выбор</button>
          </div>
          <div className="supplier-cart-list">
            {q && filteredRows.length !== rows.length ? <div className="soft-empty compact"><Search size={14} /> {filteredRows.length} из {rows.length}</div> : null}
            {filteredRows.map((row) => {
              const disabled = !row.ready || row.alreadyCommitted;
              return (
                <article className={`supplier-cart-row ${row.ready ? "ready" : "skipped"}${row.isExpress ? " is-express" : ""}`} key={row.key}>
                  <label className="checkline">
                    <input type="checkbox" disabled={disabled} checked={selected.has(row.key)} onChange={() => toggleRow(row.key)} />
                    <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
                    {row.isExpress ? <span className="express-badge"><Zap size={12} /> Экспресс</span> : null}
                  </label>
                  <strong>{row.productName || row.offerId}</strong>
                  <div className="meta-grid">
                    <span>Кол-во: {row.quantity}</span>
                    {row.saleAmount ? <span className="tone-success">Продажа: {money(row.saleAmount)}</span> : null}
                    <span>Поставщик: {row.supplierName || "-"}</span>
                    <span>PM row: {row.offerRowId || "-"}</span>
                    <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                    <span>Доверие: {row.trustFactor ?? 100}/100</span>
                    <span>{row.orderCutoffTime ? `Заказы до ${row.orderCutoffTime}` : "Без дедлайна"}</span>
                    {row.reseller ? <span>Перекупщик</span> : null}
                    {row.stockOnlyFallback ? <span>Складской fallback</span> : null}
                    {row.isExpress ? <span className="express-badge"><Zap size={12} /> Экспресс — подтверждение Ozon после «Собрал»</span> : null}
                  </div>
                  {row.alreadyCommitted ? (
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <small>Уже в заявке PriceMaster: Doc {row.requestDocId}, Row {row.requestRowId}</small>
                      <button className="secondary-action" type="button" style={{ padding: "1px 6px", fontSize: 11 }} disabled={clearProcessedMutation.isPending} onClick={() => clearProcessedMutation.mutate([row.key])} title="Снять пометку «уже в заявке» — если PM-строка была удалена или заказ устарел">
                        Открыть снова
                      </button>
                    </div>
                  ) : null}
                  {row.stockOnlyFallback ? <small>Заказ уйдёт через «Наш склад» — цена в PriceMaster будет 0, остаток со склада.</small> : null}
                  {row.skipReason === "supplier_cutoff_passed_no_alternative" ? <small className="danger-text">Все подходящие поставщики уже закрыли прием заказов на сегодня.</small> : null}
                  {!row.ready && !row.alreadyCommitted ? <small className="danger-text">Причина: {row.skipReason || "не готово"}</small> : null}
                  {!row.alreadyCommitted ? (
                    <div className="supplier-cart-actions">
                      <button className="secondary-action" type="button" disabled={overrideMutation.isPending} onClick={() => setAltKey(altKey === row.key ? null : row.key)}>
                        <Repeat2 size={14} /> Заменить поставщика
                      </button>
                    </div>
                  ) : null}
                  {altKey === row.key ? (
                    <SupplierAltPicker
                      offerId={row.offerId}
                      currentPartnerId={String(row.partnerId || "")}
                      busy={overrideMutation.isPending}
                      actionLabel="Выбрать"
                      onPick={(option) => overrideMutation.mutate({ key: row.key, partnerId: option.partnerId, rowId: option.rowId })}
                      onClose={() => setAltKey(null)}
                    />
                  ) : null}
                </article>
              );
            })}
          </div>
        </>
      ) : previewData ? <div className="soft-empty">Новых заказов для автокорзины не найдено.</div> : <div className="soft-empty">Загружаю заказы...</div>}
      {commitMutation.data ? (
        <>
          <div className="success-strip">Добавлено в PriceMaster: {commitMutation.data.inserted}. Проверено в PM: {commitMutation.data.verifiedRows}. База: {commitMutation.data.priceMasterDb || "-"}. Документы: {commitMutation.data.docIds.join(", ") || "-"}</div>
          {(commitMutation.data.pmBlocked?.length ?? 0) > 0 && (
            <div className="inline-warning" style={{ margin: "4px 0" }}>
              <strong>Не добавлено (уже в заявке PM):</strong> {commitMutation.data.pmBlocked!.length} шт. — заказ у поставщика уже открыт, товар ещё не получен (Recieved=0).{" "}
              Документы: {[...new Set(commitMutation.data.pmBlocked!.map((b) => b.existingDocId).filter(Boolean))].join(", ")}.{" "}
              {commitMutation.data.pmBlocked!.slice(0, 3).map((b) => b.offerId || b.productName).filter(Boolean).join(", ")}
              {(commitMutation.data.pmBlocked!.length ?? 0) > 3 ? ` и ещё ${commitMutation.data.pmBlocked!.length - 3}` : ""}.
            </div>
          )}
        </>
      ) : null}
      <div style={{ display: "flex", gap: 8, margin: "4px 0", flexWrap: "wrap" }}>
        <button className="secondary-action" type="button" disabled={reconfirmMutation.isPending} onClick={() => reconfirmMutation.mutate()} title="Повторно отправить подтверждение отгрузки на Ozon/Yandex для товаров, синхронизированных из PM">
          {reconfirmMutation.isPending ? <Loader2 className="spin" size={14} /> : null}
          Переотправить подтверждения отгрузки
        </button>
        {reconfirmMutation.data && <span className="tone-success" style={{ fontSize: 13, alignSelf: "center" }}>Подтверждено: {reconfirmMutation.data.confirmed}</span>}
        {reconfirmMutation.error && <span className="tone-warn" style={{ fontSize: 13, alignSelf: "center" }}>{errorMessage(reconfirmMutation.error)}</span>}
      </div>
      {generateMutation.error && <div className="inline-error">{errorMessage(generateMutation.error)}</div>}
      {commitMutation.error && <div className="inline-error">{errorMessage(commitMutation.error)}</div>}
      {overrideMutation.error && <div className="inline-error">Замена поставщика: {errorMessage(overrideMutation.error)}</div>}
      <div className="section-title compact-title">
        <div><span>История автокорзины</span><h3>{historyQuery.data?.totalProcessed || 0} обработано</h3></div>
        <button className="secondary-action" type="button" onClick={() => historyQuery.refetch()}><RefreshCw size={16} /> Обновить</button>
      </div>
      {(historyQuery.data?.history || []).slice(0, 5).map((item, index) => {
        const row = asRecord(item);
        return (
          <div className="soft-empty compact" key={`${String(row.at || "")}-${index}`}>
            {String(row.at || "")} · {String(row.user || "system")} · добавлено {String(row.inserted || 0)} · docs {Array.isArray(row.docIds) ? row.docIds.join(", ") : "-"}
          </div>
        );
      })}
    </section>
  );
}

export function OperationsPage() {
  const [limit, setLimit] = useState(30000);
  const [sendLimit, setSendLimit] = useState(5000);
  const [stock, setStock] = useState(3);
  const [threshold, setThreshold] = useState(40);
  const [draftLimit, setDraftLimit] = useState(20);
  const [selectedJobId, setSelectedJobId] = useState("");
  const queryClient = useQueryClient();
  const jobsQuery = useQuery({
    queryKey: ["operations"],
    queryFn: () => fetchJson("/api/operations?limit=80", OperationsSchema),
    refetchInterval: 5000,
  });
  const bulkStaleRecovery = useMutation({
    mutationFn: () => fetchJson("/api/warehouse/links/bulk-stale-recovery", z.object({
      ok: z.boolean(),
      found: z.number().default(0),
      fixed: z.number().default(0),
      productCount: z.number().default(0),
      recovered: z.number().default(0),
    }).passthrough(), mutationBody({})),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["warehouse"] }),
  });

  const startMutation = useMutation({
    mutationFn: (arg: string | { type: string; extraPayload?: Record<string, unknown> }) => {
      const type = typeof arg === "string" ? arg : arg.type;
      const extraPayload = typeof arg === "string" ? {} : (arg.extraPayload || {});
      return fetchJson("/api/operations", OperationCreateSchema, mutationBody({
        type,
        payload: type === "yandex-import-send"
          ? { limit, sendLimit }
          : type === "restore-archived-stock"
            ? { limit, stock, marketplace: "yandex" }
            : type === "ozon-linked-unarchive"
              ? { limit, marketplace: "ozon", force: true }
            : type === "yandex-card-quality-ai-drafts"
              ? { limit, threshold, draftLimit, generateImages: true }
              : type === "yandex-price-push"
                ? { limit, force: false, onlyChanged: true }
              : type === "initialize-linked-ozon-stock"
                ? { limit, stock, ...extraPayload }
              : { limit, ...extraPayload },
      }));
    },
    onSuccess: (payload) => {
      const id = String(payload.job?.id || "");
      if (id) setSelectedJobId(id);
      queryClient.invalidateQueries({ queryKey: ["operations"] });
    },
  });
  const jobs = jobsQuery.data?.jobs || [];
  const selectedJob = selectedJobId || String(jobs[0]?.id || "");
  const runningCount = jobs.filter((job) => job.status === "running").length;
  const queuedCount = jobs.filter((job) => job.status === "queued").length;
  const failedCount = jobs.filter((job) => job.status === "failed").length;
  return (
    <section className="page-section operations-page">
      <PageHeader title="Операции" subtitle="Массовые задачи, прогресс, частичные ошибки и быстрый повтор через очередь." action={<button className="secondary-action" onClick={() => jobsQuery.refetch()}><RefreshCw size={16} /> Обновить</button>} />
      <section className="dashboard-metrics">
        <Stat label="Всего задач" value={jobs.length} tone="accent" icon={<ListChecks size={18} />} />
        <Stat label="В работе" value={runningCount} tone={runningCount ? "warn" : "success"} icon={<Loader2 size={18} />} />
        <Stat label="В очереди" value={queuedCount} tone={queuedCount ? "warn" : "success"} icon={<Clock3 size={18} />} />
        <Stat label="Ошибки" value={failedCount} tone={failedCount ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
      </section>
      <section className="control-grid">
        <label>Лимит товаров<input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 30000))} /></label>
        <label>Лимит отправки<input type="number" value={sendLimit} onChange={(event) => setSendLimit(numberValue(event.target.value, 5000))} /></label>
        <label>Остаток восстановления<input type="number" value={stock} onChange={(event) => setStock(numberValue(event.target.value, 3))} /></label>
        <label>Качество до<input type="number" value={threshold} onChange={(event) => setThreshold(numberValue(event.target.value, 40))} /></label>
        <label>AI drafts<input type="number" value={draftLimit} onChange={(event) => setDraftLimit(numberValue(event.target.value, 20))} /></label>
      </section>
      <section className="action-strip">
        <button className="primary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("linked-supplier-recovery")}>Восстановить привязанные</button>
        <button className="primary-action" disabled={startMutation.isPending} onClick={() => { if (!window.confirm("Операция изменит данные на маркетплейсе. Продолжить?")) return; startMutation.mutate("ozon-linked-unarchive"); }}>Вернуть Ozon автоархив</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("restore-archived-stock")}>Восстановить архив</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("initialize-linked-ozon-stock")}>Инициализировать остатки Ozon (привязки)</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate({ type: "scan-and-fix-zero-stock", extraPayload: { dryRun: true } })}>Нулевые остатки: проверить (dry run)</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => { if (!window.confirm("Отправить остатки на маркетплейсы для всех привязанных товаров с нулём? Продолжить?")) return; startMutation.mutate({ type: "scan-and-fix-zero-stock", extraPayload: { dryRun: false } }); }}>Нулевые остатки: исправить</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => { if (!window.confirm("Принудительная инициализация — включит также товары, у которых ранее исчез поставщик, но сейчас привязка восстановлена. Продолжить?")) return; startMutation.mutate({ type: "initialize-linked-ozon-stock", extraPayload: { force: true } }); }}>Инициализировать Ozon (принудительно)</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("yandex-card-quality-ai-drafts")}>AI качество карточек</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("yandex-price-push")}>Отправить новые цены Yandex</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("sales-automation-run")}>Запустить автоматизацию продаж</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("brand-index-rebuild")}>Пересобрать бренды</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate({ type: "repair-dalik-disambiguation-links", extraPayload: { dryRun: true } })}>Далик: проверить привязки (dry run)</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => { if (!window.confirm("Исправить неверные привязки Далик? Это обновит exactName/sourceRowId в БД.")) return; startMutation.mutate({ type: "repair-dalik-disambiguation-links", extraPayload: { dryRun: false } }); }}>Далик: исправить привязки</button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => { if (!window.confirm("Операция изменит данные на маркетплейсе. Продолжить?")) return; startMutation.mutate("repair-pricemaster-group-links"); }}>Починить привязки Ozon/Yandex</button>
        <button
          className="secondary-action"
          disabled={bulkStaleRecovery.isPending}
          onClick={() => { if (!window.confirm("Найти все товары со устаревшими PM-привязками и починить их (обновить source_row_id + пересчитать остатки)? Может занять несколько минут.")) return; bulkStaleRecovery.mutate(); }}
          title="Ищет товары, у которых source_row_id указывает на неактивную PM-запись, обновляет его до актуального и пересчитывает остатки"
        >
          {bulkStaleRecovery.isPending ? <Loader2 className="spin" size={14} /> : null} Починить все стальные привязки (batch)
        </button>
        <button className="secondary-action" disabled={startMutation.isPending} onClick={() => startMutation.mutate("health-deep")}>Глубокий health</button>
      </section>
      {bulkStaleRecovery.data && (
        <div className="success-strip">
          Починено: найдено {bulkStaleRecovery.data.found} стальных привязок, исправлено {bulkStaleRecovery.data.fixed}, товаров обработано {bulkStaleRecovery.data.productCount}.
        </div>
      )}
      {bulkStaleRecovery.error && <div className="inline-error">{errorMessage(bulkStaleRecovery.error)}</div>}
      {startMutation.error && <div className="inline-error">{errorMessage(startMutation.error)}</div>}
      <section className="table-panel supplier-cart-panel">
        <div className="section-title">
          <div>
            <span>Автокорзина PriceMaster</span>
            <h3>Черновики закупок по заказам вынесены в отдельный раздел</h3>
          </div>
          <a className="secondary-action" href="/app/supplier-cart">Открыть автокорзину</a>
        </div>
      </section>
      <section className="table-panel">
        {jobsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю операции...</div>}
        {jobs.map((job) => (
          <article className={`job-row ${String(job.id) === selectedJob ? "is-selected" : ""}`} key={String(job.id)}>
            <div>
              <strong>{String(job.title || job.type)}</strong>
              <span>{jobStatusLabel(job.status)} · {String(job.user || "system")} · {compactDate(String(job.createdAt || ""))}</span>
              <small>{jobSummary(job)}</small>
            </div>
            <div className="row-actions">
              <button className="secondary-action" type="button" onClick={() => setSelectedJobId(String(job.id))}>Детали</button>
              <div className="progress-pill">{Math.round(numberValue(job.progress, 0))}%</div>
            </div>
          </article>
        ))}
        {!jobsQuery.isLoading && !jobs.length && <div className="soft-empty">Задач пока нет.</div>}
      </section>
      {selectedJob ? <OperationDetailPanel jobId={selectedJob} /> : null}
    </section>
  );
}
