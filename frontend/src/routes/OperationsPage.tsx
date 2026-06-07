import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Copy, Loader2, RefreshCw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { OperationCreateSchema, OperationDetailSchema, OperationsSchema, SupplierCartCommitSchema, SupplierCartHistorySchema, SupplierCartPreviewSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { DiagnosticValue } from "../components/DiagnosticValue";
import { asRecord, compactDate, copyPlainText, errorMessage, numberValue } from "../lib/common";

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
  if (text.includes("pending") || text.includes("not_visible_after_api") || text.includes("accepted")) return "pending";
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
              {issues.slice(0, 20).map((item) => (
                <div className={`operation-issue ${item.type.replaceAll(" ", "-")}`} key={item.key}>
                  <strong>{item.type}</strong>
                  <span>{item.offerId}</span>
                  <small>{item.detail}</small>
                </div>
              ))}
              {issues.length > 20 && <div className="soft-empty compact">Показаны первые 20 записей. Полный список можно скопировать кнопкой выше.</div>}
            </div>
          ) : <div className="success-strip">Критичных ошибок в результате операции не найдено.</div>}
          {issues.length ? <div className="soft-empty compact">Повтор только ошибок будет доступен после того, как backend начнет сохранять точный список SKU для безопасного повторного запуска.</div> : null}
        </>
      )}
      {detailQuery.error && <div className="inline-error">{errorMessage(detailQuery.error)}</div>}
    </section>
  );
}

export function SupplierCartPanel() {
  const [marketplace, setMarketplace] = useState("all");
  const [limit, setLimit] = useState(100);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const queryClient = useQueryClient();
  const draftQuery = useQuery({
    queryKey: ["supplier-cart-draft"],
    queryFn: () => fetchJson("/api/supplier-cart/draft", SupplierCartPreviewSchema),
  });
  const generateMutation = useMutation({
    mutationFn: () => fetchJson("/api/supplier-cart/generate", SupplierCartPreviewSchema, mutationBody({ marketplace, limit })),
    onSuccess: (payload) => {
      queryClient.invalidateQueries({ queryKey: ["supplier-cart-draft"] });
      setSelected(new Set((payload.rows || []).filter((row) => row.ready && !row.alreadyCommitted).map((row) => row.key)));
    },
  });
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
  const historyQuery = useQuery({
    queryKey: ["supplier-cart-history"],
    queryFn: () => fetchJson("/api/supplier-cart/history", SupplierCartHistorySchema),
  });
  const previewData = generateMutation.data || draftQuery.data;
  const rows = previewData?.rows || [];
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
        <button className="secondary-action" type="button" onClick={() => generateMutation.mutate()} disabled={generateMutation.isPending}>
          {generateMutation.isPending ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Сгенерировать корзину
        </button>
      </div>
      <div className="control-grid compact-controls">
        <label>Маркетплейс
          <select value={marketplace} onChange={(event) => setMarketplace(event.target.value)}>
            <option value="all">Ozon + Yandex</option>
            <option value="ozon">Ozon</option>
            <option value="yandex">Yandex</option>
          </select>
        </label>
        <label>Лимит строк
          <input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 100))} />
        </label>
      </div>
      {previewData ? (
        <div className="operation-stats">
          <DiagnosticValue label="Найдено" value={previewData.total} />
          <DiagnosticValue label="Готово" value={previewData.ready} tone={previewData.ready ? "success" : ""} />
          <DiagnosticValue label="Уже в PM" value={previewData.alreadyCommitted} />
          <DiagnosticValue label="Пропущено" value={previewData.skipped} tone={previewData.skipped ? "warning" : ""} />
          <DiagnosticValue label="Черновик" value={previewData.generatedAt ? compactDate(previewData.generatedAt) : "-"} />
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
            {rows.map((row) => {
              const disabled = !row.ready || row.alreadyCommitted;
              return (
                <article className={`supplier-cart-row ${row.ready ? "ready" : "skipped"}`} key={row.key}>
                  <label className="checkline">
                    <input type="checkbox" disabled={disabled} checked={selected.has(row.key)} onChange={() => toggleRow(row.key)} />
                    <span>{row.marketplace.toUpperCase()} · {row.orderId || row.postingNumber || "-"} · {row.offerId}</span>
                  </label>
                  <strong>{row.productName || row.offerId}</strong>
                  <div className="meta-grid">
                    <span>Кол-во: {row.quantity}</span>
                    <span>Поставщик: {row.supplierName || "-"}</span>
                    <span>PM row: {row.offerRowId || "-"}</span>
                    <span>Цена PM: {row.price ? `${row.price} ${row.priceCurrency}` : "-"}</span>
                    <span>Доверие: {row.trustFactor ?? 100}/100</span>
                    <span>{row.orderCutoffTime ? `Заказы до ${row.orderCutoffTime}` : "Без дедлайна"}</span>
                    {row.reseller ? <span>Перекупщик</span> : null}
                  </div>
                  {row.alreadyCommitted ? <small>Уже в заявке PriceMaster: Doc {row.requestDocId}, Row {row.requestRowId}</small> : null}
                  {row.skipReason === "supplier_cutoff_passed_no_alternative" ? <small className="danger-text">Все подходящие поставщики уже закрыли прием заказов на сегодня.</small> : null}
                  {!row.ready && !row.alreadyCommitted ? <small className="danger-text">Причина: {row.skipReason || "не готово"}</small> : null}
                </article>
              );
            })}
          </div>
        </>
      ) : previewData ? <div className="soft-empty">Новых заказов для автокорзины не найдено.</div> : <div className="soft-empty">Утром после обновления прайсов нажмите “Сгенерировать корзину”: заказы сохранятся в черновик, и их можно будет спокойно отправить в PriceMaster позже.</div>}
      {commitMutation.data ? <div className="success-strip">Добавлено в PriceMaster: {commitMutation.data.inserted}. Проверено в PM: {commitMutation.data.verifiedRows}. База: {commitMutation.data.priceMasterDb || "-"}. Документы: {commitMutation.data.docIds.join(", ") || "-"}</div> : null}
      {generateMutation.error && <div className="inline-error">{errorMessage(generateMutation.error)}</div>}
      {commitMutation.error && <div className="inline-error">{errorMessage(commitMutation.error)}</div>}
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
  const startMutation = useMutation({
    mutationFn: (type: string) => fetchJson("/api/operations", OperationCreateSchema, mutationBody({
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
            : { limit },
    })),
    onSuccess: (payload) => {
      const id = String(payload.job?.id || "");
      if (id) setSelectedJobId(id);
      queryClient.invalidateQueries({ queryKey: ["operations"] });
    },
  });
  const jobs = jobsQuery.data?.jobs || [];
  const selectedJob = selectedJobId || String(jobs[0]?.id || "");
  return (
    <>
      <PageHeader title="Операции" subtitle="Массовые задачи, прогресс, частичные ошибки и быстрый повтор через очередь." action={<button className="secondary-action" onClick={() => jobsQuery.refetch()}><RefreshCw size={16} /> Обновить</button>} />
      <section className="control-grid">
        <label>Лимит товаров<input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 30000))} /></label>
        <label>Лимит отправки<input type="number" value={sendLimit} onChange={(event) => setSendLimit(numberValue(event.target.value, 5000))} /></label>
        <label>Остаток восстановления<input type="number" value={stock} onChange={(event) => setStock(numberValue(event.target.value, 3))} /></label>
        <label>Качество до<input type="number" value={threshold} onChange={(event) => setThreshold(numberValue(event.target.value, 40))} /></label>
        <label>AI drafts<input type="number" value={draftLimit} onChange={(event) => setDraftLimit(numberValue(event.target.value, 20))} /></label>
      </section>
      <section className="action-strip">
        <button className="primary-action" onClick={() => startMutation.mutate("linked-supplier-recovery")} disabled={startMutation.isPending}>Восстановить привязанные</button>
        <button className="primary-action" onClick={() => startMutation.mutate("ozon-linked-unarchive")} disabled={startMutation.isPending}>Вернуть Ozon автоархив</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("restore-archived-stock")} disabled={startMutation.isPending}>Восстановить архив</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("yandex-card-quality-ai-drafts")} disabled={startMutation.isPending}>AI качество карточек</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("yandex-price-push")} disabled={startMutation.isPending}>Отправить новые цены Yandex</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("sales-automation-run")} disabled={startMutation.isPending}>Запустить автоматизацию продаж</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("brand-index-rebuild")} disabled={startMutation.isPending}>Пересобрать бренды</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("repair-pricemaster-group-links")} disabled={startMutation.isPending}>Починить привязки Ozon/Yandex</button>
        <button className="secondary-action" onClick={() => startMutation.mutate("health-deep")} disabled={startMutation.isPending}>Глубокий health</button>
      </section>
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
    </>
  );
}
