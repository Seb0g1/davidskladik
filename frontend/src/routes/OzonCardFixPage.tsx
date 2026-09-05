import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  Bot,
  Check,
  ChevronDown,
  ChevronUp,
  Loader2,
  RefreshCw,
  Send,
  Wrench,
} from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import {
  OzonCardAiFixResponseSchema,
  OzonCardApplyResponseSchema,
  OzonCardErrorsResponseSchema,
  type OzonCardErrorItem,
} from "../types";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { errorMessage } from "../lib/common";

type FixResult = { name: string; description: string; model: string };
type LogEntry = { offerId: string; label: string; action: "fixed" | "applied"; at: string; model?: string };
type FilterMode = "all" | "rework" | "error";

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString("ru", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function ErrorBadge({ item }: { item: OzonCardErrorItem }) {
  const isRework = item.stateName?.includes("доработк") || item.state === "NOT_MODERATED";
  const isFail = item.validationState === "FAIL" || item.state === "STATE_FAILED";
  return (
    <div className="card-fix-badges">
      {isRework && <span className="badge badge-warn">На доработку</span>}
      {isFail && !isRework && <span className="badge badge-error">Ошибка</span>}
      {!isRework && !isFail && item.stateName && <span className="badge badge-warn">{item.stateName}</span>}
    </div>
  );
}

function DiffBlock({ label, before, after }: { label: string; before: string; after: string }) {
  if (!before && !after) return null;
  const changed = before !== after;
  return (
    <div className={`diff-field${changed ? " diff-changed" : ""}`}>
      <div className="diff-field-label">{label}</div>
      <div className="diff-columns">
        <div className="diff-col diff-col-before">
          <span className="diff-col-tag">До</span>
          <p>{before || "—"}</p>
        </div>
        <div className="diff-col diff-col-after">
          <span className="diff-col-tag">После (AI)</span>
          <p>{after || "—"}</p>
        </div>
      </div>
    </div>
  );
}

export function OzonCardFixPage() {
  const [fixResults, setFixResults] = useState<Record<string, FixResult>>({});
  const [appliedIds, setAppliedIds] = useState<Set<string>>(new Set());
  const [errorMap, setErrorMap] = useState<Record<string, string>>({});
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [log, setLog] = useState<LogEntry[]>([]);
  const [filter, setFilter] = useState<FilterMode>("all");

  const queryClient = useQueryClient();

  const errorsQuery = useQuery({
    queryKey: ["ozon-card-errors"],
    queryFn: () => fetchJson("/api/ozon/card-errors", OzonCardErrorsResponseSchema),
    staleTime: 120_000,
  });

  const aiFixMutation = useMutation({
    mutationFn: (item: OzonCardErrorItem) =>
      fetchJson(
        "/api/ozon/card-errors/ai-fix",
        OzonCardAiFixResponseSchema,
        mutationBody({ offerId: item.offerId, name: item.name, description: item.description, errors: item.errors }),
      ),
    onSuccess: (data) => {
      setFixResults((prev) => ({
        ...prev,
        [data.offerId]: { name: data.fixed.name, description: data.fixed.description, model: data.model },
      }));
      setErrorMap((prev) => { const next = { ...prev }; delete next[data.offerId]; return next; });
      setExpandedId(data.offerId);
      setLog((prev) => [
        { offerId: data.offerId, label: data.fixed.name.slice(0, 60) || data.offerId, action: "fixed" as const, at: new Date().toISOString(), model: data.model },
        ...prev,
      ].slice(0, 100));
    },
    onError: (error, item) => {
      setErrorMap((prev) => ({ ...prev, [item.offerId]: errorMessage(error) }));
    },
  });

  const applyMutation = useMutation({
    mutationFn: ({ item, fix }: { item: OzonCardErrorItem; fix: FixResult }) =>
      fetchJson(
        "/api/ozon/card-errors/apply",
        OzonCardApplyResponseSchema,
        mutationBody({
          offerId: item.offerId,
          warehouseProductId: item.warehouseProductId || undefined,
          name: fix.name,
          description: fix.description,
        }),
      ),
    onSuccess: (data, { item }) => {
      setAppliedIds((prev) => new Set([...prev, item.offerId]));
      setErrorMap((prev) => { const next = { ...prev }; delete next[item.offerId]; return next; });
      setLog((prev) => [
        { offerId: item.offerId, label: item.name.slice(0, 60) || item.offerId, action: "applied" as const, at: new Date().toISOString() },
        ...prev,
      ].slice(0, 100));
      void queryClient.invalidateQueries({ queryKey: ["ozon-card-errors"] });
    },
    onError: (error, { item }) => {
      setErrorMap((prev) => ({ ...prev, [item.offerId]: errorMessage(error) }));
    },
  });

  const allItems = errorsQuery.data?.items ?? [];

  const filteredItems = allItems.filter((item) => {
    if (filter === "rework") return item.stateName?.includes("доработк") || item.state === "NOT_MODERATED";
    if (filter === "error") return item.validationState === "FAIL" || item.state === "STATE_FAILED";
    return true;
  });

  const fixedCount = Object.keys(fixResults).length;
  const appliedCount = appliedIds.size;

  const filterTabs: Array<{ key: FilterMode; label: string; count?: number }> = [
    { key: "all", label: "Все", count: allItems.length },
    {
      key: "rework",
      label: "На доработку",
      count: allItems.filter((i) => i.stateName?.includes("доработк") || i.state === "NOT_MODERATED").length,
    },
    {
      key: "error",
      label: "Ошибки",
      count: allItems.filter((i) => i.validationState === "FAIL" || i.state === "STATE_FAILED").length,
    },
  ];

  return (
    <section className="page-section ozon-card-fix-page">
      <PageHeader
        title="Исправление карточек Ozon"
        subtitle="Товары с ошибками модерации и статусом «На доработку». AI исправляет текст — вы подтверждаете и отправляете."
        action={
          <button
            className="secondary-action"
            onClick={() => void queryClient.invalidateQueries({ queryKey: ["ozon-card-errors"] })}
          >
            <RefreshCw size={16} /> Обновить
          </button>
        }
      />

      <section className="dashboard-metrics">
        <Stat
          label="С ошибками"
          value={allItems.length}
          tone={allItems.length ? "warn" : "success"}
          icon={<AlertTriangle size={18} />}
        />
        <Stat label="Исправлено AI" value={fixedCount} tone="accent" icon={<Bot size={18} />} />
        <Stat label="Отправлено" value={appliedCount} tone="success" icon={<Check size={18} />} />
      </section>

      <div className="filter-tabs">
        {filterTabs.map(({ key, label, count }) => (
          <button
            key={key}
            type="button"
            className={`filter-tab${filter === key ? " is-active" : ""}`}
            onClick={() => setFilter(key)}
          >
            {label}
            {count !== undefined && count > 0 && <span className="filter-tab-count">{count}</span>}
          </button>
        ))}
      </div>

      {errorsQuery.isLoading && (
        <div className="inline-loading">
          <Loader2 className="spin" size={18} /> Загружаю карточки с ошибками из Ozon…
        </div>
      )}
      {errorsQuery.error && (
        <div className="inline-error">{errorMessage(errorsQuery.error)}</div>
      )}

      {!errorsQuery.isLoading && !filteredItems.length && (
        <div className="empty-state">
          <Check size={24} />
          <span>Карточек с ошибками не найдено</span>
        </div>
      )}

      <div className="card-fix-list">
        {filteredItems.map((item) => {
          const fix = fixResults[item.offerId];
          const isApplied = appliedIds.has(item.offerId);
          const isExpanded = expandedId === item.offerId;
          const isFixing = aiFixMutation.isPending && (aiFixMutation.variables as OzonCardErrorItem | undefined)?.offerId === item.offerId;
          const isApplying = applyMutation.isPending && (applyMutation.variables as { item: OzonCardErrorItem } | undefined)?.item.offerId === item.offerId;
          const itemError = errorMap[item.offerId];

          return (
            <div key={item.offerId} className={`card-fix-item${isApplied ? " is-applied" : ""}${fix ? " has-fix" : ""}`}>
              <div className="card-fix-header">
                {item.primaryImage ? (
                  <img src={item.primaryImage} alt="" className="card-fix-thumb" width={52} height={52} />
                ) : (
                  <div className="card-fix-thumb card-fix-thumb-empty" />
                )}

                <div className="card-fix-meta">
                  <strong className="card-fix-name" title={item.name}>{item.name || "Без названия"}</strong>
                  <span className="card-fix-offerid">{item.offerId}</span>
                  {!item.warehouseProductId && (
                    <span className="card-fix-badge-no-wh" title="Товар не найден в складе — применить нельзя">
                      Нет на складе
                    </span>
                  )}
                </div>

                <ErrorBadge item={item} />
                {isApplied && <span className="badge badge-success"><Check size={12} /> Отправлено</span>}

                <div className="card-fix-actions">
                  {!isApplied && (
                    <button
                      type="button"
                      className="secondary-action"
                      disabled={isFixing || aiFixMutation.isPending}
                      onClick={() => aiFixMutation.mutate(item)}
                    >
                      {isFixing ? <Loader2 size={14} className="spin" /> : <Bot size={14} />}
                      {fix ? "Переисправить" : "AI Исправить"}
                    </button>
                  )}

                  {fix && !isApplied && (
                    <button
                      type="button"
                      className="primary-action"
                      disabled={isApplying || !item.warehouseProductId}
                      title={!item.warehouseProductId ? "Товар не найден в складе" : undefined}
                      onClick={() => applyMutation.mutate({ item, fix })}
                    >
                      {isApplying ? <Loader2 size={14} className="spin" /> : <Send size={14} />}
                      Применить
                    </button>
                  )}

                  {fix && (
                    <button
                      type="button"
                      className="icon-action"
                      aria-label={isExpanded ? "Свернуть" : "Показать изменения"}
                      onClick={() => setExpandedId(isExpanded ? null : item.offerId)}
                    >
                      {isExpanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
                    </button>
                  )}
                </div>
              </div>

              {/* Error list */}
              {item.errors.length > 0 && (
                <div className="card-fix-errors">
                  {item.errors.map((err, idx) => (
                    <div key={idx} className={`card-fix-error-row ${err.level === "error" ? "is-error" : "is-warn"}`}>
                      <AlertTriangle size={13} />
                      {err.field && <span className="err-field">{err.field}:</span>}
                      <span className="err-desc">{err.description}</span>
                    </div>
                  ))}
                </div>
              )}
              {!item.errors.length && item.stateDescription && (
                <div className="card-fix-errors">
                  <div className="card-fix-error-row is-warn">
                    <AlertTriangle size={13} />
                    <span className="err-desc">{item.stateDescription}</span>
                  </div>
                </div>
              )}

              {itemError && <div className="inline-error small-error">{itemError}</div>}

              {/* Before / After diff */}
              {fix && isExpanded && (
                <div className="card-fix-diff">
                  <DiffBlock label="Название" before={item.name} after={fix.name} />
                  <DiffBlock label="Описание" before={item.description} after={fix.description} />
                  {fix.model && <div className="diff-model-note">Модель: {fix.model}</div>}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Action log */}
      {log.length > 0 && (
        <section className="card-fix-log">
          <h3><Wrench size={15} /> Журнал действий</h3>
          <div className="log-list">
            {log.map((entry, idx) => (
              <div key={idx} className={`log-entry log-${entry.action}`}>
                <span className="log-time">{formatTime(entry.at)}</span>
                <span className={`log-badge ${entry.action === "fixed" ? "log-badge-ai" : "log-badge-sent"}`}>
                  {entry.action === "fixed" ? "AI исправил" : "Отправлено на Ozon"}
                </span>
                <span className="log-label">{entry.label}</span>
                {entry.model && <span className="log-model">{entry.model}</span>}
              </div>
            ))}
          </div>
        </section>
      )}
    </section>
  );
}
