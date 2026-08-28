import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Archive, CheckCircle2, Clock3, Hourglass, ListChecks, Loader2, RefreshCcw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";
import { OzonUnarchiveQueueSchema } from "../types";
import { useState } from "react";
import { z } from "zod";

type YandexFastStatus = {
  ok?: boolean;
  enabled?: boolean;
  running?: boolean;
  intervalSeconds?: number;
  nextRunAt?: string | null;
  lastRunAt?: string | null;
  archivedLinked?: number | null;
  lastResult?: {
    at?: string;
    products?: number;
    unarchived?: number;
    created?: number;
    locallyDeleted?: number;
    failed?: number;
    failedSample?: string[];
    stockSent?: number;
    priceQueued?: number;
  } | null;
};

const text = (value: unknown) => String(value ?? "").trim();

function formatDate(value: unknown) {
  const raw = text(value);
  if (!raw) return "-";
  const date = new Date(raw);
  return Number.isFinite(date.getTime()) ? date.toLocaleString("ru-RU") : raw;
}

function numberValue(value: unknown) {
  return Number(value || 0) || 0;
}

function hasLocalLimit(value: unknown) {
  return value !== null && value !== undefined && Number.isFinite(Number(value));
}

function availableLimitText(data: Record<string, unknown> | undefined) {
  return hasLocalLimit(data?.dailyLimit) ? String(numberValue(data?.availableToday)) : "без локального лимита";
}

function lastResultText(value: unknown) {
  if (!value || typeof value !== "object") return "пока не запускалась";
  const row = value as Record<string, unknown>;
  const selected = numberValue(row.selected);
  const unarchived = numberValue(row.unarchived);
  const pending = numberValue(row.unarchivePending);
  const queueSize = numberValue(row.queueSize);
  const error = text(row.error);
  if (error) return `ошибка: ${error}`;
  return `выбрано ${selected}, разархив ${unarchived}, ожидает ${pending}, в очереди ${queueSize}`;
}

function queueAttemptLabel(data: Record<string, unknown> | undefined) {
  const due = numberValue(data?.due);
  const unlimited = !hasLocalLimit(data?.dailyLimit);
  const availableToday = numberValue(data?.availableToday);
  const verificationPending = numberValue(data?.verificationPending);
  if (due > 0 && (unlimited || availableToday > 0)) return "сейчас";
  if (verificationPending > 0) return "ждет проверку Ozon";
  if (due > 0) return "ждет новый дневной лимит Ozon";
  return formatDate(data?.nextAutoRunAt || data?.nextRetryAt);
}

function warningLabel(value: unknown) {
  const warning = text(value);
  if (warning === "ozon_unarchive_verify_pending") return "Ozon принял, проверяем выход из архива";
  if (warning === "still_archived_after_unarchive") return "Ozon все еще держит в архиве";
  if (warning === "unarchive_not_visible_after_api") return "Ozon пока не показывает товар";
  if (warning === "ozon_unarchive_daily_limit_queued") return "ждет лимит Ozon";
  if (warning === "ozon_product_id_missing") return "нет Ozon product_id";
  if (warning === "ozon_unarchive_api_error") return "ошибка Ozon unarchive";
  if (warning.startsWith("ozon_unarchive_verify_pending:")) return `проверка Ozon: ${warning.replace("ozon_unarchive_verify_pending:", "").trim()}`;
  if (warning.startsWith("unarchive_verify_pending:")) return `проверка маркетплейса: ${warning.replace("unarchive_verify_pending:", "").trim()}`;
  return warning;
}

function itemStatusLabel(item: Record<string, unknown>) {
  const warning = text(item.warning);
  if (warning) return warningLabel(warning);
  if (item.due) {
    return !hasLocalLimit(item.dailyLimit) || numberValue(item.availableToday) > 0 ? "готов к разархиву" : "ждет лимит Ozon";
  }
  return warningLabel(item.status || "pending");
}

function itemWhenLabel(item: Record<string, unknown>) {
  const warning = text(item.warning);
  if (warning === "ozon_unarchive_verify_pending") return formatDate(item.nextRetryAt);
  if (item.due) {
    return !hasLocalLimit(item.dailyLimit) || numberValue(item.availableToday) > 0 ? "сейчас" : "после сброса лимита";
  }
  return formatDate(item.nextRetryAt);
}

function YandexFastBlock({ status, error }: { status: YandexFastStatus | null; error: string }) {
  const last = status?.lastResult;
  return (
    <>
      <div className="section-title">
        <div>
          <span>Yandex архив</span>
          <h3>Быстрый разархив Яндекса</h3>
        </div>
      </div>
      {error ? <div className="inline-error">{error}</div> : null}
      <div className="summary-grid">
        <div><span>В архиве (привязанные)</span><strong>{status?.archivedLinked ?? "…"}</strong></div>
        <div><span>Проход</span><strong>{status?.enabled ? (status?.running ? "идет" : `каждые ${status?.intervalSeconds ?? 60} c`) : "выключен"}</strong></div>
        <div><span>Следующий</span><strong>{formatDate(status?.nextRunAt)}</strong></div>
        <div><span>Последний</span><strong>{formatDate(status?.lastRunAt)}</strong></div>
        <div>
          <span>Последний результат</span>
          <strong>
            {last
              ? `разархив ${numberValue(last.unarchived)} · создано ${numberValue(last.created)} · удалено ${numberValue(last.locallyDeleted)} · остатки ${numberValue(last.stockSent)} · отказы ${numberValue(last.failed)}`
              : "архив пуст"}
          </strong>
        </div>
      </div>
      {last?.failed && last.failedSample?.length ? (
        <div className="info-strip">
          <Clock3 size={18} />
          <div>
            <strong>Яндекс отказал по {last.failed} товарам.</strong>
            <span>Артикулы: {last.failedSample.slice(0, 15).join(", ")}{last.failed > 15 ? "…" : ""}</span>
          </div>
        </div>
      ) : null}
    </>
  );
}

export function RecoveryQueuePage() {
  const queryClient = useQueryClient();
  const [queueSearch, setQueueSearch] = useState("");
  const queue = useQuery({
    queryKey: ["ozon", "unarchive-queue"],
    queryFn: () => fetchJson("/api/ozon/unarchive-queue?limit=1000", OzonUnarchiveQueueSchema),
  });
  const process = useMutation({
    mutationFn: () => fetchJson("/api/ozon/unarchive-queue/process", OzonUnarchiveQueueSchema, mutationBody({ limit: 100 })),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ozon", "unarchive-queue"] });
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
    },
  });
  const rebuildMut = useMutation({
    mutationFn: () => fetchJson("/api/ozon/unarchive-queue/rebuild", z.unknown(), { method: "POST" }),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ["ozon", "unarchive-queue"] }); },
  });
  const yandexFastQuery = useQuery({
    queryKey: ["yandex-fast-status"],
    queryFn: () => fetchJson("/api/yandex/fast-unarchive/status", z.unknown()),
    refetchInterval: 30_000,
  });
  const yandexFastStatus = yandexFastQuery.data as YandexFastStatus | undefined;
  const yandexFastError = yandexFastQuery.error ? String((yandexFastQuery.error as Error)?.message || yandexFastQuery.error) : "";
  const data = queue.data;
  const items = data?.items || [];
  const visibleItems = (items ?? []).filter(i => !queueSearch || String((i as Record<string, unknown>).offerId ?? "").includes(queueSearch) || String((i as Record<string, unknown>).sku ?? "").includes(queueSearch)).slice(0, 200);
  return (
    <section className="page-section">
      <PageHeader
        title="Очередь восстановления"
        subtitle="Ozon autoarchive и быстрый разархив Яндекса: что в очереди, что готово к попытке и что ждет лимит."
        action={(
          <button className="primary-action" type="button" onClick={() => process.mutate()} disabled={process.isPending || queue.isLoading || data?.autoRunning}>
            {process.isPending || data?.autoRunning ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Запустить сейчас
          </button>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Всего в очереди" value={data?.total ?? 0} tone="accent" icon={<ListChecks size={18} />} />
        <Stat label="Готовы к попытке" value={data?.due ?? 0} tone={data?.due ? "warn" : "success"} icon={<Hourglass size={18} />} />
        <Stat label="Ждут проверки Ozon" value={data?.verificationPending ?? 0} tone={data?.verificationPending ? "warn" : "success"} icon={<Clock3 size={18} />} />
        <Stat label="В архиве Яндекс" value={yandexFastStatus?.archivedLinked ?? "…"} tone="accent" icon={<Archive size={18} />} />
      </section>
      <YandexFastBlock status={yandexFastStatus ?? null} error={yandexFastError} />
      <div className="summary-grid">
        <div><span>Всего в очереди</span><strong>{data?.total ?? 0}</strong></div>
        <div><span>Готовы к попытке</span><strong>{data?.due ?? 0}</strong></div>
        <div><span>Ждут проверки Ozon</span><strong>{data?.verificationPending ?? 0}</strong></div>
        <div><span>Осталось лимита</span><strong>{availableLimitText(data)}</strong></div>
        <div><span>Следующая попытка</span><strong>{queueAttemptLabel(data)}</strong></div>
      </div>
      <div className="info-strip success">
        <CheckCircle2 size={18} />
        <div>
          <strong>Очередь обрабатывается автоматически.</strong>
          <span>
            Сервер проверяет Ozon autoarchive каждые 30 минут и продолжает восстановление после нового дневного лимита.
            Ручная кнопка нужна только как аварийный запуск.
          </span>
        </div>
      </div>
      <div className="summary-grid">
        <div><span>Автообработка</span><strong>{data?.autoEnabled ? (data?.autoRunning ? "идет" : "включена") : "выключена"}</strong></div>
        <div><span>Последний запуск</span><strong>{formatDate(data?.lastAutoRunAt)}</strong></div>
        <div><span>Следующий автостарт</span><strong>{formatDate(data?.nextAutoRunAt)}</strong></div>
        <div><span>Последний результат</span><strong>{lastResultText(data?.lastAutoResult)}</strong></div>
      </div>
      <div className="info-strip">
        <Clock3 size={18} />
        <div>
          <strong>Старая дата в строке не значит, что товар застрял.</strong>
          <span>
            Если статус “готов к разархиву”, строка уже ожидает ближайшую попытку. Если статус “ждет лимит Ozon”,
            цена и остаток поддерживаются, а разархив продолжится после сброса дневного лимита.
          </span>
        </div>
      </div>
      {process.error ? <div className="inline-error">{String((process.error as Error).message || process.error)}</div> : null}
      {queue.error ? <div className="inline-error">{String((queue.error as Error).message || queue.error)}</div> : null}
      <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
        <button className="secondary-action" onClick={() => { if (window.confirm("Перестроить очередь Ozon?")) rebuildMut.mutate(); }} disabled={rebuildMut.isPending}>Перестроить очередь</button>
        {rebuildMut.error ? <span className="inline-error" style={{ margin: 0 }}>{String((rebuildMut.error as Error)?.message || rebuildMut.error)}</span> : null}
      </div>
      <input type="text" placeholder="Поиск по SKU..." value={queueSearch} onChange={e => setQueueSearch(e.target.value)} style={{marginBottom:8, padding:"4px 8px", border:"1px solid var(--border)", borderRadius:4}} />
      <div className="table-panel queue-table">
        <div className="table-head">
          <span>SKU</span><span>OfferId</span><span>Цель</span><span>Статус</span><span>Попытки</span><span>Когда</span>
        </div>
        {items.length > visibleItems.length ? (
          <div className="table-note">Показаны первые {visibleItems.length} строк из {items.length}. Остальные останутся в очереди и обработаются автоматически по лимиту Ozon.</div>
        ) : null}
        {visibleItems.length ? visibleItems.map((item) => (
          <div className="table-row" key={text(item.queueKey || item.id || item.offerId)}>
            <span data-label="SKU">{text(item.id) || "-"}</span>
            <span data-label="OfferId">{text(item.offerId) || "-"}</span>
            <span data-label="Цель">{text(item.target) || "-"}</span>
            <span data-label="Статус">{itemStatusLabel(item)}</span>
            <span data-label="Попытки">{numberValue(item.attempts)}</span>
            <span data-label="Когда">{itemWhenLabel(item)}</span>
          </div>
        )) : <div className="empty-state">Очередь восстановления Ozon пустая.</div>}
      </div>
    </section>
  );
}
