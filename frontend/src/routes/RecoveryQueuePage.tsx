import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Clock3, Loader2, RefreshCcw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { OzonUnarchiveQueueSchema } from "../types";

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

export function RecoveryQueuePage() {
  const queryClient = useQueryClient();
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
  const data = queue.data;
  const items = data?.items || [];
  const visibleItems = items.slice(0, 200);
  return (
    <section className="page-section">
      <div className="section-title">
        <div>
          <span>Ozon autoarchive</span>
          <h2>Очередь восстановления</h2>
        </div>
        <button className="primary-action" type="button" onClick={() => process.mutate()} disabled={process.isPending || queue.isLoading || data?.autoRunning}>
          {process.isPending || data?.autoRunning ? <Loader2 className="spin" size={16} /> : <RefreshCcw size={16} />} Запустить сейчас
        </button>
      </div>
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
