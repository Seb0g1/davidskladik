import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Bot, CheckCircle2, ChevronDown, Loader2, Play, Send, X } from "lucide-react";
import { z } from "zod";
import { fetchJson } from "../api";
import { MarketplaceBadge } from "./MarketplaceBadge";

type Check = { ok: boolean; answersQuestion: boolean; issues: string[] };
type Pending = { id: string; marketplace: string; target: string; productName?: string; text: string; draft: string; facts: string[]; check: Check; createdAt: string };
type LogEntry = { at: string; kind: string; action: string; marketplace?: string; product?: string; question?: string; text?: string; error?: string; reason?: string; rating?: number; check?: string };
type Settings = { reviews: boolean; originality: boolean; questionDrafts: boolean };
type State = { ok: boolean; enabled: boolean; intervalMinutes: number; settings: Settings; lastRun: Record<string, unknown> | null; runRequestedAt: string | null; pending: Pending[]; log: LogEntry[] };

const time = (iso?: unknown) => (iso ? new Date(String(iso)).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—");

const MODES: Array<{ key: keyof Settings; label: string; hint: string }> = [
  { key: "reviews", label: "Отзывы 4–5★", hint: "ИИ отвечает и сразу отправляет" },
  { key: "originality", label: "Вопрос «оригинал?»", hint: "ответ с эмодзи сразу, без проверки" },
  { key: "questionDrafts", label: "Остальные вопросы", hint: "ИИ собирает данные товара, проверяет ответ — отправляете вы" },
];

function PendingCard({ item, onDone }: { item: Pending; onDone: () => void }) {
  const [text, setText] = useState(item.draft);
  const send = useMutation({
    mutationFn: () => fetchJson(`/api/feedback-autopilot/pending/${encodeURIComponent(item.id)}/send`, z.unknown(), { method: "POST", body: JSON.stringify({ text }) }),
    onSuccess: onDone,
  });
  const dismiss = useMutation({
    mutationFn: () => fetchJson(`/api/feedback-autopilot/pending/${encodeURIComponent(item.id)}/dismiss`, z.unknown(), { method: "POST", body: "{}" }),
    onSuccess: onDone,
  });
  const verified = item.check?.ok && item.check?.answersQuestion;
  return (
    <article className={`ap-draft${verified ? " is-ok" : " is-warn"}`}>
      <header>
        <MarketplaceBadge marketplace={item.marketplace} />
        <strong>{item.productName || "товар"}</strong>
        <small>{time(item.createdAt)}</small>
      </header>
      <p className="ap-question">«{item.text}»</p>
      <div className={`ap-check${verified ? " is-ok" : " is-warn"}`}>
        {verified ? <CheckCircle2 size={14} /> : <AlertTriangle size={14} />}
        {verified ? "Проверка: ответ подтверждён данными товара" : `Проверка: ${(item.check?.issues || []).join("; ") || "ответ не подтверждён данными — проверьте"}`}
      </div>
      <textarea value={text} onChange={(e) => setText(e.target.value)} rows={4} aria-label="Ответ покупателю" />
      {item.facts?.length ? (
        <details className="ap-facts">
          <summary>Данные о товаре ({item.facts.length})</summary>
          <ul>{item.facts.map((f) => <li key={f}>{f}</li>)}</ul>
        </details>
      ) : <div className="ap-facts-empty">Данных о товаре не нашлось — ответ на общих знаниях, проверьте внимательно.</div>}
      <div className="ap-actions">
        <button className="secondary-action" type="button" disabled={dismiss.isPending || send.isPending} onClick={() => dismiss.mutate()}>
          {dismiss.isPending ? <Loader2 className="spin" size={14} /> : <X size={14} />} Не отвечать
        </button>
        <button className="primary-action" type="button" disabled={send.isPending || !text.trim()} onClick={() => send.mutate()}>
          {send.isPending ? <Loader2 className="spin" size={14} /> : <Send size={14} />} Отправить
        </button>
      </div>
      {send.error || dismiss.error ? <div className="inline-error">{String(((send.error || dismiss.error) as Error).message)}</div> : null}
    </article>
  );
}

/** Autopilot settings, last run, drafts waiting for a person (questions tab) and the log. */
export function FeedbackAutopilot({ tab }: { tab: "reviews" | "questions" }) {
  const queryClient = useQueryClient();
  const state = useQuery({
    queryKey: ["feedback-autopilot"],
    queryFn: () => fetchJson("/api/feedback-autopilot", z.unknown()) as Promise<State>,
    refetchInterval: 30_000,
  });
  const refresh = () => void queryClient.invalidateQueries({ queryKey: ["feedback-autopilot"] });
  const saveSettings = useMutation({
    mutationFn: (patch: Partial<Settings>) => fetchJson("/api/feedback-autopilot/settings", z.unknown(), { method: "POST", body: JSON.stringify(patch) }),
    onSuccess: refresh,
  });
  const run = useMutation({
    mutationFn: () => fetchJson("/api/feedback-autopilot/run", z.unknown(), { method: "POST", body: "{}" }),
    onSuccess: refresh,
  });
  const data = state.data;
  if (!data) return state.isLoading ? <div className="ap-panel is-loading"><Loader2 className="spin" size={14} /> Автоответы…</div> : null;
  const last = data.lastRun || {};
  const pending = data.pending || [];
  const log = (data.log || []).filter((e) => (tab === "reviews" ? e.kind === "review" : e.kind === "question"));
  const lastText = last.at
    ? `${time(last.at)}: отзывов ${Number(last.reviewsAnswered || 0)}, «оригинал» ${Number(last.originalityAnswered || 0)}, черновиков ${Number(last.drafts || 0)}${Number(last.errors || 0) ? `, ошибок ${Number(last.errors)}` : ""}${last.error ? ` — ${String(last.error)}` : ""}`
    : "ещё не запускались";

  return (
    <section className="ap-panel" aria-label="Автоответы ИИ">
      <div className="ap-head">
        <strong><Bot size={16} /> Автоответы ИИ</strong>
        <span className="ap-last">Последний запуск {lastText} · каждые {data.intervalMinutes} мин</span>
        <button className="secondary-action" type="button" disabled={run.isPending || Boolean(data.runRequestedAt)} onClick={() => run.mutate()}>
          {run.isPending || data.runRequestedAt ? <Loader2 className="spin" size={14} /> : <Play size={14} />} {data.runRequestedAt ? "Запущено…" : "Запустить сейчас"}
        </button>
      </div>
      <div className="ap-modes">
        {MODES.map((m) => (
          <label key={m.key} className={`ap-mode${data.settings[m.key] ? " is-on" : ""}`}>
            <input type="checkbox" checked={data.settings[m.key]} disabled={saveSettings.isPending} onChange={(e) => saveSettings.mutate({ [m.key]: e.target.checked })} />
            <span><b>{m.label}</b><small>{m.hint}</small></span>
          </label>
        ))}
      </div>
      {!data.enabled ? <div className="inline-warn">Автоответы выключены на сервере (FEEDBACK_AUTOPILOT=false).</div> : null}

      {tab === "questions" && pending.length ? (
        <div className="ap-pending">
          <h3>Ответы на проверку <span>{pending.length}</span></h3>
          {pending.map((item) => <PendingCard key={item.id} item={item} onDone={refresh} />)}
        </div>
      ) : null}

      {log.length ? (
        <details className="ap-log">
          <summary>Журнал автоответов <ChevronDown size={14} /></summary>
          <ul>
            {log.slice(0, 30).map((e, i) => (
              <li key={`${e.at}-${i}`} className={`is-${e.action}`}>
                <time>{time(e.at)}</time>
                <span>{e.action === "sent" ? "Отправлено" : e.action === "draft" ? "Черновик" : e.action === "dismissed" ? "Не отвечаем" : "Ошибка"}{e.rating ? ` · ${e.rating}★` : ""}{e.reason ? ` · ${e.reason}` : ""}</span>
                <span className="ap-log-product">{e.product || e.question || ""}</span>
                {e.error ? <em>{e.error}</em> : null}
              </li>
            ))}
          </ul>
        </details>
      ) : null}
    </section>
  );
}
