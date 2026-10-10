import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Bot, ChevronDown, Loader2, RefreshCw, Send, Sparkles, X } from "lucide-react";
import { apiJson, mutationBody } from "../api";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";
import { MarketplaceBadge } from "./MarketplaceBadge";
import "./chat-autopilot.css";

type Settings = { avito: boolean; ozon: boolean; yandex: boolean; originality: boolean; productAnswers: "auto" | "draft" | "off"; thankYou: boolean };
type Check = { ok: boolean; answersQuestion: boolean; issues: string[] };
type Pending = { id: string; marketplace: string; target: string; chatId: string; productName?: string; text: string; draft: string; check?: Check; facts?: string[]; createdAt: string };
type LogEntry = { at: string; kind: string; action: string; reason?: string; marketplace?: string; product?: string; question?: string; text?: string; error?: string; by?: string };
type State = {
  enabled: boolean; intervalMinutes: number; settings: Settings;
  lastRun: { at: string; error?: string; warnings?: string[] } | null; runRequestedAt: string | null;
  day: { originality: number; answered: number; thanked: number };
  pending: Pending[]; log: LogEntry[];
};

const KEY = ["chat-autopilot"];

function when(value?: string) {
  if (!value) return "";
  const d = new Date(value);
  return d.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function logLabel(entry: LogEntry) {
  if (entry.kind === "thank_you") return entry.action === "sent" ? "Спасибо за покупку" : "Спасибо за покупку — ошибка";
  if (entry.action === "draft") return "Черновик ждёт проверки";
  if (entry.action === "dismissed") return "Черновик отклонён";
  if (entry.action === "error") return "Ошибка";
  return entry.reason === "оригинальность" ? "Ответ «оригинал»" : entry.reason === "проверено человеком" ? "Отправлено после проверки" : "Ответ об аромате";
}

function PendingCard({ item }: { item: Pending }) {
  const queryClient = useQueryClient();
  const [text, setText] = useState(item.draft);
  const [showFacts, setShowFacts] = useState(false);
  const refresh = () => queryClient.invalidateQueries({ queryKey: KEY });
  const send = useMutation({
    mutationFn: () => apiJson("/api/chats/autopilot/pending/send", mutationBody({ id: item.id, text })),
    onSuccess: () => { toast.success("Ответ отправлен"); void refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const redraft = useMutation({
    mutationFn: () => apiJson<{ item: Pending }>("/api/chats/autopilot/pending/redraft", mutationBody({ id: item.id })),
    onSuccess: (res) => { if (res.item) setText(res.item.draft); void refresh(); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const dismiss = useMutation({
    mutationFn: () => apiJson("/api/chats/autopilot/pending/dismiss", mutationBody({ id: item.id })),
    onSuccess: () => void refresh(),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const busy = send.isPending || redraft.isPending || dismiss.isPending;
  const issues = item.check && !(item.check.ok && item.check.answersQuestion) ? item.check.issues : [];
  return (
    <div className="cap-draft">
      <div className="cap-draft-head">
        <MarketplaceBadge marketplace={item.marketplace} />
        <strong>{item.productName || "Товар не определён"}</strong>
        <small>{when(item.createdAt)}</small>
      </div>
      <p className="cap-question">«{item.text}»</p>
      {issues.length ? <div className="cap-issues">Проверка сомневается: {issues.join("; ")}</div> : null}
      <textarea rows={3} value={text} onChange={(e) => setText(e.target.value)} disabled={busy} />
      <div className="cap-draft-actions">
        <button type="button" className="primary-action" disabled={busy || !text.trim()} onClick={() => send.mutate()}>
          {send.isPending ? <Loader2 className="spin" size={15} /> : <Send size={15} />} Отправить
        </button>
        <button type="button" className="secondary-action" disabled={busy} onClick={() => redraft.mutate()}>
          {redraft.isPending ? <Loader2 className="spin" size={15} /> : <Sparkles size={15} />} Переписать
        </button>
        <button type="button" className="secondary-action" disabled={busy} onClick={() => dismiss.mutate()}>
          <X size={15} /> Не отвечать
        </button>
        {item.facts?.length ? (
          <button type="button" className="cap-link" onClick={() => setShowFacts((v) => !v)}>
            Данные товара ({item.facts.length}) <ChevronDown size={13} className={showFacts ? "is-open" : ""} />
          </button>
        ) : null}
      </div>
      {showFacts ? <ul className="cap-facts">{item.facts!.map((f) => <li key={f}>{f}</li>)}</ul> : null}
    </div>
  );
}

/** Chat autopilot: what it answers by itself, drafts that wait for a person, and its log. */
export function ChatAutopilotPanel() {
  const queryClient = useQueryClient();
  const [logOpen, setLogOpen] = useState(false);
  const state = useQuery({ queryKey: KEY, queryFn: () => apiJson<State>("/api/chats/autopilot"), refetchInterval: 30_000 });
  const save = useMutation({
    mutationFn: (patch: Partial<Settings>) => apiJson<{ settings: Settings }>("/api/chats/autopilot/settings", mutationBody(patch)),
    onSuccess: (res) => queryClient.setQueryData<State>(KEY, (old) => (old ? { ...old, settings: res.settings } : old)),
    onError: (error) => toast.error(errorMessage(error)),
  });
  const run = useMutation({
    mutationFn: () => apiJson("/api/chats/autopilot/run", mutationBody({})),
    onSuccess: () => { toast.success("Проверим чаты в ближайшие полминуты"); void queryClient.invalidateQueries({ queryKey: KEY }); },
    onError: (error) => toast.error(errorMessage(error)),
  });
  const data = state.data;
  if (!data) return state.isError ? <div className="inline-error">{errorMessage(state.error)}</div> : null;
  const s = data.settings;
  const toggle = (key: keyof Settings, label: string) => (
    <label className="settings-toggle">
      <input type="checkbox" checked={Boolean(s[key])} disabled={save.isPending} onChange={(e) => save.mutate({ [key]: e.target.checked })} />
      {label}
    </label>
  );
  return (
    <section className="cap-panel">
      <div className="cap-head">
        <Bot size={18} />
        <strong>Автоответы</strong>
        <span className="cap-status">
          {!data.enabled ? "выключены на сервере" : data.lastRun?.error ? `ошибка: ${data.lastRun.error}` : data.lastRun ? `проверено ${when(data.lastRun.at)}, каждые ${data.intervalMinutes} мин` : "ещё не запускались"}
        </span>
        <button type="button" className="secondary-action" disabled={run.isPending || Boolean(data.runRequestedAt)} onClick={() => run.mutate()}>
          {run.isPending || data.runRequestedAt ? <Loader2 className="spin" size={15} /> : <RefreshCw size={15} />} Проверить сейчас
        </button>
      </div>
      <div className="cap-day">
        За сутки: <b>{data.day.originality}</b> ответов «оригинал» · <b>{data.day.answered}</b> об ароматах · <b>{data.day.thanked}</b> «спасибо за покупку»
      </div>
      <div className="cap-settings">
        <div className="cap-group">
          <span>Где</span>
          {toggle("avito", "Авито")}
          {toggle("ozon", "Ozon")}
          {toggle("yandex", "Яндекс Маркет")}
        </div>
        <div className="cap-group">
          <span>Что</span>
          {toggle("originality", "«Это оригинал?» — сразу")}
          {toggle("thankYou", "«Спасибо за покупку» на Авито после сделки")}
          <label className="cap-select">
            Вопросы об аромате, стране, сравнение
            <select value={s.productAnswers} disabled={save.isPending} onChange={(e) => save.mutate({ productAnswers: e.target.value as Settings["productAnswers"] })}>
              <option value="auto">отвечать, если проверка прошла</option>
              <option value="draft">только черновик</option>
              <option value="off">не трогать</option>
            </select>
          </label>
        </div>
      </div>
      {(data.lastRun?.warnings || []).map((w) => <div key={w} className="inline-warn">{w}</div>)}
      {data.pending.length ? (
        <div className="cap-drafts">
          <div className="cap-subhead">Ждут проверки: {data.pending.length}</div>
          {data.pending.map((item) => <PendingCard key={item.id} item={item} />)}
        </div>
      ) : null}
      <button type="button" className="cap-link" onClick={() => setLogOpen((v) => !v)}>
        Журнал ({data.log.length}) <ChevronDown size={13} className={logOpen ? "is-open" : ""} />
      </button>
      {logOpen ? (
        <ul className="cap-log">
          {data.log.map((entry, index) => (
            <li key={`${entry.at}-${index}`} className={entry.action === "error" ? "is-error" : ""}>
              <small>{when(entry.at)}</small>
              {entry.marketplace ? <MarketplaceBadge marketplace={entry.marketplace} /> : null}
              <b>{logLabel(entry)}</b>
              {entry.product ? <span className="cap-log-product">{entry.product}</span> : null}
              {entry.question ? <span className="cap-log-q">«{entry.question}»</span> : null}
              {entry.text ? <span className="cap-log-a">{entry.text}</span> : null}
              {entry.error ? <span className="cap-log-a">{entry.error}</span> : null}
            </li>
          ))}
          {!data.log.length ? <li>Пока пусто.</li> : null}
        </ul>
      ) : null}
    </section>
  );
}
