import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, HelpCircle, Loader2, MessageSquareReply, RefreshCw } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

type QuestionRow = {
  id: string;
  marketplace: string;
  target: string;
  externalId: string;
  sku?: string;
  productName?: string;
  text?: string;
  authorName?: string;
  createdAt?: string;
  needsAnswer?: boolean;
  answersCount?: number;
};

const EMOJI_ROW = ["🙏", "😊", "✨", "👍", "🤝", "💬"];

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error((data as any)?.error || `HTTP ${response.status}`);
  return data as T;
}

function QuestionCard({ question, onReplied }: { question: QuestionRow; onReplied: () => void }) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const reply = useMutation({
    mutationFn: () => apiJson("/api/questions/reply", {
      method: "POST",
      body: JSON.stringify({ externalId: question.externalId, sku: question.sku, target: question.target, text }),
    }),
    onSuccess: () => { setOpen(false); setText(""); onReplied(); },
  });
  return (
    <div className={`review-card${question.needsAnswer ? " needs-reply" : ""}`}>
      <div className="review-head">
        <span className="market-badge market-ozon">Ozon</span>
        <HelpCircle size={15} color="#c792ea" />
        <strong className="review-product">{question.productName || question.sku || "товар"}</strong>
        <small>{question.createdAt ? new Date(question.createdAt).toLocaleString("ru-RU") : ""}</small>
      </div>
      {question.authorName ? <small className="review-author">{question.authorName}</small> : null}
      {question.text ? <p>{question.text}</p> : null}
      <div className="review-actions">
        {question.needsAnswer ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено ({question.answersCount})</span>}
        <button className="secondary-action" type="button" onClick={() => setOpen((value) => !value)}>
          <MessageSquareReply size={15} /> Ответить
        </button>
      </div>
      {open ? (
        <div className="review-reply-box">
          <div className="emoji-row">
            {EMOJI_ROW.map((emoji) => (
              <button key={emoji} type="button" onClick={() => setText((current) => current + emoji)}>{emoji}</button>
            ))}
          </div>
          <textarea rows={4} value={text} onChange={(event) => setText(event.target.value)} placeholder="Ответ покупателю" />
          {reply.error ? <div className="inline-error">{String((reply.error as Error).message)}</div> : null}
          <button className="primary-action" type="button" disabled={!text.trim() || reply.isPending} onClick={() => reply.mutate()}>
            {reply.isPending ? <Loader2 className="spin" size={15} /> : <MessageSquareReply size={15} />} Отправить ответ
          </button>
        </div>
      ) : null}
    </div>
  );
}

export function QuestionsPage() {
  const queryClient = useQueryClient();
  const [unanswered, setUnanswered] = useState(true);
  const questionsQuery = useQuery({
    queryKey: ["questions", unanswered],
    queryFn: () => apiJson<{ rows: QuestionRow[]; warnings: string[] }>(`/api/questions?unanswered=${unanswered}&limit=50`),
    refetchInterval: 120_000,
  });
  const rows = questionsQuery.data?.rows || [];
  return (
    <section className="page-section questions-page">
      <PageHeader
        title="Вопросы по товарам"
        subtitle="Отвечай на вопросы покупателей по товарам на Ozon."
        action={(
          <button className="secondary-action" type="button" onClick={() => questionsQuery.refetch()}>
            {questionsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
          </button>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Показано" value={rows.length} tone="accent" icon={<HelpCircle size={18} />} />
        <Stat label="Ждут ответа" value={rows.filter((row) => row.needsAnswer).length} tone={rows.some((row) => row.needsAnswer) ? "warn" : "success"} icon={<AlertCircle size={18} />} />
      </section>
      <div className="filters-row">
        <label className="settings-toggle">
          <input type="checkbox" checked={unanswered} onChange={(event) => setUnanswered(event.target.checked)} />
          Только без ответа
        </label>
        <small className="review-author">Вопросы доступны только на Ozon — у Яндекс.Маркета нет API вопросов.</small>
      </div>
      {(questionsQuery.data?.warnings || []).map((warning) => (
        <div className="inline-error" key={warning}>{warning}</div>
      ))}
      {questionsQuery.error ? <div className="inline-error">{String((questionsQuery.error as Error).message)}</div> : null}
      <div className="reviews-grid">
        {rows.map((question) => (
          <QuestionCard key={question.id} question={question} onReplied={() => void queryClient.invalidateQueries({ queryKey: ["questions"] })} />
        ))}
        {!rows.length && !questionsQuery.isFetching ? <div className="empty-state">Вопросов по фильтру нет.</div> : null}
      </div>
    </section>
  );
}
