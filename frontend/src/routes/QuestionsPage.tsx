import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { AlertCircle, BookOpen, HelpCircle, Loader2, MessageSquareReply, RefreshCw } from "lucide-react";
import { fetchJson } from "../api";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { TemplatesDrawer } from "../components/TemplatesDrawer";

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
  productUrl?: string;
};

type QuestionTemplate = { id: string; title: string; text: string };

const EMOJI_ROW = ["🙏", "😊", "✨", "👍", "🤝", "💬"];


function QuestionCard({ question, templates, onReplied }: { question: QuestionRow; templates: QuestionTemplate[]; onReplied: () => void }) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const reply = useMutation({
    mutationFn: () => fetchJson("/api/questions/reply", z.unknown(), {
      method: "POST",
      body: JSON.stringify({ marketplace: question.marketplace, externalId: question.externalId, sku: question.sku, target: question.target, text }),
    }),
    onSuccess: () => { setOpen(false); setText(""); onReplied(); },
  });
  return (
    <div className={`review-card${question.needsAnswer ? " needs-reply" : ""}`}>
      <div className="review-head">
        <span className={`market-badge market-${question.marketplace}`}>{question.marketplace === "wb" ? "WB" : "Ozon"}</span>
        <HelpCircle size={15} color="#c792ea" />
        {question.needsAnswer ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено ({question.answersCount})</span>}
        <small className="review-date">{question.createdAt ? new Date(question.createdAt).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" }) : ""}</small>
      </div>
      <strong className="review-product">{question.productName || question.sku || "товар"}</strong>
      {question.productUrl && <a href={question.productUrl} target="_blank" rel="noreferrer" style={{fontSize: 12}}>↗ Товар</a>}
      {question.authorName ? <small className="review-author">{question.authorName}</small> : null}
      {question.text ? <p className="review-text review-question-text">{question.text}</p> : null}
      <div className="review-actions">
        <button className="secondary-action" type="button" onClick={() => setOpen((value) => !value)}>
          <MessageSquareReply size={15} /> {open ? "Скрыть" : "Ответить"}
        </button>
      </div>
      {open ? (
        <div className="review-reply-box">
          {templates.length ? (
            <select defaultValue="" onChange={(event) => {
              const template = templates.find((item) => item.id === event.target.value);
              if (template) setText((current) => (current ? `${current}\n${template.text}` : template.text));
              event.target.value = "";
            }}>
              <option value="" disabled>Вставить шаблон…</option>
              {templates.map((template) => <option key={template.id} value={template.id}>{template.title}</option>)}
            </select>
          ) : null}
          <div className="emoji-row">
            {EMOJI_ROW.map((emoji) => (
              <button key={emoji} type="button" onClick={() => setText((current) => current + emoji)}>{emoji}</button>
            ))}
          </div>
          <textarea rows={4} value={text} onChange={(event) => setText(event.target.value)} placeholder="Ответ покупателю" />
          <div style={{fontSize: 11, color: "var(--muted)", textAlign: "right"}}>{text.length}/5000</div>
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
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const questionsQuery = useQuery({
    queryKey: ["questions", marketplace, unanswered],
    queryFn: () => fetchJson(`/api/questions?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`, z.unknown()) as Promise<{ rows: QuestionRow[]; warnings: string[] }>,
    refetchInterval: 120_000,
  });
  const templatesQuery = useQuery({
    queryKey: ["question-templates"],
    queryFn: () => fetchJson("/api/questions/templates", z.unknown()) as Promise<{ templates: QuestionTemplate[] }>,
  });
  const rows = questionsQuery.data?.rows || [];
  const templates = templatesQuery.data?.templates || [];
  return (
    <section className="page-section questions-page">
      <PageHeader
        title="Вопросы по товарам"
        subtitle="Отвечай на вопросы покупателей по товарам на Ozon и Wildberries."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={() => setTemplatesOpen(true)}>
              <BookOpen size={16} /> Шаблоны
            </button>
            <button className="secondary-action" type="button" onClick={() => questionsQuery.refetch()}>
              {questionsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Показано" value={rows.length} tone="accent" icon={<HelpCircle size={18} />} />
        <Stat label="Ждут ответа" value={rows.filter((row) => row.needsAnswer).length} tone={rows.some((row) => row.needsAnswer) ? "warn" : "success"} icon={<AlertCircle size={18} />} />
      </section>
      <div className="filters-row">
        <SelectField
          ariaLabel="Маркетплейс"
          value={marketplace}
          onChange={setMarketplace}
          options={[
            { value: "all", label: "Все маркетплейсы" },
            { value: "ozon", label: "Ozon" },
            { value: "wb", label: "Wildberries" },
          ]}
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unanswered} onChange={(event) => setUnanswered(event.target.checked)} />
          Только без ответа
        </label>
        <small className="review-author">Вопросы приходят с Ozon и Wildberries — у Яндекс.Маркета нет API вопросов.</small>
      </div>
      {(questionsQuery.data?.warnings || []).map((warning) => (
        <div className="inline-error" key={warning}>{warning}</div>
      ))}
      {questionsQuery.error ? <div className="inline-error">{String((questionsQuery.error as Error).message)}</div> : null}
      <div className="reviews-grid">
        {rows.map((question) => (
          <QuestionCard key={question.id} question={question} templates={templates} onReplied={() => void queryClient.invalidateQueries({ queryKey: ["questions"] })} />
        ))}
        {!rows.length && !questionsQuery.isFetching ? <div className="empty-state">Вопросов по фильтру нет.</div> : null}
      </div>
      <TemplatesDrawer
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title="Ответы на вопросы"
        description="Готовые тексты для быстрого ответа на вопросы покупателей."
        apiBase="/api/questions/templates"
        queryKey={["question-templates"]}
        templates={templates}
      />
    </section>
  );
}
