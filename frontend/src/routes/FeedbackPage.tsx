import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, BookOpen, BotMessageSquare, HelpCircle, Loader2, MessageSquareReply, RefreshCw, Star } from "lucide-react";
import { z } from "zod";
import { fetchJson } from "../api";
import { MarketplaceBadge } from "../components/MarketplaceBadge";
import { PageHeader } from "../components/PageHeader";
import { ReplyBox } from "../components/ReplyBox";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { TemplatesDrawer } from "../components/TemplatesDrawer";

type ReviewRow = {
  id: string;
  marketplace: string;
  target: string;
  externalId: string;
  offerId?: string;
  productName?: string;
  rating: number;
  text?: string;
  advantages?: string;
  disadvantages?: string;
  authorName?: string;
  createdAt?: string;
  needsReply?: boolean;
  commentsCount?: number;
};

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

type Template = { id: string; title: string; text: string };

const REVIEW_EMOJI = ["🙏", "❤️", "😊", "🌸", "✨", "👍", "🎁", "🥰", "💫", "🤝"];
const QUESTION_EMOJI = ["🙏", "😊", "✨", "👍", "🤝", "💬"];

function Stars({ rating }: { rating: number }) {
  return (
    <span className="review-stars" title={`${rating} из 5`}>
      {[1, 2, 3, 4, 5].map((value) => (
        <Star key={value} size={14} fill={value <= rating ? "#ffb020" : "none"} color={value <= rating ? "#ffb020" : "#5a6a8f"} />
      ))}
    </span>
  );
}

function formatDate(iso?: string) {
  if (!iso) return "";
  return new Date(iso).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function ReviewCard({ review, templates, onReplied }: { review: ReviewRow; templates: Template[]; onReplied: () => void }) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const reply = useMutation({
    mutationFn: () => fetchJson("/api/reviews/reply", z.unknown(), {
      method: "POST",
      body: JSON.stringify({ marketplace: review.marketplace, target: review.target, externalId: review.externalId, text }),
    }),
    onSuccess: () => { setOpen(false); setText(""); onReplied(); },
  });
  const aiDraft = useMutation({
    mutationFn: () => fetchJson("/api/reviews/ai-draft", z.unknown(), {
      method: "POST",
      body: JSON.stringify({
        marketplace: review.marketplace,
        rating: review.rating,
        reviewText: review.text,
        advantages: review.advantages,
        disadvantages: review.disadvantages,
        productName: review.productName,
      }),
    }) as Promise<{ ok: boolean; draft: string }>,
    onSuccess: (data) => { if (data.draft) setText(data.draft); setOpen(true); },
  });
  return (
    <div className={`review-card${review.needsReply ? " needs-reply" : ""}`}>
      <div className="review-head">
        <MarketplaceBadge marketplace={review.marketplace} />
        <Stars rating={review.rating} />
        {review.needsReply ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено</span>}
        <small className="review-date">{formatDate(review.createdAt)}</small>
      </div>
      <strong className="review-product">{review.productName || review.offerId || "товар"}</strong>
      {review.authorName ? <small className="review-author">{review.authorName}</small> : null}
      {review.text ? <p className="review-text">{review.text}</p> : null}
      {review.advantages ? <p className="review-pros"><b>Плюсы</b>{review.advantages}</p> : null}
      {review.disadvantages ? <p className="review-cons"><b>Минусы</b>{review.disadvantages}</p> : null}
      <div className="review-actions">
        <button className="secondary-action" type="button" onClick={() => setOpen((v) => !v)}>
          <MessageSquareReply size={15} /> {open ? "Скрыть" : "Ответить"}
        </button>
        <button className="secondary-action" type="button" disabled={aiDraft.isPending} onClick={() => aiDraft.mutate()} title="Сгенерировать ответ с помощью AI">
          {aiDraft.isPending ? <Loader2 className="spin" size={15} /> : <BotMessageSquare size={15} />} AI-ответ
        </button>
        {aiDraft.error ? <span className="inline-error" style={{fontSize: 11}}>{String((aiDraft.error as Error).message)}</span> : null}
      </div>
      {open ? (
        <ReplyBox
          text={text}
          setText={setText}
          templates={templates}
          onSubmit={() => reply.mutate()}
          isPending={reply.isPending}
          error={reply.error as Error | null}
          emojiRow={REVIEW_EMOJI}
          placeholder="Текст ответа покупателю"
          confirmMessage="Отправить ответ? Это действие необратимо."
        />
      ) : null}
    </div>
  );
}

function QuestionCard({ question, templates, onReplied }: { question: QuestionRow; templates: Template[]; onReplied: () => void }) {
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
        <MarketplaceBadge marketplace={question.marketplace} />
        <HelpCircle size={15} color="#c792ea" />
        {question.needsAnswer ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено ({question.answersCount})</span>}
        <small className="review-date">{formatDate(question.createdAt)}</small>
      </div>
      <strong className="review-product">{question.productName || question.sku || "товар"}</strong>
      {question.productUrl ? <a href={question.productUrl} target="_blank" rel="noreferrer" style={{fontSize: 12}}>↗ Товар</a> : null}
      {question.authorName ? <small className="review-author">{question.authorName}</small> : null}
      {question.text ? <p className="review-text review-question-text">{question.text}</p> : null}
      <div className="review-actions">
        <button className="secondary-action" type="button" onClick={() => setOpen((v) => !v)}>
          <MessageSquareReply size={15} /> {open ? "Скрыть" : "Ответить"}
        </button>
      </div>
      {open ? (
        <ReplyBox
          text={text}
          setText={setText}
          templates={templates}
          onSubmit={() => reply.mutate()}
          isPending={reply.isPending}
          error={reply.error as Error | null}
          emojiRow={QUESTION_EMOJI}
          placeholder="Ответ покупателю"
        />
      ) : null}
    </div>
  );
}

export function FeedbackPage({ defaultTab }: { defaultTab: "reviews" | "questions" }) {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<"reviews" | "questions">(defaultTab);
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
  const [templatesOpen, setTemplatesOpen] = useState(false);

  function switchTab(next: "reviews" | "questions") {
    setTab(next);
    setMarketplace("all");
  }

  const reviewsQuery = useQuery({
    queryKey: ["reviews", marketplace, unanswered],
    queryFn: () => fetchJson(`/api/reviews?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`, z.unknown()) as Promise<{ rows: ReviewRow[]; warnings: string[] }>,
    refetchInterval: 120_000,
    enabled: tab === "reviews",
  });
  const reviewTemplatesQuery = useQuery({
    queryKey: ["review-templates"],
    queryFn: () => fetchJson("/api/reviews/templates", z.unknown()) as Promise<{ templates: Template[] }>,
    enabled: tab === "reviews",
  });

  const questionsQuery = useQuery({
    queryKey: ["questions", marketplace, unanswered],
    queryFn: () => fetchJson(`/api/questions?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`, z.unknown()) as Promise<{ rows: QuestionRow[]; warnings: string[] }>,
    refetchInterval: 120_000,
    enabled: tab === "questions",
  });
  const questionTemplatesQuery = useQuery({
    queryKey: ["question-templates"],
    queryFn: () => fetchJson("/api/questions/templates", z.unknown()) as Promise<{ templates: Template[] }>,
    enabled: tab === "questions",
  });

  const isReviews = tab === "reviews";
  const activeQuery = isReviews ? reviewsQuery : questionsQuery;
  const reviewRows = reviewsQuery.data?.rows || [];
  const questionRows = questionsQuery.data?.rows || [];
  const reviewTemplates = reviewTemplatesQuery.data?.templates || [];
  const questionTemplates = questionTemplatesQuery.data?.templates || [];

  const reviewCounters = useMemo(() => ({
    total: reviewRows.length,
    needsReply: reviewRows.filter((r) => r.needsReply).length,
    avg: reviewRows.length ? (reviewRows.reduce((s, r) => s + r.rating, 0) / reviewRows.length).toFixed(1) : "-",
  }), [reviewRows]);

  const questionCounters = useMemo(() => ({
    total: questionRows.length,
    needsAnswer: questionRows.filter((q) => q.needsAnswer).length,
  }), [questionRows]);

  const warnings = activeQuery.data?.warnings || [];

  return (
    <section className="page-section reviews-page">
      <PageHeader
        title="Обратная связь"
        subtitle="Отзывы и вопросы покупателей на Ozon, Яндекс.Маркете и Wildberries."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={() => setTemplatesOpen(true)}>
              <BookOpen size={16} /> Шаблоны
            </button>
            <button className="secondary-action" type="button" onClick={() => activeQuery.refetch()}>
              {activeQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
        )}
      />

      <nav className="settings-tabs" aria-label="feedback sections">
        <button className={tab === "reviews" ? "is-active" : ""} type="button" onClick={() => switchTab("reviews")}>
          <Star size={14} /> Отзывы{reviewCounters.needsReply ? ` (${reviewCounters.needsReply})` : ""}
        </button>
        <button className={tab === "questions" ? "is-active" : ""} type="button" onClick={() => switchTab("questions")}>
          <HelpCircle size={14} /> Вопросы{questionCounters.needsAnswer ? ` (${questionCounters.needsAnswer})` : ""}
        </button>
      </nav>

      {isReviews ? (
        <section className="dashboard-metrics">
          <Stat label="Показано" value={reviewCounters.total} tone="accent" icon={<Star size={18} />} />
          <Stat label="Ждут ответа" value={reviewCounters.needsReply} tone={reviewCounters.needsReply ? "warn" : "success"} icon={<AlertCircle size={18} />} />
          <Stat label="Средняя оценка" value={reviewCounters.avg} tone="success" icon={<Star size={18} />} />
        </section>
      ) : (
        <section className="dashboard-metrics">
          <Stat label="Показано" value={questionCounters.total} tone="accent" icon={<HelpCircle size={18} />} />
          <Stat label="Ждут ответа" value={questionCounters.needsAnswer} tone={questionCounters.needsAnswer ? "warn" : "success"} icon={<AlertCircle size={18} />} />
        </section>
      )}

      <div className="filters-row">
        <SelectField
          ariaLabel="Маркетплейс"
          value={marketplace}
          onChange={setMarketplace}
          options={isReviews
            ? [
                { value: "all", label: "Все маркетплейсы" },
                { value: "ozon", label: "Ozon" },
                { value: "yandex", label: "Яндекс" },
                { value: "wb", label: "Wildberries" },
              ]
            : [
                { value: "all", label: "Все маркетплейсы" },
                { value: "ozon", label: "Ozon" },
                { value: "wb", label: "Wildberries" },
              ]
          }
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unanswered} onChange={(e) => setUnanswered(e.target.checked)} />
          {isReviews ? "Только без ответа" : "Только без ответа"}
        </label>
        {!isReviews ? <small className="review-author">Вопросы приходят с Ozon и Wildberries — у Яндекс.Маркета нет API вопросов.</small> : null}
      </div>

      {warnings.map((w) => <div className="inline-warn" key={w}>{w}</div>)}
      {activeQuery.error ? <div className="inline-error">{String((activeQuery.error as Error).message)}</div> : null}

      {isReviews ? (
        <div className="reviews-grid">
          {reviewRows.map((review) => (
            <ReviewCard
              key={review.id}
              review={review}
              templates={reviewTemplates}
              onReplied={() => void queryClient.invalidateQueries({ queryKey: ["reviews"] })}
            />
          ))}
          {!reviewRows.length && !reviewsQuery.isFetching ? <div className="empty-state">Отзывов по фильтру нет.</div> : null}
        </div>
      ) : (
        <div className="reviews-grid">
          {questionRows.map((question) => (
            <QuestionCard
              key={question.id}
              question={question}
              templates={questionTemplates}
              onReplied={() => void queryClient.invalidateQueries({ queryKey: ["questions"] })}
            />
          ))}
          {!questionRows.length && !questionsQuery.isFetching ? <div className="empty-state">Вопросов по фильтру нет.</div> : null}
        </div>
      )}

      <TemplatesDrawer
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title={isReviews ? "Ответы на отзывы" : "Ответы на вопросы"}
        description={isReviews ? "Готовые тексты для быстрого ответа на отзывы покупателей." : "Готовые тексты для быстрого ответа на вопросы покупателей."}
        apiBase={isReviews ? "/api/reviews/templates" : "/api/questions/templates"}
        queryKey={isReviews ? ["review-templates"] : ["question-templates"]}
        templates={isReviews ? reviewTemplates : questionTemplates}
      />
    </section>
  );
}
