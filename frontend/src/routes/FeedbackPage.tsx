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
import { FeedbackAutopilot } from "../components/FeedbackAutopilot";

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
  offerId?: string;
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
        target: review.target,
        rating: review.rating,
        reviewText: review.text,
        advantages: review.advantages,
        disadvantages: review.disadvantages,
        productName: review.productName,
      }),
    }) as Promise<{ ok: boolean; draft: string }>,
    onSuccess: (data) => { if (data.draft) setText(data.draft); setOpen(true); },
  });
  const tone = review.rating <= 2 ? " is-neg" : review.rating === 3 ? " is-mid" : " is-pos";
  return (
    <div className={`review-card fb-card${tone}${review.needsReply ? " needs-reply" : " is-answered"}`}>
      <div className="review-head">
        <MarketplaceBadge marketplace={review.marketplace} />
        <Stars rating={review.rating} />
        {review.needsReply ? null : <span className="pill ok">отвечено</span>}
        <small className="review-date">{[review.authorName, formatDate(review.createdAt)].filter(Boolean).join(" · ")}</small>
      </div>
      <strong className="review-product">{review.productName || review.offerId || "товар"}</strong>
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
  // the answer is drafted by AI and signed by the cabinet's store (Magic Stick / AURA / Parfumerius)
  const aiDraft = useMutation({
    mutationFn: () => fetchJson("/api/questions/ai-draft", z.unknown(), {
      method: "POST",
      body: JSON.stringify({ marketplace: question.marketplace, target: question.target, sku: question.sku, offerId: question.offerId, questionText: question.text, productName: question.productName }),
    }) as Promise<{ ok: boolean; draft: string }>,
    onSuccess: (data) => { if (data.draft) setText(data.draft); setOpen(true); },
  });
  return (
    <div className={`review-card fb-card is-question${question.needsAnswer ? " needs-reply" : " is-answered"}`}>
      <div className="review-head">
        <MarketplaceBadge marketplace={question.marketplace} />
        {question.needsAnswer ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено ({question.answersCount})</span>}
        <small className="review-date">{[question.authorName, formatDate(question.createdAt)].filter(Boolean).join(" · ")}</small>
      </div>
      {question.productUrl
        ? <a className="review-product fb-product-link" href={question.productUrl} target="_blank" rel="noreferrer">{question.productName || question.sku || question.offerId || "товар"}</a>
        : <strong className="review-product">{question.productName || question.sku || question.offerId || "товар"}</strong>}
      {question.text ? <p className="review-text review-question-text">{question.text}</p> : null}
      <div className="review-actions">
        <button className="secondary-action" type="button" onClick={() => setOpen((v) => !v)}>
          <MessageSquareReply size={15} /> {open ? "Скрыть" : "Ответить"}
        </button>
        <button className="secondary-action" type="button" disabled={aiDraft.isPending || !question.text} onClick={() => aiDraft.mutate()} title="Сгенерировать ответ с помощью AI">
          {aiDraft.isPending ? <Loader2 className="spin" size={15} /> : <BotMessageSquare size={15} />} AI-ответ
        </button>
        {aiDraft.error ? <span className="inline-error" style={{ fontSize: 11 }}>{String((aiDraft.error as Error).message)}</span> : null}
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

// marketplace API errors → what the operator should know; an unknown error is shown as is
function feedbackWarning(text: string) {
  const mp = /^(ozon|yandex|wb|wildberries|avito)\b/i.exec(text)?.[1] || "";
  const name = { ozon: "Ozon", yandex: "Яндекс Маркет", wb: "Wildberries", wildberries: "Wildberries", avito: "Avito" }[mp.toLowerCase()] || "Маркетплейс";
  if (/not available with existing subscription|PermissionDenied/i.test(text)) return `${name}: отзывы и вопросы по API доступны только с подпиской Premium Plus — с этого кабинета они сюда не загружаются.`;
  if (/\b(401|403)\b|unauthori[sz]ed|forbidden|invalid api key|api-key/i.test(text)) return `${name}: ключ API не подходит или без доступа к отзывам — проверьте кабинет в Настройках.`;
  if (/\b429\b|too many requests|rate limit|\b420\b/i.test(text)) return `${name}: маркетплейс просит подождать (много запросов) — список обновится сам.`;
  if (/timeout|timed out|ETIMEDOUT|ECONNRESET|5\d\d/i.test(text)) return `${name}: маркетплейс не ответил — попробуйте обновить через минуту.`;
  return text;
}

export function FeedbackPage({ defaultTab }: { defaultTab: "reviews" | "questions" }) {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState<"reviews" | "questions">(defaultTab);
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
  const [negativeOnly, setNegativeOnly] = useState(false);
  const [templatesOpen, setTemplatesOpen] = useState(false);

  function switchTab(next: "reviews" | "questions") {
    setTab(next);
    setMarketplace("all");
    setNegativeOnly(false);
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
    negative: reviewRows.filter((r) => r.rating <= 3).length,
    avg: reviewRows.length ? (reviewRows.reduce((s, r) => s + r.rating, 0) / reviewRows.length).toFixed(1) : "–",
  }), [reviewRows]);
  // 1–3★ first among the ones waiting: they hurt the rating and need the answer soonest
  const shownReviews = useMemo(() => {
    const list = negativeOnly ? reviewRows.filter((r) => r.rating <= 3) : reviewRows;
    return [...list].sort((a, b) => Number(Boolean(b.needsReply)) - Number(Boolean(a.needsReply))
      || Number(b.rating <= 3) - Number(a.rating <= 3)
      || String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
  }, [reviewRows, negativeOnly]);

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

      <FeedbackAutopilot tab={tab} />

      <div className="fb-toolbar">
        {isReviews ? (
          <div className="fb-summary" role="group" aria-label="Сводка по отзывам">
            <span className={`fb-chip${reviewCounters.needsReply ? " is-warn" : ""}`}><AlertCircle size={14} /> Ждут ответа <b>{reviewCounters.needsReply}</b></span>
            <button type="button" className={`fb-chip is-button${reviewCounters.negative ? " is-neg" : ""}${negativeOnly ? " is-on" : ""}`}
              aria-pressed={negativeOnly} onClick={() => setNegativeOnly((v) => !v)} title="Показать только оценки 1–3">
              <Star size={14} /> 1–3★ <b>{reviewCounters.negative}</b>
            </button>
            <span className="fb-chip"><Star size={14} fill="#ffb020" color="#ffb020" /> Средняя <b>{reviewCounters.avg}</b></span>
            <span className="fb-chip is-muted">показано {reviewCounters.total}</span>
          </div>
        ) : (
          <div className="fb-summary" role="group" aria-label="Сводка по вопросам">
            <span className={`fb-chip${questionCounters.needsAnswer ? " is-warn" : ""}`}><AlertCircle size={14} /> Ждут ответа <b>{questionCounters.needsAnswer}</b></span>
            <span className="fb-chip is-muted">показано {questionCounters.total}</span>
          </div>
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
                { value: "yandex", label: "Яндекс" },
                { value: "wb", label: "Wildberries" },
              ]
          }
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unanswered} onChange={(e) => setUnanswered(e.target.checked)} />
          {isReviews ? "Только без ответа" : "Только без ответа"}
        </label>
      </div>
      </div>

      {warnings.map((w) => <div className="inline-warn" key={w}>{feedbackWarning(w)}</div>)}
      {activeQuery.error ? <div className="inline-error">{String((activeQuery.error as Error).message)}</div> : null}

      {isReviews ? (
        <div className="reviews-grid">
          {shownReviews.map((review) => (
            <ReviewCard
              key={review.id}
              review={review}
              templates={reviewTemplates}
              onReplied={() => void queryClient.invalidateQueries({ queryKey: ["reviews"] })}
            />
          ))}
          {!shownReviews.length && !reviewsQuery.isFetching ? <div className="empty-state">{negativeOnly ? "Отзывов с оценкой 1–3 нет." : unanswered ? "Все отзывы с ответом." : "Отзывов по фильтру нет."}</div> : null}
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
          {!questionRows.length && !questionsQuery.isFetching ? <div className="empty-state">{unanswered ? "На все вопросы ответили." : "Вопросов по фильтру нет."}</div> : null}
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
