import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BookOpen, BotMessageSquare, HelpCircle, Loader2, MessageSquareReply, RefreshCw, Star } from "lucide-react";
import { z } from "zod";
import { fetchJson } from "../api";
import { MarketplaceBadge } from "../components/MarketplaceBadge";
import { PageHeader } from "../components/PageHeader";
import { ReplyBox } from "../components/ReplyBox";
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
        <span className="pill fb-kind"><HelpCircle size={12} /> Вопрос</span>
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

type Kind = "all" | "reviews" | "questions";
type FeedItem = { kind: "review"; at: string; waiting: boolean; urgent: boolean; row: ReviewRow } | { kind: "question"; at: string; waiting: boolean; urgent: boolean; row: QuestionRow };

const MARKETPLACES = [
  { value: "all", label: "Все" },
  { value: "ozon", label: "Ozon" },
  { value: "yandex", label: "Маркет" },
  { value: "wb", label: "WB" },
];

/**
 * Отзывы и вопросы — одна лента: сначала то, что ждёт ответа (1–3★ первыми), потом остальное по дате.
 * Фильтры кнопками: что показывать (всё / отзывы / вопросы / 1–3★) и маркетплейс.
 */
export function FeedbackPage({ defaultTab }: { defaultTab: "reviews" | "questions" }) {
  const queryClient = useQueryClient();
  const [kind, setKind] = useState<Kind>(defaultTab === "questions" ? "questions" : "all");
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
  const [negativeOnly, setNegativeOnly] = useState(false);
  const [templatesKind, setTemplatesKind] = useState<"reviews" | "questions" | null>(null);

  const reviewsQuery = useQuery({
    queryKey: ["reviews", marketplace, unanswered],
    queryFn: () => fetchJson(`/api/reviews?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`, z.unknown()) as Promise<{ rows: ReviewRow[]; warnings: string[] }>,
    refetchInterval: 120_000,
  });
  const questionsQuery = useQuery({
    queryKey: ["questions", marketplace, unanswered],
    queryFn: () => fetchJson(`/api/questions?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`, z.unknown()) as Promise<{ rows: QuestionRow[]; warnings: string[] }>,
    refetchInterval: 120_000,
  });
  const reviewTemplatesQuery = useQuery({
    queryKey: ["review-templates"],
    queryFn: () => fetchJson("/api/reviews/templates", z.unknown()) as Promise<{ templates: Template[] }>,
  });
  const questionTemplatesQuery = useQuery({
    queryKey: ["question-templates"],
    queryFn: () => fetchJson("/api/questions/templates", z.unknown()) as Promise<{ templates: Template[] }>,
  });

  const reviewRows = reviewsQuery.data?.rows || [];
  const questionRows = questionsQuery.data?.rows || [];
  const reviewTemplates = reviewTemplatesQuery.data?.templates || [];
  const questionTemplates = questionTemplatesQuery.data?.templates || [];

  const counts = useMemo(() => ({
    reviewsWaiting: reviewRows.filter((r) => r.needsReply).length,
    questionsWaiting: questionRows.filter((q) => q.needsAnswer).length,
    negative: reviewRows.filter((r) => r.rating <= 3).length,
    avg: reviewRows.length ? (reviewRows.reduce((s, r) => s + r.rating, 0) / reviewRows.length).toFixed(1) : "–",
  }), [reviewRows, questionRows]);

  const feed = useMemo(() => {
    const items: FeedItem[] = [];
    if (kind !== "questions") {
      for (const row of reviewRows) {
        if (negativeOnly && row.rating > 3) continue;
        items.push({ kind: "review", at: String(row.createdAt || ""), waiting: Boolean(row.needsReply), urgent: row.rating <= 3, row });
      }
    }
    if (kind !== "reviews" && !negativeOnly) {
      for (const row of questionRows) items.push({ kind: "question", at: String(row.createdAt || ""), waiting: Boolean(row.needsAnswer), urgent: false, row });
    }
    return items.sort((a, b) => Number(b.waiting) - Number(a.waiting) || Number(b.urgent) - Number(a.urgent) || b.at.localeCompare(a.at));
  }, [kind, negativeOnly, reviewRows, questionRows]);

  const loading = (kind !== "questions" && reviewsQuery.isFetching) || (kind !== "reviews" && questionsQuery.isFetching);
  const warnings = [...new Set([
    ...(kind !== "questions" ? reviewsQuery.data?.warnings || [] : []),
    ...(kind !== "reviews" ? questionsQuery.data?.warnings || [] : []),
  ])];
  const errors = [kind !== "questions" ? reviewsQuery.error : null, kind !== "reviews" ? questionsQuery.error : null].filter(Boolean) as Error[];
  const refetch = () => { void reviewsQuery.refetch(); void questionsQuery.refetch(); };
  const chooseKind = (next: Kind) => { setKind(next); if (next === "questions") setNegativeOnly(false); };

  return (
    <section className="page-section reviews-page fb-inbox">
      <PageHeader
        title="Отзывы и вопросы"
        subtitle="Всё, что пишут покупатели на Ozon, Маркете и Wildberries, — в одной ленте. Сначала то, что ждёт ответа."
        action={(
          <div className="row-actions">
            <details className="fb-tpl-menu">
              <summary className="secondary-action"><BookOpen size={16} /> Шаблоны</summary>
              <div role="menu">
                <button type="button" role="menuitem" onClick={(e) => { (e.currentTarget.closest("details") as HTMLDetailsElement).open = false; setTemplatesKind("reviews"); }}>
                  <Star size={14} /> Ответы на отзывы
                </button>
                <button type="button" role="menuitem" onClick={(e) => { (e.currentTarget.closest("details") as HTMLDetailsElement).open = false; setTemplatesKind("questions"); }}>
                  <HelpCircle size={14} /> Ответы на вопросы
                </button>
              </div>
            </details>
            <button className="secondary-action" type="button" onClick={refetch}>
              {loading ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
        )}
      />

      <FeedbackAutopilot tab="all" />

      <div className="fb-bar" role="toolbar" aria-label="Фильтры">
        <div className="fb-seg" role="group" aria-label="Что показать">
          <button type="button" className={kind === "all" && !negativeOnly ? "is-on" : ""} aria-pressed={kind === "all" && !negativeOnly} onClick={() => { chooseKind("all"); setNegativeOnly(false); }}>
            Всё{counts.reviewsWaiting + counts.questionsWaiting ? <b>{counts.reviewsWaiting + counts.questionsWaiting}</b> : null}
          </button>
          <button type="button" className={kind === "reviews" && !negativeOnly ? "is-on" : ""} aria-pressed={kind === "reviews" && !negativeOnly} onClick={() => { chooseKind("reviews"); setNegativeOnly(false); }}>
            <Star size={14} /> Отзывы{counts.reviewsWaiting ? <b>{counts.reviewsWaiting}</b> : null}
          </button>
          <button type="button" className={kind === "questions" ? "is-on" : ""} aria-pressed={kind === "questions"} onClick={() => chooseKind("questions")}>
            <HelpCircle size={14} /> Вопросы{counts.questionsWaiting ? <b>{counts.questionsWaiting}</b> : null}
          </button>
          <button type="button" className={`is-neg${negativeOnly ? " is-on" : ""}`} aria-pressed={negativeOnly} onClick={() => { setNegativeOnly((v) => !v); if (kind === "questions") setKind("all"); }} title="Только отзывы с оценкой 1–3">
            1–3★{counts.negative ? <b>{counts.negative}</b> : null}
          </button>
        </div>
        <div className="fb-seg is-mp" role="group" aria-label="Маркетплейс">
          {MARKETPLACES.map((m) => (
            <button key={m.value} type="button" className={marketplace === m.value ? "is-on" : ""} aria-pressed={marketplace === m.value} onClick={() => setMarketplace(m.value)}>{m.label}</button>
          ))}
        </div>
        <label className="settings-toggle fb-only-new">
          <input type="checkbox" checked={unanswered} onChange={(e) => setUnanswered(e.target.checked)} />
          Только без ответа
        </label>
        <span className="fb-avg" title="Средняя оценка показанных отзывов"><Star size={14} fill="#ffb020" color="#ffb020" /> {counts.avg}</span>
      </div>

      {warnings.map((w) => <div className="inline-warn" key={w}>{feedbackWarning(w)}</div>)}
      {errors.map((e) => <div className="inline-error" key={e.message}>{e.message}</div>)}

      <div className="reviews-grid fb-feed">
        {feed.map((item) => item.kind === "review" ? (
          <ReviewCard key={`r:${item.row.id}`} review={item.row} templates={reviewTemplates}
            onReplied={() => void queryClient.invalidateQueries({ queryKey: ["reviews"] })} />
        ) : (
          <QuestionCard key={`q:${item.row.id}`} question={item.row} templates={questionTemplates}
            onReplied={() => void queryClient.invalidateQueries({ queryKey: ["questions"] })} />
        ))}
        {!feed.length && loading ? <div className="empty-state"><Loader2 className="spin" size={16} /> Загружаем отзывы и вопросы…</div> : null}
        {!feed.length && !loading ? (
          <div className="empty-state">
            {negativeOnly ? "Отзывов с оценкой 1–3 нет." : unanswered ? "Всё отвечено — новых отзывов и вопросов нет." : "По этому фильтру пусто."}
          </div>
        ) : null}
      </div>

      <TemplatesDrawer
        open={templatesKind !== null}
        onClose={() => setTemplatesKind(null)}
        title={templatesKind === "questions" ? "Ответы на вопросы" : "Ответы на отзывы"}
        description={templatesKind === "questions" ? "Готовые тексты для быстрого ответа на вопросы покупателей." : "Готовые тексты для быстрого ответа на отзывы покупателей."}
        apiBase={templatesKind === "questions" ? "/api/questions/templates" : "/api/reviews/templates"}
        queryKey={templatesKind === "questions" ? ["question-templates"] : ["review-templates"]}
        templates={templatesKind === "questions" ? questionTemplates : reviewTemplates}
      />
    </section>
  );
}
