import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, BookOpen, Loader2, MessageSquareReply, RefreshCw, Star } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
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

type ReviewTemplate = { id: string; title: string; text: string };

const EMOJI_ROW = ["🙏", "❤️", "😊", "🌸", "✨", "👍", "🎁", "🥰", "💫", "🤝"];

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

function Stars({ rating }: { rating: number }) {
  return (
    <span className="review-stars" title={`${rating} из 5`}>
      {[1, 2, 3, 4, 5].map((value) => (
        <Star key={value} size={14} fill={value <= rating ? "#ffb020" : "none"} color={value <= rating ? "#ffb020" : "#5a6a8f"} />
      ))}
    </span>
  );
}

function ReviewCard({ review, templates, onReplied }: { review: ReviewRow; templates: ReviewTemplate[]; onReplied: () => void }) {
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const reply = useMutation({
    mutationFn: () => apiJson("/api/reviews/reply", {
      method: "POST",
      body: JSON.stringify({ marketplace: review.marketplace, target: review.target, externalId: review.externalId, text }),
    }),
    onSuccess: () => {
      setOpen(false);
      setText("");
      onReplied();
    },
  });
  return (
    <div className={`review-card${review.needsReply ? " needs-reply" : ""}`}>
      <div className="review-head">
        <span className={`market-badge market-${review.marketplace}`}>{review.marketplace === "ozon" ? "Ozon" : "Яндекс"}</span>
        <Stars rating={review.rating} />
        {review.needsReply ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено</span>}
        <small className="review-date">{review.createdAt ? new Date(review.createdAt).toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" }) : ""}</small>
      </div>
      <strong className="review-product">{review.productName || review.offerId || "товар"}</strong>
      {review.authorName ? <small className="review-author">{review.authorName}</small> : null}
      {review.text ? <p className="review-text">{review.text}</p> : null}
      {review.advantages ? <p className="review-pros"><b>Плюсы</b>{review.advantages}</p> : null}
      {review.disadvantages ? <p className="review-cons"><b>Минусы</b>{review.disadvantages}</p> : null}
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
          <textarea rows={4} value={text} onChange={(event) => setText(event.target.value)} placeholder="Текст ответа покупателю" />
          {reply.error ? <div className="inline-error">{String((reply.error as Error).message)}</div> : null}
          <button className="primary-action" type="button" disabled={!text.trim() || reply.isPending} onClick={() => reply.mutate()}>
            {reply.isPending ? <Loader2 className="spin" size={15} /> : <MessageSquareReply size={15} />} Отправить ответ
          </button>
        </div>
      ) : null}
    </div>
  );
}

export function ReviewsPage() {
  const queryClient = useQueryClient();
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const reviewsQuery = useQuery({
    queryKey: ["reviews", marketplace, unanswered],
    queryFn: () => apiJson<{ rows: ReviewRow[]; warnings: string[] }>(`/api/reviews?marketplace=${marketplace}&unanswered=${unanswered}&limit=50`),
    refetchInterval: 120_000,
  });
  const templatesQuery = useQuery({
    queryKey: ["review-templates"],
    queryFn: () => apiJson<{ templates: ReviewTemplate[] }>("/api/reviews/templates"),
  });
  const rows = reviewsQuery.data?.rows || [];
  const templates = templatesQuery.data?.templates || [];
  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ["reviews"] });
  };
  const counters = useMemo(() => ({
    total: rows.length,
    needsReply: rows.filter((row) => row.needsReply).length,
    avg: rows.length ? (rows.reduce((sum, row) => sum + row.rating, 0) / rows.length).toFixed(1) : "-",
  }), [rows]);

  return (
    <section className="page-section reviews-page">
      <PageHeader
        title="Отзывы"
        subtitle="Отвечай покупателям на Ozon и Яндекс.Маркете и держи рейтинг товаров высоким."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={() => setTemplatesOpen(true)}>
              <BookOpen size={16} /> Шаблоны
            </button>
            <button className="secondary-action" type="button" onClick={() => reviewsQuery.refetch()}>
              {reviewsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Показано" value={counters.total} tone="accent" icon={<Star size={18} />} />
        <Stat label="Ждут ответа" value={counters.needsReply} tone={counters.needsReply ? "warn" : "success"} icon={<AlertCircle size={18} />} />
        <Stat label="Средняя оценка" value={counters.avg} tone="success" icon={<Star size={18} />} />
      </section>
      <div className="filters-row">
        <SelectField
          ariaLabel="Маркетплейс"
          value={marketplace}
          onChange={setMarketplace}
          options={[
            { value: "all", label: "Оба маркетплейса" },
            { value: "ozon", label: "Ozon" },
            { value: "yandex", label: "Яндекс" },
          ]}
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unanswered} onChange={(event) => setUnanswered(event.target.checked)} />
          Только без ответа
        </label>
      </div>
      {(reviewsQuery.data?.warnings || []).map((warning) => (
        <div className="inline-error" key={warning}>{warning}</div>
      ))}
      {reviewsQuery.error ? <div className="inline-error">{String((reviewsQuery.error as Error).message)}</div> : null}
      <div className="reviews-grid">
        {rows.map((review) => (
          <ReviewCard key={review.id} review={review} templates={templates} onReplied={refresh} />
        ))}
        {!rows.length && !reviewsQuery.isFetching ? <div className="empty-state">Отзывов по фильтру нет.</div> : null}
      </div>
      <TemplatesDrawer
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title="Ответы на отзывы"
        description="Готовые тексты для быстрого ответа на отзывы покупателей."
        apiBase="/api/reviews/templates"
        queryKey={["review-templates"]}
        templates={templates}
      />
    </section>
  );
}
