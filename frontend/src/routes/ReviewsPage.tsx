import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertCircle, Loader2, MessageSquareReply, Plus, RefreshCw, Star, Trash2 } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

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
        <strong className="review-product">{review.productName || review.offerId || "товар"}</strong>
        <small>{review.createdAt ? new Date(review.createdAt).toLocaleString("ru-RU") : ""}</small>
      </div>
      {review.authorName ? <small className="review-author">{review.authorName}</small> : null}
      {review.text ? <p>{review.text}</p> : null}
      {review.advantages ? <p><b>Плюсы:</b> {review.advantages}</p> : null}
      {review.disadvantages ? <p><b>Минусы:</b> {review.disadvantages}</p> : null}
      <div className="review-actions">
        {review.needsReply ? <span className="pill warn">ждёт ответа</span> : <span className="pill ok">отвечено</span>}
        <button className="secondary-action" type="button" onClick={() => setOpen((value) => !value)}>
          <MessageSquareReply size={15} /> Ответить
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

function TemplatesPanel({ templates, refresh }: { templates: ReviewTemplate[]; refresh: () => void }) {
  const [title, setTitle] = useState("");
  const [text, setText] = useState("");
  const save = useMutation({
    mutationFn: () => apiJson("/api/reviews/templates", { method: "POST", body: JSON.stringify({ title, text }) }),
    onSuccess: () => { setTitle(""); setText(""); refresh(); },
  });
  const remove = useMutation({
    mutationFn: (id: string) => apiJson(`/api/reviews/templates/${encodeURIComponent(id)}`, { method: "DELETE" }),
    onSuccess: refresh,
  });
  return (
    <section className="settings-panel">
      <div className="section-title"><div><span>Шаблоны</span><h3>Ответы на отзывы</h3></div></div>
      <div className="settings-form-row">
        <input placeholder="Название шаблона" value={title} onChange={(event) => setTitle(event.target.value)} />
      </div>
      <textarea rows={3} placeholder="Текст ответа (эмодзи приветствуются 😊)" value={text} onChange={(event) => setText(event.target.value)} />
      <div className="emoji-row">
        {EMOJI_ROW.map((emoji) => (
          <button key={emoji} type="button" onClick={() => setText((current) => current + emoji)}>{emoji}</button>
        ))}
      </div>
      <button className="secondary-action" type="button" disabled={!title.trim() || !text.trim() || save.isPending} onClick={() => save.mutate()}>
        {save.isPending ? <Loader2 className="spin" size={15} /> : <Plus size={15} />} Сохранить шаблон
      </button>
      <div className="template-list">
        {templates.map((template) => (
          <div className="template-chip" key={template.id} title={template.text}>
            <strong>{template.title}</strong>
            <button type="button" onClick={() => remove.mutate(template.id)} title="Удалить"><Trash2 size={13} /></button>
          </div>
        ))}
        {!templates.length ? <small>Шаблонов пока нет.</small> : null}
      </div>
    </section>
  );
}

export function ReviewsPage() {
  const queryClient = useQueryClient();
  const [marketplace, setMarketplace] = useState("all");
  const [unanswered, setUnanswered] = useState(true);
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
          <button className="secondary-action" type="button" onClick={() => reviewsQuery.refetch()}>
            {reviewsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
          </button>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Показано" value={counters.total} tone="accent" icon={<Star size={18} />} />
        <Stat label="Ждут ответа" value={counters.needsReply} tone={counters.needsReply ? "warn" : "success"} icon={<AlertCircle size={18} />} />
        <Stat label="Средняя оценка" value={counters.avg} tone="success" icon={<Star size={18} />} />
      </section>
      <div className="filters-row">
        <select value={marketplace} onChange={(event) => setMarketplace(event.target.value)}>
          <option value="all">Оба маркетплейса</option>
          <option value="ozon">Ozon</option>
          <option value="yandex">Яндекс</option>
        </select>
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
      <TemplatesPanel templates={templates} refresh={() => void queryClient.invalidateQueries({ queryKey: ["review-templates"] })} />
    </section>
  );
}
