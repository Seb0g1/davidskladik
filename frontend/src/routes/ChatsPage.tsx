import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BellRing, BookOpen, Loader2, MessageCircle, RefreshCw, Send } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { ListSkeleton } from "../components/Skeleton";
import { Stat } from "../components/Stat";
import { TemplatesDrawer } from "../components/TemplatesDrawer";

type ChatRow = {
  id: string;
  marketplace: string;
  target: string;
  chatId: string;
  orderId?: string;
  title: string;
  subtitle?: string;
  type?: string;
  status?: string;
  unreadCount: number;
  lastMessageAt?: string;
};

type ChatAttachment = { type: "video" | "image" | "file"; url: string; name?: string; previewUrl?: string };

type ChatMessage = {
  id: string;
  author?: string;
  isSeller?: boolean;
  text: string;
  attachments?: ChatAttachment[];
  createdAt?: string;
};

type ChatTemplate = { id: string; title: string; text: string };

type ChatContext = { postingNumber?: string; orderId?: string; buyerName?: string; productName?: string; offerId?: string } | null;

const EMOJI_ROW = ["🙏", "😊", "✨", "👍", "🤝", "📦", "🚚", "❤️"];

function decodeSafe(s: string): string {
  try { return decodeURIComponent(s.replace(/\+/g, " ")); } catch { return s; }
}

function isVideoUrl(url: string): boolean {
  return /\.(mp4|mov|webm|avi|m4v)(\?|#|$)/i.test(url);
}

function renderChatMarkdown(text: string): ReactNode {
  if (!text) return null;
  const nodes: ReactNode[] = [];
  const lines = text.split("\n");
  const re = /\*\*([^*\n]+)\*\*|\[([^\]\n]+)\]\(([^)\n]+)\)/g;
  lines.forEach((line, li) => {
    if (li > 0) nodes.push(<br key={`br${li}`} />);
    let last = 0;
    let ki = 0;
    let m: RegExpExecArray | null;
    re.lastIndex = 0;
    while ((m = re.exec(line)) !== null) {
      if (m.index > last) nodes.push(<span key={`${li}_${ki++}`}>{line.slice(last, m.index)}</span>);
      if (m[1] !== undefined) {
        nodes.push(<strong key={`${li}_${ki++}`}>{m[1]}</strong>);
      } else {
        const linkText = decodeSafe(m[2]);
        const url = m[3];
        if (isVideoUrl(url)) {
          nodes.push(
            <div key={`${li}_${ki++}`} className="chat-video-wrap">
              <video controls src={url} className="chat-video" />
            </div>,
          );
        } else {
          nodes.push(<a key={`${li}_${ki++}`} href={url} target="_blank" rel="noopener noreferrer" className="chat-link">{linkText}</a>);
        }
      }
      last = m.index + m[0].length;
    }
    if (last < line.length) nodes.push(<span key={`${li}_${ki++}`}>{line.slice(last)}</span>);
  });
  return <>{nodes}</>;
}

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

function chatTime(value?: string) {
  if (!value) return "";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return value;
  const today = new Date();
  const sameDay = date.toDateString() === today.toDateString();
  return sameDay
    ? date.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })
    : date.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export function ChatsPage() {
  const queryClient = useQueryClient();
  const [marketplace, setMarketplace] = useState("all");
  const [unreadOnly, setUnreadOnly] = useState(false);
  const [selected, setSelected] = useState<ChatRow | null>(null);
  const [text, setText] = useState("");
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);

  const chatsQuery = useQuery({
    queryKey: ["chats", marketplace, unreadOnly],
    queryFn: () => apiJson<{ rows: ChatRow[]; warnings: string[] }>(`/api/chats?marketplace=${marketplace}&unread=${unreadOnly}`),
    refetchInterval: 30_000,
  });
  const templatesQuery = useQuery({
    queryKey: ["chat-templates"],
    queryFn: () => apiJson<{ templates: ChatTemplate[] }>("/api/chats/templates"),
  });
  const historyQuery = useQuery({
    queryKey: ["chat-history", selected?.id],
    enabled: Boolean(selected),
    queryFn: () => apiJson<{ rows: ChatMessage[]; context?: ChatContext }>(
      `/api/chats/history?marketplace=${selected!.marketplace}&target=${encodeURIComponent(selected!.target)}&chatId=${encodeURIComponent(selected!.chatId)}${selected!.orderId ? `&orderId=${encodeURIComponent(selected!.orderId)}` : ""}`,
    ),
    refetchInterval: 15_000,
  });

  const send = useMutation({
    mutationFn: () => apiJson("/api/chats/send", {
      method: "POST",
      body: JSON.stringify({ marketplace: selected!.marketplace, target: selected!.target, chatId: selected!.chatId, text }),
    }),
    onSuccess: () => {
      setText("");
      void queryClient.invalidateQueries({ queryKey: ["chat-history", selected?.id] });
      void queryClient.invalidateQueries({ queryKey: ["chats"] });
    },
  });

  const rows = chatsQuery.data?.rows || [];
  const messages = historyQuery.data?.rows || [];
  const templates = templatesQuery.data?.templates || [];
  const unreadTotal = useMemo(() => rows.reduce((sum, row) => sum + (row.unreadCount || 0), 0), [rows]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages.length, selected?.id]);

  return (
    <section className="page-section chats-page">
      <PageHeader
        title="Чаты"
        subtitle="Переписка с покупателями на Ozon и Яндекс.Маркете в одном месте."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={() => setTemplatesOpen(true)}>
              <BookOpen size={16} /> Шаблоны
            </button>
            <button className="secondary-action" type="button" onClick={() => chatsQuery.refetch()}>
              {chatsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
            </button>
          </div>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label="Чатов" value={rows.length} tone="accent" icon={<MessageCircle size={18} />} />
        <Stat label="Непрочитанных" value={unreadTotal} tone={unreadTotal ? "warn" : "success"} icon={<BellRing size={18} />} />
      </section>
      <div className="filters-row">
        <SelectField
          ariaLabel="Маркетплейс"
          value={marketplace}
          onChange={(next) => { setMarketplace(next); setSelected(null); }}
          options={[
            { value: "all", label: "Оба маркетплейса" },
            { value: "ozon", label: "Ozon" },
            { value: "yandex", label: "Яндекс" },
          ]}
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unreadOnly} onChange={(event) => { setUnreadOnly(event.target.checked); setSelected(null); }} />
          Только непрочитанные
        </label>
      </div>
      {(chatsQuery.data?.warnings || []).map((warning) => <div className="inline-error" key={warning}>{warning}</div>)}

      <div className="chats-layout">
        <div className="chats-list">
          {rows.map((chat) => (
            <button
              type="button"
              key={chat.id}
              className={`chat-item${selected?.id === chat.id ? " is-active" : ""}${chat.unreadCount ? " has-unread" : ""}`}
              onClick={() => setSelected(chat)}
            >
              <span className={`market-badge market-${chat.marketplace}`}>{chat.marketplace === "ozon" ? "Ozon" : "Яндекс"}</span>
              <strong>{chat.title}</strong>
              {chat.subtitle ? <small className="chat-subtitle">{chat.subtitle}</small> : null}
              <small>{chatTime(chat.lastMessageAt)}</small>
              {chat.unreadCount ? <span className="notify-badge chat-unread">{chat.unreadCount}</span> : null}
            </button>
          ))}
          {chatsQuery.isFetching && !rows.length ? <ListSkeleton rows={7} /> : null}
          {!rows.length && !chatsQuery.isFetching ? <div className="empty-state">Чатов нет.</div> : null}
        </div>

        <div className="chat-thread">
          {!selected ? (
            <div className="chat-placeholder"><MessageCircle size={34} /> Выбери чат слева</div>
          ) : (
            <>
              <div className="chat-thread-head">
                <span className={`market-badge market-${selected.marketplace}`}>{selected.marketplace === "ozon" ? "Ozon" : "Яндекс"}</span>
                <strong>{selected.title}</strong>
                {selected.subtitle ? <small className="chat-subtitle">{selected.subtitle}</small> : null}
                {historyQuery.data?.context?.buyerName ? (
                  <strong className="chat-buyer">{historyQuery.data.context.buyerName}</strong>
                ) : null}
                {historyQuery.data?.context?.postingNumber ? (
                  <small className="chat-subtitle">
                    Отправление {historyQuery.data.context.postingNumber}
                    {historyQuery.data.context.productName ? ` · ${historyQuery.data.context.productName}` : ""}
                  </small>
                ) : null}
                {!historyQuery.data?.context?.postingNumber && historyQuery.data?.context?.productName && !selected.subtitle ? (
                  <small className="chat-subtitle">{historyQuery.data.context.productName}</small>
                ) : null}
                {historyQuery.isFetching ? <Loader2 className="spin" size={14} /> : null}
              </div>
              <div className="chat-messages">
                {messages.map((message) => (
                  <div className={`chat-bubble${message.isSeller ? " mine" : ""}`} key={message.id}>
                    {message.text ? <div className="chat-text">{renderChatMarkdown(message.text)}</div> : null}
                    {(message.attachments || []).map((att, i) => (
                      att.type === "video" ? (
                        <div key={i} className="chat-video-wrap">
                          <video controls src={att.url} poster={att.previewUrl} className="chat-video" />
                        </div>
                      ) : att.type === "image" ? (
                        <img key={i} src={att.url} alt="вложение" className="chat-image" />
                      ) : (
                        <a key={i} href={att.url} target="_blank" rel="noopener noreferrer" className="chat-file-link">
                          📎 {att.name || "Файл"}
                        </a>
                      )
                    ))}
                    {!message.text && !(message.attachments || []).length ? <p className="chat-empty-att">(вложение)</p> : null}
                    <small>{message.author && !message.isSeller ? `${message.author} · ` : ""}{chatTime(message.createdAt)}{message.isSeller ? " · вы" : ""}</small>
                  </div>
                ))}
                {!messages.length && !historyQuery.isFetching ? <div className="empty-state">Сообщений нет.</div> : null}
                <div ref={messagesEndRef} />
              </div>
              <div className="chat-composer">
                {templates.length ? (
                  <select defaultValue="" onChange={(event) => {
                    const template = templates.find((item) => item.id === event.target.value);
                    if (template) setText((current) => (current ? `${current}\n${template.text}` : template.text));
                    event.target.value = "";
                  }}>
                    <option value="" disabled>Шаблон…</option>
                    {templates.map((template) => <option key={template.id} value={template.id}>{template.title}</option>)}
                  </select>
                ) : null}
                <div className="emoji-row">
                  {EMOJI_ROW.map((emoji) => (
                    <button key={emoji} type="button" onClick={() => setText((current) => current + emoji)}>{emoji}</button>
                  ))}
                </div>
                <div className="chat-input-row">
                  <textarea
                    rows={2}
                    value={text}
                    placeholder="Сообщение покупателю…"
                    onChange={(event) => setText(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" && (event.ctrlKey || event.metaKey) && text.trim()) send.mutate();
                    }}
                  />
                  <button className="primary-action" type="button" disabled={!text.trim() || send.isPending} onClick={() => send.mutate()}>
                    {send.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />}
                  </button>
                </div>
                {send.error ? <div className="inline-error">{String((send.error as Error).message)}</div> : null}
              </div>
            </>
          )}
        </div>
      </div>
      <TemplatesDrawer
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title="Ответы в чатах"
        description="Готовые тексты для быстрых ответов покупателям."
        apiBase="/api/chats/templates"
        queryKey={["chat-templates"]}
        templates={templates}
        onInsert={selected ? (value) => setText((current) => (current ? `${current}\n${value}` : value)) : undefined}
      />
    </section>
  );
}
