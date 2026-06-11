import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Loader2, MessageCircle, RefreshCw, Send } from "lucide-react";

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

type ChatMessage = {
  id: string;
  author?: string;
  isSeller?: boolean;
  text: string;
  createdAt?: string;
};

type ChatTemplate = { id: string; title: string; text: string };

type ChatContext = { postingNumber?: string; orderId?: string; buyerName?: string; productName?: string; offerId?: string } | null;

const EMOJI_ROW = ["🙏", "😊", "✨", "👍", "🤝", "📦", "🚚", "❤️"];

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
      <div className="section-title">
        <div>
          <span>Покупатели</span>
          <h2>Чаты</h2>
        </div>
        <button className="secondary-action" type="button" onClick={() => chatsQuery.refetch()}>
          {chatsQuery.isFetching ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить
        </button>
      </div>
      <div className="summary-grid">
        <div><span>Чатов</span><strong>{rows.length}</strong></div>
        <div><span>Непрочитанных</span><strong>{unreadTotal}</strong></div>
      </div>
      <div className="filters-row">
        <select value={marketplace} onChange={(event) => { setMarketplace(event.target.value); setSelected(null); }}>
          <option value="all">Оба маркетплейса</option>
          <option value="ozon">Ozon</option>
          <option value="yandex">Яндекс</option>
        </select>
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
                    <p>{message.text || "(вложение)"}</p>
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
    </section>
  );
}
