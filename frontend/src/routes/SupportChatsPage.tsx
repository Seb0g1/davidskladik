import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MessageCircleHeart, Send, Check, RefreshCw, ChevronRight, Clock, User, Headphones, X, ArrowLeft } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { fetchJson } from "../api";
import { z } from "zod";

const ChatListItemSchema = z.object({
  id: z.string(),
  visitorName: z.string(),
  category: z.string(),
  status: z.string(),
  unreadAdmin: z.boolean(),
  lastMessageAt: z.string().nullish(),
  createdAt: z.string(),
  lastMessage: z.object({ role: z.string(), body: z.string() }).nullish(),
});
const ChatListSchema = z.object({ ok: z.boolean(), chats: z.array(ChatListItemSchema) });

const MessageSchema = z.object({ id: z.string(), role: z.string(), body: z.string(), createdAt: z.string() });
const ChatDetailSchema = z.object({
  ok: z.boolean(),
  chat: z.object({
    id: z.string(), visitorName: z.string(), category: z.string(), status: z.string(),
    createdAt: z.string(), messages: z.array(MessageSchema),
  }),
});
const ReplySchema = z.object({ ok: z.boolean() });
const StatusSchema = z.object({ ok: z.boolean(), status: z.string() });

type ChatListItem = z.infer<typeof ChatListItemSchema>;

function timeAgo(iso: string) {
  const ms = Date.now() - new Date(iso).getTime();
  const m = Math.floor(ms / 60_000);
  if (m < 1) return "только что";
  if (m < 60) return `${m} мин`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ч`;
  return `${Math.floor(h / 24)} д`;
}

export function SupportChatsPage() {
  const qc = useQueryClient();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<"open" | "closed" | "all">("open");
  const [reply, setReply] = useState("");
  const [mobileView, setMobileView] = useState<"list" | "thread">("list");
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const listQuery = useQuery({
    queryKey: ["support-chats", statusFilter],
    queryFn: () => fetchJson(`/api/support/chats?status=${statusFilter}`, ChatListSchema),
    refetchInterval: 8000,
  });

  const detailQuery = useQuery({
    queryKey: ["support-chat-detail", selectedId],
    queryFn: () => fetchJson(`/api/support/chats/${selectedId}`, ChatDetailSchema),
    enabled: Boolean(selectedId),
    refetchInterval: 5000,
  });

  const replyMutation = useMutation({
    mutationFn: (msg: string) =>
      fetchJson(`/api/support/chats/${selectedId}/reply`, ReplySchema, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg }),
      }),
    onSuccess: () => {
      setReply("");
      void qc.invalidateQueries({ queryKey: ["support-chat-detail", selectedId] });
      void qc.invalidateQueries({ queryKey: ["support-chats"] });
    },
  });

  const statusMutation = useMutation({
    mutationFn: (status: string) =>
      fetchJson(`/api/support/chats/${selectedId}/status`, StatusSchema, {
        method: "PATCH", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status }),
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["support-chat-detail", selectedId] });
      void qc.invalidateQueries({ queryKey: ["support-chats"] });
    },
  });

  const messageCount = detailQuery.data?.chat?.messages?.length ?? 0;
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messageCount]);

  const chats = listQuery.data?.chats ?? [];
  const chat = detailQuery.data?.chat;
  const unreadCount = chats.filter((c) => c.unreadAdmin && c.status === "open").length;

  function handleSend(e: React.FormEvent) {
    e.preventDefault();
    if (!selectedId || !reply.trim() || replyMutation.isPending) return;
    replyMutation.mutate(reply.trim());
  }

  function renderChatItem(item: ChatListItem) {
    const isSelected = selectedId === item.id;
    return (
      <button
        key={item.id}
        type="button"
        onClick={() => { setSelectedId(item.id); setMobileView("thread"); }}
        className={`chat-item${isSelected ? " is-active" : ""}${item.unreadAdmin && item.status === "open" ? " has-unread" : ""}`}
      >
        <span className="chat-item-top">
          <span className="market-badge support-category-badge">
            {item.category}
          </span>
          <strong>{item.visitorName}</strong>
          {item.unreadAdmin && item.status === "open" && (
            <span className="notify-badge chat-unread" />
          )}
        </span>
        <span className="chat-item-bottom">
          <small className="chat-subtitle">
            {item.lastMessage
              ? (item.lastMessage.role === "admin" ? "Вы: " : "") + item.lastMessage.body
              : ""}
          </small>
          <small className="chat-item-time">
            {item.lastMessageAt ? timeAgo(item.lastMessageAt) : ""}
          </small>
        </span>
        {item.status === "closed" && (
          <small className="chat-item-closed">закрыт</small>
        )}
      </button>
    );
  }

  return (
    <section className="page-section">
      <PageHeader
        title="Поддержка сайта"
        subtitle={unreadCount > 0 ? `${unreadCount} новых обращений` : "Обращения покупателей"}
        action={
          <button
            type="button"
            onClick={() => void qc.invalidateQueries({ queryKey: ["support-chats"] })}
            className="secondary-action icon-action"
          >
            <RefreshCw size={14} className={listQuery.isFetching ? "spin" : ""} />
          </button>
        }
      />

      <div className="chats-layout">
        {/* ── LEFT LIST ── */}
        <div className={`chats-list${mobileView === "thread" ? " mobile-hidden" : ""}`}>
          {/* Filter tabs */}
          <div className="support-filter-tabs">
            {(["open", "closed", "all"] as const).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => setStatusFilter(s)}
                className={`secondary-action support-filter-btn${statusFilter === s ? " is-active" : ""}`}
              >
                {s === "open" ? "Открытые" : s === "closed" ? "Закрытые" : "Все"}
              </button>
            ))}
          </div>

          {listQuery.isLoading && (
            <div className="empty-state">Загрузка…</div>
          )}
          {!listQuery.isLoading && chats.length === 0 && (
            <div className="empty-state"><MessageCircleHeart size={18} /> Обращений нет</div>
          )}
          <div className="chats-list-scroll">
            {chats.map(renderChatItem)}
          </div>
        </div>

        {/* ── RIGHT THREAD ── */}
        <div className={`chat-thread${mobileView === "list" ? " mobile-hidden" : ""}`}>
          {!selectedId && (
            <div className="chat-placeholder">
              <Headphones size={34} />
              <span>Выберите обращение слева</span>
            </div>
          )}

          {selectedId && detailQuery.isLoading && (
            <div className="chat-placeholder">Загрузка…</div>
          )}

          {selectedId && chat && (
            <>
              {/* Header */}
              <div className="chat-thread-head">
                <button type="button" className="chat-back-btn" onClick={() => setMobileView("list")} title="Назад">
                  <ArrowLeft size={18} />
                </button>
                <User size={15} className="support-thread-user-icon" />
                <strong>{chat.visitorName}</strong>
                <span className="support-thread-category">{chat.category}</span>
                <span className={`support-thread-status${chat.status === "open" ? " is-open" : ""}`}>
                  {chat.status === "open" ? "открыт" : "закрыт"}
                </span>
                <span className="support-thread-age">
                  <Clock size={10} />{timeAgo(chat.createdAt)}
                </span>
                <button
                  type="button"
                  onClick={() => statusMutation.mutate(chat.status === "open" ? "closed" : "open")}
                  disabled={statusMutation.isPending}
                  className="secondary-action support-thread-toggle"
                >
                  {chat.status === "open" ? <><X size={12} /> Закрыть</> : <><Check size={12} /> Открыть</>}
                </button>
              </div>

              {/* Messages */}
              <div className="chat-messages chat-messages--thread">
                {chat.messages.map((msg) => (
                  <div key={msg.id} className={`chat-bubble${msg.role === "admin" ? " mine" : ""}`}>
                    <p className="chat-text">{msg.body}</p>
                    <small>{new Date(msg.createdAt).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}</small>
                  </div>
                ))}
                {chat.messages.length === 0 && (
                  <div className="chat-empty-note">Сообщений нет</div>
                )}
                <div ref={messagesEndRef} />
              </div>

              {/* Reply input */}
              {statusMutation.error && (
                <div className="inline-error chat-status-error">
                  {statusMutation.error instanceof Error ? statusMutation.error.message : String(statusMutation.error)}
                </div>
              )}
              {chat.status === "open" ? (
                <form onSubmit={handleSend} className="chat-composer">
                  <div className="chat-input-row">
                    <textarea
                      value={reply}
                      onChange={(e) => setReply(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(e); } }}
                      placeholder="Напишите ответ…"
                      rows={2}
                      className="chat-composer-textarea"
                    />
                    <button
                      type="submit"
                      disabled={!reply.trim() || replyMutation.isPending}
                      className="primary-action chat-send-btn"
                    >
                      <Send size={15} />
                    </button>
                  </div>
                  {replyMutation.error && (
                    <div className="inline-error">
                      {replyMutation.error instanceof Error ? replyMutation.error.message : String(replyMutation.error)}
                    </div>
                  )}
                </form>
              ) : (
                <div className="chat-composer chat-composer--closed">
                  Чат закрыт. Нажмите «Открыть» чтобы ответить.
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}
