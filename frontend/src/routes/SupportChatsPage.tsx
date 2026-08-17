import { useState } from "react";
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

  const chats = listQuery.data?.chats ?? [];
  const chat = detailQuery.data?.chat;
  const unreadCount = chats.filter((c) => c.unreadAdmin && c.status === "open").length;

  function handleSend(e: React.FormEvent) {
    e.preventDefault();
    if (!reply.trim() || replyMutation.isPending) return;
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
          <span className="market-badge" style={{ background: "rgba(124,58,237,0.2)", color: "#c4b5fd", border: "1px solid rgba(124,58,237,0.25)", fontSize: 10 }}>
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
          <small style={{ color: "var(--muted-soft)", fontSize: 10, flexShrink: 0 }}>
            {item.lastMessageAt ? timeAgo(item.lastMessageAt) : ""}
          </small>
        </span>
        {item.status === "closed" && (
          <small style={{ color: "var(--muted-soft)", fontSize: 10, marginTop: 2, display: "block" }}>закрыт</small>
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
        <div className={`chats-list${mobileView === "thread" ? " mobile-hidden" : ""}`} style={{ display: "flex", flexDirection: "column", gap: 0 }}>
          {/* Filter tabs */}
          <div className="settings-tabs" style={{ marginBottom: 8, padding: "0 0 8px 0", borderBottom: "1px solid var(--line)" }}>
            {(["open", "closed", "all"] as const).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => setStatusFilter(s)}
                className={`secondary-action${statusFilter === s ? " is-active" : ""}`}
                style={{ fontSize: 12 }}
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
          <div style={{ display: "flex", flexDirection: "column", gap: 6, overflowY: "auto", flex: 1 }}>
            {chats.map(renderChatItem)}
          </div>
        </div>

        {/* ── RIGHT THREAD ── */}
        <div className={`chat-thread${mobileView === "list" ? " mobile-hidden" : ""}`} style={{ display: "flex", flexDirection: "column", minHeight: 520 }}>
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
                <User size={15} style={{ color: "#c4b5fd", flexShrink: 0 }} />
                <strong>{chat.visitorName}</strong>
                <span style={{ fontSize: 11, color: "var(--muted-soft)" }}>{chat.category}</span>
                <span
                  style={{
                    fontSize: 10, padding: "2px 8px", borderRadius: 8, marginLeft: "auto",
                    background: chat.status === "open" ? "rgba(74,222,128,0.15)" : "rgba(255,255,255,0.08)",
                    color: chat.status === "open" ? "#4ade80" : "var(--muted-soft)",
                  }}
                >
                  {chat.status === "open" ? "открыт" : "закрыт"}
                </span>
                <span style={{ color: "var(--muted-soft)", fontSize: 10, display: "flex", alignItems: "center", gap: 4 }}>
                  <Clock size={10} />{timeAgo(chat.createdAt)}
                </span>
                <button
                  type="button"
                  onClick={() => statusMutation.mutate(chat.status === "open" ? "closed" : "open")}
                  disabled={statusMutation.isPending}
                  className="secondary-action"
                  style={{ fontSize: 12, padding: "4px 10px", marginLeft: 4 }}
                >
                  {chat.status === "open" ? <><X size={12} /> Закрыть</> : <><Check size={12} /> Открыть</>}
                </button>
              </div>

              {/* Messages */}
              <div className="chat-messages" style={{ maxHeight: "none", flex: 1, minHeight: 200, overflowY: "auto", padding: "14px 16px", display: "flex", flexDirection: "column", gap: 8 }}>
                {chat.messages.map((msg) => (
                  <div key={msg.id} className={`chat-bubble${msg.role === "admin" ? " mine" : ""}`}>
                    <p className="chat-text">{msg.body}</p>
                    <small>{new Date(msg.createdAt).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}</small>
                  </div>
                ))}
                {chat.messages.length === 0 && (
                  <div style={{ color: "var(--muted-soft)", fontSize: 13, textAlign: "center", marginTop: 16 }}>Сообщений нет</div>
                )}
              </div>

              {/* Reply input */}
              {chat.status === "open" ? (
                <form onSubmit={handleSend} className="chat-composer">
                  <div className="chat-input-row">
                    <textarea
                      value={reply}
                      onChange={(e) => setReply(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(e); } }}
                      placeholder="Напишите ответ…"
                      rows={2}
                      style={{ resize: "vertical" }}
                    />
                    <button
                      type="submit"
                      disabled={!reply.trim() || replyMutation.isPending}
                      className="primary-action"
                      style={{ height: 44, padding: "0 14px" }}
                    >
                      <Send size={15} />
                    </button>
                  </div>
                </form>
              ) : (
                <div className="chat-composer" style={{ textAlign: "center", color: "var(--muted-soft)", fontSize: 12 }}>
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
