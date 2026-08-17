import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MessageCircleHeart, Send, Check, RefreshCw, ChevronRight, Clock, User, Headphones, X } from "lucide-react";
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
  if (m < 60) return `${m} мин назад`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ч назад`;
  return `${Math.floor(h / 24)} д назад`;
}

export function SupportChatsPage() {
  const qc = useQueryClient();
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<"open" | "closed" | "all">("open");
  const [reply, setReply] = useState("");

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

  function renderChat(item: ChatListItem) {
    const isSelected = selectedId === item.id;
    return (
      <button
        key={item.id}
        type="button"
        onClick={() => setSelectedId(item.id)}
        className={`w-full text-left px-4 py-3 border-b border-white/5 transition-colors ${isSelected ? "bg-violet-600/15" : "hover:bg-white/4"}`}
      >
        <div className="flex items-start gap-3">
          <div className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 text-xs font-bold ${item.unreadAdmin && item.status === "open" ? "bg-violet-600 text-white" : "bg-white/10 text-white/50"}`}>
            {item.visitorName.slice(0, 1).toUpperCase()}
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-0.5">
              <span className="text-sm font-semibold text-white truncate">{item.visitorName}</span>
              {item.unreadAdmin && item.status === "open" && (
                <span className="w-2 h-2 rounded-full bg-violet-400 flex-shrink-0" />
              )}
              {item.status === "closed" && (
                <span className="text-[10px] bg-white/10 text-white/40 px-1.5 py-0.5 rounded-full">закрыт</span>
              )}
            </div>
            <div className="text-[11px] text-violet-300/70 mb-1">{item.category}</div>
            {item.lastMessage && (
              <p className="text-xs text-white/40 truncate">
                {item.lastMessage.role === "admin" ? "Вы: " : ""}{item.lastMessage.body}
              </p>
            )}
          </div>
          <div className="flex flex-col items-end gap-1 flex-shrink-0">
            <span className="text-[10px] text-white/30">{item.lastMessageAt ? timeAgo(item.lastMessageAt) : ""}</span>
            <ChevronRight size={12} className="text-white/20" />
          </div>
        </div>
      </button>
    );
  }

  return (
    <div className="flex flex-col h-full">
      <PageHeader
        title="Поддержка сайта"
        subtitle={unreadCount > 0 ? `${unreadCount} новых обращений` : "Обращения покупателей"}
        action={
          <button
            type="button"
            onClick={() => void qc.invalidateQueries({ queryKey: ["support-chats"] })}
            className="p-2 rounded-lg bg-white/5 hover:bg-white/10 transition-colors text-white/50 hover:text-white"
          >
            <RefreshCw size={14} />
          </button>
        }
      />

      <div className="flex flex-1 min-h-0 overflow-hidden">
        {/* Chat list */}
        <div className="flex flex-col w-72 flex-shrink-0 border-r border-white/5">
          {/* Tabs */}
          <div className="flex gap-1 p-2 border-b border-white/5">
            {(["open", "closed", "all"] as const).map((s) => (
              <button
                key={s}
                type="button"
                onClick={() => setStatusFilter(s)}
                className={`flex-1 text-xs py-1.5 rounded-md font-medium transition-colors ${statusFilter === s ? "bg-violet-600 text-white" : "text-white/40 hover:text-white/70"}`}
              >
                {s === "open" ? "Открытые" : s === "closed" ? "Закрытые" : "Все"}
              </button>
            ))}
          </div>
          <div className="flex-1 overflow-y-auto">
            {listQuery.isLoading && (
              <div className="flex items-center justify-center py-12 text-white/30 text-sm">Загрузка…</div>
            )}
            {!listQuery.isLoading && chats.length === 0 && (
              <div className="flex flex-col items-center justify-center py-12 text-center px-4">
                <MessageCircleHeart size={32} className="text-white/20 mb-3" />
                <p className="text-sm text-white/30">Обращений нет</p>
              </div>
            )}
            {chats.map(renderChat)}
          </div>
        </div>

        {/* Chat detail */}
        {!selectedId && (
          <div className="flex-1 flex flex-col items-center justify-center text-center p-8">
            <Headphones size={48} className="text-white/10 mb-4" />
            <p className="text-white/30 text-sm">Выберите обращение слева</p>
          </div>
        )}

        {selectedId && (
          <div className="flex-1 flex flex-col min-w-0">
            {detailQuery.isLoading && (
              <div className="flex items-center justify-center flex-1 text-white/30 text-sm">Загрузка…</div>
            )}
            {chat && (
              <>
                {/* Chat header */}
                <div className="flex items-center justify-between px-4 py-3 border-b border-white/5 flex-shrink-0">
                  <div>
                    <div className="flex items-center gap-2">
                      <User size={14} className="text-violet-300" />
                      <span className="font-semibold text-white text-sm">{chat.visitorName}</span>
                      <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${chat.status === "open" ? "bg-green-500/20 text-green-400" : "bg-white/10 text-white/40"}`}>
                        {chat.status === "open" ? "открыт" : "закрыт"}
                      </span>
                    </div>
                    <div className="flex items-center gap-2 mt-0.5">
                      <span className="text-[11px] text-violet-300/60">{chat.category}</span>
                      <span className="text-[10px] text-white/20"><Clock size={9} className="inline mr-0.5" />{timeAgo(chat.createdAt)}</span>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      type="button"
                      onClick={() => statusMutation.mutate(chat.status === "open" ? "closed" : "open")}
                      disabled={statusMutation.isPending}
                      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${chat.status === "open" ? "bg-white/5 hover:bg-white/10 text-white/50" : "bg-green-500/20 hover:bg-green-500/30 text-green-400"}`}
                    >
                      {chat.status === "open" ? <><X size={12} /> Закрыть</> : <><Check size={12} /> Открыть</>}
                    </button>
                  </div>
                </div>

                {/* Messages */}
                <div className="flex-1 overflow-y-auto p-4 space-y-3">
                  {chat.messages.map((msg) => (
                    <div key={msg.id} className={`flex ${msg.role === "admin" ? "justify-end" : "justify-start"}`}>
                      <div className={`max-w-[75%] rounded-2xl px-3.5 py-2.5 ${msg.role === "admin" ? "bg-violet-600 text-white" : "bg-white/8 text-white/90"}`}>
                        <p className="text-sm leading-relaxed whitespace-pre-wrap break-words">{msg.body}</p>
                        <p className={`text-[10px] mt-1 ${msg.role === "admin" ? "text-violet-200/60" : "text-white/30"}`}>
                          {new Date(msg.createdAt).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}
                        </p>
                      </div>
                    </div>
                  ))}
                </div>

                {/* Reply input */}
                {chat.status === "open" && (
                  <form onSubmit={handleSend} className="flex gap-2 p-3 border-t border-white/5 flex-shrink-0">
                    <textarea
                      value={reply}
                      onChange={(e) => setReply(e.target.value)}
                      onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSend(e); } }}
                      placeholder="Напишите ответ…"
                      rows={1}
                      className="flex-1 resize-none bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-sm text-white placeholder-white/30 outline-none focus:border-violet-500/50 transition-colors"
                      style={{ maxHeight: 120, overflowY: "auto" }}
                    />
                    <button
                      type="submit"
                      disabled={!reply.trim() || replyMutation.isPending}
                      className="p-2.5 rounded-xl bg-violet-600 hover:bg-violet-500 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex-shrink-0"
                    >
                      <Send size={15} className="text-white" />
                    </button>
                  </form>
                )}
                {chat.status === "closed" && (
                  <div className="px-4 py-3 border-t border-white/5 text-xs text-white/30 text-center flex-shrink-0">
                    Чат закрыт. Откройте его чтобы ответить.
                  </div>
                )}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
