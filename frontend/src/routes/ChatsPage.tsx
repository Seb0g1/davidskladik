import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { apiJson } from "../api";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft, BellRing, BookOpen, Download, ImageIcon, Loader2, MessageCircle, Paperclip, RefreshCw, Send, X } from "lucide-react";
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
  replySign?: string;
  title: string;
  subtitle?: string;
  type?: string;
  status?: string;
  unreadCount: number;
  lastMessageAt?: string;
};

function marketplaceLabel(marketplace: string): string {
  if (marketplace === "ozon") return "Ozon";
  if (marketplace === "wb") return "WB";
  if (marketplace === "avito") return "Avito";
  return "Яндекс";
}

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

function substituteTemplate(text: string, context: ChatContext): string {
  if (!context) return text;
  return text
    .replace(/\{\{name\}\}/gi, context.buyerName || "")
    .replace(/\{\{order\}\}/gi, context.postingNumber || context.orderId || "")
    .replace(/\{\{product\}\}/gi, context.productName || "");
}

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

function chatDayLabel(value?: string): string {
  if (!value) return "";
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return "";
  const today = new Date();
  const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
  if (date.toDateString() === today.toDateString()) return "Сегодня";
  if (date.toDateString() === yesterday.toDateString()) return "Вчера";
  return date.toLocaleDateString("ru-RU", { day: "numeric", month: "long", ...(date.getFullYear() !== today.getFullYear() ? { year: "numeric" } : {}) });
}

type LightboxMedia = { type: "image" | "video"; url: string } | null;

function ChatLightbox({ media, onClose }: { media: LightboxMedia; onClose: () => void }) {
  useEffect(() => {
    if (!media) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [media, onClose]);
  if (!media) return null;
  return (
    <div className="chat-lightbox" role="dialog" aria-label="Просмотр вложения" onClick={onClose}>
      <div className="chat-lightbox-body" onClick={(event) => event.stopPropagation()}>
        {media.type === "video" ? (
          <video controls autoPlay src={media.url} className="chat-lightbox-media" />
        ) : (
          <img src={media.url} alt="Вложение" className="chat-lightbox-media" />
        )}
        <div className="chat-lightbox-actions">
          <a className="secondary-action" href={media.url} target="_blank" rel="noopener noreferrer" download>
            <Download size={15} /> Скачать
          </a>
          <button className="secondary-action" type="button" onClick={onClose}><X size={15} /> Закрыть</button>
        </div>
      </div>
    </div>
  );
}

function ChatAttachmentImage({ url, name, onOpen }: { url: string; name?: string; onOpen: () => void }) {
  const [broken, setBroken] = useState(false);
  if (broken) {
    return (
      <a href={url} target="_blank" rel="noopener noreferrer" className="chat-attachment-broken" title="Открыть фото в новой вкладке">
        <ImageIcon size={22} />
        <span>Фото</span>
      </a>
    );
  }
  return (
    <button type="button" className="chat-attachment-thumb" title="Открыть фото" onClick={onOpen}>
      <img src={url} alt={name || "фото"} loading="lazy" onError={() => setBroken(true)} />
    </button>
  );
}

function ChatAttachments({ attachments, onOpen }: { attachments: ChatAttachment[]; onOpen: (media: LightboxMedia) => void }) {
  if (!attachments.length) return null;
  return (
    <div className="chat-attachments">
      {attachments.map((att, index) => (
        att.type === "image" ? (
          <ChatAttachmentImage key={index} url={att.url} name={att.name} onOpen={() => onOpen({ type: "image", url: att.url })} />
        ) : att.type === "video" ? (
          <div key={index} className="chat-attachment-video">
            <video controls preload="metadata" src={att.url} poster={att.previewUrl || undefined} />
            <button type="button" className="chat-attachment-expand" title="Открыть на весь экран" onClick={() => onOpen({ type: "video", url: att.url })}>⛶</button>
          </div>
        ) : (
          <a key={index} href={att.url} target="_blank" rel="noopener noreferrer" className="chat-file-link">
            <Paperclip size={13} /> {att.name || "Файл"}
          </a>
        )
      ))}
    </div>
  );
}

type PendingImage = { localUrl: string; serverUrl: string };

export function ChatsPage() {
  const queryClient = useQueryClient();
  const [marketplace, setMarketplace] = useState("all");
  const [unreadOnly, setUnreadOnly] = useState(false);
  const [selected, setSelected] = useState<ChatRow | null>(null);
  const [mobileView, setMobileView] = useState<"list" | "thread">("list");
  const [text, setText] = useState("");
  const [templatesOpen, setTemplatesOpen] = useState(false);
  const [lightbox, setLightbox] = useState<LightboxMedia>(null);
  const [pendingImages, setPendingImages] = useState<PendingImage[]>([]);
  const pendingImagesRef = useRef<PendingImage[]>([]);
  pendingImagesRef.current = pendingImages;
  const [uploadingImages, setUploadingImages] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const draftRef = useRef<Map<string, string>>(new Map());

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

  const handleFileChange = useCallback(async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.target.files || []);
    if (!files.length) return;
    setUploadingImages(true);
    try {
      const form = new FormData();
      files.forEach((file) => form.append("images", file));
      const res = await fetch("/api/uploads/images", { method: "POST", credentials: "same-origin", body: form });
      const data = await res.json().catch(() => ({})) as any;
      if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
      const uploaded: Array<{ url: string }> = data.files || [];
      const newItems: PendingImage[] = files
        .map((file, i) => ({ localUrl: URL.createObjectURL(file), serverUrl: uploaded[i]?.url || "" }))
        .filter((item) => item.serverUrl);
      setPendingImages((prev) => [...prev, ...newItems]);
    } catch (err) {
      console.error("Ошибка загрузки фото:", err);
    } finally {
      setUploadingImages(false);
      if (event.target) event.target.value = "";
    }
  }, []);

  const removePendingImage = useCallback((index: number) => {
    setPendingImages((prev) => {
      URL.revokeObjectURL(prev[index].localUrl);
      return prev.filter((_, j) => j !== index);
    });
  }, []);

  const send = useMutation({
    mutationFn: () => apiJson("/api/chats/send", {
      method: "POST",
      body: JSON.stringify({
        marketplace: selected!.marketplace,
        target: selected!.target,
        chatId: selected!.chatId,
        replySign: selected!.replySign,
        text,
        imageUrls: pendingImages.map((img) => img.serverUrl),
      }),
    }),
    onSuccess: () => {
      setText("");
      setPendingImages((prev) => { prev.forEach((img) => URL.revokeObjectURL(img.localUrl)); return []; });
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

  useEffect(() => {
    return () => {
      pendingImagesRef.current.forEach((img: PendingImage) => URL.revokeObjectURL(img.localUrl));
    };
  }, []);

  // Restore draft when switching chats
  useEffect(() => {
    if (selected) {
      setText(draftRef.current.get(selected.chatId) ?? "");
    }
  }, [selected?.chatId]);

  // Persist draft as user types
  useEffect(() => {
    if (selected?.chatId) draftRef.current.set(selected.chatId, text);
  }, [text, selected?.chatId]);

  return (
    <section className="page-section chats-page">
      <PageHeader
        title="Чаты"
        subtitle="Переписка с покупателями на Ozon, Avito, Яндекс.Маркете и Wildberries в одном месте."
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
            { value: "all", label: "Все маркетплейсы" },
            { value: "ozon", label: "Ozon" },
            { value: "yandex", label: "Яндекс" },
            { value: "wb", label: "Wildberries" },
            { value: "avito", label: "Avito" },
          ]}
        />
        <label className="settings-toggle">
          <input type="checkbox" checked={unreadOnly} onChange={(event) => { setUnreadOnly(event.target.checked); setSelected(null); }} />
          Только непрочитанные
        </label>
      </div>
      {(chatsQuery.data?.warnings || []).map((warning) => <div className="inline-warn" key={warning}>{warning}</div>)}

      <div className="chats-layout">
        <div className={`chats-list${mobileView === "thread" ? " mobile-hidden" : ""}`}>
          {rows.map((chat) => (
            <button
              type="button"
              key={chat.id}
              className={`chat-item${selected?.id === chat.id ? " is-active" : ""}${chat.unreadCount ? " has-unread" : ""}`}
              onClick={() => { setSelected(chat); setMobileView("thread"); }}
            >
              <span className="chat-item-top">
                <span className={`market-badge market-${chat.marketplace}`}>{marketplaceLabel(chat.marketplace)}</span>
                <strong>{chat.title}</strong>
                {chat.unreadCount ? <span className="notify-badge chat-unread">{chat.unreadCount}</span> : null}
              </span>
              <span className="chat-item-bottom">
                {chat.subtitle ? <small className="chat-subtitle">{chat.subtitle}</small> : <small className="chat-subtitle chat-subtitle-empty" />}
                <small className="chat-item-time">{chatTime(chat.lastMessageAt)}</small>
              </span>
            </button>
          ))}
          {chatsQuery.isFetching && !rows.length ? <ListSkeleton rows={7} /> : null}
          {!rows.length && !chatsQuery.isFetching ? <div className="empty-state">Чатов нет.</div> : null}
        </div>

        <div className={`chat-thread${mobileView === "list" ? " mobile-hidden" : ""}`}>
          {!selected ? (
            <div className="chat-placeholder"><MessageCircle size={34} /> Выбери чат слева</div>
          ) : (
            <>
              <div className="chat-thread-head">
                <button type="button" className="chat-back-btn" title="Назад к чатам" onClick={() => { setMobileView("list"); }}>
                  <ArrowLeft size={18} />
                </button>
                <span className={`market-badge market-${selected.marketplace}`}>{marketplaceLabel(selected.marketplace)}</span>
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
                {messages.map((message, index) => {
                  const prev = messages[index - 1];
                  const dayLabel = chatDayLabel(message.createdAt);
                  const showDay = dayLabel && dayLabel !== chatDayLabel(prev?.createdAt);
                  const showAuthor = !message.isSeller && message.author
                    && (prev?.isSeller !== message.isSeller || prev?.author !== message.author || showDay);
                  return (
                    <div className="chat-message-group" key={message.id}>
                      {showDay ? <div className="chat-day-sep"><span>{dayLabel}</span></div> : null}
                      <div className={`chat-bubble${message.isSeller ? " mine" : ""}`}>
                        {showAuthor ? <span className="chat-author">{message.author}</span> : null}
                        {message.text ? <div className="chat-text">{renderChatMarkdown(message.text)}</div> : null}
                        <ChatAttachments attachments={message.attachments || []} onOpen={setLightbox} />
                        {!message.text && !(message.attachments || []).length ? <p className="chat-empty-att">(вложение)</p> : null}
                        <small className="chat-meta">{chatTime(message.createdAt)}</small>
                      </div>
                    </div>
                  );
                })}
                {!messages.length && !historyQuery.isFetching ? <div className="empty-state">Сообщений нет.</div> : null}
                {historyQuery.isFetching && !messages.length ? <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю переписку…</div> : null}
                <div ref={messagesEndRef} />
              </div>
              <div className="chat-composer">
                {templates.length ? (
                  <select defaultValue="" onChange={(event) => {
                    const template = templates.find((item) => item.id === event.target.value);
                    if (template) {
                      const resolved = substituteTemplate(template.text, historyQuery.data?.context ?? null);
                      setText((current) => (current ? `${current}\n${resolved}` : resolved));
                    }
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
                {pendingImages.length > 0 && (
                  <div className="chat-pending-images">
                    {pendingImages.map((img, i) => (
                      <div key={i} className="chat-pending-thumb">
                        <img src={img.localUrl} alt="фото" />
                        <button type="button" className="chat-pending-remove" title="Убрать" onClick={() => removePendingImage(i)}>
                          <X size={10} />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
                <div className="chat-input-row">
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept="image/*"
                    multiple
                    className="chat-file-input"
                    onChange={handleFileChange}
                  />
                  <button
                    type="button"
                    className="secondary-action chat-upload-btn"
                    title="Прикрепить фото"
                    disabled={uploadingImages}
                    onClick={() => fileInputRef.current?.click()}
                  >
                    {uploadingImages ? <Loader2 className="spin" size={15} /> : <ImageIcon size={15} />}
                  </button>
                  <textarea
                    rows={2}
                    value={text}
                    placeholder="Сообщение покупателю… (Ctrl+Enter — отправить)"
                    disabled={send.isPending}
                    onChange={(event) => setText(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" && (event.ctrlKey || event.metaKey) && (text.trim() || pendingImages.length)) send.mutate();
                    }}
                  />
                  {selected?.marketplace === "wb" && (
                    <div style={{ fontSize: 11, color: "var(--muted)", textAlign: "right" }}>{text.length}/1000</div>
                  )}
                  <button className="primary-action chat-send-btn" type="button" title="Отправить (Ctrl+Enter)" disabled={(!text.trim() && !pendingImages.length) || send.isPending || uploadingImages} onClick={() => send.mutate()}>
                    {send.isPending ? <Loader2 className="spin" size={16} /> : <Send size={16} />}
                  </button>
                </div>
                {send.error ? <div className="inline-error">{String((send.error as Error).message)}</div> : null}
              </div>
            </>
          )}
        </div>
      </div>
      <ChatLightbox media={lightbox} onClose={() => setLightbox(null)} />
      <TemplatesDrawer
        open={templatesOpen}
        onClose={() => setTemplatesOpen(false)}
        title="Ответы в чатах"
        description="Готовые тексты для быстрых ответов покупателям."
        apiBase="/api/chats/templates"
        queryKey={["chat-templates"]}
        templates={templates}
        onInsert={selected ? (value) => {
          const resolved = substituteTemplate(value, historyQuery.data?.context ?? null);
          setText((current) => (current ? `${current}\n${resolved}` : resolved));
        } : undefined}
      />
    </section>
  );
}
