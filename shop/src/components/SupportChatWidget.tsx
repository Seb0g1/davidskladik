import { useEffect, useRef, useState } from "react";
import { MessageCircleHeart, X, Send, ChevronDown, Loader2, CheckCheck, Clock } from "lucide-react";

const API_BASE = (import.meta.env.VITE_API_BASE ?? "") + "/api/shop/support";

const S = {
  bg:      "#09060F",
  surface: "#130F1D",
  surface2:"#1C1630",
  border:  "rgba(255,255,255,0.07)",
  borderMd:"rgba(255,255,255,0.12)",
  text:    "#F0EBFF",
  muted:   "rgba(240,235,255,0.45)",
  accent:  "#7C3AED",
  accentLight: "#A78BFA",
  success: "#4ade80",
};

type Msg = { id: string; role: "user" | "admin"; body: string; createdAt: string };
type Phase = "closed" | "start" | "chat";

const POLL_INTERVAL = 5000;

export default function SupportChatWidget() {
  const [phase, setPhase] = useState<Phase>("closed");
  const [categories, setCategories] = useState<string[]>([]);
  const [name, setName] = useState("");
  const [category, setCategory] = useState("");
  const [firstMsg, setFirstMsg] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  const [token, setToken] = useState<string | null>(() => sessionStorage.getItem("mv_support_token"));
  const [chatStatus, setChatStatus] = useState<"open" | "closed">("open");
  const [messages, setMessages] = useState<Msg[]>([]);
  const [reply, setReply] = useState("");
  const [sending, setSending] = useState(false);
  const [unread, setUnread] = useState(false);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Load categories
  useEffect(() => {
    fetch(`${API_BASE}/categories`).then((r) => r.json()).then((d) => {
      if (Array.isArray(d.categories)) setCategories(d.categories);
    }).catch(() => {});
  }, []);

  // Restore existing session
  useEffect(() => {
    if (token) setPhase("chat");
  }, []);

  // Poll messages when chat is open
  useEffect(() => {
    if (phase !== "chat" || !token) return;
    void fetchMessages();
    pollRef.current = setInterval(() => void fetchMessages(), POLL_INTERVAL);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [phase, token]);

  // Scroll to bottom on new messages
  useEffect(() => {
    if (phase === "chat") {
      messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages, phase]);

  async function fetchMessages() {
    if (!token) return;
    try {
      const r = await fetch(`${API_BASE}/chats/${token}/messages`);
      if (!r.ok) { if (r.status === 404) { sessionStorage.removeItem("mv_support_token"); setToken(null); setPhase("start"); } return; }
      const d = await r.json();
      if (Array.isArray(d.messages)) {
        setMessages(d.messages);
        setChatStatus(d.status || "open");
        if (phase !== "chat") return;
        const lastAdmin = d.messages.filter((m: Msg) => m.role === "admin").at(-1);
        if (lastAdmin) setUnread(true);
      }
    } catch {}
  }

  async function handleStart(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim() || !category || !firstMsg.trim()) { setError("Заполните все поля"); return; }
    setSubmitting(true); setError("");
    try {
      const r = await fetch(`${API_BASE}/chats`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: name.trim(), category, message: firstMsg.trim() }),
      });
      const d = await r.json();
      if (!r.ok) { setError(d.error || "Ошибка"); return; }
      sessionStorage.setItem("mv_support_token", d.token);
      setToken(d.token);
      setPhase("chat");
    } catch { setError("Ошибка соединения"); }
    finally { setSubmitting(false); }
  }

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    if (!reply.trim() || sending || !token) return;
    const text = reply.trim();
    setReply("");
    setSending(true);
    try {
      const r = await fetch(`${API_BASE}/chats/${token}/messages`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text }),
      });
      if (r.ok) await fetchMessages();
    } catch {}
    finally { setSending(false); }
  }

  function openWidget() {
    setPhase(token ? "chat" : "start");
    setUnread(false);
  }

  function closeWidget() { setPhase("closed"); }

  function startNew() {
    sessionStorage.removeItem("mv_support_token");
    setToken(null); setMessages([]); setName(""); setCategory(""); setFirstMsg(""); setError(""); setPhase("start");
  }

  const btnSize = 56;

  return (
    <>
      {/* Floating button */}
      {phase === "closed" && (
        <button
          type="button"
          onClick={openWidget}
          aria-label="Открыть поддержку"
          style={{
            position: "fixed", bottom: 24, right: 24, zIndex: 9999,
            width: btnSize, height: btnSize, borderRadius: "50%",
            background: "linear-gradient(135deg, #7c3aed, #9333ea)",
            boxShadow: "0 8px 32px rgba(124,58,237,0.45), 0 2px 8px rgba(0,0,0,0.3)",
            border: "none", cursor: "pointer",
            display: "flex", alignItems: "center", justifyContent: "center",
            transition: "transform 0.2s ease, box-shadow 0.2s ease",
          }}
          onMouseEnter={e => { (e.currentTarget as HTMLElement).style.transform = "scale(1.08)"; (e.currentTarget as HTMLElement).style.boxShadow = "0 12px 40px rgba(124,58,237,0.55), 0 2px 8px rgba(0,0,0,0.3)"; }}
          onMouseLeave={e => { (e.currentTarget as HTMLElement).style.transform = ""; (e.currentTarget as HTMLElement).style.boxShadow = "0 8px 32px rgba(124,58,237,0.45), 0 2px 8px rgba(0,0,0,0.3)"; }}
        >
          <MessageCircleHeart size={24} style={{ color: "#fff" }} />
          {unread && (
            <span style={{
              position: "absolute", top: 4, right: 4, width: 12, height: 12,
              borderRadius: "50%", background: "#f43f5e",
              border: "2px solid #09060F",
            }} />
          )}
        </button>
      )}

      {/* Chat panel */}
      {phase !== "closed" && (
        <div style={{
          position: "fixed", bottom: 24, right: 24, zIndex: 9999,
          width: 360, maxWidth: "calc(100vw - 32px)",
          maxHeight: "min(600px, calc(100vh - 48px))",
          display: "flex", flexDirection: "column",
          background: S.surface, borderRadius: 20,
          border: `1px solid ${S.borderMd}`,
          boxShadow: "0 24px 64px rgba(0,0,0,0.5), 0 4px 16px rgba(124,58,237,0.15)",
          overflow: "hidden",
        }}>
          {/* Header */}
          <div style={{
            display: "flex", alignItems: "center", gap: 10, padding: "12px 16px",
            background: "linear-gradient(135deg, rgba(124,58,237,0.25), rgba(147,51,234,0.15))",
            borderBottom: `1px solid ${S.border}`, flexShrink: 0,
          }}>
            <div style={{
              width: 36, height: 36, borderRadius: 12, flexShrink: 0,
              background: "linear-gradient(135deg, #7c3aed, #9333ea)",
              display: "flex", alignItems: "center", justifyContent: "center",
            }}>
              <MessageCircleHeart size={18} style={{ color: "#fff" }} />
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 700, fontSize: 14, color: S.text }}>Поддержка Magic Vibes</div>
              <div style={{ fontSize: 11, color: S.muted }}>Обычно отвечаем в течение часа</div>
            </div>
            <button type="button" onClick={closeWidget} style={{
              padding: 6, borderRadius: 8, background: "rgba(255,255,255,0.06)", border: "none",
              color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
            }}>
              <ChevronDown size={16} />
            </button>
          </div>

          {/* START FORM */}
          {phase === "start" && (
            <div style={{ flex: 1, overflowY: "auto", padding: 16 }}>
              <p style={{ fontSize: 13, color: S.muted, marginBottom: 16, lineHeight: 1.5 }}>
                Здравствуйте! Заполните форму, и мы ответим вам как можно скорее.
              </p>
              <form onSubmit={handleStart} style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                <div>
                  <label style={{ fontSize: 11, fontWeight: 600, color: S.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>Ваше имя *</label>
                  <input
                    type="text" value={name} onChange={(e) => setName(e.target.value)} required maxLength={100}
                    placeholder="Как к вам обращаться"
                    style={{
                      marginTop: 4, width: "100%", padding: "10px 12px", borderRadius: 10, fontSize: 13,
                      background: S.surface2, border: `1.5px solid ${S.border}`, color: S.text,
                      outline: "none", fontFamily: "inherit", boxSizing: "border-box",
                    }}
                    onFocus={e => (e.target.style.borderColor = "rgba(124,58,237,0.5)")}
                    onBlur={e => (e.target.style.borderColor = S.border)}
                  />
                </div>
                <div>
                  <label style={{ fontSize: 11, fontWeight: 600, color: S.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>Категория *</label>
                  <select
                    value={category} onChange={(e) => setCategory(e.target.value)} required
                    style={{
                      marginTop: 4, width: "100%", padding: "10px 12px", borderRadius: 10, fontSize: 13,
                      background: S.surface2, border: `1.5px solid ${S.border}`, color: category ? S.text : S.muted,
                      outline: "none", fontFamily: "inherit", cursor: "pointer", boxSizing: "border-box",
                      appearance: "none",
                    }}
                    onFocus={e => (e.target.style.borderColor = "rgba(124,58,237,0.5)")}
                    onBlur={e => (e.target.style.borderColor = S.border)}
                  >
                    <option value="">Выберите категорию</option>
                    {categories.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <label style={{ fontSize: 11, fontWeight: 600, color: S.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>Вопрос *</label>
                  <textarea
                    value={firstMsg} onChange={(e) => setFirstMsg(e.target.value)} required maxLength={2000}
                    placeholder="Опишите ваш вопрос…"
                    rows={3}
                    style={{
                      marginTop: 4, width: "100%", padding: "10px 12px", borderRadius: 10, fontSize: 13,
                      background: S.surface2, border: `1.5px solid ${S.border}`, color: S.text,
                      outline: "none", fontFamily: "inherit", resize: "none", boxSizing: "border-box",
                    }}
                    onFocus={e => (e.target.style.borderColor = "rgba(124,58,237,0.5)")}
                    onBlur={e => (e.target.style.borderColor = S.border)}
                  />
                </div>
                {error && <p style={{ fontSize: 12, color: "#f87171" }}>{error}</p>}
                <button
                  type="submit" disabled={submitting}
                  style={{
                    padding: "11px", borderRadius: 12, fontSize: 13, fontWeight: 700, color: "#fff",
                    background: "linear-gradient(135deg, #7c3aed, #9333ea)", border: "none", cursor: "pointer",
                    boxShadow: "0 4px 16px rgba(124,58,237,0.4)", opacity: submitting ? 0.6 : 1,
                    display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                  }}
                >
                  {submitting ? <><Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} /> Отправка…</> : "Начать чат"}
                </button>
              </form>
            </div>
          )}

          {/* CHAT */}
          {phase === "chat" && (
            <>
              <div style={{ flex: 1, overflowY: "auto", padding: "12px 12px 4px", display: "flex", flexDirection: "column", gap: 8 }}>
                {messages.length === 0 && (
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "center", flex: 1, color: S.muted, fontSize: 13 }}>
                    <Loader2 size={16} style={{ animation: "spin 1s linear infinite", marginRight: 8 }} /> Загрузка…
                  </div>
                )}
                {messages.map((msg) => (
                  <div key={msg.id} style={{ display: "flex", justifyContent: msg.role === "user" ? "flex-end" : "flex-start" }}>
                    {msg.role === "admin" && (
                      <div style={{
                        width: 24, height: 24, borderRadius: "50%", flexShrink: 0, marginRight: 6, marginTop: 2,
                        background: "linear-gradient(135deg, #7c3aed, #9333ea)",
                        display: "flex", alignItems: "center", justifyContent: "center",
                      }}>
                        <MessageCircleHeart size={12} style={{ color: "#fff" }} />
                      </div>
                    )}
                    <div style={{
                      maxWidth: "78%", padding: "9px 12px", borderRadius: msg.role === "user" ? "16px 16px 4px 16px" : "16px 16px 16px 4px",
                      background: msg.role === "user" ? "linear-gradient(135deg, #7c3aed, #9333ea)" : S.surface2,
                      border: msg.role === "admin" ? `1px solid ${S.border}` : "none",
                    }}>
                      <p style={{ fontSize: 13, color: S.text, lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-word", margin: 0 }}>{msg.body}</p>
                      <p style={{ fontSize: 10, color: msg.role === "user" ? "rgba(255,255,255,0.5)" : S.muted, marginTop: 3, display: "flex", alignItems: "center", gap: 3 }}>
                        <Clock size={9} />
                        {new Date(msg.createdAt).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}
                        {msg.role === "user" && <CheckCheck size={10} style={{ marginLeft: 2 }} />}
                      </p>
                    </div>
                  </div>
                ))}
                {chatStatus === "closed" && (
                  <div style={{ textAlign: "center", padding: "8px 0" }}>
                    <span style={{ fontSize: 11, color: S.muted, background: S.surface2, padding: "4px 10px", borderRadius: 20 }}>Чат закрыт</span>
                  </div>
                )}
                <div ref={messagesEndRef} />
              </div>

              {/* Input */}
              {chatStatus === "open" ? (
                <form onSubmit={handleSend} style={{
                  display: "flex", gap: 8, padding: "10px 12px",
                  borderTop: `1px solid ${S.border}`, flexShrink: 0,
                }}>
                  <input
                    type="text" value={reply} onChange={(e) => setReply(e.target.value)}
                    placeholder="Напишите сообщение…"
                    maxLength={2000}
                    style={{
                      flex: 1, padding: "9px 12px", borderRadius: 10, fontSize: 13,
                      background: S.surface2, border: `1.5px solid ${S.border}`, color: S.text,
                      outline: "none", fontFamily: "inherit",
                    }}
                    onFocus={e => (e.target.style.borderColor = "rgba(124,58,237,0.5)")}
                    onBlur={e => (e.target.style.borderColor = S.border)}
                  />
                  <button type="submit" disabled={!reply.trim() || sending} style={{
                    padding: "9px 10px", borderRadius: 10, border: "none", cursor: "pointer",
                    background: "linear-gradient(135deg, #7c3aed, #9333ea)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    opacity: (!reply.trim() || sending) ? 0.4 : 1, flexShrink: 0,
                  }}>
                    {sending ? <Loader2 size={15} style={{ color: "#fff", animation: "spin 1s linear infinite" }} /> : <Send size={15} style={{ color: "#fff" }} />}
                  </button>
                </form>
              ) : (
                <div style={{ padding: "10px 12px", borderTop: `1px solid ${S.border}`, flexShrink: 0 }}>
                  <button type="button" onClick={startNew} style={{
                    width: "100%", padding: "9px", borderRadius: 10, fontSize: 12, fontWeight: 600,
                    color: S.accentLight, background: "rgba(124,58,237,0.1)", border: `1px solid rgba(124,58,237,0.2)`,
                    cursor: "pointer",
                  }}>
                    Открыть новое обращение
                  </button>
                </div>
              )}

              {/* Footer */}
              <div style={{ padding: "4px 12px 8px", display: "flex", justifyContent: "center" }}>
                <button type="button" onClick={startNew} style={{ fontSize: 10, color: S.muted, background: "none", border: "none", cursor: "pointer" }}>
                  Новое обращение
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </>
  );
}
