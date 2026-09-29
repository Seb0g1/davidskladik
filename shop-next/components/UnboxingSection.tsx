"use client";
import { useState, useEffect, useRef } from "react";

const SHOP_API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

interface Unboxing {
  id: string;
  name: string;
  mediaUrl: string;
  text: string;
  createdAt: string;
}

export default function UnboxingSection() {
  const [unboxings, setUnboxings] = useState<Unboxing[]>([]);
  const [name, setName] = useState("");
  const [mediaUrl, setMediaUrl] = useState("");
  const [text, setText] = useState("");
  const [success, setSuccess] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [preview, setPreview] = useState<{ url: string; isVideo: boolean } | null>(null);
  const [pending, setPending] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    fetch(SHOP_API + "/unboxings").then(r => r.json()).then(d => { if (d.unboxings) setUnboxings(d.unboxings); }).catch(() => {});
  }, []);

  async function handleFile(file: File) {
    setUploading(true);
    try {
      const fd = new FormData(); fd.append("file", file);
      const r = await fetch(SHOP_API + "/upload", { method: "POST", body: fd });
      const d = await r.json();
      if (d.ok) { setMediaUrl(d.url); setPreview({ url: d.url, isVideo: d.isVideo }); }
    } catch { /* best-effort */ }
    setUploading(false);
  }

  async function submit() {
    if (pending) return;
    setPending(true);
    try {
      const r = await fetch(SHOP_API + "/unboxings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name, mediaUrl, text }) });
      const d = await r.json();
      if (d.ok) { setSuccess(true); setName(""); setMediaUrl(""); setText(""); setPreview(null); }
    } catch { /* best-effort */ }
    setPending(false);
  }

  const inputStyle: React.CSSProperties = { width: "100%", padding: "9px 0", background: "transparent", border: "none", borderBottom: "1px solid rgba(var(--ink-rgb),0.096)", color: "var(--ink)", fontSize: 13.5, outline: "none", boxSizing: "border-box", transition: "border-bottom-color 0.3s", fontFamily: "inherit" };

  return (
    <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) clamp(56px,8vw,96px)" }}>
      <p className="eyebrow" style={{ marginBottom: 10 }}>Наши покупатели</p>
      <h2 className="serif" style={{ margin: "0 0 34px", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "var(--ink)" }}>Распаковки</h2>

      {unboxings.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))", gap: 18, marginBottom: 48 }}>
          {unboxings.map((u) => {
            const isYt = /youtube\.com|youtu\.be/.test(u.mediaUrl);
            let embedUrl = "";
            if (isYt) { const m = u.mediaUrl.match(/(?:v=|youtu\.be\/)([A-Za-z0-9_-]{11})/); if (m) embedUrl = `https://www.youtube.com/embed/${m[1]}`; }
            return (
              <div key={u.id} style={{ padding: 20, border: "1px solid rgba(var(--ink-rgb),0.056)", borderRadius: 14, background: "var(--surface)", display: "flex", flexDirection: "column", gap: 12 }}>
                {isYt && embedUrl ? (
                  <iframe src={embedUrl} style={{ width: "100%", height: 180, border: "none", borderRadius: 10 }} allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowFullScreen loading="lazy" title={`Распаковка от ${u.name}`} />
                ) : u.mediaUrl ? (
                  <a href={u.mediaUrl} target="_blank" rel="noreferrer" style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--accent)" }}>Смотреть →</a>
                ) : null}
                {u.text && <p style={{ margin: 0, fontSize: 13, color: "rgba(var(--ink-rgb),0.66)", lineHeight: 1.7 }}>{u.text.length > 220 ? u.text.slice(0, 220) + "…" : u.text}</p>}
                <div>
                  <p style={{ margin: 0, fontSize: 12, color: "var(--text)" }}>{u.name}</p>
                  <p style={{ margin: "3px 0 0", fontSize: 10, color: "rgba(var(--ink-rgb),0.55)" }}>{new Date(u.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}</p>
                </div>
              </div>
            );
          })}
        </div>
      )}

      <div style={{ maxWidth: 560, padding: "clamp(24px,3vw,40px)", border: "1px solid rgba(var(--ink-rgb),0.056)", borderRadius: 14, background: "var(--surface)" }}>
        <p style={{ margin: "0 0 20px", fontSize: 13, color: "rgba(var(--ink-rgb),0.55)", letterSpacing: "0.06em" }}>Поделитесь своей распаковкой</p>
        {success ? (
          <div>
            <p style={{ margin: "0 0 8px", fontSize: 13.5, color: "var(--success)" }}>Спасибо! Ваша распаковка отправлена на проверку.</p>
            <p style={{ margin: 0, fontSize: 12.5, color: "rgba(var(--ink-rgb),0.55)" }}>За публикацию вы получите промокод <span style={{ color: "var(--accent)", fontWeight: 600 }}>UNBOX7</span> на скидку 7%</p>
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <input type="text" value={name} onChange={e => setName(e.target.value)} placeholder="Ваше имя" style={inputStyle}
              onFocus={e => (e.target.style.borderBottomColor = "rgba(var(--accent-rgb),0.55)")}
              onBlur={e => (e.target.style.borderBottomColor = "rgba(var(--ink-rgb),0.096)")} />

            <input ref={fileRef} type="file" accept="image/jpeg,image/png,image/webp,video/mp4,video/webm" style={{ display: "none" }} onChange={e => { const f = e.target.files?.[0]; if (f) handleFile(f); }} />

            {preview ? (
              <div style={{ position: "relative", borderRadius: 14, overflow: "hidden", border: "1px solid rgba(var(--accent-rgb),0.3)" }}>
                {preview.isVideo
                  ? <video src={preview.url} controls style={{ width: "100%", maxHeight: 220, display: "block" }} />
                  : <img src={preview.url} alt="" style={{ width: "100%", maxHeight: 220, objectFit: "contain", display: "block", background: "var(--paper)" }} />}
                <button onClick={() => { setPreview(null); setMediaUrl(""); }} style={{ position: "absolute", top: 6, right: 6, background: "rgba(var(--ink-rgb),0.245)", border: "none", borderRadius: 10, color: "rgba(var(--ink-rgb),0.66)", fontSize: 12, padding: "3px 8px", cursor: "pointer" }}>✕</button>
              </div>
            ) : (
              <div
                onClick={() => fileRef.current?.click()}
                onDragOver={e => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={e => { e.preventDefault(); setDragOver(false); const f = e.dataTransfer.files[0]; if (f) handleFile(f); }}
                style={{ padding: "20px 16px", borderRadius: 14, cursor: "pointer", textAlign: "center", border: `1px dashed ${dragOver ? "rgba(var(--accent-rgb),0.7)" : "rgba(var(--ink-rgb),0.12)"}`, background: dragOver ? "rgba(var(--accent-rgb),0.05)" : "transparent", transition: "border-color 0.2s, background 0.2s" }}
              >
                {uploading ? (
                  <span style={{ fontSize: 12, color: "var(--accent)" }}>Загрузка…</span>
                ) : (
                  <>
                    <div style={{ fontSize: 22, marginBottom: 6, opacity: 0.4 }}>📎</div>
                    <div style={{ fontSize: 12, color: "rgba(var(--ink-rgb),0.55)" }}>Перетащите фото/видео или нажмите для выбора</div>
                    <div style={{ fontSize: 11, color: "rgba(var(--ink-rgb),0.45)", marginTop: 4 }}>JPG, PNG, WEBP, MP4 · до 50 МБ</div>
                  </>
                )}
              </div>
            )}

            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div style={{ flex: 1, height: 1, background: "rgba(var(--ink-rgb),0.056)" }} />
              <span style={{ fontSize: 10, color: "rgba(var(--ink-rgb),0.45)", letterSpacing: "0.08em" }}>или</span>
              <div style={{ flex: 1, height: 1, background: "rgba(var(--ink-rgb),0.056)" }} />
            </div>

            <input type="text" value={preview ? "" : mediaUrl} onChange={e => { setMediaUrl(e.target.value); setPreview(null); }} placeholder="Ссылка на YouTube или фото" disabled={!!preview} style={{ ...inputStyle, opacity: preview ? 0.3 : 1 }}
              onFocus={e => (e.target.style.borderBottomColor = "rgba(var(--accent-rgb),0.55)")}
              onBlur={e => (e.target.style.borderBottomColor = "rgba(var(--ink-rgb),0.096)")} />

            <textarea value={text} onChange={e => setText(e.target.value)} placeholder="Расскажите о вашей покупке..." rows={2} style={{ ...inputStyle, resize: "none" }}
              onFocus={e => (e.target.style.borderBottomColor = "rgba(var(--accent-rgb),0.55)")}
              onBlur={e => (e.target.style.borderBottomColor = "rgba(var(--ink-rgb),0.096)")} />

            <button onClick={submit} disabled={pending || (!name.trim() && !mediaUrl.trim() && !text.trim())} style={{ alignSelf: "flex-start", padding: "11px 28px", background: "transparent", border: "1px solid rgba(var(--accent-rgb),0.4)", borderRadius: 10, color: "var(--accent)", fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer", transition: "background 0.3s, color 0.3s", opacity: pending ? 0.6 : 1 }}
              onMouseEnter={e => { if (!pending) { e.currentTarget.style.background = "rgba(var(--accent-rgb),0.12)"; e.currentTarget.style.color = "var(--accent2)"; } }}
              onMouseLeave={e => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.color = "var(--accent)"; }}>
              {pending ? "Отправка…" : "Отправить"}
            </button>
          </div>
        )}
      </div>
    </section>
  );
}
