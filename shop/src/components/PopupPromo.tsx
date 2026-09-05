import { useState, useEffect } from "react";

const STORAGE_KEY = "mv_promo_dismissed";
const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000;

export default function PopupPromo() {
  const [visible, setVisible] = useState(false);
  const [email, setEmail] = useState("");
  const [sent, setSent] = useState(false);

  useEffect(() => {
    const dismissed = localStorage.getItem(STORAGE_KEY);
    if (dismissed && Date.now() - Number(dismissed) < THIRTY_DAYS) return;

    const timer = setTimeout(() => setVisible(true), 10000);

    const onMouseLeave = (e: MouseEvent) => {
      if (e.clientY < 5 && e.relatedTarget === null) {
        clearTimeout(timer);
        setVisible(true);
      }
    };
    document.addEventListener("mouseleave", onMouseLeave);
    return () => {
      clearTimeout(timer);
      document.removeEventListener("mouseleave", onMouseLeave);
    };
  }, []);

  function dismiss() {
    localStorage.setItem(STORAGE_KEY, String(Date.now()));
    setVisible(false);
  }

  function submit() {
    setSent(true);
    setTimeout(dismiss, 2800);
  }

  if (!visible) return null;

  return (
    <div
      onClick={(e) => { if (e.target === e.currentTarget) dismiss(); }}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(0,0,0,0.72)",
        backdropFilter: "blur(4px)",
        WebkitBackdropFilter: "blur(4px)",
        zIndex: 9000,
        display: "flex", alignItems: "center", justifyContent: "center",
      }}
    >
      <div style={{
        position: "relative",
        width: "min(440px, 90vw)",
        padding: "clamp(32px,5vw,52px)",
        background: "#0f0f0f",
        border: "1px solid rgba(201,162,94,0.32)",
        borderRadius: 3,
        animation: "mv-popup-in 0.6s cubic-bezier(0.16,1,0.3,1) both",
      }}>
        <button
          onClick={dismiss}
          aria-label="Закрыть"
          style={{
            position: "absolute", top: 16, right: 18,
            background: "transparent", border: "none",
            color: "#6f6c66", fontSize: 22, cursor: "pointer",
            lineHeight: 1, padding: 4,
            transition: "color 0.2s ease",
          }}
          onMouseEnter={e => (e.currentTarget.style.color = "#f5f4f0")}
          onMouseLeave={e => (e.currentTarget.style.color = "#6f6c66")}
        >
          ×
        </button>

        <p style={{ margin: "0 0 20px", fontSize: 10, letterSpacing: "0.34em", textTransform: "uppercase", color: "#c9a25e" }}>
          ✦ Magic Vibes
        </p>

        <span style={{
          display: "block",
          fontFamily: "'Cormorant Garamond',Georgia,serif",
          fontStyle: "italic",
          fontSize: "clamp(56px,12vw,72px)",
          color: "#c9a25e",
          lineHeight: 1,
          marginBottom: 6,
        }}>–10%</span>
        <p style={{ margin: "0 0 22px", fontSize: 16, color: "#8b8880", lineHeight: 1.4 }}>на первый заказ</p>

        {!sent ? (
          <>
            <p style={{ margin: "0 0 14px", fontSize: 13, color: "#6f6c66" }}>Введите почту — получите промокод</p>
            <div style={{ display: "flex", gap: 10, alignItems: "flex-end" }}>
              <input
                type="email"
                value={email}
                onChange={e => setEmail(e.target.value)}
                onKeyDown={e => e.key === "Enter" && submit()}
                placeholder="your@email.com"
                style={{
                  flex: 1, padding: "11px 0",
                  background: "transparent", border: "none",
                  borderBottom: "1px solid rgba(255,255,255,0.15)",
                  color: "#f5f4f0", fontSize: 14, outline: "none",
                  transition: "border-bottom-color 0.3s ease",
                }}
                onFocus={e => (e.target.style.borderBottomColor = "rgba(201,162,94,0.6)")}
                onBlur={e => (e.target.style.borderBottomColor = "rgba(255,255,255,0.15)")}
              />
              <button
                onClick={submit}
                style={{
                  padding: "11px 20px",
                  background: "#c9a25e", color: "#0b0b0b",
                  border: "none", borderRadius: 2,
                  fontSize: 12, letterSpacing: "0.12em",
                  fontWeight: 600, cursor: "pointer",
                  whiteSpace: "nowrap",
                  transition: "background 0.3s ease",
                }}
                onMouseEnter={e => (e.currentTarget.style.background = "#e8d5a3")}
                onMouseLeave={e => (e.currentTarget.style.background = "#c9a25e")}
              >
                Получить
              </button>
            </div>
            <p style={{ margin: "14px 0 0", fontSize: 11, color: "#3a3730" }}>
              Нажимая, вы соглашаетесь на получение писем от Magic Vibes
            </p>
          </>
        ) : (
          <div style={{ paddingTop: 4 }}>
            <p style={{ margin: "0 0 10px", fontSize: 13, color: "#8b8880" }}>Ваш промокод:</p>
            <p style={{
              margin: "0 0 14px",
              fontFamily: "'Cormorant Garamond',Georgia,serif",
              fontStyle: "italic",
              fontSize: 36, color: "#c9a25e",
              letterSpacing: "0.12em",
            }}>VIBES10</p>
            <p style={{ margin: 0, fontSize: 13, color: "#6f6c66" }}>Введите его при оформлении заказа</p>
          </div>
        )}
      </div>
    </div>
  );
}
