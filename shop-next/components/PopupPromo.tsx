"use client";
import { useState, useEffect } from "react";
import ConsentCheck from "@/components/ConsentCheck";
import { usePathname } from "next/navigation";
import { emailSubscribe } from "@/lib/client";

const STORAGE_KEY = "mv_promo_dismissed";
const THIRTY_DAYS = 30 * 24 * 60 * 60 * 1000;

export default function PopupPromo() {
  const [visible, setVisible] = useState(false);
  const [email, setEmail] = useState("");
  const [sent, setSent] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [pd, setPd] = useState(false);
  const [ads, setAds] = useState(false);
  const [sheet, setSheet] = useState(false);
  const pathname = usePathname();
  // never interrupt sign-in or checkout
  const quiet = /^\/(login|checkout|cart|account|orders|order-|studio)/.test(pathname || "");

  useEffect(() => {
    if (quiet) return;
    let dismissed: string | null = null;
    try { dismissed = localStorage.getItem(STORAGE_KEY); } catch {}
    if (dismissed && Date.now() - Number(dismissed) < THIRTY_DAYS) return;

    // phones: later, and as a bottom sheet instead of a full-screen blocker
    const phone = window.matchMedia("(max-width: 767px)").matches;
    setSheet(phone);
    const timer = setTimeout(() => setVisible(true), phone ? 35000 : 15000);

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
  }, [quiet]);

  function dismiss() {
    try { localStorage.setItem(STORAGE_KEY, String(Date.now())); } catch {}
    setVisible(false);
  }

  async function submit() {
    if (!email || submitting || !pd || !ads) return;
    setSubmitting(true);
    try {
      await emailSubscribe(email, "popup");
    } catch {}
    setSubmitting(false);
    setSent(true);
    setTimeout(dismiss, 3000);
  }

  if (!visible || quiet) return null;

  return (
    <>
      <style>{`
        @keyframes mv-popup-in {
          from { opacity: 0; transform: scale(0.94) translateY(16px); }
          to   { opacity: 1; transform: scale(1) translateY(0); }
        }
      `}</style>
      <div
        onClick={(e) => { if (e.target === e.currentTarget) dismiss(); }}
        style={{
          position: "fixed", inset: 0,
          background: "rgba(var(--ink-rgb),0.252)",
          backdropFilter: "blur(4px)",
          WebkitBackdropFilter: "blur(4px)",
          zIndex: 9000,
          display: "flex", alignItems: sheet ? "flex-end" : "center", justifyContent: "center",
        }}
      >
        <div style={{
          position: "relative",
          width: sheet ? "100%" : "min(440px, 90vw)",
          padding: sheet ? "28px 20px calc(24px + env(safe-area-inset-bottom))" : "clamp(32px,5vw,52px)",
          background: "var(--surface)",
          border: "1px solid rgba(var(--accent-rgb),0.32)",
          borderRadius: sheet ? "24px 24px 0 0" : 14,
          animation: "mv-popup-in 0.6s cubic-bezier(0.16,1,0.3,1) both",
        }}>
          <button
            onClick={dismiss}
            aria-label="Закрыть"
            style={{
              position: "absolute", top: 16, right: 18,
              background: "transparent", border: "none",
              color: "rgba(var(--ink-rgb),0.55)", fontSize: 22, cursor: "pointer",
              lineHeight: 1, padding: 4,
              transition: "color 0.2s ease",
            }}
            onMouseEnter={e => (e.currentTarget.style.color = "var(--ink)")}
            onMouseLeave={e => (e.currentTarget.style.color = "rgba(var(--ink-rgb),0.55)")}
          >
            ×
          </button>

          <p style={{ margin: "0 0 20px", fontSize: 10, letterSpacing: "0.34em", textTransform: "uppercase", color: "var(--accent)" }}>
            ✦ Magic Vibes
          </p>

          <span style={{
            display: "block",
            fontFamily: "var(--font-display)",
            fontSize: "clamp(56px,12vw,72px)",
            color: "var(--accent)",
            lineHeight: 1,
            marginBottom: 6,
          }}>–10%</span>
          <p style={{ margin: "0 0 22px", fontSize: 16, color: "rgba(var(--ink-rgb),0.66)", lineHeight: 1.4 }}>на первый заказ</p>

          {!sent ? (
            <>
              <p style={{ margin: "0 0 14px", fontSize: 13, color: "rgba(var(--ink-rgb),0.55)" }}>Введите почту — получите промокод</p>
              <div style={{ display: "flex", gap: 10, alignItems: "flex-end" }}>
                <input
                  type="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && !submitting && submit()}
                  placeholder="your@email.com"
                  style={{
                    flex: 1, padding: "11px 0",
                    background: "transparent", border: "none",
                    borderBottom: "1px solid rgba(var(--ink-rgb),0.12)",
                    color: "var(--ink)", fontSize: 14, outline: "none",
                    transition: "border-bottom-color 0.3s ease",
                  }}
                  onFocus={e => (e.target.style.borderBottomColor = "rgba(var(--accent-rgb),0.6)")}
                  onBlur={e => (e.target.style.borderBottomColor = "rgba(var(--ink-rgb),0.12)")}
                />
                <button
                  onClick={submit}
                  disabled={!email || submitting || !pd || !ads}
                  style={{
                    padding: "11px 20px",
                    background: "var(--accent)", color: "var(--paper)",
                    border: "none", borderRadius: 10,
                    fontSize: 12, letterSpacing: "0.12em",
                    fontWeight: 600, cursor: "pointer",
                    whiteSpace: "nowrap",
                    transition: "background 0.3s ease",
                    opacity: (!email || submitting || !pd || !ads) ? 0.6 : 1,
                  }}
                  onMouseEnter={e => { if (!submitting && email) e.currentTarget.style.background = "var(--accent2)"; }}
                  onMouseLeave={e => (e.currentTarget.style.background = "var(--accent)")}
                >
                  {submitting ? "…" : "Получить"}
                </button>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 14 }}>
                <ConsentCheck kind="pd" compact checked={pd} onChange={setPd} />
                <ConsentCheck kind="ads" subscribe compact checked={ads} onChange={setAds} />
              </div>
            </>
          ) : (
            <div style={{ paddingTop: 4 }}>
              <div style={{ margin: "0 0 14px", width: 40, height: 40, borderRadius: "50%", background: "rgba(var(--success-rgb),0.12)", border: "1px solid rgba(var(--success-rgb),0.3)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 20 }}>✓</div>
              <p style={{ margin: "0 0 8px", fontSize: 15, fontFamily: "var(--font-display)", color: "var(--ink)" }}>Промокод отправлен!</p>
              <p style={{ margin: "0 0 14px", fontSize: 13, color: "rgba(var(--ink-rgb),0.66)", lineHeight: 1.6 }}>Проверьте вашу почту — письмо с промокодом на <strong style={{ color: "var(--accent)" }}>−10%</strong> уже в пути.</p>
              <p style={{ margin: 0, fontSize: 11, color: "rgba(var(--ink-rgb),0.45)" }}>Не пришло? Проверьте папку «Спам»</p>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
