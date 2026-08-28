import { useState, useEffect, useRef } from "react";
import { X, Mail, ArrowRight, Loader2, CheckCircle, RefreshCw } from "lucide-react";
import { useAuth } from "../AuthContext";

interface Props {
  open: boolean;
  onClose: () => void;
  defaultTab?: "login" | "register";
}

type Step = "email" | "code" | "done";

const CODE_LEN = 6;
const RESEND_SECONDS = 60;

export default function AuthModal({ open, onClose }: Props) {
  const { sendCode, verifyCode } = useAuth();

  const [step, setStep] = useState<Step>("email");
  const [email, setEmail] = useState("");
  const [digits, setDigits] = useState<string[]>(Array(CODE_LEN).fill(""));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [countdown, setCountdown] = useState(0);

  const inputRefs = useRef<(HTMLInputElement | null)[]>([]);
  const emailInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setStep("email");
      setEmail("");
      setDigits(Array(CODE_LEN).fill(""));
      setError("");
      setCountdown(0);
      setTimeout(() => emailInputRef.current?.focus(), 80);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  useEffect(() => {
    if (countdown <= 0) return;
    const t = setTimeout(() => setCountdown((c) => c - 1), 1000);
    return () => clearTimeout(t);
  }, [countdown]);

  if (!open) return null;

  async function handleSendCode(e: React.FormEvent) {
    e.preventDefault();
    if (!email.trim()) return;
    setError("");
    setLoading(true);
    try {
      await sendCode(email.trim());
      setStep("code");
      setCountdown(RESEND_SECONDS);
      setTimeout(() => inputRefs.current[0]?.focus(), 80);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  async function handleVerify(codeStr: string) {
    setError("");
    setLoading(true);
    try {
      await verifyCode(email.trim(), codeStr);
      setStep("done");
      setTimeout(onClose, 1400);
    } catch (err) {
      setError((err as Error).message);
      setDigits(Array(CODE_LEN).fill(""));
      setTimeout(() => inputRefs.current[0]?.focus(), 50);
    } finally {
      setLoading(false);
    }
  }

  function handleDigitChange(idx: number, val: string) {
    const char = val.replace(/\D/g, "").slice(-1);
    const next = [...digits];
    next[idx] = char;
    setDigits(next);
    setError("");
    if (char && idx < CODE_LEN - 1) inputRefs.current[idx + 1]?.focus();
    if (next.every((d) => d) && next.join("").length === CODE_LEN) handleVerify(next.join(""));
  }

  function handleDigitKeyDown(idx: number, e: React.KeyboardEvent) {
    if (e.key === "Backspace" && !digits[idx] && idx > 0) {
      inputRefs.current[idx - 1]?.focus();
    }
  }

  function handleDigitPaste(e: React.ClipboardEvent) {
    const pasted = e.clipboardData.getData("text").replace(/\D/g, "").slice(0, CODE_LEN);
    if (!pasted) return;
    e.preventDefault();
    const next = Array(CODE_LEN).fill("");
    for (let i = 0; i < pasted.length; i++) next[i] = pasted[i];
    setDigits(next);
    const focusIdx = Math.min(pasted.length, CODE_LEN - 1);
    inputRefs.current[focusIdx]?.focus();
    if (pasted.length === CODE_LEN) handleVerify(pasted);
  }

  async function handleResend() {
    if (countdown > 0) return;
    setError("");
    setDigits(Array(CODE_LEN).fill(""));
    setLoading(true);
    try {
      await sendCode(email.trim());
      setCountdown(RESEND_SECONDS);
      setTimeout(() => inputRefs.current[0]?.focus(), 50);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div
      style={{ position: "fixed", inset: 0, zIndex: 100, display: "flex", alignItems: "flex-end", justifyContent: "center" }}
      className="sm:items-center"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      {/* Backdrop */}
      <div
        className="modal-overlay"
        style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.6)", backdropFilter: "blur(6px)" }}
        onClick={onClose}
      />

      {/* Panel */}
      <div
        className="modal-content"
        style={{
          position: "relative",
          width: "100%", maxWidth: 440,
          background: "var(--surface2)",
          borderRadius: "20px 20px 0 0",
          border: "1px solid var(--border-md)",
          borderBottom: "none",
          overflow: "hidden",
          boxShadow: "0 -20px 60px rgba(0,0,0,0.5)",
        }}
      >
        <style>{`@media(min-width:640px){.auth-panel{border-radius:16px!important;border-bottom:1px solid var(--border-md)!important;}}`}</style>
        <div className="auth-panel">
          {/* Gold top accent */}
          <div style={{ height: 2, background: "linear-gradient(90deg, transparent, var(--accent), transparent)" }} />

          <div style={{ padding: "28px 28px 32px" }}>
            {/* Close */}
            <button
              onClick={onClose}
              style={{ position: "absolute", top: 14, right: 14, padding: 8, borderRadius: 8, color: "var(--subtle)", background: "transparent", border: "none", cursor: "pointer", display: "flex", transition: "color 0.15s" }}
              onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
              onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
            >
              <X size={17} />
            </button>

            {/* STEP: email */}
            {step === "email" && (
              <form onSubmit={handleSendCode}>
                <div style={{ marginBottom: 24 }}>
                  <div style={{ width: 42, height: 42, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 18, background: "rgba(201,169,110,0.1)", border: "1px solid rgba(201,169,110,0.2)" }}>
                    <Mail size={19} style={{ color: "var(--accent)" }} strokeWidth={1.7} />
                  </div>
                  <h2 className="serif" style={{ fontSize: 20, fontWeight: 500, color: "var(--text)", marginBottom: 6, fontStyle: "italic" }}>Вход или регистрация</h2>
                  <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.5 }}>Введите email — пришлём код для входа</p>
                </div>

                <input
                  ref={emailInputRef}
                  type="email"
                  placeholder="your@email.com"
                  required
                  value={email}
                  onChange={(e) => { setEmail(e.target.value); setError(""); }}
                  className="input-base"
                  style={{ marginBottom: 12 }}
                />

                {error && (
                  <div style={{ fontSize: 13, color: "#F87171", background: "rgba(248,113,113,0.08)", border: "1px solid rgba(248,113,113,0.2)", borderRadius: 8, padding: "10px 14px", marginBottom: 12 }}>{error}</div>
                )}

                <button
                  type="submit"
                  disabled={loading || !email.trim()}
                  className="btn-primary"
                  style={{ width: "100%", justifyContent: "center", opacity: (loading || !email.trim()) ? 0.6 : 1 }}
                >
                  {loading ? <Loader2 size={15} style={{ animation: "spin 1s linear infinite" }} /> : <ArrowRight size={15} />}
                  Получить код
                </button>

                <p style={{ textAlign: "center", fontSize: 11, color: "var(--subtle)", marginTop: 16 }}>
                  Нажимая кнопку, вы соглашаетесь с{" "}
                  <a href="/privacy" style={{ color: "var(--accent)", textDecoration: "none" }}>политикой конфиденциальности</a>
                </p>
              </form>
            )}

            {/* STEP: code */}
            {step === "code" && (
              <div>
                <div style={{ marginBottom: 24 }}>
                  <div style={{ width: 42, height: 42, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 18, background: "rgba(201,169,110,0.1)", border: "1px solid rgba(201,169,110,0.2)" }}>
                    <Mail size={19} style={{ color: "var(--accent)" }} strokeWidth={1.7} />
                  </div>
                  <h2 className="serif" style={{ fontSize: 20, fontWeight: 500, color: "var(--text)", marginBottom: 6, fontStyle: "italic" }}>Введите код</h2>
                  <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.5 }}>
                    Отправили 6-значный код на{" "}
                    <button onClick={() => { setStep("email"); setError(""); }} style={{ color: "var(--accent)", background: "none", border: "none", cursor: "pointer", fontFamily: "inherit", fontSize: 13, padding: 0 }}>{email}</button>
                  </p>
                </div>

                {/* OTP boxes */}
                <div style={{ display: "flex", gap: 8, justifyContent: "space-between", marginBottom: 14 }} onPaste={handleDigitPaste}>
                  {digits.map((d, i) => (
                    <input
                      key={i}
                      ref={(el) => { inputRefs.current[i] = el; }}
                      type="text"
                      inputMode="numeric"
                      maxLength={1}
                      value={d}
                      onChange={(e) => handleDigitChange(i, e.target.value)}
                      onKeyDown={(e) => handleDigitKeyDown(i, e)}
                      disabled={loading}
                      style={{
                        width: "13.5%", aspectRatio: "1", textAlign: "center",
                        fontSize: 22, fontWeight: 600, borderRadius: 10,
                        background: d ? "rgba(201,169,110,0.1)" : "rgba(255,252,245,0.04)",
                        border: error ? "1px solid rgba(248,113,113,0.5)" : d ? "1px solid rgba(201,169,110,0.35)" : "1px solid var(--border)",
                        color: d ? "var(--accent3)" : "var(--text)",
                        outline: "none",
                        transition: "border-color 0.15s, background 0.15s",
                        fontFamily: "inherit",
                      }}
                      onFocus={e => { if (!error) (e.target as HTMLInputElement).style.borderColor = "rgba(201,169,110,0.5)"; }}
                      onBlur={e => { (e.target as HTMLInputElement).style.borderColor = error ? "rgba(248,113,113,0.5)" : digits[i] ? "rgba(201,169,110,0.35)" : "var(--border)"; }}
                    />
                  ))}
                </div>

                {loading && (
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8, fontSize: 13, color: "var(--muted)", marginBottom: 10 }}>
                    <Loader2 size={14} style={{ animation: "spin 1s linear infinite" }} />
                    Проверяем...
                  </div>
                )}

                {error && (
                  <div style={{ fontSize: 13, color: "#F87171", background: "rgba(248,113,113,0.08)", border: "1px solid rgba(248,113,113,0.2)", borderRadius: 8, padding: "10px 14px", marginBottom: 12 }}>{error}</div>
                )}

                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, marginTop: 8 }}>
                  <button
                    onClick={handleResend}
                    disabled={countdown > 0 || loading}
                    style={{ display: "flex", alignItems: "center", gap: 5, color: countdown > 0 ? "var(--subtle)" : "var(--accent)", background: "none", border: "none", cursor: countdown > 0 ? "default" : "pointer", fontFamily: "inherit", fontSize: 13, padding: 0, transition: "color 0.15s" }}
                  >
                    <RefreshCw size={12} strokeWidth={2} />
                    {countdown > 0 ? `Снова (${countdown}с)` : "Отправить снова"}
                  </button>
                  <button
                    onClick={() => { setStep("email"); setError(""); setDigits(Array(CODE_LEN).fill("")); }}
                    style={{ color: "var(--subtle)", background: "none", border: "none", cursor: "pointer", fontFamily: "inherit", fontSize: 13, padding: 0, transition: "color 0.15s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
                    onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
                  >
                    Изменить email
                  </button>
                </div>
              </div>
            )}

            {/* STEP: done */}
            {step === "done" && (
              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", padding: "24px 0 8px", gap: 12 }}>
                <div style={{ width: 56, height: 56, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(74,222,128,0.1)", border: "1px solid rgba(74,222,128,0.2)" }}>
                  <CheckCircle size={28} style={{ color: "#4ade80" }} strokeWidth={1.5} />
                </div>
                <h2 className="serif" style={{ fontSize: 20, fontWeight: 500, color: "var(--text)", fontStyle: "italic" }}>Вы вошли</h2>
                <p style={{ fontSize: 13, color: "var(--muted)" }}>Добро пожаловать в Magic Vibes</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
