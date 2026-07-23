import { useState, useEffect, useRef } from "react";
import { X, Mail, ArrowRight, Loader2, CheckCircle, RefreshCw } from "lucide-react";
import { useAuth } from "../AuthContext";
import clsx from "clsx";

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
    <div className="fixed inset-0 z-[100] flex items-end sm:items-center justify-center" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="absolute inset-0 bg-black/40 backdrop-blur-sm modal-overlay" onClick={onClose} />
      <div className="relative bg-white w-full sm:max-w-md rounded-t-3xl sm:rounded-3xl shadow-2xl modal-content overflow-hidden">

        {/* Header gradient bar */}
        <div style={{ height: 4, background: "linear-gradient(90deg, #7c3aed, #9333ea, #c026d3)" }} />

        <div className="p-7 pb-8">
          {/* Close */}
          <button onClick={onClose} className="absolute top-5 right-5 p-2 rounded-full text-gray-400 hover:bg-gray-100 hover:text-gray-600 transition-colors">
            <X size={18} />
          </button>

          {/* STEP: email */}
          {step === "email" && (
            <form onSubmit={handleSendCode}>
              <div className="mb-6">
                <div className="w-11 h-11 rounded-2xl flex items-center justify-center mb-4" style={{ background: "#f5f3ff" }}>
                  <Mail size={20} style={{ color: "#7c3aed" }} strokeWidth={1.8} />
                </div>
                <h2 className="text-xl font-bold text-apple-black tracking-tight">Вход или регистрация</h2>
                <p className="text-sm text-apple-gray mt-1">Введите email — пришлём код для входа</p>
              </div>

              <input
                ref={emailInputRef}
                type="email"
                placeholder="your@email.com"
                required
                value={email}
                onChange={(e) => { setEmail(e.target.value); setError(""); }}
                className="w-full px-4 py-3.5 bg-gray-50 rounded-2xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all mb-3"
                style={{ border: "1px solid #e8e8ec" }}
              />

              {error && (
                <div className="text-sm text-red-600 bg-red-50 rounded-xl px-4 py-3 mb-3">{error}</div>
              )}

              <button
                type="submit"
                disabled={loading || !email.trim()}
                className="w-full py-3.5 rounded-2xl font-semibold text-sm text-white flex items-center justify-center gap-2 transition-all duration-200 disabled:opacity-60"
                style={{ background: "linear-gradient(135deg, #7c3aed, #9333ea)" }}
              >
                {loading ? <Loader2 size={16} className="animate-spin" /> : <ArrowRight size={16} />}
                Получить код
              </button>

              <p className="text-center text-xs text-apple-gray mt-4">
                Нажимая кнопку, вы соглашаетесь с&nbsp;
                <a href="/privacy" className="text-violet-600 hover:underline">политикой конфиденциальности</a>
              </p>
            </form>
          )}

          {/* STEP: code */}
          {step === "code" && (
            <div>
              <div className="mb-6">
                <div className="w-11 h-11 rounded-2xl flex items-center justify-center mb-4" style={{ background: "#f5f3ff" }}>
                  <Mail size={20} style={{ color: "#7c3aed" }} strokeWidth={1.8} />
                </div>
                <h2 className="text-xl font-bold text-apple-black tracking-tight">Введите код</h2>
                <p className="text-sm text-apple-gray mt-1">
                  Отправили 6-значный код на{" "}
                  <button onClick={() => { setStep("email"); setError(""); }} className="text-violet-600 font-medium hover:underline">{email}</button>
                </p>
              </div>

              {/* OTP input boxes */}
              <div className="flex gap-2 justify-between mb-4" onPaste={handleDigitPaste}>
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
                    className={clsx(
                      "w-[13%] aspect-square text-center text-xl font-bold rounded-2xl transition-all duration-150 focus:outline-none",
                      error ? "ring-2 ring-red-400" : "focus:ring-2 focus:ring-violet-400",
                      d ? "bg-violet-50 text-violet-700" : "bg-gray-50 text-apple-black"
                    )}
                    style={{ border: error ? "1px solid #fca5a5" : d ? "1px solid #ddd6fe" : "1px solid #e8e8ec" }}
                  />
                ))}
              </div>

              {loading && (
                <div className="flex items-center justify-center gap-2 text-sm text-apple-gray mb-3">
                  <Loader2 size={14} className="animate-spin" />
                  Проверяем...
                </div>
              )}

              {error && (
                <div className="text-sm text-red-600 bg-red-50 rounded-xl px-4 py-3 mb-3">{error}</div>
              )}

              <div className="flex items-center justify-between text-sm mt-2">
                <button
                  onClick={handleResend}
                  disabled={countdown > 0 || loading}
                  className={clsx(
                    "flex items-center gap-1.5 font-medium transition-colors",
                    countdown > 0 ? "text-apple-gray cursor-default" : "text-violet-600 hover:text-violet-700"
                  )}
                >
                  <RefreshCw size={13} strokeWidth={2} />
                  {countdown > 0 ? `Отправить снова (${countdown}с)` : "Отправить снова"}
                </button>
                <button onClick={() => { setStep("email"); setError(""); setDigits(Array(CODE_LEN).fill("")); }} className="text-apple-gray hover:text-apple-black transition-colors">
                  Изменить email
                </button>
              </div>
            </div>
          )}

          {/* STEP: done */}
          {step === "done" && (
            <div className="flex flex-col items-center py-6 gap-3">
              <div className="w-14 h-14 rounded-full flex items-center justify-center" style={{ background: "#ecfdf5" }}>
                <CheckCircle size={28} style={{ color: "#10b981" }} strokeWidth={1.8} />
              </div>
              <h2 className="text-xl font-bold text-apple-black">Вы вошли!</h2>
              <p className="text-sm text-apple-gray">Добро пожаловать в Magic Vibes</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
