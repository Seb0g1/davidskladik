"use client";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { ArrowLeft, ArrowRight, Loader2, Mail } from "lucide-react";
import { useAuth } from "@/components/AuthContext";
import ConsentCheck from "@/components/ConsentCheck";

const RESEND_SECONDS = 60;

// Only same-site paths: ?next= comes from the URL and must not become an open redirect.
function safeNext(raw: string | null) {
  return raw && raw.startsWith("/") && !raw.startsWith("//") ? raw : "/account";
}

export default function LoginClient() {
  const { customer, loading, sendCode, verifyCode, startYandexLogin, yandexLoading, yandexError } = useAuth();
  const router = useRouter();
  const next = safeNext(useSearchParams().get("next"));

  const [step, setStep] = useState<"email" | "code">("email");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [consent, setConsent] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [resendIn, setResendIn] = useState(0);
  const codeRef = useRef<HTMLInputElement>(null);

  useEffect(() => { if (!loading && customer) router.replace(next); }, [loading, customer, next, router]);
  useEffect(() => {
    if (resendIn <= 0) return;
    const t = setTimeout(() => setResendIn((s) => s - 1), 1000);
    return () => clearTimeout(t);
  }, [resendIn]);
  useEffect(() => { if (step === "code") codeRef.current?.focus(); }, [step]);

  async function requestCode(e?: React.FormEvent) {
    e?.preventDefault();
    if (!consent) { setError("Отметьте согласие на обработку персональных данных"); return; }
    setError(null);
    setBusy(true);
    try {
      await sendCode(email.trim());
      setStep("code");
      setCode("");
      setResendIn(RESEND_SECONDS);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Не удалось отправить код");
    } finally {
      setBusy(false);
    }
  }

  async function submitCode(value: string) {
    if (busy) return;
    setError(null);
    setBusy(true);
    try {
      await verifyCode(email.trim(), value);
      router.replace(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Неверный код");
      setBusy(false);
    }
  }

  function onCodeChange(v: string) {
    const digits = v.replace(/\D/g, "").slice(0, 6);
    setCode(digits);
    if (digits.length === 6) submitCode(digits);
  }

  return (
    <div className="mv-login">
      <div className="mv-login-card">
        <div className="mv-login-kicker">Личный кабинет</div>
        <h1 className="mv-login-title">
          {step === "email" ? "Вход" : "Код из письма"}
        </h1>
        <p className="mv-login-sub">
          {step === "email"
            ? "Пароль не нужен — пришлём шестизначный код на почту. Новый аккаунт создастся автоматически."
            : <>Отправили код на <b>{email}</b>. Письмо может прийти в «Спам».</>}
        </p>

        {step === "email" ? (
          <form onSubmit={requestCode} className="mv-login-form">
            <label className="mv-login-field">
              <Mail size={18} aria-hidden />
              <input
                type="email" inputMode="email" autoComplete="email" required autoFocus
                placeholder="you@mail.ru" value={email} onChange={(e) => setEmail(e.target.value)}
                aria-label="Email"
              />
            </label>
            <ConsentCheck kind="pd" checked={consent} onChange={(v) => { setConsent(v); if (v) setError(null); }} />
            <button type="submit" className="btn-primary mv-login-btn" disabled={busy || !email.includes("@") || !consent}>
              {busy ? <Loader2 size={18} className="mv-spin" /> : <>Получить код <ArrowRight size={16} /></>}
            </button>
          </form>
        ) : (
          <form onSubmit={(e) => { e.preventDefault(); if (code.length === 6) submitCode(code); }} className="mv-login-form">
            <input
              ref={codeRef} className="mv-login-code" inputMode="numeric" autoComplete="one-time-code"
              pattern="[0-9]*" maxLength={6} placeholder="••••••" value={code}
              onChange={(e) => onCodeChange(e.target.value)} aria-label="Код из письма"
            />
            <button type="submit" className="btn-primary mv-login-btn" disabled={busy || code.length !== 6}>
              {busy ? <Loader2 size={18} className="mv-spin" /> : "Войти"}
            </button>
            <div className="mv-login-row">
              <button type="button" className="mv-login-link" onClick={() => { setStep("email"); setError(null); }}>
                <ArrowLeft size={14} /> Другой email
              </button>
              <button type="button" className="mv-login-link" disabled={resendIn > 0 || busy} onClick={() => requestCode()}>
                {resendIn > 0 ? `Отправить снова через ${resendIn} с` : "Отправить код снова"}
              </button>
            </div>
          </form>
        )}

        {(error || yandexError) && <div className="mv-login-error" role="alert">{error || yandexError}</div>}

        <div className="mv-login-or"><span>или</span></div>
        <button type="button" className="mv-login-yandex" onClick={() => consent ? startYandexLogin().catch(() => setError("Яндекс ID сейчас недоступен")) : setError("Отметьте согласие на обработку персональных данных")} disabled={yandexLoading}>
          {yandexLoading ? <Loader2 size={18} className="mv-spin" /> : <span className="mv-login-ya" aria-hidden>Я</span>}
          Войти с Яндекс ID
        </button>

        <p className="mv-login-legal">
          Условия: <Link href="/terms">оферта</Link> · <Link href="/privacy">политика обработки персональных данных</Link> · <Link href="/cookies">cookie</Link>
        </p>
      </div>
    </div>
  );
}
