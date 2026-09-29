"use client";
import { useState, useMemo } from "react";
import ConsentCheck from "@/components/ConsentCheck";
import { CONSENT_VERSION } from "@/lib/legal";
import Link from "next/link";

const QUIZ = [
  { q: "Для какого случая ищете аромат?",            opts: ["Повседневный образ", "Вечерний выход", "Особый повод", "В подарок"] },
  { q: "Какое настроение должен передавать аромат?",  opts: ["Свежий и лёгкий", "Тёплый и уютный", "Загадочный и глубокий", "Яркий и бодрящий"] },
  { q: "Какие ноты вам ближе?",                      opts: ["Цветочные", "Восточные и пряные", "Древесные", "Морские и цитрусовые"] },
  { q: "Ваш бюджет?",                                opts: ["До 3 000 ₽", "3 000 – 7 000 ₽", "7 000 – 15 000 ₽", "Без ограничений"] },
  { q: "Аромат для кого?",                            opts: ["Для себя", "В подарок близкому", "На особый случай", "Для коллекции"] },
];

const RESULTS = [
  { title: "Цветочные ароматы",    text: "Нежные, романтичные, универсальные. Идеальны для дня и особых моментов.",   cat: "edp" },
  { title: "Восточная парфюмерия", text: "Глубокие, чувственные, запоминающиеся. Для тех, кто любит оставлять след.", cat: "parfum" },
  { title: "Древесные ароматы",    text: "Уверенные, элегантные, вне времени. Образ силы и утончённости.",            cat: "edt" },
  { title: "Свежая парфюмерия",    text: "Лёгкая, бодрящая, универсальная. Для любого случая и сезона.",              cat: "edt" },
];

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

export default function ScentQuiz() {
  const [pd, setPd] = useState(false);
  const [ads, setAds] = useState(false);
  const [step, setStep]     = useState(0);
  const [answers, setAnswers] = useState<number[]>([]);
  const [done, setDone]     = useState(false);
  const [email, setEmail]   = useState("");
  const [emailSent, setEmailSent] = useState(false);

  function pick(idx: number) {
    const next = [...answers, idx];
    if (step < QUIZ.length - 1) { setAnswers(next); setStep(s => s + 1); }
    else { setAnswers(next); setDone(true); }
  }

  function reset() { setStep(0); setAnswers([]); setDone(false); setEmail(""); setEmailSent(false); }

  async function submitEmail() {
    if (!email || !pd || !ads) return;
    try {
      await fetch(API_BASE + "/email-subscribe", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, source: "quiz", quizCategory: result?.title, consents: { pd: true, ads: true, version: CONSENT_VERSION } }),
      });
    } catch { /* best-effort */ }
    setEmailSent(true);
  }

  const result = useMemo(() => (done ? RESULTS[answers[2] ?? 0] : null), [done, answers]);

  return (
    <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
      <div style={{ maxWidth: 860, margin: "0 auto", padding: "clamp(28px,4vw,52px)", border: "1px solid rgba(var(--accent-rgb),0.22)", borderRadius: 14, background: "linear-gradient(150deg,var(--surface) 0%,var(--surface) 72%)", textAlign: "center" }}>
        <p className="eyebrow" style={{ marginBottom: 12 }}>
          Аромат-гид · {done ? "Готово" : `Шаг ${step + 1} из ${QUIZ.length}`}
        </p>

        {!done ? (
          <div>
            <h2 className="serif" style={{ margin: "0 0 26px", fontWeight: 400, fontSize: "clamp(28px,3.6vw,46px)", lineHeight: 1.1, color: "var(--ink)" }}>
              {QUIZ[step].q}
            </h2>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, justifyContent: "center" }}>
              {QUIZ[step].opts.map((opt, i) => (
                <button
                  key={opt}
                  onClick={() => pick(i)}
                  style={{ minHeight: 48, padding: "0 26px", border: "1px solid rgba(var(--ink-rgb),0.096)", borderRadius: 10, background: "rgba(var(--ink-rgb),0.016)", color: "var(--ink)", fontSize: 13, letterSpacing: "0.1em", cursor: "pointer", transition: "transform 0.4s cubic-bezier(0.16,1,0.3,1), border-color 0.4s, background 0.4s, color 0.4s" }}
                  onMouseEnter={e => { const el = e.currentTarget; el.style.transform = "translateY(-3px)"; el.style.borderColor = "rgba(var(--accent-rgb),0.7)"; el.style.background = "rgba(var(--accent-rgb),0.1)"; el.style.color = "var(--ink)"; }}
                  onMouseLeave={e => { const el = e.currentTarget; el.style.transform = ""; el.style.borderColor = "rgba(var(--ink-rgb),0.096)"; el.style.background = "rgba(var(--ink-rgb),0.016)"; el.style.color = "var(--ink)"; }}
                >
                  {opt}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div>
            <h2 className="serif" style={{ margin: "0 0 14px", fontWeight: 400, fontSize: "clamp(28px,3.6vw,46px)", lineHeight: 1.1, color: "var(--ink)" }}>{result?.title}</h2>
            <p style={{ margin: "0 auto 26px", maxWidth: "44ch", fontSize: 14, lineHeight: 1.7, color: "rgba(var(--ink-rgb),0.66)" }}>{result?.text}</p>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, justifyContent: "center" }}>
              <Link href={`/catalog?category=${result?.cat}`} className="btn-primary" style={{ minHeight: 50 }}>Смотреть подборку</Link>
              <button onClick={reset} style={{ minHeight: 50, padding: "0 26px", border: "1px solid rgba(var(--ink-rgb),0.096)", borderRadius: 10, background: "transparent", color: "rgba(var(--ink-rgb),0.66)", fontSize: 12, letterSpacing: "0.14em", textTransform: "uppercase", cursor: "pointer", transition: "border-color 0.3s, color 0.3s" }}
                onMouseEnter={e => { e.currentTarget.style.borderColor = "rgba(var(--accent-rgb),0.6)"; e.currentTarget.style.color = "var(--ink)"; }}
                onMouseLeave={e => { e.currentTarget.style.borderColor = "rgba(var(--ink-rgb),0.096)"; e.currentTarget.style.color = "rgba(var(--ink-rgb),0.66)"; }}>
                Пройти заново
              </button>
            </div>
            <div style={{ marginTop: 28, paddingTop: 24, borderTop: "1px solid rgba(var(--ink-rgb),0.048)" }}>
              {!emailSent ? (
                <>
                  <p style={{ margin: "0 0 14px", fontSize: 12, color: "rgba(var(--ink-rgb),0.55)" }}>Получить подборку на почту</p>
                  <div style={{ display: "flex", gap: 10, alignItems: "flex-end", maxWidth: 400, margin: "0 auto" }}>
                    <input
                      type="email" value={email} onChange={e => setEmail(e.target.value)}
                      onKeyDown={e => { if (e.key === "Enter" && email && pd && ads) submitEmail(); }}
                      placeholder="your@email.com"
                      style={{ flex: 1, padding: "9px 0", background: "transparent", border: "none", borderBottom: "1px solid rgba(var(--ink-rgb),0.096)", color: "var(--ink)", fontSize: 13.5, outline: "none", transition: "border-bottom-color 0.3s" }}
                      onFocus={e => (e.target.style.borderBottomColor = "rgba(var(--accent-rgb),0.55)")}
                      onBlur={e => (e.target.style.borderBottomColor = "rgba(var(--ink-rgb),0.096)")}
                    />
                    <button onClick={submitEmail} disabled={!email || !pd || !ads} style={{ opacity: !email || !pd || !ads ? 0.5 : 1, padding: "9px 18px", background: "transparent", border: "1px solid rgba(var(--accent-rgb),0.4)", borderRadius: 10, color: "var(--accent)", fontSize: 11, letterSpacing: "0.12em", cursor: "pointer", transition: "background 0.3s" }}
                      onMouseEnter={e => { e.currentTarget.style.background = "rgba(var(--accent-rgb),0.12)"; }}
                      onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}>
                      Отправить
                    </button>
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 400, margin: "14px auto 0" }}>
                    <ConsentCheck kind="pd" compact checked={pd} onChange={setPd} />
                    <ConsentCheck kind="ads" subscribe compact checked={ads} onChange={setAds} />
                  </div>
                </>
              ) : (
                <p style={{ margin: 0, fontSize: 12.5, color: "var(--success)" }}>Подборка отправлена — проверьте почту</p>
              )}
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
