import { useRef, useEffect, useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Quote, Star } from "lucide-react";
import { api } from "../api";
import ProductCard from "../components/ProductCard";

/* ── Reveal on scroll ──────────────────────────────────────────── */
function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll(".reveal-section");
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) { e.target.classList.add("is-visible"); io.unobserve(e.target); }
      });
    }, { threshold: 0.06 });
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

/* ── Card reveal on scroll ──────────────────────────────────────── */
function useCardReveal() {
  useEffect(() => {
    const els = document.querySelectorAll("[data-mv-card]");
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e, i) => {
        if (e.isIntersecting) {
          setTimeout(() => e.target.classList.add("mv-in"), i * 60);
          io.unobserve(e.target);
        }
      });
    }, { threshold: 0.1 });
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

const BRANDS = ["Chanel","Dior","Tom Ford","Hermès","Byredo","Jo Malone","Creed","Guerlain","Givenchy","Prada","Valentino","Burberry","Versace","Montale","Kilian","YSL","Bvlgari","Lancôme","Amouage","Xerjoff","Maison Margiela","Acqua di Parma"];
const doubled = [...BRANDS, ...BRANDS];

const QUIZ = [
  { q: "Для какого случая ищете аромат?",        opts: ["Повседневный образ", "Вечерний выход", "Особый повод", "В подарок"] },
  { q: "Какое настроение должен передавать аромат?", opts: ["Свежий и лёгкий", "Тёплый и уютный", "Загадочный и глубокий", "Яркий и бодрящий"] },
  { q: "Какие ноты вам ближе?",                  opts: ["Цветочные", "Восточные и пряные", "Древесные", "Морские и цитрусовые"] },
];

const QUIZ_RESULTS = [
  { title: "Цветочные ароматы",    text: "Нежные, романтичные, универсальные. Идеальны для дня и особых моментов.",   cat: "edp" },
  { title: "Восточная парфюмерия", text: "Глубокие, чувственные, запоминающиеся. Для тех, кто любит оставлять след.", cat: "parfum" },
  { title: "Древесные ароматы",    text: "Уверенные, элегантные, вне времени. Образ силы и утончённости.",            cat: "edt" },
  { title: "Свежая парфюмерия",    text: "Лёгкая, бодрящая, универсальная. Для любого случая и сезона.",              cat: "edt" },
];

function CardSkeleton() {
  return (
    <div className="product-card" style={{ width: 200, flexShrink: 0 }}>
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div style={{ padding: "15px 15px 17px", display: "flex", flexDirection: "column", gap: 8 }}>
        <div className="skeleton" style={{ height: 8, width: "40%" }} />
        <div className="skeleton" style={{ height: 13, marginBottom: 2 }} />
        <div className="skeleton" style={{ height: 13, width: "70%" }} />
        <div className="skeleton" style={{ height: 14, width: "50%", marginTop: 4 }} />
      </div>
    </div>
  );
}

export default function HomePage() {
  useReveal();
  useCardReveal();

  /* refs */
  const auraRef      = useRef<HTMLDivElement>(null);
  const particlesRef = useRef<HTMLDivElement>(null);
  const tiltRef      = useRef<HTMLDivElement>(null);
  const glowRef      = useRef<HTMLDivElement>(null);

  /* quiz */
  const [quizStep, setQuizStep]   = useState(0);
  const [quizAnswers, setQuizAnswers] = useState<number[]>([]);
  const [quizDone, setQuizDone]   = useState(false);

  function pickQuizAnswer(idx: number) {
    const next = [...quizAnswers, idx];
    if (quizStep < QUIZ.length - 1) {
      setQuizAnswers(next);
      setQuizStep(s => s + 1);
    } else {
      setQuizAnswers(next);
      setQuizDone(true);
    }
  }
  function resetQuiz() { setQuizStep(0); setQuizAnswers([]); setQuizDone(false); }

  const quizResult = useMemo(() => {
    if (!quizDone) return null;
    return QUIZ_RESULTS[quizAnswers[2] ?? 0];
  }, [quizDone, quizAnswers]);

  /* data */
  const { data: popularData, isLoading } = useQuery({
    queryKey: ["shop-popular"],
    queryFn: () => api.popular(8),
    staleTime: 10 * 60_000,
  });
  const { data: reviewsData } = useQuery({
    queryKey: ["shop-reviews"],
    queryFn: () => api.reviews(6),
    staleTime: 5 * 60_000,
  });
  const { data: newsData } = useQuery({
    queryKey: ["shop-news"],
    queryFn: () => api.news(3),
    staleTime: 5 * 60_000,
  });

  /* ── Cursor aura ──────────────────────────────────────────────── */
  useEffect(() => {
    const aura = auraRef.current;
    if (!aura) return;
    let raf: number;
    let tx = -500, ty = -500, cx = -500, cy = -500;
    const onMove = (e: MouseEvent) => { tx = e.clientX; ty = e.clientY; };
    const loop = () => {
      cx += (tx - cx) * 0.07;
      cy += (ty - cy) * 0.07;
      aura.style.transform = `translate3d(${cx}px,${cy}px,0)`;
      raf = requestAnimationFrame(loop);
    };
    document.addEventListener("mousemove", onMove);
    raf = requestAnimationFrame(loop);
    return () => { document.removeEventListener("mousemove", onMove); cancelAnimationFrame(raf); };
  }, []);

  /* ── Particles ────────────────────────────────────────────────── */
  useEffect(() => {
    const container = particlesRef.current;
    if (!container) return;
    const spawn = () => {
      const p = document.createElement("div");
      const dx = (Math.random() - 0.5) * 40;
      p.style.cssText = `
        position:absolute;left:${Math.random()*100}%;bottom:0;
        width:${2+Math.random()*3}px;height:${2+Math.random()*3}px;
        border-radius:50%;
        background:rgba(201,162,94,${0.3+Math.random()*0.5});
        --mv-dx:${dx}px;
        animation:mv-drift ${4+Math.random()*6}s ease-out ${Math.random()*2}s both;
        pointer-events:none;
      `;
      container.appendChild(p);
      setTimeout(() => p.remove(), 10000);
    };
    const id = setInterval(spawn, 500);
    return () => clearInterval(id);
  }, []);

  /* ── Hero 3D tilt ─────────────────────────────────────────────── */
  useEffect(() => {
    const el = tiltRef.current;
    const glow = glowRef.current;
    if (!el) return;
    const onMove = (e: MouseEvent) => {
      const r = el.getBoundingClientRect();
      const x = (e.clientX - r.left - r.width / 2) / r.width;
      const y = (e.clientY - r.top - r.height / 2) / r.height;
      el.style.transition = "transform 0.1s ease";
      el.style.transform = `perspective(1000px) rotateX(${-y*5}deg) rotateY(${x*8}deg)`;
      if (glow) glow.style.transform = `translate(${x*16}px,${y*16}px)`;
    };
    const onLeave = () => {
      el.style.transition = "transform 0.8s cubic-bezier(0.16,1,0.3,1)";
      el.style.transform = "perspective(1000px) rotateX(0) rotateY(0)";
      if (glow) glow.style.transform = "";
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseleave", onLeave);
    return () => { document.removeEventListener("mousemove", onMove); document.removeEventListener("mouseleave", onLeave); };
  }, []);


  return (
    <div style={{ background: "#0b0b0b", color: "var(--text)", minHeight: "100vh" }}>

      {/* ── Cursor aura ─────────────────────────────────────────── */}
      <div ref={auraRef} aria-hidden="true" className="cursor-aura" />

      {/* ════════════════════ HERO ════════════════════ */}
      <section style={{ position: "relative", overflow: "hidden", minHeight: "84vh", display: "flex", alignItems: "center", justifyContent: "center", padding: "9vh clamp(18px,4vw,56px)" }}>
        <div style={{ position: "absolute", inset: "-10%", pointerEvents: "none", background: "radial-gradient(44% 38% at 50% 44%, rgba(201,162,94,0.16) 0%, rgba(201,162,94,0.05) 40%, rgba(11,11,11,0) 72%)" }} />
        <div ref={particlesRef} style={{ position: "absolute", inset: "-8%", pointerEvents: "none", willChange: "transform" }} />

        <div style={{ position: "relative", width: "100%", maxWidth: 1040, display: "flex", flexDirection: "column", alignItems: "center", gap: "clamp(20px,3vw,36px)", textAlign: "center" }}>
          <p className="eyebrow anim-fade-in" style={{ animationDelay: "0.1s" }}>Оригинальная парфюмерия</p>

          {/* 3D tilt title */}
          <div style={{ perspective: 1000, width: "100%" }}>
            <div ref={tiltRef} style={{ position: "relative", transformStyle: "preserve-3d", willChange: "transform" }}>
              <div ref={glowRef} style={{ position: "absolute", left: "50%", top: "50%", width: "76%", height: "128%", marginLeft: "-38%", marginTop: "-64%", borderRadius: "50%", pointerEvents: "none", background: "radial-gradient(50% 50% at 50% 50%, rgba(240,220,170,0.28) 0%, rgba(201,162,94,0.14) 42%, rgba(201,162,94,0) 74%)", filter: "blur(28px)", transition: "transform 0.3s ease" }} />
              <div className="serif anim-slide-up" style={{
                position: "relative",
                fontWeight: 300,
                fontStyle: "italic",
                fontSize: "clamp(60px,13vw,176px)",
                lineHeight: 0.88,
                animationDelay: "0.15s",
              }}>
                {/* Magic */}
                <div style={{ position: "relative" }}>
                  <span style={{ display: "block", color: "#f5f4f0" }}>Magic</span>
                  <span aria-hidden="true" style={{
                    position: "absolute", left: 0, top: 0, width: "100%", display: "block",
                    color: "transparent",
                    backgroundImage: "linear-gradient(104deg,#fffdf7 0%,#f5f4f0 26%,#e9d2a0 46%,#fffdf7 60%,#d7b880 82%,#f5f4f0 100%)",
                    backgroundSize: "260% 100%",
                    WebkitBackgroundClip: "text",
                    backgroundClip: "text",
                    animation: "mv-sheen 11s linear infinite alternate",
                  }}>Magic</span>
                </div>
                {/* Vibes */}
                <div style={{ position: "relative", marginLeft: "clamp(18px,5vw,84px)" }}>
                  <span style={{ display: "block", color: "#f5f4f0" }}>Vibes</span>
                  <span aria-hidden="true" style={{
                    position: "absolute", left: 0, top: 0, width: "100%", display: "block",
                    color: "transparent",
                    backgroundImage: "linear-gradient(104deg,#fffdf7 0%,#f5f4f0 26%,#e9d2a0 46%,#fffdf7 60%,#d7b880 82%,#f5f4f0 100%)",
                    backgroundSize: "260% 100%",
                    WebkitBackgroundClip: "text",
                    backgroundClip: "text",
                    animation: "mv-sheen 11s linear infinite alternate reverse",
                  }}>Vibes</span>
                </div>
              </div>
            </div>
          </div>

          {/* Gold divider */}
          <div className="anim-fade-in" style={{ width: "min(400px,60%)", height: 1, background: "linear-gradient(90deg,rgba(201,162,94,0) 0%,rgba(201,162,94,0.8) 50%,rgba(201,162,94,0) 100%)", animationDelay: "0.3s" }} />

          <p className="anim-slide-up" style={{ margin: 0, maxWidth: "30ch", fontSize: "clamp(14px,1.3vw,17px)", lineHeight: 1.65, color: "#888888", animationDelay: "0.35s" }}>
            Мировые ароматы с доставкой по России через Ozon
          </p>

          <div className="anim-slide-up" style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "center", gap: 14, animationDelay: "0.45s" }}>
            <Link to="/catalog" className="btn-primary">Каталог ароматов</Link>
            <Link to="/brands" className="btn-ghost">Все бренды →</Link>
          </div>
        </div>

        {/* Scroll hint */}
        <div style={{ position: "absolute", bottom: 28, left: "50%", transform: "translateX(-50%)", display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
          <div style={{ width: 1, height: 36, background: "linear-gradient(to bottom, transparent, rgba(201,162,94,0.4))" }} />
        </div>
      </section>

      {/* ════════════════════ STATS BAND ════════════════════ */}
      <section className="reveal-section" style={{ borderTop: "1px solid rgba(255,255,255,0.06)", borderBottom: "1px solid rgba(255,255,255,0.06)", margin: "0 clamp(18px,4vw,56px)" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(180px,1fr))" }}>
          {[
            { value: "22 000+", label: "Ароматов в наличии" },
            { value: "100%",    label: "Гарантия оригинала" },
            { value: "1–5 дн.", label: "Доставка по России" },
            { value: "4.9",     label: "Рейтинг на Ozon" },
          ].map(({ value, label }, i) => (
            <div key={label} style={{
              padding: "34px 26px",
              borderRight: i < 3 ? "1px solid rgba(255,255,255,0.06)" : undefined,
            }}>
              <p className="serif" style={{ margin: 0, fontStyle: "italic", fontSize: 40, lineHeight: 1, color: "#f5f4f0" }}>{value}</p>
              <p style={{ margin: "10px 0 0", fontSize: 12.5, letterSpacing: "0.08em", color: "#7d7a73" }}>{label}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ════════════════════ SCENT QUIZ ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
        <div style={{ maxWidth: 860, margin: "0 auto", padding: "clamp(28px,4vw,52px)", border: "1px solid rgba(201,162,94,0.22)", borderRadius: 3, background: "linear-gradient(150deg,#15130f 0%,#0d0d0d 72%)", textAlign: "center" }}>
          <p className="eyebrow" style={{ marginBottom: 12 }}>
            Аромат-гид · {quizDone ? "Готово" : `Шаг ${quizStep + 1} из ${QUIZ.length}`}
          </p>
          {!quizDone ? (
            <div>
              <h2 className="serif" style={{ margin: "0 0 26px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(28px,3.6vw,46px)", lineHeight: 1.1, color: "#f5f4f0" }}>
                {QUIZ[quizStep].q}
              </h2>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 12, justifyContent: "center" }}>
                {QUIZ[quizStep].opts.map((opt, idx) => (
                  <button
                    key={opt}
                    onClick={() => pickQuizAnswer(idx)}
                    style={{
                      minHeight: 48, padding: "0 26px",
                      border: "1px solid rgba(255,255,255,0.12)", borderRadius: 2,
                      background: "rgba(255,255,255,0.02)", color: "#d8d5cc",
                      fontSize: 13, letterSpacing: "0.1em",
                      cursor: "pointer",
                      transition: "transform 0.4s cubic-bezier(0.16,1,0.3,1), border-color 0.4s ease, background 0.4s ease, color 0.4s ease",
                    }}
                    onMouseEnter={e => { const el = e.currentTarget; el.style.transform = "translateY(-3px)"; el.style.borderColor = "rgba(201,162,94,0.7)"; el.style.background = "rgba(201,162,94,0.1)"; el.style.color = "#fffdf7"; }}
                    onMouseLeave={e => { const el = e.currentTarget; el.style.transform = ""; el.style.borderColor = "rgba(255,255,255,0.12)"; el.style.background = "rgba(255,255,255,0.02)"; el.style.color = "#d8d5cc"; }}
                  >
                    {opt}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div>
              <h2 className="serif" style={{ margin: "0 0 14px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(28px,3.6vw,46px)", lineHeight: 1.1, color: "#f5f4f0" }}>
                {quizResult?.title}
              </h2>
              <p style={{ margin: "0 auto 26px", maxWidth: "44ch", fontSize: 14, lineHeight: 1.7, color: "#8b8880" }}>
                {quizResult?.text}
              </p>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 12, justifyContent: "center" }}>
                <Link
                  to={`/catalog?category=${quizResult?.cat}`}
                  className="btn-primary"
                  style={{ minHeight: 50 }}
                >
                  Смотреть подборку
                </Link>
                <button
                  onClick={resetQuiz}
                  style={{ minHeight: 50, padding: "0 26px", border: "1px solid rgba(255,255,255,0.12)", borderRadius: 2, background: "transparent", color: "#a8a49b", fontSize: 12, letterSpacing: "0.14em", textTransform: "uppercase", cursor: "pointer", transition: "border-color 0.3s, color 0.3s" }}
                  onMouseEnter={e => { e.currentTarget.style.borderColor = "rgba(201,162,94,0.6)"; e.currentTarget.style.color = "#f5f4f0"; }}
                  onMouseLeave={e => { e.currentTarget.style.borderColor = "rgba(255,255,255,0.12)"; e.currentTarget.style.color = "#a8a49b"; }}
                >
                  Пройти заново
                </button>
              </div>
            </div>
          )}
        </div>
      </section>

      {/* ════════════════════ HOW IT WORKS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
        <p className="eyebrow" style={{ marginBottom: "clamp(20px,3vw,38px)" }}>Как мы работаем</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: 18 }}>
          {[
            { n: "01", title: "Подбираем аромат",        body: "Расскажите о предпочтениях — подберём парфюм по нотам, стойкости и поводу из 22 000 позиций.", cta: null },
            { n: "02", title: "Проверяем оригинал",      body: "Каждый флакон проходит проверку подлинности перед отправкой — только оригинал, без компромиссов.", cta: null },
            { n: "03", title: "Упаковываем как подарок", body: "Фирменная упаковка и защита: заказ выглядит дороже, чем в бутике.", cta: null },
            { n: "04", title: "Доставляем через Ozon",   body: "1–5 дней в любой город России, бесплатно до пункта выдачи рядом с домом.", cta: "/catalog" },
          ].map(({ n, title, body, cta }) => (
            <article key={n} style={{
              display: "flex", flexDirection: "column", justifyContent: "space-between",
              minHeight: "clamp(260px,28vh,340px)",
              padding: "clamp(22px,3vw,36px)",
              border: cta ? "1px solid rgba(201,162,94,0.24)" : "1px solid rgba(255,255,255,0.07)",
              borderRadius: 3,
              background: cta ? "linear-gradient(150deg,#17140f 0%,#0d0d0d 70%)" : "linear-gradient(150deg,#131110 0%,#0d0d0d 68%)",
            }}>
              <span className="serif" style={{ fontStyle: "italic", fontSize: 28, color: "#c9a25e" }}>{n}</span>
              <div>
                <h3 className="serif" style={{ margin: "0 0 12px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(22px,2.6vw,36px)", lineHeight: 1.1, color: "#f5f4f0" }}>{title}</h3>
                <p style={{ margin: 0, fontSize: 13.5, lineHeight: 1.7, color: "#8b8880" }}>{body}</p>
                {cta && (
                  <Link to={cta} className="btn-primary" style={{ display: "inline-flex", marginTop: 22 }}>
                    Выбрать аромат
                  </Link>
                )}
              </div>
            </article>
          ))}
        </div>
      </section>

      {/* ════════════════════ FEATURED PRODUCTS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0" }}>
        <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 34 }}>
          <div>
            <p className="eyebrow" style={{ marginBottom: 10 }}>Популярное</p>
            <h2 className="serif" style={{ margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Хиты сезона</h2>
          </div>
          <Link to="/catalog" className="btn-ghost">Смотреть всё →</Link>
        </div>

        {isLoading ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 18 }}>
            {Array.from({ length: 8 }).map((_, i) => <CardSkeleton key={i} />)}
          </div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 18 }}>
            {popularData?.products.map((p) => (
              <div key={p.offerId} data-mv-card="1">
                <ProductCard product={p} />
              </div>
            ))}
          </div>
        )}
      </section>

      {/* ════════════════════ WHY US ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0", textAlign: "center" }}>
        <p className="eyebrow" style={{ marginBottom: 10 }}>Почему мы</p>
        <h2 className="serif" style={{ margin: "0 0 44px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Ваш выбор</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(230px,1fr))", gap: 1, background: "rgba(255,255,255,0.07)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, textAlign: "left", maxWidth: 1180, margin: "0 auto" }}>
          {[
            { n: "01", title: "100% оригинал",      text: "Прямые поставки от авторизованных дистрибьюторов, сертификат на каждый бренд." },
            { n: "02", title: "Доставка по России",  text: "Через Ozon за 1–5 дней в любой город. Удобные пункты выдачи рядом с домом." },
            { n: "03", title: "4.9 на Ozon",         text: "Тысячи довольных покупателей и рейтинг 4.9 из 5 на маркетплейсе." },
            { n: "04", title: "Широкий выбор",       text: "Более 22 000 ароматов от 200+ брендов — от масс-маркета до нишевой парфюмерии." },
          ].map(({ n, title, text }) => (
            <div key={n} style={{ background: "#0e0e0e", padding: "30px 26px" }}>
              <p className="serif" style={{ margin: "0 0 14px", fontStyle: "italic", fontSize: 26, color: "#c9a25e" }}>{n}</p>
              <p style={{ margin: "0 0 10px", fontSize: 15, color: "#f5f4f0" }}>{title}</p>
              <p style={{ margin: 0, fontSize: 13, lineHeight: 1.6, color: "#7d7a73" }}>{text}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ════════════════════ PROMO CARDS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))", gap: 18 }}>
          {[
            { tag: "Для близких", title: "Подарите аромат",  body: "Подарочные наборы от мировых парфюмерных домов в фирменной упаковке.", link: "/catalog?category=sets",  cta: "Выбрать набор →" },
            { tag: "Эксклюзив",   title: "Духи Parfum",      body: "Редкие ароматы высочайшей концентрации от нишевых парфюмерных домов.",  link: "/catalog?category=parfum", cta: "Смотреть →" },
          ].map(({ tag, title, body, link, cta }) => (
            <div key={tag} style={{ position: "relative", overflow: "hidden", padding: "clamp(30px,4vw,52px)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "linear-gradient(140deg,#131110 0%,#0d0d0d 70%)" }}>
              <p className="eyebrow" style={{ marginBottom: 14 }}>{tag}</p>
              <h3 className="serif" style={{ margin: "0 0 14px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(28px,3.2vw,40px)", color: "#f5f4f0" }}>{title}</h3>
              <p style={{ margin: "0 0 24px", maxWidth: "34ch", fontSize: 13.5, lineHeight: 1.6, color: "#7d7a73" }}>{body}</p>
              <Link to={link} style={{ fontSize: 12.5, letterSpacing: "0.14em", textTransform: "uppercase", color: "var(--accent)" }}>{cta}</Link>
            </div>
          ))}
        </div>
      </section>

      {/* ════════════════════ BRAND MARQUEE ════════════════════ */}
      <section className="reveal-section" style={{ marginTop: "clamp(56px,8vw,104px)", padding: "30px 0", borderTop: "1px solid rgba(255,255,255,0.06)", borderBottom: "1px solid rgba(255,255,255,0.06)", overflow: "hidden" }}>
        <p style={{ margin: "0 0 22px", textAlign: "center", fontSize: 10, letterSpacing: "0.34em", textTransform: "uppercase", color: "#5d5a54" }}>Мировые парфюмерные дома</p>
        <div style={{ position: "relative" }}>
          <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to right,#0b0b0b,transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to left,#0b0b0b,transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link
                key={i}
                to={`/catalog?brand=${encodeURIComponent(brand)}`}
                className="serif"
                style={{ padding: "0 30px", fontSize: 20, letterSpacing: "0.04em", color: "#4a473f", whiteSpace: "nowrap", transition: "color 0.3s ease" }}
                onMouseEnter={e => (e.currentTarget.style.color = "#c9a25e")}
                onMouseLeave={e => (e.currentTarget.style.color = "#4a473f")}
              >
                {brand}
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ════════════════════ REVIEWS ════════════════════ */}
      {reviewsData && reviewsData.reviews.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
          <p className="eyebrow" style={{ marginBottom: 10 }}>Отзывы покупателей</p>
          <h2 className="serif" style={{ margin: "0 0 34px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Что говорят клиенты</h2>
          <div className="scroll-x">
            <div style={{ display: "flex", gap: 18, width: "max-content", paddingBottom: 4 }}>
              {reviewsData.reviews.map((r) => (
                <div key={r.id} style={{
                  width: 300, flexShrink: 0, padding: "24px 22px",
                  border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "#0e0e0e",
                }}>
                  <Quote size={16} style={{ color: "#c9a25e", opacity: 0.4, marginBottom: 12 }} />
                  <div style={{ display: "flex", gap: 2, marginBottom: 12 }}>
                    {Array.from({ length: 5 }).map((_, i) => (
                      <Star key={i} size={11} fill={i < r.rating ? "#c9a25e" : "none"} stroke={i < r.rating ? "#c9a25e" : "rgba(255,255,255,0.2)"} strokeWidth={1.5} />
                    ))}
                  </div>
                  {r.productName && (
                    <p style={{ margin: "0 0 8px", fontSize: 9.5, letterSpacing: "0.1em", textTransform: "uppercase", color: "#6f6c66" }}>{r.productName}</p>
                  )}
                  <p style={{ margin: "0 0 16px", fontSize: 13, color: "#8b8880", lineHeight: 1.7 }}>
                    {r.text.length > 220 ? r.text.slice(0, 220) + "…" : r.text}
                  </p>
                  <p style={{ margin: 0, fontSize: 12, color: "var(--text)" }}>{r.author ?? "Покупатель"}</p>
                  <p style={{ margin: "3px 0 0", fontSize: 10, color: "#6f6c66" }}>
                    {new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
                  </p>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {/* ════════════════════ NEWS ════════════════════ */}
      {newsData && newsData.posts.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 28 }}>
            <div>
              <p className="eyebrow" style={{ marginBottom: 10 }}>Новости</p>
              <h2 className="serif" style={{ margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Magic Vibes</h2>
            </div>
            <Link to="/news" className="btn-ghost">Все новости →</Link>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))", gap: 18 }}>
            {newsData.posts.map((post) => (
              <a
                key={post.id}
                href="https://t.me/magicvibes_ru"
                target="_blank"
                rel="noreferrer"
                style={{ borderRadius: 3, border: "1px solid rgba(255,255,255,0.07)", background: "#0e0e0e", overflow: "hidden", textDecoration: "none", display: "flex", flexDirection: "column", transition: "border-color 0.4s ease, transform 0.5s cubic-bezier(0.16,1,0.3,1)" }}
                onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,162,94,0.4)"; el.style.transform = "translateY(-4px)"; }}
                onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(255,255,255,0.07)"; el.style.transform = ""; }}
              >
                {post.photoUrl && (
                  <div style={{ width: "100%", aspectRatio: "16/9", overflow: "hidden", background: "#141414" }}>
                    <img src={post.photoUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover" }} loading="lazy" />
                  </div>
                )}
                <div style={{ padding: "18px 18px 20px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
                  <p style={{ margin: 0, fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "#6f6c66" }}>
                    {new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}
                  </p>
                  <p style={{ margin: 0, fontSize: 13.5, color: "#8b8880", lineHeight: 1.65, flex: 1 }}>
                    {post.text.replace(/#\S+/g, "").trim().slice(0, 220)}{post.text.length > 220 && "…"}
                  </p>
                  <span style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--accent)" }}>
                    Читать в Telegram →
                  </span>
                </div>
              </a>
            ))}
          </div>
        </section>
      )}

    </div>
  );
}
