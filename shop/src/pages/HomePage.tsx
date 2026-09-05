import { useRef, useEffect, useState, useMemo } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Quote, Star } from "lucide-react";
import { api } from "../api";
import ProductCard from "../components/ProductCard";
import HeroShader from "../components/HeroShader";

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

const BRAND_NOTES: Record<string, string> = {
  "Chanel": "Chanel No. 5 — альдегиды, роза, жасмин",
  "Dior": "Sauvage — бергамот, амброксан",
  "Tom Ford": "Black Orchid — трюфель, чёрная орхидея",
  "Hermès": "Terre d'Hermès — грейпфрут, кедр, кремний",
  "Byredo": "Gypsy Water — сосна, ваниль, янтарь",
  "Jo Malone": "Wood Sage & Sea Salt — морская соль, шалфей",
  "Creed": "Aventus — ананас, берёза, мускус",
  "Guerlain": "Shalimar — ваниль, ирис, бергамот",
  "Givenchy": "L'Interdit — белые цветы, пачули",
  "Prada": "La Femme — иланг, ирис, ладан",
  "Valentino": "Valentina — белая трюфель, апельсин",
  "Burberry": "Her — ягоды, пион, амброксан",
  "Versace": "Eros — мята, зелёное яблоко, тонка",
  "Montale": "Black Aoud — уд, роза, пачули",
  "Kilian": "Angels' Share — коньяк, корица, миндаль",
  "YSL": "Black Opium — кофе, ваниль, белые цветы",
  "Bvlgari": "Man in Black — ром, ирис, гваяк",
  "Lancôme": "La Vie est Belle — ирис, пачули, ваниль",
  "Amouage": "Interlude — ладан, сосна, орхидея",
  "Xerjoff": "Naxos — лаванда, мёд, табак",
  "Maison Margiela": "Replica — в зависимости от аромата",
  "Acqua di Parma": "Colonia — цитрус, лаванда, сандал",
};

const QUIZ = [
  { q: "Для какого случая ищете аромат?",          opts: ["Повседневный образ", "Вечерний выход", "Особый повод", "В подарок"] },
  { q: "Какое настроение должен передавать аромат?", opts: ["Свежий и лёгкий", "Тёплый и уютный", "Загадочный и глубокий", "Яркий и бодрящий"] },
  { q: "Какие ноты вам ближе?",                    opts: ["Цветочные", "Восточные и пряные", "Древесные", "Морские и цитрусовые"] },
  { q: "Ваш бюджет?",                              opts: ["До 3 000 ₽", "3 000 – 7 000 ₽", "7 000 – 15 000 ₽", "Без ограничений"] },
  { q: "Аромат для кого?",                          opts: ["Для себя", "В подарок близкому", "На особый случай", "Для коллекции"] },
];

const QUIZ_RESULTS = [
  { title: "Цветочные ароматы",    text: "Нежные, романтичные, универсальные. Идеальны для дня и особых моментов.",   cat: "edp" },
  { title: "Восточная парфюмерия", text: "Глубокие, чувственные, запоминающиеся. Для тех, кто любит оставлять след.", cat: "parfum" },
  { title: "Древесные ароматы",    text: "Уверенные, элегантные, вне времени. Образ силы и утончённости.",            cat: "edt" },
  { title: "Свежая парфюмерия",    text: "Лёгкая, бодрящая, универсальная. Для любого случая и сезона.",              cat: "edt" },
];

function useMagneticHero(containerRef: React.RefObject<HTMLDivElement | null>) {
  useEffect(() => {
    const c = containerRef.current;
    if (!c) return;
    const btns = c.querySelectorAll<HTMLElement>(".btn-primary,.btn-ghost");
    const cleanup: Array<() => void> = [];
    btns.forEach((btn) => {
      const onMove = (e: MouseEvent) => {
        const r = btn.getBoundingClientRect();
        const dx = e.clientX - (r.left + r.width / 2);
        const dy = e.clientY - (r.top + r.height / 2);
        const dist = Math.hypot(dx, dy);
        if (dist < 110) {
          btn.style.transition = "transform 0.12s ease";
          btn.style.transform = `translate(${dx * 0.26}px, ${dy * 0.26}px)`;
        } else if (btn.style.transform) {
          btn.style.transition = "transform 0.65s cubic-bezier(0.16,1,0.3,1)";
          btn.style.transform = "";
        }
      };
      document.addEventListener("mousemove", onMove);
      cleanup.push(() => document.removeEventListener("mousemove", onMove));
    });
    return () => cleanup.forEach((f) => f());
  }, []);
}

function BrandGallery() {
  const trackRef = useRef<HTMLDivElement>(null);
  const [hoveredBrand, setHoveredBrand] = useState<string | null>(null);
  const [hoverPos, setHoverPos] = useState({ x: 0, y: 0 });
  const isDragging = useRef(false);
  const startX = useRef(0);
  const scrollLeft = useRef(0);

  function onMouseDown(e: React.MouseEvent) {
    if (!trackRef.current) return;
    isDragging.current = true;
    startX.current = e.pageX - trackRef.current.offsetLeft;
    scrollLeft.current = trackRef.current.scrollLeft;
    trackRef.current.style.cursor = "grabbing";
  }
  function onMouseMove(e: React.MouseEvent) {
    if (!isDragging.current || !trackRef.current) return;
    e.preventDefault();
    const x = e.pageX - trackRef.current.offsetLeft;
    trackRef.current.scrollLeft = scrollLeft.current - (x - startX.current) * 1.4;
  }
  function onMouseUp() {
    isDragging.current = false;
    if (trackRef.current) trackRef.current.style.cursor = "grab";
  }

  return (
    <div style={{ position: "relative" }}>
      {/* fade edges */}
      <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to right, #0b0b0b, transparent)", zIndex: 10, pointerEvents: "none" }} />
      <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to left, #0b0b0b, transparent)", zIndex: 10, pointerEvents: "none" }} />

      <div
        ref={trackRef}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
        onMouseUp={onMouseUp}
        onMouseLeave={() => { onMouseUp(); setHoveredBrand(null); }}
        style={{
          display: "flex", gap: 6, overflowX: "auto", cursor: "grab",
          scrollbarWidth: "none", padding: "8px clamp(80px,8vw,120px)",
          userSelect: "none",
        }}
      >
        {BRANDS.map((brand) => (
          <Link
            key={brand}
            to={`/catalog?brand=${encodeURIComponent(brand)}`}
            draggable={false}
            onMouseEnter={(e) => {
              setHoveredBrand(brand);
              setHoverPos({ x: e.clientX, y: e.clientY });
            }}
            onMouseLeave={() => setHoveredBrand(null)}
            onMouseMove={(e) => setHoverPos({ x: e.clientX, y: e.clientY })}
            style={{
              flexShrink: 0,
              padding: "12px 28px",
              border: "1px solid rgba(255,255,255,0.07)",
              borderRadius: 2,
              background: hoveredBrand === brand ? "rgba(201,162,94,0.08)" : "transparent",
              borderColor: hoveredBrand === brand ? "rgba(201,162,94,0.4)" : "rgba(255,255,255,0.07)",
              fontFamily: "'Cormorant Garamond',Georgia,serif",
              fontStyle: "italic",
              fontSize: 18,
              color: hoveredBrand === brand ? "#e8d5a3" : "#4a473f",
              whiteSpace: "nowrap",
              textDecoration: "none",
              transition: "background 0.3s ease, border-color 0.3s ease, color 0.3s ease",
            }}
          >
            {brand}
          </Link>
        ))}
      </div>

      {/* Hover tooltip */}
      {hoveredBrand && BRAND_NOTES[hoveredBrand] && (
        <div style={{
          position: "fixed",
          left: hoverPos.x + 14,
          top: hoverPos.y - 48,
          pointerEvents: "none",
          zIndex: 1000,
          background: "#0f0f0f",
          border: "1px solid rgba(201,162,94,0.3)",
          borderRadius: 3,
          padding: "10px 16px",
          maxWidth: 260,
          boxShadow: "0 8px 24px rgba(0,0,0,0.6)",
        }}>
          <p style={{ margin: "0 0 4px", fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 15, color: "#f5f4f0" }}>{hoveredBrand}</p>
          <p style={{ margin: 0, fontSize: 11.5, color: "#8b8880", lineHeight: 1.5 }}>{BRAND_NOTES[hoveredBrand]}</p>
        </div>
      )}
    </div>
  );
}

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
  const heroBtnsRef  = useRef<HTMLDivElement>(null);
  const heroGradRef  = useRef<HTMLDivElement>(null);

  useMagneticHero(heroBtnsRef);

  /* quiz */
  const [quizStep, setQuizStep]     = useState(0);
  const [quizAnswers, setQuizAnswers] = useState<number[]>([]);
  const [quizDone, setQuizDone]     = useState(false);
  const [quizEmail, setQuizEmail]   = useState("");
  const [quizEmailSent, setQuizEmailSent] = useState(false);

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
  function resetQuiz() { setQuizStep(0); setQuizAnswers([]); setQuizDone(false); setQuizEmail(""); setQuizEmailSent(false); }

  async function submitQuizEmail() {
    if (!quizEmail) return;
    try {
      await fetch((import.meta.env.VITE_API_BASE ?? "") + "/api/shop/email-subscribe", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: quizEmail, source: "quiz", quizCategory: quizResult?.title }),
      });
    } catch { /* best-effort */ }
    setQuizEmailSent(true);
  }

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
  const { data: unboxingsData } = useQuery({
    queryKey: ["shop-unboxings"],
    queryFn: () => api.unboxings(),
    staleTime: 5 * 60_000,
  });

  /* unboxing form */
  const [ubName, setUbName] = useState("");
  const [ubMediaUrl, setUbMediaUrl] = useState("");
  const [ubText, setUbText] = useState("");
  const [ubSuccess, setUbSuccess] = useState(false);
  const [ubDragOver, setUbDragOver] = useState(false);
  const [ubUploading, setUbUploading] = useState(false);
  const [ubPreview, setUbPreview] = useState<{ url: string; isVideo: boolean } | null>(null);
  const ubFileRef = useRef<HTMLInputElement>(null);

  const ubMutation = useMutation({
    mutationFn: (data: { name: string; mediaUrl: string; text: string }) => api.submitUnboxing(data),
    onSuccess: () => {
      setUbSuccess(true);
      setUbName(""); setUbMediaUrl(""); setUbText(""); setUbPreview(null);
    },
  });

  async function handleUbFile(file: File) {
    if (!file) return;
    setUbUploading(true);
    try {
      const res = await api.uploadMedia(file);
      if (res.ok) {
        setUbMediaUrl(res.url);
        setUbPreview({ url: res.url, isVideo: res.isVideo });
      }
    } catch { /* best-effort */ }
    setUbUploading(false);
  }

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

  /* ── Hero gradient parallax ───────────────────────────────────── */
  useEffect(() => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const el = heroGradRef.current;
    if (!el) return;
    let raf = 0;
    const onScroll = () => {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        el.style.transform = `translateY(${window.scrollY * 0.28}px)`;
      });
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => { window.removeEventListener("scroll", onScroll); cancelAnimationFrame(raf); };
  }, []);

  return (
    <div style={{ background: "#0b0b0b", color: "var(--text)", minHeight: "100vh" }}>

      {/* ── Cursor aura ─────────────────────────────────────────── */}
      <div ref={auraRef} aria-hidden="true" className="cursor-aura" />

      {/* ════════════════════ HERO ════════════════════ */}
      <section style={{ position: "relative", overflow: "hidden", minHeight: "84vh", display: "flex", alignItems: "center", justifyContent: "center", padding: "9vh clamp(18px,4vw,56px)" }}>
        <HeroShader />
        <div ref={heroGradRef} style={{ position: "absolute", inset: "-10%", pointerEvents: "none", background: "radial-gradient(44% 38% at 50% 44%, rgba(201,162,94,0.16) 0%, rgba(201,162,94,0.05) 40%, rgba(11,11,11,0) 72%)", willChange: "transform" }} />
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

          <div ref={heroBtnsRef} className="anim-slide-up" style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "center", gap: 14, animationDelay: "0.45s" }}>
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

              {/* Email capture */}
              <div style={{ marginTop: 28, paddingTop: 24, borderTop: "1px solid rgba(255,255,255,0.06)" }}>
                {!quizEmailSent ? (
                  <>
                    <p style={{ margin: "0 0 14px", fontSize: 12, color: "#6f6c66" }}>Получить подборку на почту</p>
                    <div style={{ display: "flex", gap: 10, alignItems: "flex-end", maxWidth: 400, margin: "0 auto" }}>
                      <input
                        type="email"
                        value={quizEmail}
                        onChange={e => setQuizEmail(e.target.value)}
                        onKeyDown={e => { if (e.key === "Enter" && quizEmail) submitQuizEmail(); }}
                        placeholder="your@email.com"
                        style={{
                          flex: 1, padding: "9px 0",
                          background: "transparent", border: "none",
                          borderBottom: "1px solid rgba(255,255,255,0.12)",
                          color: "#f5f4f0", fontSize: 13.5, outline: "none",
                          transition: "border-bottom-color 0.3s ease",
                        }}
                        onFocus={e => (e.target.style.borderBottomColor = "rgba(201,162,94,0.55)")}
                        onBlur={e => (e.target.style.borderBottomColor = "rgba(255,255,255,0.12)")}
                      />
                      <button
                        onClick={submitQuizEmail}
                        disabled={!quizEmail}
                        style={{
                          padding: "9px 18px", background: "transparent",
                          border: "1px solid rgba(201,162,94,0.4)", borderRadius: 2,
                          color: "#c9a25e", fontSize: 11,
                          letterSpacing: "0.12em", cursor: "pointer",
                          transition: "background 0.3s, color 0.3s",
                        }}
                        onMouseEnter={e => { e.currentTarget.style.background = "rgba(201,162,94,0.12)"; e.currentTarget.style.color = "#e8d5a3"; }}
                        onMouseLeave={e => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.color = "#c9a25e"; }}
                      >
                        Отправить
                      </button>
                    </div>
                  </>
                ) : (
                  <p style={{ margin: 0, fontSize: 12.5, color: "#5dd876" }}>
                    Подборка отправлена — проверьте почту
                  </p>
                )}
              </div>
            </div>
          )}
        </div>
      </section>

      {/* ════════════════════ GIFT CTA ════════════════════ */}
      <section style={{ margin: "clamp(40px,6vw,80px) 0", padding: "0 clamp(18px,4vw,56px)" }}>
        <Link
          to="/gift"
          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "clamp(20px,3vw,36px) clamp(24px,4vw,48px)", background: "linear-gradient(135deg, #1a1408 0%, #111113 60%)", border: "1px solid rgba(201,162,94,0.3)", borderRadius: 3, textDecoration: "none", gap: 16, flexWrap: "wrap", transition: "border-color 0.3s ease" }}
          onMouseEnter={e => ((e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.6)")}
          onMouseLeave={e => ((e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.3)")}
        >
          <div>
            <p style={{ fontSize: 10, letterSpacing: "0.26em", textTransform: "uppercase", color: "#c9a25e", margin: "0 0 6px" }}>Идеальный подарок</p>
            <p className="serif" style={{ fontSize: "clamp(22px,3vw,34px)", fontStyle: "italic", fontWeight: 300, color: "#f5f4f0", margin: 0, lineHeight: 1.15 }}>Собери подарочный набор</p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
            <span style={{ fontSize: 13, color: "#c9a25e", letterSpacing: "0.08em" }}>Выбрать аромат →</span>
          </div>
        </Link>
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

      {/* ════════════════════ BRAND GALLERY ════════════════════ */}
      <section className="reveal-section" style={{ marginTop: "clamp(56px,8vw,104px)", padding: "clamp(32px,4vw,52px) 0", borderTop: "1px solid rgba(255,255,255,0.06)", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
        <p style={{ margin: "0 0 28px", textAlign: "center", fontSize: 10, letterSpacing: "0.34em", textTransform: "uppercase", color: "#5d5a54" }}>Мировые парфюмерные дома</p>
        <BrandGallery />
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

      {/* ════════════════════ UNBOXING ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) clamp(56px,8vw,96px)" }}>
        <p className="eyebrow" style={{ marginBottom: 10 }}>Наши покупатели</p>
        <h2 className="serif" style={{ margin: "0 0 34px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Распаковки</h2>

        {unboxingsData && unboxingsData.unboxings.length > 0 && (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))", gap: 18, marginBottom: 48 }}>
            {unboxingsData.unboxings.map((u) => {
              const isYoutube = /youtube\.com|youtu\.be/.test(u.mediaUrl);
              let embedUrl = "";
              if (isYoutube) {
                const m = u.mediaUrl.match(/(?:v=|youtu\.be\/)([A-Za-z0-9_-]{11})/);
                if (m) embedUrl = `https://www.youtube.com/embed/${m[1]}`;
              }
              return (
                <div key={u.id} style={{ padding: 20, border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "#0e0e0e", display: "flex", flexDirection: "column", gap: 12 }}>
                  {isYoutube && embedUrl ? (
                    <iframe
                      src={embedUrl}
                      style={{ width: "100%", height: 180, border: "none", borderRadius: 2 }}
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                      allowFullScreen
                      loading="lazy"
                      title={`Распаковка от ${u.name}`}
                    />
                  ) : u.mediaUrl ? (
                    <a
                      href={u.mediaUrl}
                      target="_blank"
                      rel="noreferrer"
                      style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--accent)" }}
                    >
                      Смотреть →
                    </a>
                  ) : null}
                  {u.text && (
                    <p style={{ margin: 0, fontSize: 13, color: "#8b8880", lineHeight: 1.7 }}>
                      {u.text.length > 220 ? u.text.slice(0, 220) + "…" : u.text}
                    </p>
                  )}
                  <div>
                    <p style={{ margin: 0, fontSize: 12, color: "var(--text)" }}>{u.name}</p>
                    <p style={{ margin: "3px 0 0", fontSize: 10, color: "#6f6c66" }}>
                      {new Date(u.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Submission form */}
        <div style={{ maxWidth: 560, padding: "clamp(24px,3vw,40px)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "#0e0e0e" }}>
          <p style={{ margin: "0 0 20px", fontSize: 13, color: "#6f6c66", letterSpacing: "0.06em" }}>Поделитесь своей распаковкой</p>
          {ubSuccess ? (
            <div>
              <p style={{ margin: "0 0 8px", fontSize: 13.5, color: "#5dd876" }}>Спасибо! Ваша распаковка отправлена на проверку.</p>
              <p style={{ margin: 0, fontSize: 12.5, color: "#6f6c66" }}>За публикацию вы получите промокод <span style={{ color: "#c9a25e", fontWeight: 600 }}>UNBOX7</span> на скидку 7%</p>
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
              <input
                type="text"
                value={ubName}
                onChange={e => setUbName(e.target.value)}
                placeholder="Ваше имя"
                style={{ width: "100%", padding: "9px 0", background: "transparent", border: "none", borderBottom: "1px solid rgba(255,255,255,0.12)", color: "#f5f4f0", fontSize: 13.5, outline: "none", boxSizing: "border-box", transition: "border-bottom-color 0.3s ease" }}
                onFocus={e => (e.target.style.borderBottomColor = "rgba(201,162,94,0.55)")}
                onBlur={e => (e.target.style.borderBottomColor = "rgba(255,255,255,0.12)")}
              />
              {/* Media upload / URL */}
              <input ref={ubFileRef} type="file" accept="image/jpeg,image/png,image/webp,video/mp4,video/webm" style={{ display: "none" }} onChange={e => { const f = e.target.files?.[0]; if (f) handleUbFile(f); }} />
              {ubPreview ? (
                <div style={{ position: "relative", borderRadius: 3, overflow: "hidden", border: "1px solid rgba(201,162,94,0.3)" }}>
                  {ubPreview.isVideo
                    ? <video src={ubPreview.url} controls style={{ width: "100%", maxHeight: 220, display: "block" }} />
                    : <img src={ubPreview.url} alt="" style={{ width: "100%", maxHeight: 220, objectFit: "contain", display: "block", background: "#0a0a0a" }} />}
                  <button onClick={() => { setUbPreview(null); setUbMediaUrl(""); }} style={{ position: "absolute", top: 6, right: 6, background: "rgba(0,0,0,0.7)", border: "none", borderRadius: 2, color: "#9a9690", fontSize: 12, padding: "3px 8px", cursor: "pointer" }}>✕</button>
                </div>
              ) : (
                <div
                  onClick={() => ubFileRef.current?.click()}
                  onDragOver={e => { e.preventDefault(); setUbDragOver(true); }}
                  onDragLeave={() => setUbDragOver(false)}
                  onDrop={e => { e.preventDefault(); setUbDragOver(false); const f = e.dataTransfer.files[0]; if (f) handleUbFile(f); }}
                  style={{
                    padding: "20px 16px", borderRadius: 3, cursor: "pointer", textAlign: "center",
                    border: `1px dashed ${ubDragOver ? "rgba(201,162,94,0.7)" : "rgba(255,255,255,0.15)"}`,
                    background: ubDragOver ? "rgba(201,162,94,0.05)" : "transparent",
                    transition: "border-color 0.2s, background 0.2s",
                  }}
                >
                  {ubUploading ? (
                    <span style={{ fontSize: 12, color: "#c9a25e" }}>Загрузка…</span>
                  ) : (
                    <>
                      <div style={{ fontSize: 22, marginBottom: 6, opacity: 0.4 }}>📎</div>
                      <div style={{ fontSize: 12, color: "#6f6c66" }}>Перетащите фото/видео или нажмите для выбора</div>
                      <div style={{ fontSize: 11, color: "#4a4740", marginTop: 4 }}>JPG, PNG, WEBP, MP4 · до 50 МБ</div>
                    </>
                  )}
                </div>
              )}
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ flex: 1, height: 1, background: "rgba(255,255,255,0.07)" }} />
                <span style={{ fontSize: 10, color: "#4a4740", letterSpacing: "0.08em" }}>или</span>
                <div style={{ flex: 1, height: 1, background: "rgba(255,255,255,0.07)" }} />
              </div>
              <input
                type="text"
                value={ubPreview ? "" : ubMediaUrl}
                onChange={e => { setUbMediaUrl(e.target.value); setUbPreview(null); }}
                placeholder="Ссылка на YouTube или фото"
                disabled={!!ubPreview}
                style={{ width: "100%", padding: "9px 0", background: "transparent", border: "none", borderBottom: "1px solid rgba(255,255,255,0.12)", color: "#f5f4f0", fontSize: 13.5, outline: "none", boxSizing: "border-box", transition: "border-bottom-color 0.3s ease", opacity: ubPreview ? 0.3 : 1 }}
                onFocus={e => (e.target.style.borderBottomColor = "rgba(201,162,94,0.55)")}
                onBlur={e => (e.target.style.borderBottomColor = "rgba(255,255,255,0.12)")}
              />
              <textarea
                value={ubText}
                onChange={e => setUbText(e.target.value)}
                placeholder="Расскажите о вашей покупке..."
                rows={2}
                style={{ width: "100%", padding: "9px 0", background: "transparent", border: "none", borderBottom: "1px solid rgba(255,255,255,0.12)", color: "#f5f4f0", fontSize: 13.5, outline: "none", resize: "none", boxSizing: "border-box", transition: "border-bottom-color 0.3s ease", fontFamily: "inherit" }}
                onFocus={e => (e.target.style.borderBottomColor = "rgba(201,162,94,0.55)")}
                onBlur={e => (e.target.style.borderBottomColor = "rgba(255,255,255,0.12)")}
              />
              <button
                onClick={() => ubMutation.mutate({ name: ubName, mediaUrl: ubMediaUrl, text: ubText })}
                disabled={ubMutation.isPending || (!ubName.trim() && !ubMediaUrl.trim() && !ubText.trim())}
                style={{ alignSelf: "flex-start", padding: "11px 28px", background: "transparent", border: "1px solid rgba(201,162,94,0.4)", borderRadius: 2, color: "#c9a25e", fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer", transition: "background 0.3s, color 0.3s", opacity: ubMutation.isPending ? 0.6 : 1 }}
                onMouseEnter={e => { if (!ubMutation.isPending) { e.currentTarget.style.background = "rgba(201,162,94,0.12)"; e.currentTarget.style.color = "#e8d5a3"; } }}
                onMouseLeave={e => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.color = "#c9a25e"; }}
              >
                {ubMutation.isPending ? "Отправка…" : "Отправить"}
              </button>
            </div>
          )}
        </div>
      </section>

    </div>
  );
}
