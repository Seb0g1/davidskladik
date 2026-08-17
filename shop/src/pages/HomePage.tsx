import { useRef, useEffect, useState, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, BadgeCheck, Truck, Star, Sparkles, ChevronDown } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

/* ── Particles canvas ─────────────────────────────────────────── */
function useParticles(ref: React.RefObject<HTMLCanvasElement | null>) {
  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf: number;
    let mouse = { x: -999, y: -999 };
    const isMobile = window.innerWidth < 768;
    const COUNT = isMobile ? 45 : 90;

    function resize() {
      canvas!.width  = canvas!.offsetWidth;
      canvas!.height = canvas!.offsetHeight;
    }
    resize();
    window.addEventListener("resize", resize);

    type P = { x: number; y: number; vx: number; vy: number; r: number; a: number };
    const particles: P[] = Array.from({ length: COUNT }, () => ({
      x:  Math.random() * canvas!.width,
      y:  Math.random() * canvas!.height,
      vx: (Math.random() - 0.5) * 0.35,
      vy: (Math.random() - 0.5) * 0.35,
      r:  Math.random() * 1.6 + 0.4,
      a:  Math.random() * 0.55 + 0.15,
    }));

    function draw() {
      const w = canvas!.width, h = canvas!.height;
      ctx!.clearRect(0, 0, w, h);

      for (let i = 0; i < particles.length; i++) {
        const p = particles[i];
        p.x += p.vx;
        p.y += p.vy;
        if (p.x < 0) p.x = w; if (p.x > w) p.x = 0;
        if (p.y < 0) p.y = h; if (p.y > h) p.y = 0;

        const dx = p.x - mouse.x, dy = p.y - mouse.y;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 100) {
          p.vx += dx / dist * 0.025;
          p.vy += dy / dist * 0.025;
        }
        const speed = Math.sqrt(p.vx * p.vx + p.vy * p.vy);
        if (speed > 0.8) { p.vx *= 0.96; p.vy *= 0.96; }

        ctx!.beginPath();
        ctx!.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx!.fillStyle = `rgba(167,139,250,${p.a})`;
        ctx!.fill();

        for (let j = i + 1; j < particles.length; j++) {
          const q = particles[j];
          const ex = p.x - q.x, ey = p.y - q.y;
          const d = Math.sqrt(ex * ex + ey * ey);
          if (d < 110) {
            ctx!.beginPath();
            ctx!.moveTo(p.x, p.y);
            ctx!.lineTo(q.x, q.y);
            ctx!.strokeStyle = `rgba(124,58,237,${(1 - d / 110) * 0.3})`;
            ctx!.lineWidth = 0.7;
            ctx!.stroke();
          }
        }
      }
      raf = requestAnimationFrame(draw);
    }
    draw();

    const onMove = (e: MouseEvent | TouchEvent) => {
      const rect = canvas!.getBoundingClientRect();
      const src  = "touches" in e ? e.touches[0] : e;
      mouse = { x: src.clientX - rect.left, y: src.clientY - rect.top };
    };
    const onLeave = () => { mouse = { x: -999, y: -999 }; };
    canvas.parentElement?.addEventListener("mousemove", onMove as EventListener);
    canvas.parentElement?.addEventListener("touchmove",  onMove as EventListener, { passive: true });
    canvas.parentElement?.addEventListener("mouseleave", onLeave);
    return () => {
      cancelAnimationFrame(raf);
      window.removeEventListener("resize", resize);
    };
  }, [ref]);
}

/* ── Scroll parallax hook ─────────────────────────────────────── */
function useScrollY() {
  const [y, setY] = useState(0);
  useEffect(() => {
    const handler = () => setY(window.scrollY);
    window.addEventListener("scroll", handler, { passive: true });
    return () => window.removeEventListener("scroll", handler);
  }, []);
  return y;
}

/* ── 3D tilt hook ─────────────────────────────────────────────── */
function useTilt() {
  const [style, setStyle] = useState<React.CSSProperties>({});
  const onMove = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width  - 0.5;
    const y = (e.clientY - rect.top)  / rect.height - 0.5;
    setStyle({
      transform: `perspective(700px) rotateY(${x * 14}deg) rotateX(${-y * 14}deg) scale3d(1.02,1.02,1.02)`,
      transition: "transform 0.08s ease",
    });
    e.currentTarget.style.setProperty("--mx", `${(x + 0.5) * 100}%`);
    e.currentTarget.style.setProperty("--my", `${(y + 0.5) * 100}%`);
  }, []);
  const onLeave = useCallback(() => {
    setStyle({ transform: "perspective(700px) rotateY(0) rotateX(0) scale3d(1,1,1)", transition: "transform 0.4s ease" });
  }, []);
  return { style, onMouseMove: onMove, onMouseLeave: onLeave };
}

/* ── Animated counter ─────────────────────────────────────────── */
function Counter({ to, suffix = "" }: { to: number; suffix?: string }) {
  const [val, setVal] = useState(0);
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    const io = new IntersectionObserver(([e]) => {
      if (!e.isIntersecting) return;
      io.disconnect();
      const start = performance.now();
      const dur = 1600;
      const tick = (now: number) => {
        const t = Math.min((now - start) / dur, 1);
        const ease = 1 - Math.pow(1 - t, 3);
        setVal(Math.round(ease * to));
        if (t < 1) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }, { threshold: 0.3 });
    if (ref.current) io.observe(ref.current);
    return () => io.disconnect();
  }, [to]);
  return <span ref={ref}>{val.toLocaleString("ru-RU")}{suffix}</span>;
}

/* ── Floating glass product card ───────────────────────────────── */
function HeroCard({ img, name, price }: { img?: string; name?: string; price?: number }) {
  return (
    <div className="glass float-1" style={{ padding: "14px", minWidth: 200, maxWidth: 220 }}>
      <div style={{ aspectRatio: "4/5", borderRadius: 12, overflow: "hidden", background: "rgba(255,255,255,0.05)", marginBottom: 10 }}>
        {img
          ? <img src={img} alt={name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 10 }} />
          : <div style={{ width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <Sparkles size={36} style={{ color: "rgba(167,139,250,0.4)" }} />
            </div>
        }
      </div>
      <p style={{ fontSize: 11, color: "var(--muted)", marginBottom: 3, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em" }}>Magic Vibes</p>
      <p style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", lineHeight: 1.3, marginBottom: 6 }} className="line-clamp-2">
        {name ?? "Оригинальный аромат"}
      </p>
      {price && (
        <p style={{ fontSize: 15, fontWeight: 700, color: "var(--accent3)" }}>
          {price.toLocaleString("ru-RU")} ₽
        </p>
      )}
    </div>
  );
}

const CAT_LABELS: Record<string, string> = {
  parfum: "Духи", edp: "Парфюмерная вода", edt: "Туалетная вода",
  sets: "Наборы", testers: "Тестеры", body: "Уход", home: "Для дома", deo: "Дезодоранты",
};
const CAT_ICONS: Record<string, string> = {
  parfum: "🖤", edp: "💜", edt: "🌊", sets: "🎁", testers: "✨", body: "🌿", home: "🏠", deo: "💧",
};
const CAT_GRADIENTS: Record<string, [string, string]> = {
  parfum:  ["rgba(109,40,217,0.25)",  "rgba(91,33,182,0.08)"],
  edp:     ["rgba(124,58,237,0.22)",  "rgba(99,43,210,0.06)"],
  edt:     ["rgba(14,116,144,0.22)",  "rgba(6,78,119,0.08)"],
  sets:    ["rgba(180,83,9,0.22)",    "rgba(120,53,15,0.08)"],
  testers: ["rgba(79,70,229,0.22)",   "rgba(55,48,163,0.08)"],
  body:    ["rgba(21,128,61,0.22)",   "rgba(6,78,59,0.08)"],
  home:    ["rgba(3,105,161,0.22)",   "rgba(7,89,133,0.08)"],
  deo:     ["rgba(107,33,168,0.22)",  "rgba(76,29,149,0.08)"],
};
const BRANDS = ["Chanel","Dior","Tom Ford","Hermès","Byredo","Jo Malone","Creed","Guerlain","Givenchy","Prada","Valentino","Burberry","Versace","Hugo Boss","Montale","Kilian","YSL","Bvlgari","Lancôme","Moschino"];

/* ── Category card ────────────────────────────────────────────── */
function CatCard({ cat, index }: { cat: AutoCategory; index: number }) {
  const tilt = useTilt();
  const [g0, g1] = CAT_GRADIENTS[cat.slug] ?? ["rgba(124,58,237,0.18)", "rgba(91,33,182,0.05)"];
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      className="flex-shrink-0"
      style={{
        display: "flex", flexDirection: "column", gap: 0, textDecoration: "none",
        width: 148, padding: "18px 16px 16px",
        borderRadius: 20, border: "1px solid rgba(255,255,255,0.08)",
        background: `linear-gradient(145deg, ${g0}, ${g1})`,
        backdropFilter: "blur(12px)",
        position: "relative", overflow: "hidden",
        transition: "border-color 0.25s, box-shadow 0.25s",
        animation: `slideUp 0.5s cubic-bezier(0.22,1,0.36,1) ${index * 0.05}s both`,
        ...tilt.style,
      }}
      onMouseMove={tilt.onMouseMove}
      onMouseLeave={tilt.onMouseLeave}
      onMouseEnter={e => {
        (e.currentTarget as HTMLElement).style.borderColor = "rgba(167,139,250,0.25)";
        (e.currentTarget as HTMLElement).style.boxShadow = "0 8px 32px rgba(124,58,237,0.2)";
      }}
      onMouseOut={e => {
        (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.08)";
        (e.currentTarget as HTMLElement).style.boxShadow = "none";
      }}
    >
      {/* Glow blob */}
      <div style={{ position: "absolute", width: 80, height: 80, borderRadius: "50%", background: g0, filter: "blur(24px)", top: -20, right: -20, pointerEvents: "none" }} />
      <div style={{ fontSize: 32, marginBottom: 14, position: "relative", zIndex: 1, filter: "drop-shadow(0 2px 8px rgba(0,0,0,0.3))" }}>
        {CAT_ICONS[cat.slug] ?? "✦"}
      </div>
      <p style={{ fontSize: 13, fontWeight: 700, color: "var(--text)", lineHeight: 1.3, marginBottom: 5, position: "relative", zIndex: 1 }}>
        {CAT_LABELS[cat.slug] || cat.label}
      </p>
      <p style={{ fontSize: 10.5, color: "rgba(240,235,255,0.4)", fontWeight: 500, position: "relative", zIndex: 1 }}>
        {cat.count.toLocaleString("ru-RU")} товаров
      </p>
    </Link>
  );
}

function CardSkeleton() {
  return (
    <div className="product-card-3d flex-shrink-0" style={{ width: 168 }}>
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div style={{ padding: "10px 12px 12px" }}>
        <div className="skeleton" style={{ height: 8, width: "40%", marginBottom: 6 }} />
        <div className="skeleton" style={{ height: 11, marginBottom: 4 }} />
        <div className="skeleton" style={{ height: 11, width: "70%", marginBottom: 8 }} />
        <div className="skeleton" style={{ height: 14, width: "50%" }} />
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════ */
export default function HomePage() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  useParticles(canvasRef);
  const scrollY = useScrollY();

  const { data: catalog, isLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn:  () => api.catalog({ pageSize: 12, sort: "name" }),
  });
  const { data: autoCategories } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn:  () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  const hero = catalog?.products?.[0];
  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div style={{ background: "var(--bg)", color: "var(--text)", minHeight: "100vh" }}>

      {/* ════════════════════ HERO ════════════════════ */}
      <section style={{ position: "relative", minHeight: "100svh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", overflow: "hidden" }}>
        {/* Canvas */}
        <canvas ref={canvasRef} className="hero-canvas" />

        {/* Background orbs — parallax layers */}
        <div className="orb orb-1" style={{ width: 600, height: 600, background: "radial-gradient(circle, rgba(109,40,217,0.55) 0%, transparent 70%)", top: "5%", left: "8%", transform: `translateY(${scrollY * 0.18}px)` }} />
        <div className="orb orb-2" style={{ width: 450, height: 450, background: "radial-gradient(circle, rgba(79,70,229,0.45) 0%, transparent 70%)", bottom: "10%", right: "3%", transform: `translateY(${-scrollY * 0.12}px)` }} />
        <div className="orb orb-3" style={{ width: 320, height: 320, background: "radial-gradient(circle, rgba(167,139,250,0.3) 0%, transparent 70%)", top: "38%", right: "18%", transform: `translateY(${scrollY * 0.08}px)` }} />
        <div className="orb" style={{ width: 200, height: 200, background: "radial-gradient(circle, rgba(236,72,153,0.2) 0%, transparent 70%)", top: "60%", left: "5%", animation: "orbPulse 13s ease-in-out infinite 6s", transform: `translateY(${-scrollY * 0.1}px)` }} />

        {/* Content — slight counter-parallax for depth */}
        <div style={{ position: "relative", zIndex: 10, textAlign: "center", padding: "0 20px", maxWidth: 900, margin: "0 auto", width: "100%", transform: `translateY(${-scrollY * 0.08}px)` }}>
          {/* Badge */}
          <div className="anim-fade-in" style={{ display: "inline-flex", alignItems: "center", gap: 8, background: "rgba(124,58,237,0.12)", border: "1px solid rgba(167,139,250,0.2)", borderRadius: 100, padding: "6px 16px", marginBottom: 32, animationDelay: "0.1s" }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#4ADE80", display: "inline-block", boxShadow: "0 0 8px #4ADE80" }} />
            <span style={{ fontSize: 11, fontWeight: 600, color: "var(--muted)", letterSpacing: "0.08em", textTransform: "uppercase" }}>22 000+ ароматов в наличии</span>
          </div>

          {/* Main headline */}
          <h1
            className="grad-text anim-slide-up"
            style={{
              fontSize: "clamp(64px, 15vw, 180px)",
              fontWeight: 700,
              lineHeight: 0.88,
              letterSpacing: "-0.045em",
              marginBottom: 28,
              animationDelay: "0.2s",
            }}
          >
            Magic<br />Vibes
          </h1>

          <p className="anim-slide-up" style={{ fontSize: "clamp(15px, 2vw, 18px)", color: "var(--muted)", maxWidth: 420, margin: "0 auto 40px", lineHeight: 1.65, animationDelay: "0.35s" }}>
            Оригинальная парфюмерия мировых брендов.<br />
            Доставка по России через Ozon.
          </p>

          {/* CTAs */}
          <div className="anim-slide-up" style={{ display: "flex", gap: 12, justifyContent: "center", flexWrap: "wrap", animationDelay: "0.45s" }}>
            <Link to="/catalog" className="btn-primary" style={{ fontSize: 15, padding: "14px 32px" }}>
              Каталог ароматов
            </Link>
            <Link to="/brands" className="btn-ghost">
              Все бренды <ArrowRight size={15} strokeWidth={2.5} />
            </Link>
          </div>
        </div>

        {/* Floating hero card */}
        {hero && (
          <div
            className="float-2 anim-fade-in"
            style={{
              position: "absolute",
              right: "clamp(16px, 6vw, 80px)",
              top: "50%",
              transform: "translateY(-50%)",
              zIndex: 10,
              display: "none",
              animationDelay: "0.6s",
            }}
          >
            <style>{`@media(min-width:1024px){.hero-float-card{display:block!important;}}`}</style>
            <div className="hero-float-card" style={{ display: "none" }}>
              <div style={{ filter: "drop-shadow(0 20px 60px rgba(124,58,237,0.4))" }}>
                <HeroCard img={hero.images[0]} name={hero.name} price={hero.priceRub} />
              </div>
            </div>
          </div>
        )}

        {/* Scroll indicator */}
        <div style={{ position: "absolute", bottom: 32, left: "50%", transform: "translateX(-50%)", zIndex: 10, display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
          <span style={{ fontSize: 10, fontWeight: 600, color: "var(--subtle)", letterSpacing: "0.12em", textTransform: "uppercase" }}>Скролл</span>
          <ChevronDown size={16} style={{ color: "var(--subtle)", animation: "float3 2s ease-in-out infinite" }} />
        </div>
      </section>

      {/* ════════════════════ STATS ════════════════════ */}
      <section className="reveal-section">
        <div className="section-divider" />
        <div style={{ maxWidth: 1200, margin: "0 auto" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 0 }}>
            {[
              { to: 22000, suffix: "+",  label: "Ароматов",      icon: "✦" },
              { to: 100,   suffix: "%",  label: "Оригинал",       icon: "✓" },
              { to: 5,     suffix: " дн", label: "Срок доставки",  icon: "↗" },
              { to: 5,     suffix: "★",  label: "Рейтинг Ozon",   icon: "◈" },
            ].map(({ to, suffix, label, icon }, i) => (
              <div key={label}
                className="glass-hover"
                style={{
                  padding: "clamp(24px,4vw,44px) clamp(20px,4vw,48px)",
                  borderRight: i % 2 === 0 ? "1px solid var(--border)" : undefined,
                  borderBottom: i < 2 ? "1px solid var(--border)" : undefined,
                }}
              >
                <div style={{ fontSize: 10, fontWeight: 700, color: "var(--subtle)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 8 }}>{icon} {label}</div>
                <div className="grad-text" style={{ fontSize: "clamp(36px,5vw,64px)", fontWeight: 700, lineHeight: 1, letterSpacing: "-0.04em" }}>
                  <Counter to={to} suffix={suffix} />
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="section-divider" />
      </section>

      {/* ════════════════════ CATEGORIES ════════════════════ */}
      {autoCategories && autoCategories.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) 0" }}>
          <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", marginBottom: 32 }}>
              <div>
                <p style={{ fontSize: 10, fontWeight: 700, color: "var(--accent3)", letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 10, display: "flex", alignItems: "center", gap: 6 }}>
                  <span style={{ display: "inline-block", width: 16, height: 1, background: "var(--accent)" }} />
                  Коллекция
                </p>
                <h2 className="grad-text" style={{ fontSize: "clamp(28px,4vw,48px)", fontWeight: 700, letterSpacing: "-0.04em", lineHeight: 1.05 }}>Категории</h2>
              </div>
              <Link to="/catalog" className="btn-ghost" style={{ flexShrink: 0 }}>
                Все товары <ArrowRight size={14} strokeWidth={2.5} />
              </Link>
            </div>
          </div>
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)", paddingBottom: 12 }}>
            <div style={{ display: "flex", gap: 10, width: "max-content" }}>
              {autoCategories.map((cat, i) => <CatCard key={cat.slug} cat={cat} index={i} />)}
            </div>
          </div>
        </section>
      )}

      {/* ════════════════════ PRODUCTS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "0 0 clamp(48px,6vw,80px)" }}>
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)", marginBottom: 24 }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
            <div>
              <p style={{ fontSize: 11, fontWeight: 700, color: "var(--muted)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 6 }}>◈ Популярное</p>
              <h2 style={{ fontSize: "clamp(24px,3.5vw,40px)", fontWeight: 700, letterSpacing: "-0.035em" }}>Хиты сезона</h2>
            </div>
            <Link to="/catalog" className="btn-ghost" style={{ flexShrink: 0 }}>Смотреть всё <ArrowRight size={14} /></Link>
          </div>
        </div>

        {isLoading ? (
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "flex", gap: 14, width: "max-content" }}>
              {Array.from({ length: 8 }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          </div>
        ) : (
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "flex", gap: 14, width: "max-content" }}>
              {catalog?.products.map((p) => (
                <div key={p.offerId} style={{ width: 168, flexShrink: 0 }}>
                  <ProductCard product={p} />
                </div>
              ))}
            </div>
          </div>
        )}
      </section>

      {/* ════════════════════ FEATURES ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) 0" }}>
        <div className="section-divider" />
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(48px,6vw,80px) clamp(16px,4vw,48px)" }}>
          <p style={{ fontSize: 11, fontWeight: 700, color: "var(--muted)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 10, textAlign: "center" }}>↗ Почему мы</p>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 14 }}>
            {[
              { icon: <BadgeCheck size={22} />, title: "100% оригинал", text: "Прямые поставки от авторизованных дистрибьюторов. Сертификаты на каждый бренд." },
              { icon: <Truck size={22} />,      title: "Быстрая доставка", text: "Доставка через Ozon по всей России за 1–5 дней." },
              { icon: <Star size={22} />,       title: "4.9 на Ozon", text: "Тысячи довольных покупателей. Рейтинг 4.9 из 5 на маркетплейсе." },
              { icon: <Sparkles size={22} />,   title: "Эксклюзивный выбор", text: "Более 22 000 ароматов от 200+ брендов — от масс-маркета до нишевой парфюмерии." },
            ].map(({ icon, title, text }, i) => {
              const tilt = useTilt(); // eslint-disable-line react-hooks/rules-of-hooks
              return (
                <div
                  key={title}
                  className="product-card-3d glass-hover"
                  style={{ padding: "24px", ...tilt.style }}
                  onMouseMove={tilt.onMouseMove}
                  onMouseLeave={tilt.onMouseLeave}
                >
                  <div style={{
                    width: 44, height: 44, borderRadius: 14,
                    background: "linear-gradient(135deg, rgba(124,58,237,0.2), rgba(167,139,250,0.1))",
                    border: "1px solid rgba(167,139,250,0.2)",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    color: "var(--accent3)", marginBottom: 16,
                  }}>
                    {icon}
                  </div>
                  <h3 style={{ fontSize: 15, fontWeight: 700, marginBottom: 8, color: "var(--text)" }}>{title}</h3>
                  <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.65 }}>{text}</p>
                </div>
              );
            })}
          </div>
        </div>
        <div className="section-divider" />
      </section>

      {/* ════════════════════ PROMO ════════════════════ */}
      <section className="reveal-section" style={{ padding: "0 clamp(16px,4vw,48px) clamp(48px,6vw,80px)", maxWidth: 1200, margin: "0 auto" }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 14 }}>
          <style>{`@media(min-width:640px){.promo-grid{grid-template-columns:1fr 1fr!important;}}`}</style>
          <div className="promo-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 14 }}>
            {/* Dark */}
            <Link to="/catalog?category=sets" className="product-card-3d glass-hover" style={{ padding: "clamp(28px,4vw,44px)", minHeight: 220, display: "flex", flexDirection: "column", justifyContent: "space-between", position: "relative", overflow: "hidden" }}>
              <div style={{ position: "absolute", width: 250, height: 250, borderRadius: "50%", background: "radial-gradient(circle, rgba(124,58,237,0.35) 0%, transparent 70%)", right: -60, bottom: -60, pointerEvents: "none" }} />
              <div style={{ position: "relative", zIndex: 1 }}>
                <p style={{ fontSize: 10, fontWeight: 700, color: "var(--subtle)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 12 }}>Для близких</p>
                <h3 style={{ fontSize: "clamp(22px,3vw,32px)", fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1.1, marginBottom: 10, color: "var(--text)" }}>Подарите<br />аромат</h3>
                <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.6, maxWidth: 280 }}>Подарочные наборы от мировых парфюмерных домов</p>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 600, color: "var(--accent3)", marginTop: 20, position: "relative", zIndex: 1 }}>
                Выбрать набор <ArrowRight size={14} strokeWidth={2.5} />
              </div>
            </Link>

            {/* Accent */}
            <Link to="/catalog?category=parfum" className="product-card-3d glass-hover" style={{ padding: "clamp(28px,4vw,44px)", minHeight: 220, display: "flex", flexDirection: "column", justifyContent: "space-between", position: "relative", overflow: "hidden", background: "linear-gradient(135deg, rgba(109,40,217,0.15), rgba(91,33,182,0.08))", borderColor: "rgba(167,139,250,0.15)" }}>
              <div style={{ position: "absolute", width: 200, height: 200, borderRadius: "50%", background: "radial-gradient(circle, rgba(167,139,250,0.2) 0%, transparent 70%)", left: -50, top: -50, pointerEvents: "none" }} />
              <div style={{ position: "relative", zIndex: 1 }}>
                <p style={{ fontSize: 10, fontWeight: 700, color: "var(--accent3)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 12 }}>Эксклюзив</p>
                <h3 style={{ fontSize: "clamp(22px,3vw,32px)", fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1.1, marginBottom: 10 }} className="grad-text">Духи Parfum</h3>
                <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.6, maxWidth: 280 }}>Редкие ароматы высочайшей концентрации от нишевых домов</p>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 600, color: "var(--accent2)", marginTop: 20, position: "relative", zIndex: 1 }}>
                Смотреть <ArrowRight size={14} strokeWidth={2.5} />
              </div>
            </Link>
          </div>
        </div>
      </section>

      {/* ════════════════════ BRANDS MARQUEE ════════════════════ */}
      <section className="reveal-section" style={{ overflow: "hidden", paddingBlock: "clamp(32px,4vw,48px)" }}>
        <div className="section-divider" />
        <p style={{ textAlign: "center", fontSize: 10, fontWeight: 700, color: "var(--subtle)", letterSpacing: "0.14em", textTransform: "uppercase", padding: "28px 0 20px" }}>
          Ведущие мировые бренды
        </p>
        <div style={{ position: "relative" }}>
          <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to right, var(--bg), transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to left, var(--bg), transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link key={i} to={`/catalog?brand=${encodeURIComponent(brand)}`}
                style={{ display: "inline-flex", alignItems: "center", marginInline: 24, fontSize: 13, fontWeight: 600, color: "var(--subtle)", textDecoration: "none", whiteSpace: "nowrap", transition: "color 0.15s ease" }}
                onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
                onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
              >
                {brand}
                <span style={{ marginLeft: 24, color: "var(--border-md)" }}>·</span>
              </Link>
            ))}
          </div>
        </div>
        <div className="section-divider" style={{ marginTop: 28 }} />
      </section>

    </div>
  );
}
