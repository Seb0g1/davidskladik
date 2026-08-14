import { useRef, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ArrowRight, Truck, Shield, RefreshCw, Headphones,
  FlaskConical, Wind, Droplets, Droplet, Leaf, Sparkles,
  Flame, Gift, Heart, Gem, Star, ChevronRight,
} from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

/* ── Canvas particle system ─────────────────────────────────────────── */
function useParticles(canvasRef: React.RefObject<HTMLCanvasElement | null>) {
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf: number;
    const resize = () => {
      canvas.width = canvas.offsetWidth;
      canvas.height = canvas.offsetHeight;
    };
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(canvas);

    type P = { x: number; y: number; r: number; vx: number; vy: number; alpha: number; hue: number };
    const particles: P[] = Array.from({ length: 70 }, () => ({
      x: Math.random() * canvas.width,
      y: Math.random() * canvas.height,
      r: Math.random() * 2.5 + 0.5,
      vx: (Math.random() - 0.5) * 0.3,
      vy: -Math.random() * 0.4 - 0.1,
      alpha: Math.random() * 0.6 + 0.1,
      hue: Math.random() > 0.6 ? 280 : (Math.random() > 0.5 ? 300 : 260),
    }));

    const draw = () => {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      particles.forEach((p) => {
        p.x += p.vx;
        p.y += p.vy;
        if (p.y < -10) { p.y = canvas.height + 10; p.x = Math.random() * canvas.width; }
        if (p.x < -10) p.x = canvas.width + 10;
        if (p.x > canvas.width + 10) p.x = -10;

        const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, p.r * 3);
        g.addColorStop(0, `hsla(${p.hue}, 80%, 75%, ${p.alpha})`);
        g.addColorStop(1, `hsla(${p.hue}, 80%, 75%, 0)`);
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r * 3, 0, Math.PI * 2);
        ctx.fillStyle = g;
        ctx.fill();
      });
      raf = requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(raf); ro.disconnect(); };
  }, [canvasRef]);
}

/* ── 3D Tilt effect ─────────────────────────────────────────────────── */
function useTilt(ref: React.RefObject<HTMLElement | null>, intensity = 12) {
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onMove = (e: MouseEvent) => {
      const rect = el.getBoundingClientRect();
      const x = (e.clientX - rect.left) / rect.width - 0.5;
      const y = (e.clientY - rect.top) / rect.height - 0.5;
      el.style.transform = `perspective(800px) rotateY(${x * intensity}deg) rotateX(${-y * intensity}deg) scale3d(1.02,1.02,1.02)`;
    };
    const onLeave = () => { el.style.transform = ""; };
    el.addEventListener("mousemove", onMove);
    el.addEventListener("mouseleave", onLeave);
    return () => { el.removeEventListener("mousemove", onMove); el.removeEventListener("mouseleave", onLeave); };
  }, [ref, intensity]);
}

/* ── Scroll reveal ─────────────────────────────────────────────────── */
function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll<HTMLElement>(".reveal-section");
    const io = new IntersectionObserver(
      (entries) => entries.forEach((e) => { if (e.isIntersecting) { e.target.classList.add("is-visible"); } }),
      { threshold: 0.12, rootMargin: "0px 0px -40px 0px" }
    );
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

/* ── Animated counter ─────────────────────────────────────────────── */
function Counter({ value, suffix = "" }: { value: number; suffix?: string }) {
  const [v, setV] = useState(0);
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    const io = new IntersectionObserver(([e]) => {
      if (!e.isIntersecting) return;
      io.disconnect();
      let start = 0;
      const step = value / 60;
      const tick = () => {
        start = Math.min(start + step, value);
        setV(Math.round(start));
        if (start < value) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    });
    if (ref.current) io.observe(ref.current);
    return () => io.disconnect();
  }, [value]);
  return <span ref={ref}>{v.toLocaleString("ru-RU")}{suffix}</span>;
}

/* ── Perfume Bottle SVG 3D ─────────────────────────────────────────── */
function BottleSvg() {
  return (
    <svg viewBox="0 0 160 340" fill="none" xmlns="http://www.w3.org/2000/svg" style={{ width: "100%", height: "100%" }}>
      <defs>
        <linearGradient id="bottleBodyGrad" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor="#6d28d9" stopOpacity="0.9"/>
          <stop offset="30%" stopColor="#8b5cf6" stopOpacity="0.95"/>
          <stop offset="60%" stopColor="#a78bfa" stopOpacity="0.85"/>
          <stop offset="100%" stopColor="#5b21b6" stopOpacity="0.9"/>
        </linearGradient>
        <linearGradient id="bottleReflect" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor="white" stopOpacity="0"/>
          <stop offset="40%" stopColor="white" stopOpacity="0.3"/>
          <stop offset="100%" stopColor="white" stopOpacity="0"/>
        </linearGradient>
        <linearGradient id="capGrad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#d4a017"/>
          <stop offset="50%" stopColor="#f0c040"/>
          <stop offset="100%" stopColor="#b8860b"/>
        </linearGradient>
        <linearGradient id="neckGrad" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor="#5b21b6"/>
          <stop offset="50%" stopColor="#7c3aed"/>
          <stop offset="100%" stopColor="#4c1d95"/>
        </linearGradient>
        <linearGradient id="labelGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="rgba(255,255,255,0.18)"/>
          <stop offset="100%" stopColor="rgba(255,255,255,0.06)"/>
        </linearGradient>
        <filter id="glow">
          <feGaussianBlur stdDeviation="4" result="blur"/>
          <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <filter id="softGlow">
          <feGaussianBlur stdDeviation="8" result="blur"/>
          <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
      </defs>

      {/* Shadow */}
      <ellipse cx="80" cy="328" rx="44" ry="7" fill="rgba(124,58,237,0.25)" filter="url(#softGlow)"/>

      {/* Cap */}
      <rect x="52" y="30" width="56" height="44" rx="8" fill="url(#capGrad)" filter="url(#glow)"/>
      <rect x="60" y="34" width="16" height="36" rx="3" fill="rgba(255,255,255,0.25)"/>
      <rect x="52" y="68" width="56" height="6" rx="2" fill="rgba(255,255,255,0.15)"/>

      {/* Neck */}
      <rect x="62" y="72" width="36" height="24" rx="4" fill="url(#neckGrad)"/>
      <rect x="66" y="72" width="8" height="24" rx="2" fill="rgba(255,255,255,0.2)"/>

      {/* Shoulder taper */}
      <path d="M34 104 Q34 94 62 94 L98 94 Q126 94 126 104 L130 120 H30 Z" fill="url(#bottleBodyGrad)"/>

      {/* Main body */}
      <rect x="28" y="120" width="104" height="176" rx="16" fill="url(#bottleBodyGrad)"/>

      {/* Side highlight — left */}
      <rect x="28" y="120" width="22" height="176" rx="16" fill="url(#bottleReflect)" style={{ mixBlendMode: "screen" }}/>

      {/* Main glass reflection strip */}
      <rect x="50" y="128" width="10" height="140" rx="5" fill="rgba(255,255,255,0.22)"/>

      {/* Secondary reflection */}
      <rect x="64" y="135" width="4" height="80" rx="2" fill="rgba(255,255,255,0.12)"/>

      {/* Label background */}
      <rect x="38" y="152" width="84" height="96" rx="10" fill="url(#labelGrad)" stroke="rgba(255,255,255,0.2)" strokeWidth="0.5"/>

      {/* Label text: MV */}
      <text x="80" y="194" textAnchor="middle" fontFamily="'Playfair Display', Georgia, serif" fontSize="28" fontWeight="700" fill="white" opacity="0.95" letterSpacing="-1">MV</text>
      <text x="80" y="215" textAnchor="middle" fontFamily="Inter, sans-serif" fontSize="8" fontWeight="600" fill="white" opacity="0.7" letterSpacing="4">MAGIC VIBES</text>
      <text x="80" y="230" textAnchor="middle" fontFamily="Inter, sans-serif" fontSize="7" fill="rgba(255,255,255,0.5)" letterSpacing="1">PARIS</text>

      {/* Bottom roundness */}
      <rect x="28" y="278" width="104" height="18" rx="0" fill="url(#bottleBodyGrad)" opacity="0.6"/>
      <ellipse cx="80" cy="296" rx="52" ry="10" fill="#4c1d95" opacity="0.5"/>

      {/* Sparkle dots */}
      <circle cx="120" cy="145" r="2.5" fill="rgba(255,255,255,0.8)" filter="url(#glow)"/>
      <circle cx="35" cy="180" r="1.8" fill="rgba(255,255,255,0.6)"/>
      <circle cx="118" cy="220" r="1.5" fill="rgba(255,255,255,0.5)"/>
    </svg>
  );
}

/* ── Category icon map ─────────────────────────────────────────────── */
const CAT_ICONS: Record<string, React.ComponentType<{ size?: number; strokeWidth?: number; className?: string; style?: React.CSSProperties }>> = {
  testers: FlaskConical, parfum: Wind, edp: Droplets,
  edt: Droplet, edc: Leaf, deo: Sparkles,
  home: Flame, sets: Gift, body: Heart, parfumery: Gem,
};
const CAT_COLORS: Record<string, { bg: string; icon: string; glow: string }> = {
  testers: { bg: "linear-gradient(135deg,#fef3c7,#fde68a)", icon: "#d97706", glow: "rgba(217,119,6,0.2)" },
  parfum:  { bg: "linear-gradient(135deg,#f5f3ff,#ede9fe)", icon: "#7c3aed", glow: "rgba(124,58,237,0.2)" },
  edp:     { bg: "linear-gradient(135deg,#fdf2f8,#fce7f3)", icon: "#be185d", glow: "rgba(190,24,93,0.2)" },
  edt:     { bg: "linear-gradient(135deg,#ecfdf5,#d1fae5)", icon: "#059669", glow: "rgba(5,150,105,0.2)" },
  edc:     { bg: "linear-gradient(135deg,#f0fdf4,#bbf7d0)", icon: "#16a34a", glow: "rgba(22,163,74,0.2)" },
  deo:     { bg: "linear-gradient(135deg,#f0f9ff,#bae6fd)", icon: "#0284c7", glow: "rgba(2,132,199,0.2)" },
  home:    { bg: "linear-gradient(135deg,#fff7ed,#fed7aa)", icon: "#ea580c", glow: "rgba(234,88,12,0.2)" },
  sets:    { bg: "linear-gradient(135deg,#fdf2f8,#f0abfc)", icon: "#c026d3", glow: "rgba(192,38,211,0.2)" },
  body:    { bg: "linear-gradient(135deg,#fff1f2,#fecdd3)", icon: "#e11d48", glow: "rgba(225,29,72,0.2)" },
  parfumery:{ bg: "linear-gradient(135deg,#f5f3ff,#ede9fe)", icon: "#7c3aed", glow: "rgba(124,58,237,0.2)" },
};
const DEFAULT_CAT_COLOR = { bg: "linear-gradient(135deg,#f5f3ff,#ede9fe)", icon: "#7c3aed", glow: "rgba(124,58,237,0.2)" };

function CategoryCard({ cat, index }: { cat: AutoCategory; index: number }) {
  const Icon = CAT_ICONS[cat.slug] || Gem;
  const color = CAT_COLORS[cat.slug] || DEFAULT_CAT_COLOR;
  const ref = useRef<HTMLAnchorElement>(null);
  useTilt(ref as React.RefObject<HTMLElement | null>, 8);
  return (
    <Link
      ref={ref}
      to={`/catalog?category=${cat.slug}`}
      className="cat-card group flex flex-col gap-4 p-5 rounded-3xl relative overflow-hidden"
      style={{
        background: "#fff",
        border: "1px solid rgba(0,0,0,0.07)",
        boxShadow: "0 2px 12px rgba(0,0,0,0.05)",
        animationDelay: `${index * 0.05}s`,
        transition: "transform 0.25s cubic-bezier(0.16,1,0.3,1), box-shadow 0.25s ease",
      }}
    >
      {/* Glow bg */}
      <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 rounded-3xl"
        style={{ background: color.bg, zIndex: 0 }} />
      <div className="relative z-10 flex flex-col gap-4">
        <div
          className="w-11 h-11 rounded-2xl flex items-center justify-center flex-shrink-0 transition-all duration-300"
          style={{ background: color.bg, boxShadow: `0 4px 16px ${color.glow}` }}
        >
          <Icon size={19} strokeWidth={1.5} style={{ color: color.icon }} />
        </div>
        <div className="flex-1">
          <div className="font-semibold text-[13px] text-gray-900 leading-snug">{cat.label}</div>
          <div className="text-[11px] text-gray-400 mt-1">{cat.count.toLocaleString("ru-RU")} товаров</div>
        </div>
        <ArrowRight
          size={14}
          className="opacity-0 group-hover:opacity-100 transition-all duration-200 -translate-x-1 group-hover:translate-x-0"
          style={{ color: color.icon }}
          strokeWidth={2}
        />
      </div>
    </Link>
  );
}

const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermes", "Byredo", "Jo Malone",
  "Creed", "Guerlain", "Givenchy", "Prada", "Valentino", "Burberry",
  "Versace", "Hugo Boss", "Montale", "Kilian", "Dolce Gabbana", "Lancome",
  "Giorgio Armani", "Yves Saint Laurent", "Moschino", "Bvlgari",
];

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-3xl overflow-hidden flex-shrink-0" style={{ width: 200, border: "1px solid #f0f0f5" }}>
      <div className="skeleton" style={{ aspectRatio: "1/1" }} />
      <div className="p-4 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3 skeleton rounded" />
        <div className="h-3 skeleton rounded w-3/4" />
        <div className="h-5 skeleton rounded w-1/2 mt-3" />
      </div>
    </div>
  );
}
function CatSkeleton() {
  return <div className="rounded-3xl p-5 flex flex-col gap-4" style={{ border: "1px solid #f0f0f5", height: 140, background: "#fafafa" }}><div className="w-11 h-11 rounded-2xl skeleton" /><div className="space-y-2"><div className="h-3.5 skeleton rounded w-3/4" /><div className="h-2.5 skeleton rounded w-1/2" /></div></div>;
}

export default function HomePage() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const trackRef = useRef<HTMLDivElement>(null);

  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 20, sort: "name" }),
  });
  const { data: autoCategories, isLoading: catsLoading } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  useParticles(canvasRef);
  useReveal();

  const scroll = (dir: -1 | 1) => trackRef.current?.scrollBy({ left: dir * 640, behavior: "smooth" });
  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen bg-white">

      {/* ═══════════════════════════════════════════════
          HERO — dark luxury 3D
      ═══════════════════════════════════════════════ */}
      <section className="relative overflow-hidden aurora-bg" style={{ minHeight: "100vh" }}>
        <canvas ref={canvasRef} id="hero-canvas" style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }} />

        {/* Glowing orbs */}
        <div className="absolute orb-1 pointer-events-none" style={{ top: "10%", left: "5%", width: 600, height: 600, borderRadius: "50%", background: "radial-gradient(circle, rgba(124,58,237,0.15) 0%, transparent 65%)", filter: "blur(40px)" }} />
        <div className="absolute orb-2 pointer-events-none" style={{ bottom: "5%", right: "10%", width: 500, height: 500, borderRadius: "50%", background: "radial-gradient(circle, rgba(168,85,247,0.12) 0%, transparent 65%)", filter: "blur(40px)" }} />
        <div className="absolute orb-3 pointer-events-none" style={{ top: "40%", right: "20%", width: 300, height: 300, borderRadius: "50%", background: "radial-gradient(circle, rgba(236,72,153,0.08) 0%, transparent 65%)", filter: "blur(30px)" }} />

        <div className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 flex flex-col lg:flex-row items-center justify-between gap-12" style={{ minHeight: "100vh", paddingTop: "120px", paddingBottom: "80px" }}>

          {/* ── Text column ── */}
          <div className="flex-1 text-center lg:text-left">
            {/* Label */}
            <div
              className="inline-flex items-center gap-2 text-[11px] font-bold px-4 py-2 rounded-full mb-8 tracking-[0.15em] uppercase anim-slide-up"
              style={{ background: "rgba(124,58,237,0.2)", color: "#c4b5fd", border: "1px solid rgba(168,85,247,0.3)", backdropFilter: "blur(10px)" }}
            >
              <Star size={10} fill="currentColor" /> Оригинальная парфюмерия • 22 000+ ароматов
            </div>

            {/* Headline */}
            <h1 className="font-bold leading-none mb-7 anim-slide-up" style={{ animationDelay: "0.1s", fontFamily: "'Playfair Display', Georgia, serif" }}>
              <span className="block text-white" style={{ fontSize: "clamp(48px, 7vw, 88px)", letterSpacing: "-0.02em" }}>
                Мир
              </span>
              <span
                className="block neon-text"
                style={{
                  fontSize: "clamp(48px, 7vw, 88px)",
                  letterSpacing: "-0.02em",
                  background: "linear-gradient(135deg, #c4b5fd 0%, #a78bfa 40%, #f0abfc 80%, #c4b5fd 100%)",
                  backgroundSize: "200% 100%",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                  backgroundClip: "text",
                  animation: "auroraShift 5s ease infinite",
                }}
              >
                Ароматов
              </span>
            </h1>

            <p className="text-[17px] text-purple-200 max-w-[460px] mx-auto lg:mx-0 leading-relaxed mb-10 anim-slide-up" style={{ animationDelay: "0.2s", opacity: 0.85 }}>
              Оригинальная продукция от мировых парфюмерных домов. Быстрая доставка по всей России через Ozon.
            </p>

            {/* CTA buttons */}
            <div className="flex flex-col sm:flex-row gap-4 justify-center lg:justify-start mb-14 anim-slide-up" style={{ animationDelay: "0.3s" }}>
              <Link
                to="/catalog"
                className="btn-shimmer inline-flex items-center justify-center gap-2.5 font-semibold px-9 py-4.5 rounded-2xl text-[15px] text-white transition-all duration-200 hover:-translate-y-1 active:scale-[0.98]"
                style={{
                  background: "linear-gradient(135deg, #7c3aed, #a855f7, #9333ea)",
                  backgroundSize: "200% 200%",
                  animation: "auroraShift 3s ease infinite",
                  boxShadow: "0 12px 36px rgba(124,58,237,0.5), 0 0 0 1px rgba(168,85,247,0.3)",
                  padding: "16px 36px",
                }}
              >
                Смотреть каталог <ArrowRight size={16} strokeWidth={2.5} />
              </Link>
              <Link
                to="/catalog?sort=price_desc"
                className="inline-flex items-center justify-center gap-2.5 font-semibold rounded-2xl text-[15px] text-white transition-all duration-200 hover:-translate-y-1"
                style={{
                  padding: "16px 36px",
                  background: "rgba(255,255,255,0.08)",
                  backdropFilter: "blur(12px)",
                  border: "1px solid rgba(255,255,255,0.2)",
                  boxShadow: "inset 0 1px 0 rgba(255,255,255,0.1)",
                }}
              >
                Топ ароматов <ChevronRight size={16} strokeWidth={2} />
              </Link>
            </div>

            {/* Stats */}
            <div className="flex items-center justify-center lg:justify-start gap-0 anim-slide-up" style={{ animationDelay: "0.4s" }}>
              {[
                { value: 22000, suffix: "+", label: "ароматов" },
                { value: 100, suffix: "%", label: "оригинал" },
                { value: 5, suffix: " дней", label: "доставка" },
              ].map(({ value, suffix, label }, i) => (
                <div key={label} className="flex items-center">
                  {i > 0 && <div className="w-px h-10 mx-7" style={{ background: "rgba(255,255,255,0.15)" }} />}
                  <div>
                    <div className="text-[22px] font-bold text-white" style={{ letterSpacing: "-0.02em" }}>
                      <Counter value={value} suffix={suffix} />
                    </div>
                    <div className="text-[11px] mt-0.5" style={{ color: "rgba(196,181,253,0.7)" }}>{label}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* ── 3D Bottle ── */}
          <div className="hidden lg:flex flex-none items-center justify-center anim-fade-in" style={{ animationDelay: "0.2s", width: 340, height: 440 }}>
            <div className="relative w-full h-full">
              {/* Glow halo */}
              <div className="absolute inset-0 rounded-full" style={{ background: "radial-gradient(circle, rgba(124,58,237,0.35) 0%, transparent 70%)", filter: "blur(30px)" }} />
              {/* Orbiting ring */}
              <div className="absolute" style={{
                top: "50%", left: "50%",
                width: 280, height: 280,
                marginTop: -140, marginLeft: -140,
                border: "1px solid rgba(168,85,247,0.25)",
                borderRadius: "50%",
                animation: "bottleFloat 20s linear infinite",
                boxShadow: "0 0 30px rgba(124,58,237,0.15), inset 0 0 30px rgba(124,58,237,0.05)",
              }} />
              {/* Bottle */}
              <div className="bottle-3d absolute" style={{ top: "5%", left: "50%", transform: "translateX(-50%)", width: 160, height: 340 }}>
                <BottleSvg />
              </div>
              {/* Floating badges */}
              <div className="badge-float glass-card absolute rounded-2xl px-4 py-2.5 flex items-center gap-2" style={{ top: "12%", right: "2%", animationDelay: "0s" }}>
                <Star size={12} fill="#f0c040" stroke="none" />
                <span style={{ fontSize: 12, fontWeight: 700, color: "#fff" }}>100% Оригинал</span>
              </div>
              <div className="badge-float glass-card absolute rounded-2xl px-4 py-2.5 flex items-center gap-2" style={{ bottom: "22%", left: "0%", animationDelay: "1.5s" }}>
                <Truck size={12} style={{ color: "#a78bfa" }} />
                <span style={{ fontSize: 12, fontWeight: 700, color: "#fff" }}>Доставка Ozon</span>
              </div>
            </div>
          </div>
        </div>

        {/* Scroll indicator */}
        <div className="absolute bottom-8 left-1/2 -translate-x-1/2 flex flex-col items-center gap-2 opacity-50">
          <span style={{ fontSize: 10, letterSpacing: "0.15em", color: "#c4b5fd", textTransform: "uppercase" }}>Скролл</span>
          <div style={{ width: 1, height: 40, background: "linear-gradient(to bottom, #a78bfa, transparent)" }} />
        </div>

        {/* Bottom fade */}
        <div className="absolute bottom-0 left-0 right-0 h-32 pointer-events-none" style={{ background: "linear-gradient(to bottom, transparent, #fff)" }} />
      </section>

      {/* ═══════════════════════════════════════════════
          PERKS STRIP
      ═══════════════════════════════════════════════ */}
      <section className="reveal-section relative bg-white" style={{ borderBottom: "1px solid #f0f0f5" }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="grid grid-cols-2 lg:grid-cols-4 divide-x divide-gray-100">
            {[
              { icon: Truck,       title: "Доставка Ozon",   text: "По всей России, 1–5 дней" },
              { icon: Shield,      title: "100% оригинал",   text: "Гарантия подлинности" },
              { icon: RefreshCw,   title: "Возврат 14 дней", text: "Без вопросов" },
              { icon: Headphones,  title: "Поддержка",        text: "Всегда на связи" },
            ].map(({ icon: Icon, title, text }) => (
              <div key={title} className="flex items-center gap-3.5 py-5 px-4 lg:px-8 group">
                <div
                  className="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0 transition-all duration-200 group-hover:scale-110"
                  style={{ background: "linear-gradient(135deg,#f5f3ff,#ede9fe)" }}
                >
                  <Icon size={16} strokeWidth={1.75} style={{ color: "#7c3aed" }} />
                </div>
                <div>
                  <div className="text-[12px] font-semibold text-gray-900">{title}</div>
                  <div className="text-[10px] text-gray-400 mt-0.5">{text}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════
          CATEGORIES — 3D tilt cards
      ═══════════════════════════════════════════════ */}
      <section className="reveal-section py-20 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-12">
            <div>
              <p className="text-[11px] font-bold uppercase tracking-[0.15em] mb-3" style={{ color: "#7c3aed" }}>Коллекция</p>
              <h2 className="text-4xl md:text-5xl font-bold tracking-tight text-gray-900" style={{ fontFamily: "'Playfair Display', Georgia, serif" }}>
                Категории <span className="headline-accent">ароматов</span>
              </h2>
            </div>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-semibold transition-all group"
              style={{ color: "#7c3aed" }}
            >
              Все товары <ArrowRight size={14} strokeWidth={2.5} className="group-hover:translate-x-1 transition-transform" />
            </Link>
          </div>

          {catsLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
              {Array.from({ length: 10 }).map((_, i) => <CatSkeleton key={i} />)}
            </div>
          ) : autoCategories && autoCategories.length > 0 ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
              {autoCategories.map((cat, i) => <CategoryCard key={cat.slug} cat={cat} index={i} />)}
            </div>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
              {[
                { slug: "edp", label: "Парфюмерная вода", count: 0 },
                { slug: "edt", label: "Туалетная вода",   count: 0 },
                { slug: "parfum", label: "Духи",          count: 0 },
                { slug: "sets", label: "Наборы",          count: 0 },
              ].map((cat, i) => <CategoryCard key={cat.slug} cat={cat} index={i} />)}
            </div>
          )}
        </div>
      </section>

      {/* ═══════════════════════════════════════════════
          EDITORIAL PROMO BANNERS
      ═══════════════════════════════════════════════ */}
      <section className="reveal-section pb-20 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-5">

            {/* Gift card — dark luxury */}
            <Link
              to="/catalog?category=sets"
              className="group relative rounded-3xl overflow-hidden flex flex-col justify-between transition-all duration-400 hover:-translate-y-2"
              style={{
                background: "linear-gradient(135deg, #09090b 0%, #1a0533 60%, #120729 100%)",
                minHeight: 260,
                padding: "clamp(32px, 5vw, 52px)",
                boxShadow: "0 4px 24px rgba(0,0,0,0.15)",
              }}
              onMouseEnter={e => { (e.currentTarget as HTMLElement).style.boxShadow = "0 24px 64px rgba(124,58,237,0.4)"; }}
              onMouseLeave={e => { (e.currentTarget as HTMLElement).style.boxShadow = "0 4px 24px rgba(0,0,0,0.15)"; }}
            >
              <div className="absolute inset-0 pointer-events-none">
                <div className="absolute top-0 right-0 w-56 h-56 rounded-full" style={{ background: "radial-gradient(circle, rgba(168,85,247,0.3) 0%, transparent 70%)", transform: "translate(30%, -30%)" }} />
                <div className="absolute bottom-0 left-0 w-40 h-40 rounded-full" style={{ background: "radial-gradient(circle, rgba(236,72,153,0.2) 0%, transparent 70%)", transform: "translate(-30%, 30%)" }} />
              </div>
              <div className="relative z-10">
                <div className="w-13 h-13 rounded-2xl flex items-center justify-center mb-6" style={{ background: "rgba(168,85,247,0.2)", border: "1px solid rgba(168,85,247,0.3)", width: 52, height: 52 }}>
                  <Gift size={24} strokeWidth={1.5} style={{ color: "#c4b5fd" }} />
                </div>
                <h3 className="text-3xl font-bold text-white mb-2 leading-tight" style={{ fontFamily: "'Playfair Display', Georgia, serif" }}>
                  Подарите аромат
                </h3>
                <p className="text-[13px] leading-relaxed" style={{ color: "rgba(196,181,253,0.75)", maxWidth: 280 }}>
                  Подарочные наборы для тех, кто хочет удивить близких
                </p>
              </div>
              <div className="flex items-center gap-2 text-[13px] font-bold text-purple-300 mt-6 group-hover:gap-4 transition-all duration-200 relative z-10">
                Выбрать набор <ArrowRight size={15} strokeWidth={2.5} />
              </div>
            </Link>

            {/* Exclusive — aurora light */}
            <Link
              to="/catalog?category=parfum"
              className="group relative rounded-3xl overflow-hidden flex flex-col justify-between transition-all duration-400 hover:-translate-y-2"
              style={{
                background: "linear-gradient(135deg, #f5f3ff 0%, #ede9fe 60%, #faf5ff 100%)",
                minHeight: 260,
                padding: "clamp(32px, 5vw, 52px)",
                border: "1px solid #ddd6fe",
                boxShadow: "0 4px 24px rgba(0,0,0,0.06)",
              }}
              onMouseEnter={e => { (e.currentTarget as HTMLElement).style.boxShadow = "0 24px 64px rgba(124,58,237,0.2)"; }}
              onMouseLeave={e => { (e.currentTarget as HTMLElement).style.boxShadow = "0 4px 24px rgba(0,0,0,0.06)"; }}
            >
              <div className="absolute top-0 right-0 w-52 h-52 rounded-full pointer-events-none" style={{ background: "radial-gradient(circle, rgba(196,181,253,0.5) 0%, transparent 65%)", transform: "translate(25%, -25%)" }} />
              <div className="relative z-10">
                <div className="w-13 h-13 rounded-2xl flex items-center justify-center mb-6" style={{ background: "rgba(196,181,253,0.3)", width: 52, height: 52 }}>
                  <Wind size={24} strokeWidth={1.5} style={{ color: "#6d28d9" }} />
                </div>
                <h3 className="text-3xl font-bold text-violet-950 mb-2 leading-tight" style={{ fontFamily: "'Playfair Display', Georgia, serif" }}>
                  Эксклюзивные духи
                </h3>
                <p className="text-[13px] text-violet-700 leading-relaxed" style={{ maxWidth: 280, opacity: 0.8 }}>
                  Редкие ароматы и шедевры от мировых парфюмерных домов
                </p>
              </div>
              <div className="flex items-center gap-2 text-[13px] font-bold text-violet-700 mt-6 group-hover:gap-4 transition-all duration-200 relative z-10">
                Смотреть духи <ArrowRight size={15} strokeWidth={2.5} />
              </div>
            </Link>
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════
          BESTSELLERS CAROUSEL
      ═══════════════════════════════════════════════ */}
      <section className="reveal-section py-20" style={{ background: "linear-gradient(180deg, #fafafa 0%, #f8f6ff 100%)" }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-12">
            <div>
              <p className="text-[11px] font-bold uppercase tracking-[0.15em] mb-3" style={{ color: "#7c3aed" }}>Популярное</p>
              <h2 className="text-4xl md:text-5xl font-bold tracking-tight text-gray-900" style={{ fontFamily: "'Playfair Display', Georgia, serif" }}>
                Хиты <span className="headline-accent">продаж</span>
              </h2>
            </div>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-semibold transition-all group"
              style={{ color: "#7c3aed" }}
            >
              Все товары <ArrowRight size={14} strokeWidth={2.5} className="group-hover:translate-x-1 transition-transform" />
            </Link>
          </div>

          {catalogLoading ? (
            <div className="flex gap-4 overflow-hidden">
              {Array.from({ length: 6 }).map((_, i) => <ProductSkeleton key={i} />)}
            </div>
          ) : catalog?.products.length ? (
            <>
              <div className="carousel-wrapper relative -mx-4 sm:-mx-6">
                <button onClick={() => scroll(-1)} className="carousel-btn left !left-1 sm:!left-0" aria-label="Назад">‹</button>
                <div ref={trackRef} className="carousel-track px-4 sm:px-6">
                  {catalog.products.map((p) => (
                    <div key={p.offerId} className="carousel-item" style={{ width: 200 }}>
                      <ProductCard product={p} />
                    </div>
                  ))}
                </div>
                <button onClick={() => scroll(1)} className="carousel-btn right !right-1 sm:!right-0" aria-label="Вперёд">›</button>
              </div>
              <div className="text-center mt-10">
                <Link
                  to="/catalog"
                  className="inline-flex items-center gap-2.5 font-semibold px-8 py-4 rounded-2xl transition-all duration-200 text-[13px] hover:-translate-y-0.5 active:scale-[0.98]"
                  style={{
                    background: "#fff",
                    color: "#1d1d1f",
                    border: "1.5px solid #e5e7eb",
                    boxShadow: "0 2px 12px rgba(0,0,0,0.06)",
                  }}
                  onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = "#7c3aed"; (e.currentTarget as HTMLElement).style.color = "#7c3aed"; }}
                  onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "#e5e7eb"; (e.currentTarget as HTMLElement).style.color = "#1d1d1f"; }}
                >
                  Показать все товары <ArrowRight size={15} strokeWidth={2} />
                </Link>
              </div>
            </>
          ) : (
            <div className="text-center py-16 text-gray-400">Загрузка товаров...</div>
          )}
        </div>
      </section>

      {/* ═══════════════════════════════════════════════
          TRUST BANNER — dark
      ═══════════════════════════════════════════════ */}
      <section className="reveal-section relative overflow-hidden aurora-bg py-20">
        <div className="absolute inset-0 pointer-events-none">
          <div className="absolute orb-1" style={{ top: "20%", left: "10%", width: 400, height: 400, borderRadius: "50%", background: "radial-gradient(circle, rgba(124,58,237,0.15) 0%, transparent 65%)", filter: "blur(40px)" }} />
          <div className="absolute orb-2" style={{ bottom: "10%", right: "5%", width: 300, height: 300, borderRadius: "50%", background: "radial-gradient(circle, rgba(236,72,153,0.1) 0%, transparent 65%)", filter: "blur(40px)" }} />
        </div>
        <div className="relative z-10 max-w-4xl mx-auto px-4 sm:px-6 text-center">
          <p className="text-[11px] font-bold uppercase tracking-[0.2em] mb-4" style={{ color: "#c4b5fd" }}>Почему Magic Vibes</p>
          <h2 className="text-4xl md:text-5xl font-bold text-white mb-6" style={{ fontFamily: "'Playfair Display', Georgia, serif", letterSpacing: "-0.02em" }}>
            Парфюмерия, которой <span style={{ background: "linear-gradient(135deg,#c4b5fd,#f0abfc)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>доверяют</span>
          </h2>
          <p className="text-[16px] leading-relaxed mb-12" style={{ color: "rgba(196,181,253,0.8)", maxWidth: 520, margin: "0 auto 3rem" }}>
            Работаем напрямую с официальными поставщиками. Каждый флакон проходит проверку подлинности.
          </p>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            {[
              { v: "22 000+", l: "ароматов" },
              { v: "5★", l: "рейтинг на Ozon" },
              { v: "1–5", l: "дней доставка" },
              { v: "100%", l: "оригинал" },
            ].map(({ v, l }) => (
              <div key={l} className="glass-card rounded-2xl py-6 px-4 text-center">
                <div className="text-2xl font-bold text-white mb-1" style={{ fontFamily: "'Playfair Display', Georgia, serif" }}>{v}</div>
                <div className="text-[11px]" style={{ color: "rgba(196,181,253,0.7)" }}>{l}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ═══════════════════════════════════════════════
          BRAND MARQUEE
      ═══════════════════════════════════════════════ */}
      <section className="bg-white border-t border-gray-100 overflow-hidden py-12">
        <p className="text-center text-[10px] font-bold uppercase tracking-[0.2em] text-gray-400 mb-8">Ведущие мировые бренды</p>
        <div className="relative">
          <div className="absolute left-0 top-0 bottom-0 w-24 bg-gradient-to-r from-white to-transparent z-10 pointer-events-none" />
          <div className="absolute right-0 top-0 bottom-0 w-24 bg-gradient-to-l from-white to-transparent z-10 pointer-events-none" />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link
                key={i}
                to={`/catalog?brand=${encodeURIComponent(brand)}`}
                className="inline-flex items-center mx-8 text-[13px] font-semibold text-gray-400 hover:text-violet-600 transition-colors whitespace-nowrap"
              >
                {brand}
                <span className="ml-8 text-gray-200">·</span>
              </Link>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
