import { useRef, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, ArrowUpRight } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

/* ── Live clock (МСК) ───────────────────────────────────────────────── */
function useClock() {
  const fmt = () =>
    new Date().toLocaleTimeString("ru-RU", {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "Europe/Moscow",
    });
  const [time, setTime] = useState(fmt);
  useEffect(() => {
    const t = setInterval(() => setTime(fmt()), 30_000);
    return () => clearInterval(t);
  }, []);
  return time;
}

/* ── Animated counter ────────────────────────────────────────────────── */
function Counter({ value, suffix = "" }: { value: number; suffix?: string }) {
  const [v, setV] = useState(0);
  const ref = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    const io = new IntersectionObserver(([e]) => {
      if (!e.isIntersecting) return;
      io.disconnect();
      let cur = 0;
      const step = value / 50;
      const tick = () => {
        cur = Math.min(cur + step, value);
        setV(Math.round(cur));
        if (cur < value) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    });
    if (ref.current) io.observe(ref.current);
    return () => io.disconnect();
  }, [value]);
  return (
    <span ref={ref}>
      {v.toLocaleString("ru-RU")}
      {suffix}
    </span>
  );
}

/* ── Scroll reveal ───────────────────────────────────────────────────── */
function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll<HTMLElement>(".reveal-section");
    const io = new IntersectionObserver(
      (entries) =>
        entries.forEach((e) => {
          if (e.isIntersecting) e.target.classList.add("is-visible");
        }),
      { threshold: 0.06, rootMargin: "0px 0px -40px 0px" }
    );
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

/* ── Mono style helper ───────────────────────────────────────────────── */
const MONO: React.CSSProperties = {
  fontFamily: "'JetBrains Mono', 'SF Mono', monospace",
  letterSpacing: "0.07em",
};

/* ── Category row (table-style, inspired by haoqi.design project list) ── */
function CategoryRow({ cat, index }: { cat: AutoCategory; index: number }) {
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      className="cat-row flex items-center justify-between py-[18px] border-b group"
      style={{ borderColor: "#e8e8e8" }}
    >
      <div className="flex items-center gap-5 sm:gap-8">
        <span style={{ ...MONO, fontSize: 10, color: "#d0d0d0", width: 26, flexShrink: 0 }}>
          {String(index + 1).padStart(2, "0")}
        </span>
        <span className="text-[15px] sm:text-[17px] font-medium text-gray-900 group-hover:text-violet-700 transition-colors duration-200 leading-snug">
          {cat.label}
        </span>
      </div>
      <div className="flex items-center gap-3 sm:gap-5 flex-shrink-0">
        <span style={{ ...MONO, fontSize: 11, color: "#b0b0b0" }}>
          {cat.count.toLocaleString("ru-RU")}
        </span>
        <ArrowRight
          size={14}
          strokeWidth={2}
          className="text-gray-200 group-hover:text-violet-500 group-hover:translate-x-0.5 transition-all duration-200 flex-shrink-0"
        />
      </div>
    </Link>
  );
}

function CatRowSkeleton() {
  return (
    <div
      className="flex items-center justify-between py-[18px] border-b"
      style={{ borderColor: "#e8e8e8" }}
    >
      <div className="flex items-center gap-8">
        <div className="w-5 h-3 skeleton rounded" />
        <div className="w-44 h-4 skeleton rounded" />
      </div>
      <div className="w-14 h-3 skeleton rounded" />
    </div>
  );
}

function ProductSkeleton() {
  return (
    <div
      className="bg-white rounded-2xl overflow-hidden flex-shrink-0"
      style={{ width: 200, border: "1px solid #efefef" }}
    >
      <div className="skeleton" style={{ aspectRatio: "1/1" }} />
      <div className="p-4 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3 skeleton rounded" />
        <div className="h-5 skeleton rounded w-1/2 mt-3" />
      </div>
    </div>
  );
}

const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermes", "Byredo", "Jo Malone",
  "Creed", "Guerlain", "Givenchy", "Prada", "Valentino", "Burberry",
  "Versace", "Hugo Boss", "Montale", "Kilian", "Dolce Gabbana", "Lancome",
  "Giorgio Armani", "Yves Saint Laurent", "Moschino", "Bvlgari",
];

const CONTENT_PX = "clamp(20px, 4.5vw, 72px)";
const MAX_W = 1440;

export default function HomePage() {
  const trackRef = useRef<HTMLDivElement>(null);
  const time = useClock();

  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 20, sort: "name" }),
  });
  const { data: autoCategories, isLoading: catsLoading } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  useReveal();

  const scroll = (dir: -1 | 1) =>
    trackRef.current?.scrollBy({ left: dir * 640, behavior: "smooth" });
  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen" style={{ background: "#fafaf9", color: "#0a0a0a" }}>

      {/* ════════════════════════════════════════════════════════
          STATUS BAR — editorial top ticker
      ════════════════════════════════════════════════════════ */}
      <div
        className="hidden sm:flex items-center justify-between border-b h-8 px-6"
        style={{ borderColor: "#e8e8e8", background: "#fafaf9" }}
      >
        <span style={{ ...MONO, fontSize: 10, color: "#b0b0b0" }}>
          MV — MAGIC VIBES · ОРИГИНАЛЬНАЯ ПАРФЮМЕРИЯ · МОСКВА, РОССИЯ
        </span>
        <span style={{ ...MONO, fontSize: 10, color: "#b0b0b0" }}>
          {time} МСК
        </span>
      </div>

      {/* ════════════════════════════════════════════════════════
          HERO — massive editorial typography
      ════════════════════════════════════════════════════════ */}
      <section
        style={{
          minHeight: "91vh",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          padding: `clamp(36px, 5vw, 72px) ${CONTENT_PX}`,
          maxWidth: MAX_W,
          margin: "0 auto",
          width: "100%",
        }}
      >
        {/* Top micro-row */}
        <div className="flex items-center justify-between">
          <span style={{ ...MONO, fontSize: 11, color: "#c0c0c0", textTransform: "uppercase" }}>
            Аромат · Оригинал · Доставка
          </span>
          <span style={{ ...MONO, fontSize: 11, color: "#c0c0c0" }}>
            22 000+
          </span>
        </div>

        {/* Main display headline */}
        <div style={{ paddingBlock: "clamp(32px, 5vw, 64px) 0" }}>
          <h1
            style={{
              fontFamily: "'Space Grotesk', 'Inter', sans-serif",
              fontSize: "clamp(72px, 13vw, 210px)",
              fontWeight: 700,
              lineHeight: 0.875,
              letterSpacing: "-0.035em",
              color: "#0a0a0a",
              userSelect: "none",
            }}
          >
            МИР<br />АРОМАТА
          </h1>
        </div>

        {/* Bottom row: divider + tagline + CTAs */}
        <div
          className="border-t pt-6 sm:pt-8 flex flex-col sm:flex-row sm:items-end justify-between gap-6 sm:gap-10"
          style={{ borderColor: "#e8e8e8" }}
        >
          <p
            className="leading-relaxed max-w-[400px]"
            style={{ fontSize: "clamp(14px, 1.6vw, 17px)", color: "#6b6b6b", fontWeight: 400 }}
          >
            Оригинальная продукция от мировых парфюмерных домов.
            Быстрая доставка по всей России через Ozon.
          </p>
          <div className="flex items-center gap-8 flex-shrink-0">
            <Link
              to="/catalog"
              className="hero-cta flex items-center gap-2 font-semibold text-[15px] text-gray-900 hover:text-violet-700 transition-colors group"
            >
              Каталог{" "}
              <ArrowUpRight
                size={16}
                strokeWidth={2}
                className="group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform"
              />
            </Link>
            <Link
              to="/catalog?sort=price_desc"
              className="hero-cta flex items-center gap-2 text-[15px] text-gray-400 hover:text-gray-900 transition-colors group"
            >
              Топ ароматов{" "}
              <ArrowRight
                size={15}
                strokeWidth={2}
                className="group-hover:translate-x-0.5 transition-transform"
              />
            </Link>
          </div>
        </div>
      </section>

      {/* ════════════════════════════════════════════════════════
          STATS BAND — dark editorial
      ════════════════════════════════════════════════════════ */}
      <div
        className="border-t border-b"
        style={{ borderColor: "#e8e8e8", background: "#0a0a0a" }}
      >
        <div style={{ maxWidth: MAX_W, margin: "0 auto" }}>
          <div className="grid grid-cols-2 sm:grid-cols-4">
            {[
              { v: 22000, suffix: "+", label: "АРОМАТОВ" },
              { v: 100, suffix: "%", label: "ОРИГИНАЛ" },
              { v: 5, suffix: " дн.", label: "ДОСТАВКА" },
              { v: 5, suffix: "★", label: "РЕЙТИНГ OZON" },
            ].map(({ v, suffix, label }, i) => (
              <div
                key={label}
                className="py-9 flex flex-col gap-2"
                style={{
                  paddingInline: CONTENT_PX,
                  borderRight: i < 3 ? "1px solid #1c1c1c" : undefined,
                }}
              >
                <div
                  style={{
                    fontFamily: "'Space Grotesk', sans-serif",
                    fontSize: "clamp(30px, 3.2vw, 52px)",
                    fontWeight: 700,
                    color: "#fafaf9",
                    lineHeight: 1,
                    letterSpacing: "-0.025em",
                  }}
                >
                  <Counter value={v} suffix={suffix} />
                </div>
                <div style={{ ...MONO, fontSize: 10, color: "#555", letterSpacing: "0.1em" }}>
                  {label}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ════════════════════════════════════════════════════════
          CATEGORIES — haoqi-style table list
      ════════════════════════════════════════════════════════ */}
      <section
        className="reveal-section"
        style={{
          maxWidth: MAX_W,
          margin: "0 auto",
          paddingInline: CONTENT_PX,
        }}
      >
        {/* Section header */}
        <div
          className="flex items-center justify-between py-8 border-b"
          style={{ borderColor: "#e8e8e8" }}
        >
          <span style={{ ...MONO, fontSize: 11, color: "#7a7a7a", textTransform: "uppercase", letterSpacing: "0.1em" }}>
            Коллекция
          </span>
          <Link
            to="/catalog"
            className="flex items-center gap-1.5 text-[13px] font-medium text-gray-400 hover:text-violet-600 transition-colors group"
          >
            Все товары{" "}
            <ArrowRight
              size={13}
              strokeWidth={2}
              className="group-hover:translate-x-0.5 transition-transform"
            />
          </Link>
        </div>

        {/* Rows */}
        {catsLoading
          ? Array.from({ length: 8 }).map((_, i) => <CatRowSkeleton key={i} />)
          : (autoCategories && autoCategories.length > 0
              ? autoCategories
              : [
                  { slug: "edp", label: "Парфюмерная вода (EDP)", count: 0 },
                  { slug: "edt", label: "Туалетная вода (EDT)", count: 0 },
                  { slug: "parfum", label: "Духи", count: 0 },
                  { slug: "sets", label: "Наборы", count: 0 },
                ]
            ).map((cat, i) => (
              <CategoryRow key={cat.slug} cat={cat} index={i} />
            ))}
      </section>

      {/* ════════════════════════════════════════════════════════
          BESTSELLERS CAROUSEL
      ════════════════════════════════════════════════════════ */}
      <section
        className="reveal-section"
        style={{
          maxWidth: MAX_W,
          margin: "0 auto",
          paddingInline: CONTENT_PX,
          paddingBlock: "clamp(60px, 7vw, 100px)",
        }}
      >
        {/* Header */}
        <div
          className="flex items-baseline justify-between mb-10 pb-6 border-b"
          style={{ borderColor: "#e8e8e8" }}
        >
          <div>
            <span
              className="block mb-2"
              style={{ ...MONO, fontSize: 10, color: "#b0b0b0", textTransform: "uppercase", letterSpacing: "0.1em" }}
            >
              Популярное
            </span>
            <h2
              style={{
                fontFamily: "'Space Grotesk', sans-serif",
                fontSize: "clamp(26px, 3vw, 42px)",
                fontWeight: 700,
                letterSpacing: "-0.025em",
                color: "#0a0a0a",
                lineHeight: 1.1,
              }}
            >
              Хиты продаж
            </h2>
          </div>
          <Link
            to="/catalog"
            className="flex items-center gap-1.5 text-[13px] font-medium text-gray-400 hover:text-violet-600 transition-colors group"
          >
            Все товары{" "}
            <ArrowRight
              size={13}
              strokeWidth={2}
              className="group-hover:translate-x-0.5 transition-transform"
            />
          </Link>
        </div>

        {catalogLoading ? (
          <div className="flex gap-4 overflow-hidden">
            {Array.from({ length: 6 }).map((_, i) => (
              <ProductSkeleton key={i} />
            ))}
          </div>
        ) : catalog?.products.length ? (
          <>
            <div className="carousel-wrapper relative">
              <button
                onClick={() => scroll(-1)}
                className="carousel-btn left"
                aria-label="Назад"
              >
                ‹
              </button>
              <div ref={trackRef} className="carousel-track">
                {catalog.products.map((p) => (
                  <div key={p.offerId} className="carousel-item" style={{ width: 200 }}>
                    <ProductCard product={p} />
                  </div>
                ))}
              </div>
              <button
                onClick={() => scroll(1)}
                className="carousel-btn right"
                aria-label="Вперёд"
              >
                ›
              </button>
            </div>
            <div className="mt-10 flex justify-start">
              <Link
                to="/catalog"
                className="inline-flex items-center gap-2.5 text-[13px] font-medium px-6 py-3 rounded-xl transition-all duration-200 border hover:bg-gray-900 hover:text-white hover:border-gray-900"
                style={{ color: "#0a0a0a", borderColor: "#e0e0e0", background: "transparent" }}
              >
                Показать все товары <ArrowRight size={14} strokeWidth={2} />
              </Link>
            </div>
          </>
        ) : null}
      </section>

      {/* ════════════════════════════════════════════════════════
          PROMO BANNERS — editorial 2-col
      ════════════════════════════════════════════════════════ */}
      <section
        className="reveal-section"
        style={{
          maxWidth: MAX_W,
          margin: "0 auto",
          paddingInline: CONTENT_PX,
          paddingBottom: "clamp(60px, 7vw, 100px)",
        }}
      >
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {/* Dark banner */}
          <Link
            to="/catalog?category=sets"
            className="promo-banner relative rounded-2xl overflow-hidden flex flex-col justify-between group"
            style={{
              background: "#0a0a0a",
              minHeight: 260,
              padding: "clamp(28px, 4vw, 48px)",
            }}
          >
            <div>
              <span style={{ ...MONO, fontSize: 10, color: "#555", textTransform: "uppercase", letterSpacing: "0.1em", display: "block", marginBottom: 12 }}>
                Для близких
              </span>
              <h3
                style={{
                  fontFamily: "'Space Grotesk', sans-serif",
                  fontSize: "clamp(22px, 2.8vw, 36px)",
                  fontWeight: 700,
                  color: "#fafaf9",
                  lineHeight: 1.1,
                  letterSpacing: "-0.025em",
                  marginBottom: 10,
                }}
              >
                Подарите аромат
              </h3>
              <p style={{ fontSize: 14, color: "#666", lineHeight: 1.65, maxWidth: 280 }}>
                Подарочные наборы от мировых парфюмерных домов
              </p>
            </div>
            <div
              className="flex items-center gap-2 text-[13px] font-medium mt-8 group-hover:gap-4 transition-all duration-200"
              style={{ color: "#6b7280" }}
            >
              Выбрать набор <ArrowRight size={14} strokeWidth={2} />
            </div>
          </Link>

          {/* Light banner */}
          <Link
            to="/catalog?category=parfum"
            className="promo-banner relative rounded-2xl overflow-hidden flex flex-col justify-between group"
            style={{
              background: "#f5f0ff",
              minHeight: 260,
              padding: "clamp(28px, 4vw, 48px)",
              border: "1px solid #e8e0ff",
            }}
          >
            <div>
              <span style={{ ...MONO, fontSize: 10, color: "#9333ea", textTransform: "uppercase", letterSpacing: "0.1em", display: "block", marginBottom: 12 }}>
                Эксклюзив
              </span>
              <h3
                style={{
                  fontFamily: "'Space Grotesk', sans-serif",
                  fontSize: "clamp(22px, 2.8vw, 36px)",
                  fontWeight: 700,
                  color: "#1e0a3c",
                  lineHeight: 1.1,
                  letterSpacing: "-0.025em",
                  marginBottom: 10,
                }}
              >
                Духи Parfum
              </h3>
              <p style={{ fontSize: 14, color: "#7c3aed", lineHeight: 1.65, maxWidth: 280, opacity: 0.75 }}>
                Редкие ароматы от мировых парфюмерных домов
              </p>
            </div>
            <div
              className="flex items-center gap-2 text-[13px] font-medium mt-8 group-hover:gap-4 transition-all duration-200"
              style={{ color: "#7c3aed" }}
            >
              Смотреть духи <ArrowRight size={14} strokeWidth={2} />
            </div>
          </Link>
        </div>
      </section>

      {/* ════════════════════════════════════════════════════════
          BRAND MARQUEE
      ════════════════════════════════════════════════════════ */}
      <div
        className="border-t overflow-hidden"
        style={{ borderColor: "#e8e8e8", background: "#fafaf9", paddingBlock: "clamp(32px, 4vw, 52px)" }}
      >
        <p
          className="text-center mb-6"
          style={{ ...MONO, fontSize: 10, color: "#c8c8c8", textTransform: "uppercase", letterSpacing: "0.12em" }}
        >
          Ведущие мировые бренды
        </p>
        <div className="relative">
          <div
            className="absolute left-0 top-0 bottom-0 w-20 pointer-events-none z-10"
            style={{ background: "linear-gradient(to right, #fafaf9, transparent)" }}
          />
          <div
            className="absolute right-0 top-0 bottom-0 w-20 pointer-events-none z-10"
            style={{ background: "linear-gradient(to left, #fafaf9, transparent)" }}
          />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link
                key={i}
                to={`/catalog?brand=${encodeURIComponent(brand)}`}
                className="inline-flex items-center mx-8 text-[13px] font-medium text-gray-400 hover:text-gray-900 transition-colors whitespace-nowrap"
              >
                {brand}
                <span className="ml-8 text-gray-200">·</span>
              </Link>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
