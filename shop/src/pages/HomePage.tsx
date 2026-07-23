import { useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, Truck, Shield, RefreshCw, Headphones } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

const PERKS = [
  { icon: Truck,      title: "Доставка Ozon",   text: "По всей России, 1–5 дней" },
  { icon: Shield,     title: "100% оригинал",   text: "Гарантия подлинности" },
  { icon: RefreshCw,  title: "Возврат 14 дней", text: "Без вопросов" },
  { icon: Headphones, title: "Поддержка",        text: "Всегда на связи" },
];

const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermès", "Byredo", "Jo Malone",
  "Creed", "Guerlain", "Givenchy", "Prada", "Valentino", "Burberry",
  "Versace", "Hugo Boss", "Montale", "Kilian", "Dolce & Gabbana", "Lancome",
];

const CAT_ICONS: Record<string, string> = {
  testers:   "🧪",
  parfum:    "🌹",
  edp:       "🫧",
  edt:       "💧",
  edc:       "🌿",
  deo:       "✨",
  home:      "🕯️",
  sets:      "🎁",
  body:      "🌸",
  parfumery: "💎",
};

const CAT_COLORS: Record<string, { bg: string; border: string; text: string; dot: string }> = {
  testers:   { bg: "#f0f9ff", border: "#bae6fd", text: "#0369a1", dot: "#38bdf8" },
  parfum:    { bg: "#fdf2f8", border: "#fbcfe8", text: "#9d174d", dot: "#ec4899" },
  edp:       { bg: "#f5f3ff", border: "#ddd6fe", text: "#5b21b6", dot: "#8b5cf6" },
  edt:       { bg: "#eff6ff", border: "#bfdbfe", text: "#1e40af", dot: "#3b82f6" },
  edc:       { bg: "#f0fdf4", border: "#bbf7d0", text: "#166534", dot: "#22c55e" },
  deo:       { bg: "#f8fafc", border: "#e2e8f0", text: "#334155", dot: "#94a3b8" },
  home:      { bg: "#fffbeb", border: "#fde68a", text: "#92400e", dot: "#f59e0b" },
  sets:      { bg: "#fff1f2", border: "#fecdd3", text: "#991b1b", dot: "#f87171" },
  body:      { bg: "#f0fdfa", border: "#99f6e4", text: "#134e4a", dot: "#14b8a6" },
  parfumery: { bg: "#f9fafb", border: "#e5e7eb", text: "#111827", dot: "#6b7280" },
};

function CategoryCard({ cat }: { cat: AutoCategory }) {
  const c = CAT_COLORS[cat.slug] || CAT_COLORS.parfumery;
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      className="group relative rounded-2xl p-5 flex flex-col gap-3 transition-all duration-300 hover:-translate-y-1"
      style={{
        background: c.bg,
        border: `1.5px solid ${c.border}`,
        boxShadow: "0 1px 4px rgba(0,0,0,0.04)",
      }}
    >
      {/* Accent dot */}
      <div
        className="absolute top-3.5 right-3.5 w-2 h-2 rounded-full scale-0 group-hover:scale-100 transition-transform duration-300"
        style={{ background: c.dot }}
      />
      <div className="text-3xl leading-none">{CAT_ICONS[cat.slug] || "🌸"}</div>
      <div>
        <div className="font-bold text-[13px] leading-snug" style={{ color: c.text }}>
          {cat.label}
        </div>
        <div className="text-[11px] mt-0.5" style={{ color: c.text, opacity: 0.55 }}>
          {cat.count.toLocaleString("ru-RU")} товаров
        </div>
      </div>
      <div
        className="flex items-center gap-1 text-[11px] font-semibold opacity-0 group-hover:opacity-100 -translate-y-1 group-hover:translate-y-0 transition-all duration-200"
        style={{ color: c.text }}
      >
        Смотреть <ArrowRight size={10} />
      </div>
    </Link>
  );
}

function CatSkeleton() {
  return <div className="skeleton rounded-2xl h-36" />;
}

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden flex-shrink-0" style={{ width: 180 }}>
      <div className="skeleton" style={{ aspectRatio: "1/1" }} />
      <div className="p-3 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3.5 skeleton rounded" />
        <div className="h-4 skeleton rounded w-1/2 mt-1" />
      </div>
    </div>
  );
}

export default function HomePage() {
  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 16, sort: "name" }),
  });

  const { data: autoCategories, isLoading: catsLoading } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  const trackRef = useRef<HTMLDivElement>(null);
  const scroll = (dir: -1 | 1) => {
    trackRef.current?.scrollBy({ left: dir * 620, behavior: "smooth" });
  };

  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen bg-white">

      {/* ── HERO ── */}
      <section className="relative overflow-hidden bg-white">
        {/* Decorative background glows */}
        <div className="absolute inset-0 pointer-events-none">
          <div
            className="absolute top-0 right-0 w-[800px] h-[800px] rounded-full"
            style={{
              background: "radial-gradient(circle, rgba(124,58,237,0.07) 0%, transparent 65%)",
              transform: "translate(25%, -35%)",
            }}
          />
          <div
            className="absolute bottom-0 left-[15%] w-[500px] h-[500px] rounded-full"
            style={{
              background: "radial-gradient(circle, rgba(192,132,252,0.06) 0%, transparent 65%)",
              transform: "translateY(40%)",
            }}
          />
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 relative z-10">
          <div className="flex flex-col lg:flex-row items-center gap-12 py-16 md:py-24">

            {/* Text column */}
            <div className="flex-1 text-center lg:text-left anim-slide-up">
              <div
                className="inline-flex items-center gap-2 text-[11px] font-bold px-4 py-2 rounded-full mb-7 tracking-widest uppercase"
                style={{ background: "#f5f3ff", color: "#7c3aed", border: "1px solid #ede9fe" }}
              >
                ✦ Оригинальная парфюмерия ✦
              </div>

              <h1 className="font-bold tracking-tight leading-none mb-6">
                <span
                  className="block text-[50px] md:text-[68px] lg:text-[76px] text-apple-black"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Мировая
                </span>
                <span
                  className="block text-[50px] md:text-[68px] lg:text-[76px]"
                  style={{
                    fontFamily: "'Playfair Display', Georgia, serif",
                    background: "linear-gradient(135deg, #7c3aed 0%, #a855f7 50%, #e879f9 100%)",
                    WebkitBackgroundClip: "text",
                    WebkitTextFillColor: "transparent",
                  }}
                >
                  парфюмерия
                </span>
              </h1>

              <p className="text-[15px] md:text-[17px] text-apple-gray max-w-[420px] mx-auto lg:mx-0 leading-relaxed mb-9">
                22&nbsp;000+ ароматов от мировых домов. Оригинальная продукция, быстрая доставка по всей России.
              </p>

              <div className="flex flex-col sm:flex-row gap-3 justify-center lg:justify-start mb-10">
                <Link
                  to="/catalog"
                  className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5 text-white"
                  style={{
                    background: "linear-gradient(135deg, #7c3aed, #9333ea)",
                    boxShadow: "0 8px 28px rgba(124,58,237,0.38)",
                  }}
                >
                  Смотреть каталог <ArrowRight size={17} />
                </Link>
                <Link
                  to="/catalog?sort=price_desc"
                  className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5 text-apple-black bg-white hover:bg-violet-50 hover:border-violet-300"
                  style={{ border: "1.5px solid #e5e7eb" }}
                >
                  Топ ароматов
                </Link>
              </div>

              {/* Stats */}
              <div className="flex items-center gap-0 justify-center lg:justify-start">
                {[
                  { value: "22 000+", label: "ароматов" },
                  { value: "100%",    label: "оригинал" },
                  { value: "1–5",     label: "дней доставка" },
                ].map(({ value, label }, i) => (
                  <div key={label} className="flex items-center">
                    {i > 0 && <div className="w-px h-9 bg-gray-200 mx-7" />}
                    <div>
                      <div className="text-xl font-bold text-apple-black tracking-tight">{value}</div>
                      <div className="text-[11px] text-apple-gray mt-0.5">{label}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Decorative visual */}
            <div
              className="hidden lg:flex flex-none w-[400px] h-[400px] items-center justify-center relative anim-fade-in"
              style={{ animationDelay: "0.2s" }}
            >
              {/* Backdrop circles */}
              <div
                className="absolute top-4 right-4 w-60 h-60 rounded-full"
                style={{ background: "linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%)" }}
              />
              <div
                className="absolute bottom-4 left-4 w-48 h-48 rounded-full"
                style={{ background: "linear-gradient(135deg, #fce7f3 0%, #fbcfe8 100%)" }}
              />
              <div
                className="absolute top-20 left-16 w-32 h-32 rounded-full"
                style={{ background: "linear-gradient(135deg, #f0fdf4 0%, #bbf7d0 100%)" }}
              />
              {/* Glass card */}
              <div
                className="relative z-10 rounded-3xl p-8 text-center flex flex-col items-center gap-3"
                style={{
                  background: "rgba(255,255,255,0.78)",
                  backdropFilter: "blur(24px)",
                  border: "1px solid rgba(255,255,255,0.95)",
                  boxShadow: "0 24px 64px rgba(124,58,237,0.15), 0 4px 16px rgba(0,0,0,0.06)",
                  width: 190,
                }}
              >
                <div className="text-5xl">🫧</div>
                <div>
                  <div className="text-sm font-bold tracking-widest uppercase" style={{ color: "#7c3aed" }}>Magic Vibes</div>
                  <div className="text-[11px] text-apple-gray mt-1">Парфюмерия</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Perks strip ── */}
      <section className="border-y border-gray-100" style={{ background: "#fafafa" }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="grid grid-cols-2 lg:grid-cols-4 divide-x divide-gray-100">
            {PERKS.map(({ icon: Icon, title, text }) => (
              <div key={title} className="flex items-center gap-3 py-4 px-4 lg:px-6">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0" style={{ background: "#f5f3ff" }}>
                  <Icon size={15} style={{ color: "#7c3aed" }} />
                </div>
                <div>
                  <div className="text-[12px] font-semibold text-apple-black leading-tight">{title}</div>
                  <div className="text-[10px] text-apple-gray mt-0.5">{text}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Categories ── */}
      <section className="py-14 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-8">
            <div>
              <p className="text-[11px] font-bold uppercase tracking-widest mb-1.5" style={{ color: "#7c3aed" }}>Навигация</p>
              <h2
                className="text-3xl md:text-4xl font-bold text-apple-black tracking-tight"
                style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
              >
                Категории
              </h2>
            </div>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium transition-colors group"
              style={{ color: "#7c3aed" }}
            >
              Весь каталог <ArrowRight size={14} className="group-hover:translate-x-0.5 transition-transform" />
            </Link>
          </div>

          {catsLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
              {Array.from({ length: 10 }).map((_, i) => <CatSkeleton key={i} />)}
            </div>
          ) : autoCategories && autoCategories.length > 0 ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
              {autoCategories.map((cat) => <CategoryCard key={cat.slug} cat={cat} />)}
            </div>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
              {[
                { slug: "edp",    label: "Парфюмерная вода", count: 0 },
                { slug: "edt",    label: "Туалетная вода",   count: 0 },
                { slug: "parfum", label: "Духи",             count: 0 },
                { slug: "sets",   label: "Наборы",           count: 0 },
              ].map((cat) => <CategoryCard key={cat.slug} cat={cat} />)}
            </div>
          )}
        </div>
      </section>

      {/* ── Promo banners ── */}
      <section className="pb-14 bg-white">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">

            {/* Gifts card */}
            <Link
              to="/catalog?category=sets"
              className="group relative rounded-3xl p-8 md:p-10 overflow-hidden flex flex-col justify-between min-h-[180px] transition-all duration-300 hover:-translate-y-1 hover:shadow-2xl"
              style={{
                background: "linear-gradient(135deg, #fdf2f8 0%, #fce7f3 60%, #fdf4ff 100%)",
                border: "1px solid #fbcfe8",
              }}
            >
              <div className="absolute top-0 right-0 w-48 h-48 rounded-full pointer-events-none"
                style={{
                  background: "radial-gradient(circle, #f9a8d4 0%, transparent 70%)",
                  opacity: 0.25,
                  transform: "translate(30%, -30%)",
                }}
              />
              <div>
                <div className="text-4xl mb-4">🎁</div>
                <h3
                  className="text-xl md:text-2xl font-bold text-rose-900 mb-2"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Подарите аромат
                </h3>
                <p className="text-[13px] text-rose-700 max-w-xs leading-relaxed" style={{ opacity: 0.8 }}>
                  Подарочные наборы для тех, кто хочет удивить близких
                </p>
              </div>
              <div className="flex items-center gap-1.5 text-[13px] font-bold text-rose-700 mt-6 group-hover:gap-3 transition-all duration-200">
                Выбрать набор <ArrowRight size={14} />
              </div>
            </Link>

            {/* Exclusive parfums card */}
            <Link
              to="/catalog?category=parfum"
              className="group relative rounded-3xl p-8 md:p-10 overflow-hidden flex flex-col justify-between min-h-[180px] transition-all duration-300 hover:-translate-y-1 hover:shadow-2xl"
              style={{
                background: "linear-gradient(135deg, #f5f3ff 0%, #ede9fe 60%, #faf5ff 100%)",
                border: "1px solid #ddd6fe",
              }}
            >
              <div className="absolute top-0 right-0 w-48 h-48 rounded-full pointer-events-none"
                style={{
                  background: "radial-gradient(circle, #c4b5fd 0%, transparent 70%)",
                  opacity: 0.3,
                  transform: "translate(30%, -30%)",
                }}
              />
              <div>
                <div className="text-4xl mb-4">🌹</div>
                <h3
                  className="text-xl md:text-2xl font-bold text-violet-900 mb-2"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Эксклюзивные духи
                </h3>
                <p className="text-[13px] text-violet-700 max-w-xs leading-relaxed" style={{ opacity: 0.8 }}>
                  Редкие ароматы и шедевры от мировых парфюмерных домов
                </p>
              </div>
              <div className="flex items-center gap-1.5 text-[13px] font-bold text-violet-700 mt-6 group-hover:gap-3 transition-all duration-200">
                Смотреть духи <ArrowRight size={14} />
              </div>
            </Link>
          </div>
        </div>
      </section>

      {/* ── Bestsellers carousel ── */}
      <section className="py-14" style={{ background: "#fafafa" }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-8">
            <div>
              <p className="text-[11px] font-bold uppercase tracking-widest mb-1.5" style={{ color: "#7c3aed" }}>Популярное</p>
              <h2
                className="text-3xl md:text-4xl font-bold text-apple-black tracking-tight"
                style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
              >
                Хиты продаж
              </h2>
            </div>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium transition-colors group"
              style={{ color: "#7c3aed" }}
            >
              Все товары <ArrowRight size={14} className="group-hover:translate-x-0.5 transition-transform" />
            </Link>
          </div>

          {catalogLoading ? (
            <div className="flex gap-3 overflow-hidden">
              {Array.from({ length: 7 }).map((_, i) => <ProductSkeleton key={i} />)}
            </div>
          ) : catalog?.products.length ? (
            <>
              <div className="carousel-wrapper relative -mx-4 sm:-mx-6">
                <button onClick={() => scroll(-1)} className="carousel-btn left !left-1 sm:!left-0" aria-label="Назад">‹</button>
                <div ref={trackRef} className="carousel-track px-4 sm:px-6">
                  {catalog.products.map((p) => (
                    <div key={p.offerId} className="carousel-item" style={{ width: 192 }}>
                      <ProductCard product={p} />
                    </div>
                  ))}
                </div>
                <button onClick={() => scroll(1)} className="carousel-btn right !right-1 sm:!right-0" aria-label="Вперёд">›</button>
              </div>
              <div className="text-center mt-8">
                <Link
                  to="/catalog"
                  className="inline-flex items-center gap-2 font-medium px-7 py-3.5 rounded-2xl transition-all duration-200 text-[13px] bg-white text-apple-black hover:text-violet-700 hover:border-violet-300"
                  style={{ border: "1.5px solid #e5e7eb" }}
                >
                  Показать все товары <ArrowRight size={14} />
                </Link>
              </div>
            </>
          ) : (
            <div className="text-center py-16 text-apple-gray">Загрузка товаров...</div>
          )}
        </div>
      </section>

      {/* ── Brand marquee ── */}
      <section className="bg-white border-t border-gray-100 overflow-hidden py-10">
        <p className="text-center text-[10px] font-bold uppercase tracking-widest text-apple-gray mb-6">
          Ведущие мировые бренды
        </p>
        <div className="relative">
          <div className="absolute left-0 top-0 bottom-0 w-20 bg-gradient-to-r from-white to-transparent z-10 pointer-events-none" />
          <div className="absolute right-0 top-0 bottom-0 w-20 bg-gradient-to-l from-white to-transparent z-10 pointer-events-none" />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link
                key={i}
                to={`/catalog?brand=${encodeURIComponent(brand)}`}
                className="inline-flex items-center mx-6 text-[13px] font-semibold text-apple-gray hover:text-violet-600 transition-colors whitespace-nowrap"
              >
                {brand}
                <span className="ml-6 text-gray-200">·</span>
              </Link>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
