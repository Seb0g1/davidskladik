import { useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import {
  ArrowRight, Truck, Shield, RefreshCw, Headphones,
  FlaskConical, Wind, Droplets, Droplet, Leaf, Sparkles,
  Flame, Gift, Heart, Gem,
} from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

const PERKS = [
  { icon: Truck,      title: "Доставка Ozon",   text: "По всей России, 1-5 дней" },
  { icon: Shield,     title: "100% оригинал",   text: "Гарантия подлинности" },
  { icon: RefreshCw,  title: "Возврат 14 дней", text: "Без вопросов" },
  { icon: Headphones, title: "Поддержка",        text: "Всегда на связи" },
];

const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermes", "Byredo", "Jo Malone",
  "Creed", "Guerlain", "Givenchy", "Prada", "Valentino", "Burberry",
  "Versace", "Hugo Boss", "Montale", "Kilian", "Dolce Gabbana", "Lancome",
];

const CAT_ICONS: Record<string, React.ComponentType<{ size?: number; strokeWidth?: number; className?: string; style?: React.CSSProperties }>> = {
  testers:   FlaskConical,
  parfum:    Wind,
  edp:       Droplets,
  edt:       Droplet,
  edc:       Leaf,
  deo:       Sparkles,
  home:      Flame,
  sets:      Gift,
  body:      Heart,
  parfumery: Gem,
};

function CategoryCard({ cat }: { cat: AutoCategory }) {
  const Icon = CAT_ICONS[cat.slug] || Gem;
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      className="group flex flex-col gap-4 p-5 bg-white rounded-2xl transition-all duration-250 hover:-translate-y-1"
      style={{
        border: "1px solid #efefef",
        boxShadow: "0 1px 4px rgba(0,0,0,0.03)",
      }}
      onMouseEnter={e => {
        (e.currentTarget as HTMLElement).style.borderColor = "#7c3aed";
        (e.currentTarget as HTMLElement).style.boxShadow = "0 8px 24px rgba(124,58,237,0.1)";
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLElement).style.borderColor = "#efefef";
        (e.currentTarget as HTMLElement).style.boxShadow = "0 1px 4px rgba(0,0,0,0.03)";
      }}
    >
      <div
        className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0 transition-colors duration-200"
        style={{ background: "#f5f3ff" }}
      >
        <Icon size={18} strokeWidth={1.5} style={{ color: "#7c3aed" }} />
      </div>
      <div className="flex-1">
        <div className="font-semibold text-[13px] text-apple-black leading-snug">
          {cat.label}
        </div>
        <div className="text-[11px] text-apple-gray mt-1">
          {cat.count.toLocaleString("ru-RU")} товаров
        </div>
      </div>
      <ArrowRight
        size={14}
        className="text-apple-gray opacity-0 group-hover:opacity-100 group-hover:text-violet-600 transition-all duration-200 -translate-x-1 group-hover:translate-x-0"
        strokeWidth={2}
      />
    </Link>
  );
}

function CatSkeleton() {
  return (
    <div className="rounded-2xl p-5 flex flex-col gap-4" style={{ border: "1px solid #efefef" }}>
      <div className="w-10 h-10 rounded-xl skeleton" />
      <div className="space-y-2">
        <div className="h-3.5 skeleton rounded w-3/4" />
        <div className="h-2.5 skeleton rounded w-1/2" />
      </div>
    </div>
  );
}

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden flex-shrink-0" style={{ width: 192, border: "1px solid #f5f5f5" }}>
      <div className="skeleton" style={{ aspectRatio: "1/1" }} />
      <div className="p-3.5 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3 skeleton rounded" />
        <div className="h-3 skeleton rounded w-3/4" />
        <div className="h-4 skeleton rounded w-1/2 mt-2" />
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
    trackRef.current?.scrollBy({ left: dir * 640, behavior: "smooth" });
  };

  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen bg-white">

      {/* ── HERO ── */}
      <section className="relative overflow-hidden bg-white">
        <div className="absolute inset-0 pointer-events-none">
          <div
            className="absolute top-0 right-0 w-[900px] h-[900px] rounded-full"
            style={{
              background: "radial-gradient(circle, rgba(124,58,237,0.06) 0%, transparent 60%)",
              transform: "translate(30%, -40%)",
            }}
          />
        </div>

        <div className="max-w-7xl mx-auto px-4 sm:px-6 relative z-10">
          <div className="flex flex-col lg:flex-row items-center gap-10 py-16 md:py-20">

            {/* Text column */}
            <div className="flex-1 text-center lg:text-left anim-slide-up">
              <div
                className="inline-flex items-center gap-2 text-[11px] font-bold px-4 py-2 rounded-full mb-7 tracking-widest uppercase"
                style={{ background: "#f5f3ff", color: "#7c3aed", border: "1px solid #ede9fe" }}
              >
                Оригинальная парфюмерия
              </div>

              <h1 className="font-bold tracking-tight leading-none mb-6">
                <span
                  className="block text-[48px] md:text-[64px] lg:text-[72px] text-apple-black"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Мировая
                </span>
                <span
                  className="block text-[48px] md:text-[64px] lg:text-[72px]"
                  style={{
                    fontFamily: "'Playfair Display', Georgia, serif",
                    background: "linear-gradient(135deg, #7c3aed 0%, #a855f7 60%, #c084fc 100%)",
                    WebkitBackgroundClip: "text",
                    WebkitTextFillColor: "transparent",
                  }}
                >
                  парфюмерия
                </span>
              </h1>

              <p className="text-[16px] text-apple-gray max-w-[400px] mx-auto lg:mx-0 leading-relaxed mb-9">
                22&nbsp;000+ ароматов от мировых домов. Оригинальная продукция, быстрая доставка.
              </p>

              <div className="flex flex-col sm:flex-row gap-3 justify-center lg:justify-start mb-10">
                <Link
                  to="/catalog"
                  className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5 active:scale-[0.98] text-white"
                  style={{
                    background: "linear-gradient(135deg, #7c3aed, #9333ea)",
                    boxShadow: "0 8px 28px rgba(124,58,237,0.32)",
                  }}
                >
                  Смотреть каталог <ArrowRight size={16} strokeWidth={2} />
                </Link>
                <Link
                  to="/catalog?sort=price_desc"
                  className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5 text-apple-black bg-white hover:bg-violet-50"
                  style={{ border: "1.5px solid #e5e7eb" }}
                >
                  Топ ароматов
                </Link>
              </div>

              <div className="flex items-center justify-center lg:justify-start">
                {[
                  { value: "22 000+", label: "ароматов" },
                  { value: "100%",    label: "оригинал" },
                  { value: "1-5",     label: "дней доставка" },
                ].map(({ value, label }, i) => (
                  <div key={label} className="flex items-center">
                    {i > 0 && <div className="w-px h-8 bg-gray-200 mx-6" />}
                    <div>
                      <div className="text-xl font-bold text-apple-black tracking-tight">{value}</div>
                      <div className="text-[11px] text-apple-gray mt-0.5">{label}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Right decorative panel */}
            <div
              className="hidden lg:flex flex-none w-[380px] h-[380px] items-center justify-center relative anim-fade-in"
              style={{ animationDelay: "0.2s" }}
            >
              <div
                className="absolute top-6 right-6 w-56 h-56 rounded-full"
                style={{ background: "linear-gradient(135deg, #ede9fe 0%, #ddd6fe 100%)" }}
              />
              <div
                className="absolute bottom-6 left-6 w-44 h-44 rounded-full"
                style={{ background: "linear-gradient(135deg, #fce7f3 0%, #fbcfe8 100%)" }}
              />
              <div
                className="absolute top-20 left-16 w-28 h-28 rounded-full"
                style={{ background: "linear-gradient(135deg, #f0fdf4 0%, #bbf7d0 100%)" }}
              />
              <div
                className="relative z-10 rounded-3xl p-8 text-center flex flex-col items-center gap-3"
                style={{
                  background: "rgba(255,255,255,0.82)",
                  backdropFilter: "blur(24px)",
                  border: "1px solid rgba(255,255,255,0.95)",
                  boxShadow: "0 24px 64px rgba(124,58,237,0.14), 0 4px 16px rgba(0,0,0,0.05)",
                  width: 190,
                }}
              >
                <div className="w-14 h-14 rounded-2xl flex items-center justify-center" style={{ background: "#f5f3ff" }}>
                  <Sparkles size={26} strokeWidth={1.5} style={{ color: "#7c3aed" }} />
                </div>
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
                  <Icon size={15} strokeWidth={1.75} style={{ color: "#7c3aed" }} />
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
      <section className="py-16 bg-white reveal-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-9">
            <h2
              className="text-3xl md:text-4xl font-bold text-apple-black tracking-tight"
              style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
            >
              Категории
            </h2>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium transition-colors group hover:text-violet-700"
              style={{ color: "#7c3aed" }}
            >
              Все товары <ArrowRight size={14} strokeWidth={2} className="group-hover:translate-x-0.5 transition-transform" />
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

      {/* ── Editorial promo banners ── */}
      <section className="pb-16 bg-white reveal-section">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">

            {/* Gift card */}
            <Link
              to="/catalog?category=sets"
              className="group relative rounded-3xl overflow-hidden flex flex-col justify-between min-h-[220px] transition-all duration-300 hover:-translate-y-1 hover:shadow-2xl"
              style={{
                background: "linear-gradient(135deg, #fdf2f8 0%, #fce7f3 70%, #fdf4ff 100%)",
                border: "1px solid #fbcfe8",
                padding: "clamp(28px, 5vw, 48px)",
              }}
            >
              <div className="absolute top-0 right-0 w-52 h-52 rounded-full pointer-events-none"
                style={{
                  background: "radial-gradient(circle, rgba(249,168,212,0.35) 0%, transparent 65%)",
                  transform: "translate(30%, -30%)",
                }}
              />
              <div>
                <div className="w-12 h-12 rounded-2xl flex items-center justify-center mb-5" style={{ background: "rgba(249,168,212,0.25)" }}>
                  <Gift size={22} strokeWidth={1.5} style={{ color: "#be185d" }} />
                </div>
                <h3
                  className="text-2xl md:text-[28px] font-bold text-rose-900 mb-2 leading-tight"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Подарите аромат
                </h3>
                <p className="text-[13px] text-rose-700 leading-relaxed" style={{ opacity: 0.8, maxWidth: 280 }}>
                  Подарочные наборы для тех, кто хочет удивить близких
                </p>
              </div>
              <div className="flex items-center gap-2 text-[13px] font-bold text-rose-700 mt-6 group-hover:gap-3 transition-all duration-200">
                Выбрать набор <ArrowRight size={14} strokeWidth={2} />
              </div>
            </Link>

            {/* Exclusive parfums card */}
            <Link
              to="/catalog?category=parfum"
              className="group relative rounded-3xl overflow-hidden flex flex-col justify-between min-h-[220px] transition-all duration-300 hover:-translate-y-1 hover:shadow-2xl"
              style={{
                background: "linear-gradient(135deg, #f5f3ff 0%, #ede9fe 70%, #faf5ff 100%)",
                border: "1px solid #ddd6fe",
                padding: "clamp(28px, 5vw, 48px)",
              }}
            >
              <div className="absolute top-0 right-0 w-52 h-52 rounded-full pointer-events-none"
                style={{
                  background: "radial-gradient(circle, rgba(196,181,253,0.35) 0%, transparent 65%)",
                  transform: "translate(30%, -30%)",
                }}
              />
              <div>
                <div className="w-12 h-12 rounded-2xl flex items-center justify-center mb-5" style={{ background: "rgba(196,181,253,0.25)" }}>
                  <Wind size={22} strokeWidth={1.5} style={{ color: "#6d28d9" }} />
                </div>
                <h3
                  className="text-2xl md:text-[28px] font-bold text-violet-900 mb-2 leading-tight"
                  style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
                >
                  Эксклюзивные духи
                </h3>
                <p className="text-[13px] text-violet-700 leading-relaxed" style={{ opacity: 0.8, maxWidth: 280 }}>
                  Редкие ароматы и шедевры от мировых парфюмерных домов
                </p>
              </div>
              <div className="flex items-center gap-2 text-[13px] font-bold text-violet-700 mt-6 group-hover:gap-3 transition-all duration-200">
                Смотреть духи <ArrowRight size={14} strokeWidth={2} />
              </div>
            </Link>
          </div>
        </div>
      </section>

      {/* ── Bestsellers carousel ── */}
      <section className="py-16 reveal-section" style={{ background: "#fafafa" }}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-9">
            <div>
              <p className="text-[11px] font-bold uppercase tracking-widest mb-2" style={{ color: "#7c3aed" }}>Популярное</p>
              <h2
                className="text-3xl md:text-4xl font-bold text-apple-black tracking-tight"
                style={{ fontFamily: "'Playfair Display', Georgia, serif" }}
              >
                Хиты продаж
              </h2>
            </div>
            <Link
              to="/catalog"
              className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium transition-colors group hover:text-violet-700"
              style={{ color: "#7c3aed" }}
            >
              Все товары <ArrowRight size={14} strokeWidth={2} className="group-hover:translate-x-0.5 transition-transform" />
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
              <div className="text-center mt-9">
                <Link
                  to="/catalog"
                  className="inline-flex items-center gap-2 font-medium px-7 py-3.5 rounded-2xl transition-all duration-200 text-[13px] bg-white text-apple-black hover:text-violet-700 hover:border-violet-300 active:scale-[0.98]"
                  style={{ border: "1.5px solid #e5e7eb" }}
                >
                  Показать все товары <ArrowRight size={14} strokeWidth={2} />
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
                className="inline-flex items-center mx-7 text-[13px] font-semibold text-apple-gray hover:text-violet-600 transition-colors whitespace-nowrap"
              >
                {brand}
                <span className="ml-7 text-gray-200">·</span>
              </Link>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}
