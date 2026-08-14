import { useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, BadgeCheck, Truck, Star } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

const BRANDS = [
  "Chanel","Dior","Tom Ford","Hermès","Byredo","Jo Malone",
  "Creed","Guerlain","Givenchy","Prada","Valentino","Burberry",
  "Versace","Hugo Boss","Montale","Kilian","Giorgio Armani","YSL","Bvlgari",
];

const CAT_LABELS: Record<string, string> = {
  testers: "Тестеры",
  parfum:  "Духи",
  edp:     "Парфюм. вода",
  edt:     "Туалет. вода",
  edc:     "Одеколон",
  deo:     "Дезодоранты",
  home:    "Дом",
  sets:    "Наборы",
  body:    "Уход",
};

function ProductSkeleton() {
  return (
    <div className="product-card flex-shrink-0" style={{ width: 160 }}>
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div className="p-3 space-y-2">
        <div className="h-2 skeleton rounded w-1/2" />
        <div className="h-3 skeleton rounded" />
        <div className="h-4 skeleton rounded w-2/3 mt-2" />
      </div>
    </div>
  );
}

function CatChip({ cat, active }: { cat: AutoCategory; active?: boolean }) {
  return (
    <Link to={`/catalog?category=${cat.slug}`} className={`chip${active ? " active" : ""} flex-shrink-0`}>
      {CAT_LABELS[cat.slug] || cat.label}
    </Link>
  );
}

function TrustCard({ icon: Icon, title, sub }: { icon: React.ElementType; title: string; sub: string }) {
  return (
    <div className="flex flex-col items-center gap-2 py-5 px-4 flex-1 text-center">
      <div className="w-10 h-10 rounded-2xl bg-violet-50 flex items-center justify-center">
        <Icon size={18} className="text-violet-600" strokeWidth={2} />
      </div>
      <div>
        <p className="text-[13px] font-semibold text-[#111]">{title}</p>
        <p className="text-[11px] text-[#888] mt-0.5">{sub}</p>
      </div>
    </div>
  );
}

function SectionTitle({ label, link }: { label: string; link?: string }) {
  return (
    <div className="section-header px-4 pt-6 pb-3">
      <h2 className="text-[18px] font-bold text-[#111] tracking-tight">{label}</h2>
      {link && (
        <Link to={link} className="text-[13px] font-semibold text-violet-600 flex items-center gap-1 hover:text-violet-800 transition-colors">
          Все <ArrowRight size={13} strokeWidth={2.5} />
        </Link>
      )}
    </div>
  );
}

export default function HomePage() {
  const trackRef = useRef<HTMLDivElement>(null);
  const marqueeRef = useRef<HTMLDivElement>(null);

  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "name" }),
  });
  const { data: autoCategories } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  /* Reveal on scroll */
  useEffect(() => {
    const io = new IntersectionObserver(
      (entries) => entries.forEach((e) => { if (e.isIntersecting) e.target.classList.add("is-visible"); }),
      { threshold: 0.05, rootMargin: "0px 0px -24px 0px" }
    );
    document.querySelectorAll(".reveal-section").forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);

  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="bg-white min-h-screen">

      {/* ════════ HERO ════════ */}
      <section className="hero-section">
        <div className="max-w-7xl mx-auto px-4 py-16 md:py-24 flex flex-col items-center text-center relative z-10">
          {/* Badge */}
          <div
            className="inline-flex items-center gap-2 rounded-full px-3.5 py-1.5 mb-8 text-[11px] font-semibold tracking-wide uppercase"
            style={{ background: "rgba(255,255,255,0.08)", color: "rgba(255,255,255,0.7)", border: "1px solid rgba(255,255,255,0.12)" }}
          >
            <span className="w-1.5 h-1.5 rounded-full bg-green-400 inline-block" />
            22 000+ ароматов в наличии
          </div>

          {/* Headline */}
          <h1
            className="font-bold text-white leading-[0.9] tracking-[-0.04em] mb-6"
            style={{ fontSize: "clamp(56px, 14vw, 160px)" }}
          >
            Magic<br />
            <span style={{ color: "#C4B5FD" }}>Vibes</span>
          </h1>

          <p className="text-[15px] text-white/60 max-w-xs mb-10 leading-relaxed">
            Оригинальная парфюмерия от мировых брендов. Доставка по всей России через Ozon.
          </p>

          <div className="flex flex-col sm:flex-row items-center gap-3">
            <Link to="/catalog" className="btn-primary text-[14px] px-8 py-3.5 rounded-2xl w-full sm:w-auto">
              Перейти в каталог
            </Link>
            <Link
              to="/brands"
              className="text-[13px] font-semibold text-white/60 hover:text-white transition-colors flex items-center gap-1.5"
            >
              Все бренды <ArrowRight size={14} strokeWidth={2.5} />
            </Link>
          </div>
        </div>

        {/* Bottom wave */}
        <div style={{ height: 32, background: "#fff", borderRadius: "50% 50% 0 0 / 32px 32px 0 0", marginTop: -1 }} />
      </section>

      {/* ════════ TRUST ════════ */}
      <section className="border-b border-[#EBEBEB]">
        <div className="max-w-7xl mx-auto">
          <div className="flex divide-x divide-[#EBEBEB] overflow-x-auto scroll-x">
            <TrustCard icon={BadgeCheck} title="100% оригинал" sub="Гарантия подлинности" />
            <TrustCard icon={Truck}      title="Доставка 1–5 дней" sub="Бесплатно от 3 000 ₽" />
            <TrustCard icon={Star}       title="4.9 на Ozon" sub="Тысячи отзывов" />
          </div>
        </div>
      </section>

      {/* ════════ CATEGORIES ════════ */}
      {autoCategories && autoCategories.length > 0 && (
        <section className="reveal-section pt-5 pb-1">
          <SectionTitle label="Категории" link="/catalog" />
          <div className="scroll-x flex gap-2.5 px-4 pb-3">
            <Link to="/catalog" className="chip flex-shrink-0">Все товары</Link>
            {autoCategories.map((cat) => (
              <CatChip key={cat.slug} cat={cat} />
            ))}
          </div>
        </section>
      )}

      {/* ════════ BESTSELLERS ════════ */}
      <section className="reveal-section">
        <SectionTitle label="Популярное" link="/catalog" />
        {catalogLoading ? (
          <div className="scroll-x flex gap-3 px-4 pb-4">
            {Array.from({ length: 8 }).map((_, i) => <ProductSkeleton key={i} />)}
          </div>
        ) : (
          <div
            ref={trackRef}
            className="scroll-x flex gap-3 px-4 pb-4"
          >
            {catalog?.products.map((p) => (
              <div key={p.offerId} className="flex-shrink-0" style={{ width: 160 }}>
                <ProductCard product={p} />
              </div>
            ))}
          </div>
        )}
      </section>

      {/* ════════ PRODUCTS GRID ════════ */}
      {catalog && catalog.products.length > 0 && (
        <section className="reveal-section">
          <SectionTitle label="Новинки" link="/catalog?sort=price_desc" />
          <div className="px-4 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3 pb-4">
            {catalog.products.slice(0, 6).map((p) => (
              <ProductCard key={p.offerId + "_g"} product={p} />
            ))}
          </div>
          <div className="px-4 pb-6">
            <Link to="/catalog" className="btn-ghost w-full justify-center py-3">
              Смотреть все товары <ArrowRight size={14} strokeWidth={2.5} />
            </Link>
          </div>
        </section>
      )}

      {/* ════════ PROMO BANNERS ════════ */}
      <section className="reveal-section px-4 pb-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {/* Dark */}
          <Link
            to="/catalog?category=sets"
            className="group relative rounded-2xl overflow-hidden flex flex-col justify-between p-6 min-h-[200px]"
            style={{ background: "#0D0A1A" }}
          >
            <div
              className="absolute inset-0 pointer-events-none"
              style={{ background: "radial-gradient(ellipse 70% 80% at 80% 80%, rgba(91,33,182,0.4) 0%, transparent 70%)" }}
            />
            <div className="relative z-10">
              <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-white/40 mb-2">Для близких</p>
              <h3 className="text-[22px] font-bold text-white leading-tight tracking-tight mb-2">
                Подарите<br />аромат
              </h3>
              <p className="text-[13px] text-white/50 leading-relaxed">Подарочные наборы от лучших парфюмерных домов</p>
            </div>
            <div className="relative z-10 flex items-center gap-2 mt-6 text-[13px] font-semibold text-violet-300 group-hover:gap-3 transition-all duration-200">
              Выбрать набор <ArrowRight size={14} strokeWidth={2.5} />
            </div>
          </Link>

          {/* Light */}
          <Link
            to="/catalog?category=parfum"
            className="group relative rounded-2xl overflow-hidden flex flex-col justify-between p-6 min-h-[200px]"
            style={{ background: "#F3F0FA", border: "1.5px solid #E8E0FF" }}
          >
            <div>
              <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-violet-400 mb-2">Эксклюзив</p>
              <h3 className="text-[22px] font-bold text-[#1E0A3C] leading-tight tracking-tight mb-2">
                Духи<br />Parfum
              </h3>
              <p className="text-[13px] text-violet-500/70 leading-relaxed">Редкие ароматы высочайшей концентрации</p>
            </div>
            <div className="flex items-center gap-2 mt-6 text-[13px] font-semibold text-violet-600 group-hover:gap-3 transition-all duration-200">
              Смотреть <ArrowRight size={14} strokeWidth={2.5} />
            </div>
          </Link>
        </div>
      </section>

      {/* ════════ BRANDS MARQUEE ════════ */}
      <section className="border-t border-[#EBEBEB] py-8 overflow-hidden reveal-section">
        <p className="text-center text-[10px] font-bold uppercase tracking-[0.14em] text-[#C8C8C8] mb-5">
          Ведущие мировые бренды
        </p>
        <div className="relative" ref={marqueeRef}>
          <div
            className="absolute left-0 top-0 bottom-0 w-16 pointer-events-none z-10"
            style={{ background: "linear-gradient(to right, #fff, transparent)" }}
          />
          <div
            className="absolute right-0 top-0 bottom-0 w-16 pointer-events-none z-10"
            style={{ background: "linear-gradient(to left, #fff, transparent)" }}
          />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link
                key={i}
                to={`/catalog?brand=${encodeURIComponent(brand)}`}
                className="inline-flex items-center mx-6 text-[13px] font-semibold text-[#ABABAB] hover:text-[#111] transition-colors whitespace-nowrap"
              >
                {brand}
                <span className="ml-6 text-[#E0E0E0]">·</span>
              </Link>
            ))}
          </div>
        </div>
      </section>

    </div>
  );
}
