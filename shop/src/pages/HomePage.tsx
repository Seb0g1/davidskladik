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

const CAT_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  testers:   { bg: "#f0f9ff", border: "#7dd3fc", text: "#0369a1" },
  parfum:    { bg: "#fdf2f8", border: "#f9a8d4", text: "#9d174d" },
  edp:       { bg: "#f5f3ff", border: "#c4b5fd", text: "#5b21b6" },
  edt:       { bg: "#eff6ff", border: "#93c5fd", text: "#1e40af" },
  edc:       { bg: "#f0fdf4", border: "#86efac", text: "#166534" },
  deo:       { bg: "#f8fafc", border: "#cbd5e1", text: "#334155" },
  home:      { bg: "#fffbeb", border: "#fcd34d", text: "#92400e" },
  sets:      { bg: "#fff1f2", border: "#fca5a5", text: "#991b1b" },
  body:      { bg: "#f0fdfa", border: "#5eead4", text: "#134e4a" },
  parfumery: { bg: "#f9fafb", border: "#d1d5db", text: "#111827" },
};

function CategoryCard({ cat }: { cat: AutoCategory }) {
  const colors = CAT_COLORS[cat.slug] || CAT_COLORS.parfumery;
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      className="group relative rounded-2xl p-5 flex flex-col gap-3 transition-all duration-300 hover:-translate-y-1 hover:shadow-card-hover"
      style={{ background: colors.bg, border: `1.5px solid ${colors.border}` }}
    >
      <div className="text-3xl leading-none">{CAT_ICONS[cat.slug] || "🌸"}</div>
      <div>
        <div className="font-bold text-[13px] leading-snug" style={{ color: colors.text }}>
          {cat.label}
        </div>
        <div className="text-[11px] mt-0.5 text-apple-gray">{cat.count.toLocaleString("ru-RU")} товаров</div>
      </div>
      <div
        className="flex items-center gap-1 text-[11px] font-semibold opacity-0 group-hover:opacity-100 transition-opacity"
        style={{ color: colors.text }}
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
    <div className="bg-white rounded-2xl overflow-hidden">
      <div className="aspect-square skeleton" />
      <div className="p-3.5 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3.5 skeleton rounded" />
        <div className="h-4 skeleton rounded w-1/2 mt-2" />
      </div>
    </div>
  );
}

export default function HomePage() {
  const { data: catalog, isLoading: catalogLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "name" }),
  });

  const { data: autoCategories, isLoading: catsLoading } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });

  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen">
      {/* ── Hero ── */}
      <section
        className="relative overflow-hidden"
        style={{ background: "linear-gradient(135deg, #150a2e 0%, #0f1840 50%, #0a1628 100%)" }}
      >
        {/* Decorative blobs */}
        <div
          className="absolute top-[-80px] left-[15%] w-[500px] h-[500px] rounded-full pointer-events-none"
          style={{ background: "radial-gradient(circle, rgba(124,58,237,0.25) 0%, transparent 70%)" }}
        />
        <div
          className="absolute bottom-[-60px] right-[10%] w-[400px] h-[400px] rounded-full pointer-events-none"
          style={{ background: "radial-gradient(circle, rgba(99,102,241,0.2) 0%, transparent 70%)" }}
        />

        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-20 md:py-32 text-center relative z-10">
          <div
            className="inline-flex items-center gap-2 text-[11px] font-bold px-4 py-2 rounded-full mb-8 tracking-widest uppercase"
            style={{ background: "rgba(255,255,255,0.08)", color: "rgba(255,255,255,0.7)", border: "1px solid rgba(255,255,255,0.12)" }}
          >
            ✦ &nbsp;Оригинальная парфюмерия&nbsp; ✦
          </div>

          <h1 className="text-5xl md:text-7xl lg:text-8xl font-bold tracking-tighter leading-none mb-5">
            <span className="text-white">Magic&nbsp;</span>
            <span
              style={{
                background: "linear-gradient(90deg, #c084fc 0%, #818cf8 50%, #38bdf8 100%)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
              }}
            >
              Vibes
            </span>
          </h1>

          <p className="text-base md:text-lg max-w-xl mx-auto leading-relaxed mb-10" style={{ color: "rgba(255,255,255,0.55)" }}>
            22&nbsp;000+ ароматов от мировых домов. Оригинальная продукция, быстрая доставка по России.
          </p>

          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Link
              to="/catalog"
              className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5"
              style={{ background: "#fff", color: "#1d1d1f", boxShadow: "0 8px 32px rgba(0,0,0,0.35)" }}
            >
              Смотреть каталог <ArrowRight size={18} />
            </Link>
            <Link
              to="/catalog?sort=price_desc"
              className="inline-flex items-center justify-center gap-2 font-semibold px-8 py-4 rounded-2xl text-[15px] transition-all duration-200 hover:-translate-y-0.5"
              style={{ background: "rgba(255,255,255,0.1)", color: "#fff", border: "1px solid rgba(255,255,255,0.18)", backdropFilter: "blur(8px)" }}
            >
              Топ ароматов
            </Link>
          </div>
        </div>
      </section>

      {/* ── Perks ── */}
      <section className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-5">
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            {PERKS.map(({ icon: Icon, title, text }) => (
              <div key={title} className="flex items-center gap-3 p-4 rounded-2xl bg-apple-gray-bg">
                <div className="w-9 h-9 rounded-xl bg-violet-50 flex items-center justify-center flex-shrink-0">
                  <Icon size={17} className="text-violet-600" />
                </div>
                <div>
                  <div className="text-[13px] font-semibold text-apple-black leading-tight">{title}</div>
                  <div className="text-[11px] text-apple-gray mt-0.5">{text}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Categories ── */}
      <section className="bg-apple-gray-bg py-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="flex items-end justify-between mb-7">
            <div>
              <h2 className="text-2xl md:text-3xl font-bold text-apple-black tracking-tight">Каталог</h2>
              <p className="text-apple-gray text-sm mt-1">Выберите категорию</p>
            </div>
            <Link to="/catalog" className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium text-violet-600 hover:text-violet-800 transition-colors">
              Все товары <ArrowRight size={14} />
            </Link>
          </div>

          {catsLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
              {Array.from({ length: 8 }).map((_, i) => <CatSkeleton key={i} />)}
            </div>
          ) : autoCategories && autoCategories.length > 0 ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
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

      {/* ── Featured products ── */}
      <section className="bg-apple-gray-bg pb-14">
        <div className="max-w-7xl mx-auto px-4 sm:px-6">
          <div className="pt-2 pb-7 flex items-end justify-between">
            <div>
              <h2 className="text-2xl md:text-3xl font-bold text-apple-black tracking-tight">Хиты продаж</h2>
              <p className="text-apple-gray text-sm mt-1">Популярные ароматы нашего каталога</p>
            </div>
            <Link to="/catalog" className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium text-violet-600 hover:text-violet-800 transition-colors">
              Все товары <ArrowRight size={14} />
            </Link>
          </div>

          {catalogLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
              {Array.from({ length: 12 }).map((_, i) => <ProductSkeleton key={i} />)}
            </div>
          ) : catalog?.products.length ? (
            <>
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
                {catalog.products.map((p) => <ProductCard key={p.offerId} product={p} />)}
              </div>
              <div className="text-center mt-8">
                <Link
                  to="/catalog"
                  className="inline-flex items-center gap-2 bg-white hover:bg-gray-50 border border-gray-200 hover:border-violet-300 text-apple-black font-medium px-8 py-3.5 rounded-2xl transition-all duration-200 text-sm"
                >
                  Показать все товары <ArrowRight size={15} />
                </Link>
              </div>
            </>
          ) : (
            <div className="text-center py-16 text-apple-gray">
              <p>Загрузка товаров...</p>
            </div>
          )}
        </div>
      </section>

      {/* ── Brand marquee ── */}
      <section className="bg-white border-t border-gray-100 overflow-hidden py-8">
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
                className="inline-flex items-center mx-6 text-[13px] font-semibold text-apple-gray hover:text-apple-black transition-colors whitespace-nowrap"
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
