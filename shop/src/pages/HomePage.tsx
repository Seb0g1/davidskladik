import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, Truck, Shield, RefreshCw, Headphones } from "lucide-react";
import { api } from "../api";
import ProductCard from "../components/ProductCard";

const PERKS = [
  { icon: Truck,       title: "Доставка Ozon",   text: "По всей России, 1–5 дней" },
  { icon: Shield,      title: "100% оригинал",   text: "Гарантия подлинности" },
  { icon: RefreshCw,   title: "Возврат 14 дней", text: "Без вопросов" },
  { icon: Headphones,  title: "Поддержка 24/7",  text: "Всегда на связи" },
];

const BRANDS = [
  "Chanel", "Dior", "Tom Ford", "Hermès", "Byredo", "Jo Malone",
  "Creed", "Guerlain", "Givenchy", "Prada", "Valentino", "Burberry",
  "Versace", "Hugo Boss", "Montale", "Kilian", "Dolce & Gabbana", "Lancome",
];

function Skeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden" style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04)" }}>
      <div className="aspect-square skeleton" />
      <div className="p-3.5 space-y-2">
        <div className="h-2.5 skeleton rounded w-1/3" />
        <div className="h-3.5 skeleton rounded" />
        <div className="h-3.5 skeleton rounded w-2/3" />
        <div className="h-4 skeleton rounded w-1/2 mt-2" />
      </div>
    </div>
  );
}

export default function HomePage() {
  const { data: catalog, isLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "price_desc" }),
  });

  const doubled = [...BRANDS, ...BRANDS];

  return (
    <div className="min-h-screen">
      {/* ── Hero ── */}
      <section className="bg-white border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-16 md:py-24 text-center">
          <div className="inline-flex items-center gap-2 bg-violet-50 text-violet-700 text-xs font-semibold px-4 py-2 rounded-full mb-8 tracking-wide uppercase">
            Оригинальная парфюмерия
          </div>
          <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold text-apple-black tracking-tight leading-none mb-6">
            Мировая<br />
            <span className="text-violet-600">парфюмерия</span>
          </h1>
          <p className="text-lg md:text-xl text-apple-gray max-w-2xl mx-auto leading-relaxed mb-10">
            22 000+ ароматов от мировых домов. Оригинальная продукция, быстрая доставка по России через Ozon.
          </p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Link
              to="/catalog"
              className="inline-flex items-center justify-center gap-2 bg-violet-600 hover:bg-violet-700 text-white font-semibold px-8 py-4 rounded-2xl transition-all duration-200 hover:-translate-y-0.5 shadow-lg shadow-violet-200 text-[15px]"
            >
              Смотреть каталог <ArrowRight size={18} />
            </Link>
            <Link
              to="/catalog?sort=price_desc"
              className="inline-flex items-center justify-center gap-2 bg-apple-gray-bg hover:bg-gray-200 text-apple-black font-semibold px-8 py-4 rounded-2xl transition-all duration-200 hover:-translate-y-0.5 text-[15px]"
            >
              Лучшие новинки
            </Link>
          </div>
        </div>
      </section>

      {/* ── Perks ── */}
      <section className="bg-apple-gray-bg border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {PERKS.map(({ icon: Icon, title, text }) => (
              <div key={title} className="flex items-center gap-3 bg-white rounded-2xl p-4" style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04)" }}>
                <div className="w-10 h-10 rounded-xl bg-violet-50 flex items-center justify-center flex-shrink-0">
                  <Icon size={18} className="text-violet-600" />
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

      {/* ── Featured products ── */}
      <section className="bg-apple-gray-bg">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-12">
          <div className="flex items-end justify-between mb-8">
            <div>
              <h2 className="text-2xl md:text-3xl font-bold text-apple-black tracking-tight">Популярные товары</h2>
              <p className="text-apple-gray text-sm mt-1">Топ ароматов нашего каталога</p>
            </div>
            <Link to="/catalog" className="hidden sm:flex items-center gap-1.5 text-[13px] font-medium text-violet-600 hover:text-violet-800 transition-colors">
              Все товары <ArrowRight size={14} />
            </Link>
          </div>

          {isLoading ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
              {Array.from({ length: 12 }).map((_, i) => <Skeleton key={i} />)}
            </div>
          ) : catalog?.products.length ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-3">
              {catalog.products.map((p) => <ProductCard key={p.offerId} product={p} />)}
            </div>
          ) : (
            <div className="text-center py-16 text-apple-gray">
              <p>Товары загружаются...</p>
            </div>
          )}

          <div className="text-center mt-8">
            <Link
              to="/catalog"
              className="inline-flex items-center gap-2 bg-white hover:bg-gray-50 border border-gray-200 hover:border-violet-300 text-apple-black font-medium px-8 py-3.5 rounded-2xl transition-all duration-200 text-sm"
            >
              Показать все товары <ArrowRight size={15} />
            </Link>
          </div>
        </div>
      </section>

      {/* ── Collections strip ── */}
      <section className="bg-white border-t border-b border-gray-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-12">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {[
              { title: "Женская парфюмерия", sub: "Нежные и чувственные ароматы", slug: "women", bg: "bg-rose-50", text: "text-rose-700", btn: "bg-rose-600 hover:bg-rose-700" },
              { title: "Мужская парфюмерия", sub: "Свежие и древесные ноты", slug: "men",   bg: "bg-indigo-50", text: "text-indigo-700", btn: "bg-indigo-600 hover:bg-indigo-700" },
              { title: "Унисекс",            sub: "Для всех — смелые ароматы", slug: "unisex", bg: "bg-amber-50", text: "text-amber-700", btn: "bg-amber-600 hover:bg-amber-700" },
            ].map((c) => (
              <Link
                key={c.slug}
                to={`/catalog/${c.slug}`}
                className={`${c.bg} rounded-3xl p-8 group hover:shadow-card-hover transition-all duration-300 hover:-translate-y-1 block`}
              >
                <div className={`text-xs font-bold uppercase tracking-widest ${c.text} mb-3 opacity-60`}>Коллекция</div>
                <h3 className="text-xl font-bold text-apple-black mb-2 tracking-tight">{c.title}</h3>
                <p className="text-sm text-apple-gray mb-6">{c.sub}</p>
                <div className={`inline-flex items-center gap-2 ${c.btn} text-white text-sm font-semibold px-5 py-2.5 rounded-xl transition-colors`}>
                  Смотреть <ArrowRight size={14} className="group-hover:translate-x-0.5 transition-transform" />
                </div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ── Brand marquee ── */}
      <section className="bg-apple-gray-bg border-t border-gray-100 overflow-hidden py-8">
        <p className="text-center text-xs font-semibold uppercase tracking-widest text-apple-gray mb-6">
          Ведущие мировые бренды
        </p>
        <div className="relative">
          <div className="absolute left-0 top-0 bottom-0 w-20 bg-gradient-to-r from-apple-gray-bg to-transparent z-10 pointer-events-none" />
          <div className="absolute right-0 top-0 bottom-0 w-20 bg-gradient-to-l from-apple-gray-bg to-transparent z-10 pointer-events-none" />
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
