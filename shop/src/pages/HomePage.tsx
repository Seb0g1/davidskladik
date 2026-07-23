import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Truck, Shield, RefreshCw, Headphones, ArrowRight, Sparkles, Flame, Star } from "lucide-react";
import { api } from "../api";
import CategoryGrid from "../components/CategoryGrid";
import BrandStrip from "../components/BrandStrip";
import ProductCard from "../components/ProductCard";

const PERKS = [
  { icon: Truck,       title: "Доставка Ozon",    text: "По всей России" },
  { icon: Shield,      title: "100% оригинал",    text: "Гарантия подлинности" },
  { icon: RefreshCw,   title: "Возврат 14 дней",  text: "Без вопросов" },
  { icon: Headphones,  title: "Поддержка 24/7",   text: "Всегда на связи" },
];

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden">
      <div className="aspect-square skeleton" />
      <div className="p-4 space-y-2">
        <div className="h-3 skeleton rounded w-1/3" />
        <div className="h-4 skeleton rounded" />
        <div className="h-4 skeleton rounded w-3/4" />
        <div className="h-6 skeleton rounded w-1/2 mt-3" />
      </div>
    </div>
  );
}

function HeroSection() {
  return (
    <div className="relative overflow-hidden rounded-3xl bg-[#0F0A1E] min-h-[420px] md:min-h-[500px] flex items-center">
      {/* Gradient blobs */}
      <div className="absolute -top-24 -left-24 w-96 h-96 bg-violet-700/40 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute -bottom-20 -right-20 w-80 h-80 bg-pink-600/30 rounded-full blur-3xl pointer-events-none" />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[200px] bg-violet-900/20 rounded-full blur-2xl pointer-events-none" />

      {/* Floating decorative elements */}
      <div className="absolute right-8 top-10 text-violet-400/30 animate-float pointer-events-none hidden md:block">
        <Sparkles size={64} />
      </div>
      <div className="absolute right-28 bottom-16 text-pink-400/20 animate-float-slow pointer-events-none hidden md:block">
        <Star size={48} />
      </div>
      <div className="absolute left-1/2 top-8 text-violet-300/10 animate-float-fast pointer-events-none hidden lg:block">
        <Sparkles size={32} />
      </div>

      {/* Content */}
      <div className="relative z-10 max-w-2xl mx-auto px-8 py-12 text-center">
        <div className="inline-flex items-center gap-2 bg-white/10 backdrop-blur-sm border border-white/15 rounded-full px-4 py-1.5 text-xs text-violet-200 font-medium mb-6 fade-up">
          <Flame size={12} className="text-rose-400" />
          Оригинальная парфюмерия · Быстрая доставка
        </div>
        <h1 className="font-display text-4xl md:text-6xl font-bold text-white leading-tight mb-4 fade-up-2">
          Мировая<br />
          <span className="bg-gradient-to-r from-violet-300 via-pink-300 to-rose-300 bg-clip-text text-transparent">
            парфюмерия
          </span>
        </h1>
        <p className="text-gray-300 text-base md:text-lg mb-8 max-w-md mx-auto leading-relaxed fade-up-3">
          Более 22 000 ароматов от мировых домов. Ozon Pay · Доставка по России
        </p>
        <div className="flex flex-col sm:flex-row gap-3 justify-center fade-up-3">
          <Link
            to="/catalog"
            className="inline-flex items-center gap-2 bg-gradient-to-r from-violet-600 to-violet-700 hover:from-violet-500 hover:to-violet-600 text-white font-semibold px-8 py-3.5 rounded-xl transition-all duration-200 shadow-lg shadow-violet-900/40 hover:shadow-violet-900/60 hover:-translate-y-0.5"
          >
            Смотреть каталог <ArrowRight size={16} />
          </Link>
          <Link
            to="/catalog?sort=price_desc"
            className="inline-flex items-center gap-2 bg-white/10 backdrop-blur-sm border border-white/20 hover:bg-white/20 text-white font-semibold px-8 py-3.5 rounded-xl transition-all duration-200 hover:-translate-y-0.5"
          >
            Лучшие новинки
          </Link>
        </div>
      </div>
    </div>
  );
}

export default function HomePage() {
  const { data: catalogData, isLoading } = useQuery({
    queryKey: ["shop-home-products"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "price_desc" }),
  });

  const { data: categories } = useQuery({
    queryKey: ["shop-categories"],
    queryFn: () => api.categories(),
    staleTime: 10 * 60 * 1000,
  });

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-12">

      {/* Hero */}
      <HeroSection />

      {/* Perks */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {PERKS.map(({ icon: Icon, title, text }) => (
          <div key={title} className="flex items-center gap-3 bg-white rounded-2xl p-4 shadow-sm border border-gray-100 hover:border-violet-200 transition-colors">
            <div className="w-10 h-10 rounded-xl bg-violet-50 flex items-center justify-center flex-shrink-0">
              <Icon size={18} className="text-violet-600" />
            </div>
            <div>
              <div className="text-sm font-semibold text-gray-800 leading-tight">{title}</div>
              <div className="text-xs text-gray-400 mt-0.5">{text}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Categories */}
      <CategoryGrid categories={categories} />

      {/* Featured products */}
      <section>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-2xl font-display font-bold text-gray-900">Популярные товары</h2>
            <p className="text-sm text-gray-400 mt-1">Топ продаж среди наших покупателей</p>
          </div>
          <Link
            to="/catalog"
            className="hidden sm:flex items-center gap-1.5 text-sm text-violet-600 font-medium hover:text-violet-800 transition-colors"
          >
            Все товары <ArrowRight size={14} />
          </Link>
        </div>

        {isLoading ? (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-4">
            {Array.from({ length: 12 }).map((_, i) => <ProductSkeleton key={i} />)}
          </div>
        ) : catalogData?.products.length ? (
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-4">
            {catalogData.products.map((p) => (
              <ProductCard key={p.offerId} product={p} />
            ))}
          </div>
        ) : (
          <div className="text-center py-16 text-gray-400">
            <p className="text-lg">Загрузка товаров...</p>
          </div>
        )}

        <div className="text-center mt-8">
          <Link
            to="/catalog"
            className="inline-flex items-center gap-2 bg-white border border-gray-200 hover:border-violet-400 hover:text-violet-700 text-gray-700 font-medium px-8 py-3 rounded-xl transition-all duration-200"
          >
            Показать все товары <ArrowRight size={16} />
          </Link>
        </div>
      </section>

      {/* Promo strip */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          {
            title: "Пробники",
            text: "Миниатюры и сэмплы",
            desc: "Попробуй перед покупкой",
            color: "from-pink-600 to-rose-700",
            slug: "samples",
          },
          {
            title: "Скидки",
            text: "До −55% на хиты",
            desc: "Ограниченное предложение",
            color: "from-violet-700 to-indigo-800",
            slug: "sale",
          },
          {
            title: "Aroma Box",
            text: "Наборы и подарки",
            desc: "Идея для подарка",
            color: "from-amber-600 to-orange-700",
            slug: "sets",
          },
        ].map((item) => (
          <Link
            key={item.slug}
            to={`/catalog/${item.slug}`}
            className={`bg-gradient-to-br ${item.color} rounded-2xl p-6 text-white group hover:opacity-95 transition-all hover:-translate-y-0.5 duration-200`}
          >
            <div className="text-xs font-semibold uppercase tracking-widest text-white/60 mb-1">{item.desc}</div>
            <div className="text-xl font-display font-bold mb-1">{item.title}</div>
            <div className="text-2xl font-black mb-4">{item.text}</div>
            <div className="inline-flex items-center gap-1.5 text-sm font-medium bg-white/15 rounded-lg px-3 py-1.5 group-hover:bg-white/25 transition-colors">
              Смотреть <ArrowRight size={13} />
            </div>
          </Link>
        ))}
      </div>

      {/* Brand marquee */}
      <BrandStrip />
    </div>
  );
}
