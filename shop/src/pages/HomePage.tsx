import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Truck, Shield, RefreshCw, Headphones } from "lucide-react";
import { api } from "../api";
import BannerSlider from "../components/BannerSlider";
import CategoryGrid from "../components/CategoryGrid";
import BrandStrip from "../components/BrandStrip";
import ProductCard from "../components/ProductCard";

const PERKS = [
  { icon: Truck, title: "Доставка Ozon", text: "По всей России" },
  { icon: Shield, title: "100% оригинал", text: "Гарантия подлинности" },
  { icon: RefreshCw, title: "Возврат 14 дней", text: "Без вопросов" },
  { icon: Headphones, title: "Поддержка 24/7", text: "Всегда на связи" },
];

function ProductSkeleton() {
  return (
    <div className="bg-white rounded-2xl overflow-hidden shadow-card animate-pulse">
      <div className="aspect-square bg-gray-200" />
      <div className="p-4 space-y-2">
        <div className="h-3 bg-gray-200 rounded w-1/3" />
        <div className="h-4 bg-gray-200 rounded w-full" />
        <div className="h-4 bg-gray-200 rounded w-3/4" />
        <div className="h-6 bg-gray-200 rounded w-1/2 mt-3" />
      </div>
    </div>
  );
}

export default function HomePage() {
  const { data: catalogData, isLoading } = useQuery({
    queryKey: ["shop-home-products"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "name" }),
  });

  const { data: categories } = useQuery({
    queryKey: ["shop-categories"],
    queryFn: () => api.categories(),
    staleTime: 10 * 60 * 1000,
  });

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-10">
      {/* Hero */}
      <BannerSlider />

      {/* Perks strip */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {PERKS.map(({ icon: Icon, title, text }) => (
          <div key={title} className="flex items-center gap-3 bg-white rounded-xl p-4 shadow-card">
            <div className="w-10 h-10 rounded-lg bg-violet-50 flex items-center justify-center flex-shrink-0">
              <Icon size={20} className="text-violet-600" />
            </div>
            <div>
              <div className="text-sm font-semibold text-gray-800">{title}</div>
              <div className="text-xs text-gray-500">{text}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Categories */}
      <CategoryGrid categories={categories} />

      {/* Featured products */}
      <section>
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-2xl font-display font-bold text-gray-900">Популярные товары</h2>
          <Link
            to="/catalog"
            className="text-sm text-violet-600 font-medium hover:text-violet-800 transition-colors"
          >
            Смотреть все →
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
            <p className="text-lg">Товары загружаются...</p>
            <p className="text-sm mt-1">Попробуйте обновить страницу</p>
          </div>
        )}
      </section>

      {/* Promo banners */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { title: "Пробники и миниатюры", text: "Скидка до -50%", color: "from-pink-500 to-rose-600", slug: "samples" },
          { title: "Акции", text: "Скидки до -55%", color: "from-violet-600 to-purple-700", slug: "sale" },
          { title: "Aroma Box", text: "Наборы на любой вкус", color: "from-amber-500 to-orange-600", slug: "sets" },
        ].map((item) => (
          <Link
            key={item.slug}
            to={`/catalog/${item.slug}`}
            className={`bg-gradient-to-br ${item.color} rounded-2xl p-6 text-white hover:opacity-90 transition-opacity group`}
          >
            <div className="text-xs font-semibold uppercase tracking-widest text-white/70 mb-1">Специальное предложение</div>
            <div className="text-xl font-display font-bold mb-1">{item.title}</div>
            <div className="text-3xl font-black mb-4">{item.text}</div>
            <div className="text-sm font-medium group-hover:underline">Смотреть →</div>
          </Link>
        ))}
      </div>

      {/* Brand strip */}
      <BrandStrip />
    </div>
  );
}
