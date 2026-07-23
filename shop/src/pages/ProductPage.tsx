import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { ShoppingBag, Check, ChevronLeft, Star, Shield, Truck, RefreshCw, Minus, Plus } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import clsx from "clsx";

export default function ProductPage() {
  const { offerId } = useParams<{ offerId: string }>();
  const { add } = useCart();
  const [qty, setQty] = useState(1);
  const [activeImg, setActiveImg] = useState(0);
  const [added, setAdded] = useState(false);

  const { data: product, isLoading, error } = useQuery({
    queryKey: ["shop-product", offerId],
    queryFn: () => api.product(offerId!),
    enabled: !!offerId,
  });

  function handleAdd() {
    if (!product || !product.inStock) return;
    add(product, qty);
    setAdded(true);
    setTimeout(() => setAdded(false), 2000);
  }

  if (isLoading) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-10 animate-pulse">
          <div className="aspect-square bg-gray-200 rounded-2xl" />
          <div className="space-y-4">
            <div className="h-4 bg-gray-200 rounded w-1/4" />
            <div className="h-8 bg-gray-200 rounded w-3/4" />
            <div className="h-4 bg-gray-200 rounded w-1/3" />
            <div className="h-10 bg-gray-200 rounded w-1/3 mt-4" />
            <div className="h-12 bg-gray-200 rounded-xl mt-4" />
          </div>
        </div>
      </div>
    );
  }

  if (error || !product) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-16 text-center">
        <div className="text-4xl mb-4">😔</div>
        <h2 className="text-xl font-semibold text-gray-700 mb-2">Товар не найден</h2>
        <Link to="/catalog" className="text-violet-600 hover:underline">← Вернуться в каталог</Link>
      </div>
    );
  }

  const discount = product.oldPriceRub
    ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100)
    : 0;

  return (
    <div className="max-w-7xl mx-auto px-4 py-6">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-gray-500 mb-6">
        <Link to="/" className="hover:text-violet-600">Главная</Link>
        <span>/</span>
        <Link to="/catalog" className="hover:text-violet-600">Каталог</Link>
        <span>/</span>
        <span className="text-gray-800 truncate max-w-xs">{product.name}</span>
      </nav>

      <Link to="/catalog" className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-violet-600 mb-6 group">
        <ChevronLeft size={16} className="group-hover:-translate-x-0.5 transition-transform" />
        Назад к каталогу
      </Link>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
        {/* Images */}
        <div>
          <div className="aspect-square bg-gray-50 rounded-2xl overflow-hidden mb-3">
            {product.images[activeImg] ? (
              <img
                src={product.images[activeImg]}
                alt={product.name}
                className="w-full h-full object-contain p-8"
              />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-gray-200 text-7xl font-display">
                {product.brand?.[0] ?? "?"}
              </div>
            )}
          </div>
          {product.images.length > 1 && (
            <div className="flex gap-2 overflow-x-auto">
              {product.images.map((img, i) => (
                <button
                  key={i}
                  onClick={() => setActiveImg(i)}
                  className={clsx(
                    "flex-shrink-0 w-16 h-16 rounded-xl overflow-hidden border-2 transition-colors",
                    i === activeImg ? "border-violet-500" : "border-gray-200 hover:border-gray-300"
                  )}
                >
                  <img src={img} alt="" className="w-full h-full object-contain p-1" />
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Info */}
        <div>
          <div className="text-sm text-violet-600 font-semibold uppercase tracking-wider mb-2">{product.brand}</div>
          <h1 className="text-2xl md:text-3xl font-display font-bold text-gray-900 mb-2 leading-snug">{product.name}</h1>

          {product.volume && (
            <div className="inline-block bg-gray-100 text-gray-600 text-sm px-3 py-1 rounded-lg mb-4">{product.volume}</div>
          )}

          {/* Rating */}
          {product.rating !== undefined && product.rating > 0 && (
            <div className="flex items-center gap-2 mb-4">
              <div className="flex">
                {[1,2,3,4,5].map((s) => (
                  <Star key={s} size={16} className={clsx("transition-colors", s <= Math.round(product.rating!) ? "text-amber-400 fill-amber-400" : "text-gray-200 fill-gray-200")} />
                ))}
              </div>
              <span className="text-sm font-medium text-gray-700">{product.rating.toFixed(1)}</span>
              {product.reviewCount && <span className="text-sm text-gray-400">({product.reviewCount} отзывов)</span>}
            </div>
          )}

          {/* Price */}
          <div className="flex items-end gap-3 mb-6">
            <div className="text-4xl font-bold text-gray-900">{product.priceRub.toLocaleString("ru-RU")} ₽</div>
            {product.oldPriceRub && (
              <>
                <div className="text-lg text-gray-400 line-through mb-1">{product.oldPriceRub.toLocaleString("ru-RU")} ₽</div>
                <div className="bg-rose-100 text-rose-600 text-sm font-bold px-2 py-0.5 rounded-lg mb-1">-{discount}%</div>
              </>
            )}
          </div>

          {/* Stock */}
          <div className={clsx("flex items-center gap-2 mb-6 text-sm font-medium", product.inStock ? "text-green-600" : "text-gray-400")}>
            <div className={clsx("w-2 h-2 rounded-full", product.inStock ? "bg-green-500" : "bg-gray-300")} />
            {product.inStock ? `В наличии${product.stockQty > 0 ? ` (${product.stockQty} шт.)` : ""}` : "Нет в наличии"}
          </div>

          {/* Qty + Add */}
          {product.inStock && (
            <div className="flex items-center gap-3 mb-6">
              <div className="flex items-center border border-gray-200 rounded-xl overflow-hidden">
                <button
                  onClick={() => setQty((q) => Math.max(1, q - 1))}
                  className="px-3 py-3 hover:bg-gray-50 transition-colors"
                >
                  <Minus size={16} />
                </button>
                <span className="px-4 py-3 text-sm font-semibold min-w-[3rem] text-center">{qty}</span>
                <button
                  onClick={() => setQty((q) => Math.min(product.stockQty, q + 1))}
                  className="px-3 py-3 hover:bg-gray-50 transition-colors"
                >
                  <Plus size={16} />
                </button>
              </div>
              <button
                onClick={handleAdd}
                className={clsx(
                  "flex-1 flex items-center justify-center gap-2 py-3.5 rounded-xl font-semibold text-sm transition-all duration-200",
                  added
                    ? "bg-green-500 text-white"
                    : "bg-violet-600 text-white hover:bg-violet-700 active:scale-[0.98]"
                )}
              >
                {added ? <><Check size={18} /> Добавлено!</> : <><ShoppingBag size={18} /> В корзину</>}
              </button>
            </div>
          )}

          {/* Perks */}
          <div className="grid grid-cols-3 gap-3 mb-6">
            {[
              { icon: Truck, text: "Доставка Ozon" },
              { icon: Shield, text: "100% оригинал" },
              { icon: RefreshCw, text: "Возврат 14 дней" },
            ].map(({ icon: Icon, text }) => (
              <div key={text} className="flex flex-col items-center gap-1.5 bg-gray-50 rounded-xl p-3 text-center">
                <Icon size={18} className="text-violet-600" />
                <span className="text-xs text-gray-600 font-medium leading-tight">{text}</span>
              </div>
            ))}
          </div>

          {/* Description */}
          {product.description && (
            <div>
              <h3 className="font-semibold text-gray-800 mb-2">Описание</h3>
              <p className="text-sm text-gray-600 leading-relaxed">{product.description}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
