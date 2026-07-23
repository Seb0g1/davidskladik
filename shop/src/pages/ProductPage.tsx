import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { ShoppingBag, Check, ChevronLeft, Star, Shield, Truck, RefreshCw, Minus, Plus, ChevronRight } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import clsx from "clsx";

export default function ProductPage() {
  const { offerId } = useParams<{ offerId: string }>();
  const { add } = useCart();
  const [qty, setQty] = useState(1);
  const [activeImg, setActiveImg] = useState(0);
  const [added, setAdded] = useState(false);
  const [imgErrors, setImgErrors] = useState<Set<number>>(new Set());

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
      <div className="bg-apple-gray-bg min-h-screen">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
          <div className="bg-white rounded-3xl p-8 animate-pulse">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
              <div className="aspect-square skeleton rounded-2xl" />
              <div className="space-y-5">
                <div className="h-3 skeleton rounded w-1/4" />
                <div className="h-8 skeleton rounded w-3/4" />
                <div className="h-5 skeleton rounded w-1/3" />
                <div className="h-12 skeleton rounded-2xl w-1/3 mt-6" />
                <div className="h-14 skeleton rounded-2xl mt-4" />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !product) {
    return (
      <div className="bg-apple-gray-bg min-h-screen flex items-center justify-center">
        <div className="text-center py-16">
          <div className="text-5xl mb-4">😔</div>
          <h2 className="text-xl font-bold text-apple-black mb-2">Товар не найден</h2>
          <Link to="/catalog" className="text-violet-600 hover:text-violet-800 font-medium text-sm">← Вернуться в каталог</Link>
        </div>
      </div>
    );
  }

  const validImages = product.images.filter((_, i) => !imgErrors.has(i));
  const activeValidImg = validImages[activeImg] || null;
  const discount = product.oldPriceRub ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100) : 0;

  return (
    <div className="bg-apple-gray-bg min-h-screen">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
        {/* Breadcrumb */}
        <nav className="flex items-center gap-1.5 text-xs text-apple-gray mb-6 flex-wrap">
          <Link to="/" className="hover:text-apple-black transition-colors">Главная</Link>
          <ChevronRight size={12} />
          <Link to="/catalog" className="hover:text-apple-black transition-colors">Каталог</Link>
          <ChevronRight size={12} />
          {product.brand && (
            <><Link to={`/catalog?brand=${encodeURIComponent(product.brand)}`} className="hover:text-apple-black transition-colors">{product.brand}</Link><ChevronRight size={12} /></>
          )}
          <span className="text-apple-black truncate max-w-xs">{product.name}</span>
        </nav>

        <div className="bg-white rounded-3xl overflow-hidden" style={{ boxShadow: "0 2px 12px rgba(0,0,0,0.06)" }}>
          <div className="grid grid-cols-1 md:grid-cols-2">
            {/* Images */}
            <div className="bg-apple-gray-bg p-8 md:p-12 flex flex-col">
              <div className="aspect-square rounded-2xl overflow-hidden bg-white flex items-center justify-center mb-4 flex-1">
                {activeValidImg ? (
                  <img
                    src={activeValidImg}
                    alt={product.name}
                    className="w-full h-full object-contain p-6"
                    onError={() => setImgErrors((s) => new Set(s).add(activeImg))}
                  />
                ) : (
                  <div className="w-full h-full flex items-center justify-center">
                    <span className="text-7xl font-bold text-black/8">{product.brand?.[0] ?? "?"}</span>
                  </div>
                )}
              </div>
              {product.images.length > 1 && (
                <div className="flex gap-2.5 overflow-x-auto pb-1">
                  {product.images.map((img, i) => !imgErrors.has(i) && (
                    <button
                      key={i}
                      onClick={() => setActiveImg(i)}
                      className={clsx(
                        "flex-shrink-0 w-14 h-14 rounded-xl overflow-hidden border-2 bg-white transition-all",
                        i === activeImg ? "border-violet-500 shadow-sm" : "border-transparent hover:border-gray-300"
                      )}
                    >
                      <img src={img} alt="" className="w-full h-full object-contain p-1" onError={() => setImgErrors((s) => new Set(s).add(i))} />
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Info */}
            <div className="p-8 md:p-12 flex flex-col">
              <Link to="/catalog" className="inline-flex items-center gap-1 text-xs text-apple-gray hover:text-violet-600 mb-4 transition-colors w-fit group">
                <ChevronLeft size={14} className="group-hover:-translate-x-0.5 transition-transform" />
                Назад к каталогу
              </Link>

              {product.brand && (
                <div className="text-xs font-bold uppercase tracking-[0.12em] text-violet-600 mb-3">{product.brand}</div>
              )}
              <h1 className="text-2xl md:text-3xl font-bold text-apple-black tracking-tight leading-snug mb-3">
                {product.name}
              </h1>

              {product.volume && (
                <span className="inline-block text-sm text-apple-gray bg-apple-gray-bg px-3 py-1 rounded-lg mb-4">{product.volume}</span>
              )}

              {product.rating && product.rating > 0 ? (
                <div className="flex items-center gap-2 mb-4">
                  <div className="flex">
                    {[1,2,3,4,5].map((s) => (
                      <Star key={s} size={14} className={s <= Math.round(product.rating!) ? "text-amber-400 fill-amber-400" : "text-gray-200 fill-gray-200"} />
                    ))}
                  </div>
                  <span className="text-sm font-medium text-apple-black">{product.rating.toFixed(1)}</span>
                  {product.reviewCount && <span className="text-sm text-apple-gray">{product.reviewCount} отзывов</span>}
                </div>
              ) : null}

              {/* Price */}
              <div className="flex items-baseline gap-3 mb-2">
                <span className="text-4xl font-bold text-apple-black tracking-tight">
                  {product.priceRub.toLocaleString("ru-RU")} ₽
                </span>
                {product.oldPriceRub && (
                  <>
                    <span className="text-lg text-apple-gray line-through">{product.oldPriceRub.toLocaleString("ru-RU")} ₽</span>
                    <span className="text-sm bg-rose-100 text-rose-600 font-semibold px-2 py-0.5 rounded-lg">−{discount}%</span>
                  </>
                )}
              </div>

              {/* Stock */}
              <div className={clsx("flex items-center gap-2 mb-6 text-sm font-medium", product.inStock ? "text-emerald-600" : "text-apple-gray")}>
                <div className={clsx("w-2 h-2 rounded-full", product.inStock ? "bg-emerald-500" : "bg-gray-300")} />
                {product.inStock ? `В наличии${product.stockQty > 0 ? ` · ${product.stockQty} шт.` : ""}` : "Нет в наличии"}
              </div>

              {product.inStock && (
                <div className="flex items-center gap-3 mb-6">
                  <div className="flex items-center bg-apple-gray-bg rounded-xl overflow-hidden">
                    <button onClick={() => setQty((q) => Math.max(1, q - 1))} className="px-4 py-3.5 hover:bg-gray-200 transition-colors"><Minus size={15} /></button>
                    <span className="px-4 py-3.5 text-sm font-semibold min-w-[3rem] text-center">{qty}</span>
                    <button onClick={() => setQty((q) => Math.min(product.stockQty || 99, q + 1))} className="px-4 py-3.5 hover:bg-gray-200 transition-colors"><Plus size={15} /></button>
                  </div>
                  <button
                    onClick={handleAdd}
                    className={clsx(
                      "flex-1 flex items-center justify-center gap-2.5 py-3.5 rounded-2xl font-semibold text-[15px] transition-all duration-200",
                      added
                        ? "bg-emerald-500 text-white"
                        : "bg-violet-600 text-white hover:bg-violet-700 active:scale-[0.98] shadow-lg shadow-violet-200"
                    )}
                  >
                    {added ? <><Check size={18} /> Добавлено!</> : <><ShoppingBag size={18} /> В корзину</>}
                  </button>
                </div>
              )}

              {/* Perks */}
              <div className="grid grid-cols-3 gap-3 mb-6">
                {[
                  { icon: Truck,     text: "Доставка Ozon" },
                  { icon: Shield,    text: "100% оригинал" },
                  { icon: RefreshCw, text: "Возврат 14 дней" },
                ].map(({ icon: Icon, text }) => (
                  <div key={text} className="flex flex-col items-center gap-2 bg-apple-gray-bg rounded-xl p-3 text-center">
                    <Icon size={16} className="text-violet-600" />
                    <span className="text-[11px] text-apple-gray font-medium leading-tight">{text}</span>
                  </div>
                ))}
              </div>

              {product.description && (
                <div>
                  <h3 className="text-sm font-semibold text-apple-black mb-2">Описание</h3>
                  <p className="text-sm text-apple-gray leading-relaxed">{product.description}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
