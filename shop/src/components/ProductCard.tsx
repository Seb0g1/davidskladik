import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check, Star } from "lucide-react";
import clsx from "clsx";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

interface Props {
  product: ShopProduct;
}

export default function ProductCard({ product }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    if (!product.inStock) return;
    add(product);
    setAdded(true);
    setTimeout(() => setAdded(false), 1500);
  }

  const img = product.images[0] || "";
  const discount = product.oldPriceRub
    ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100)
    : 0;

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="group bg-white rounded-2xl overflow-hidden shadow-card hover:shadow-card-hover transition-all duration-300 flex flex-col"
    >
      {/* Image */}
      <div className="relative aspect-square bg-gray-50 overflow-hidden">
        {img ? (
          <img
            src={img}
            alt={product.name}
            className="w-full h-full object-contain p-4 group-hover:scale-105 transition-transform duration-500"
            loading="lazy"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-gray-300 text-4xl font-display">
            {product.brand?.[0] ?? "?"}
          </div>
        )}
        {discount > 0 && (
          <div className="absolute top-3 left-3 bg-rose-500 text-white text-xs font-bold px-2 py-1 rounded-lg">
            -{discount}%
          </div>
        )}
        {!product.inStock && (
          <div className="absolute inset-0 bg-white/70 flex items-center justify-center">
            <span className="text-gray-500 text-sm font-medium">Нет в наличии</span>
          </div>
        )}
      </div>

      {/* Info */}
      <div className="p-4 flex flex-col flex-1">
        <div className="text-xs text-violet-600 font-semibold uppercase tracking-wider mb-1">
          {product.brand}
        </div>
        <div className="text-sm font-medium text-gray-800 line-clamp-2 flex-1 mb-2">
          {product.name}
        </div>
        {product.volume && (
          <div className="text-xs text-gray-400 mb-2">{product.volume}</div>
        )}

        {/* Rating */}
        {product.rating !== undefined && product.rating > 0 && (
          <div className="flex items-center gap-1 mb-2">
            <Star size={12} className="text-amber-400 fill-amber-400" />
            <span className="text-xs text-gray-500">{product.rating.toFixed(1)}</span>
            {product.reviewCount ? (
              <span className="text-xs text-gray-400">({product.reviewCount})</span>
            ) : null}
          </div>
        )}

        {/* Price + CTA */}
        <div className="flex items-end justify-between mt-auto">
          <div>
            <div className="text-lg font-bold text-gray-900">
              {product.priceRub.toLocaleString("ru-RU")} ₽
            </div>
            {product.oldPriceRub && (
              <div className="text-xs text-gray-400 line-through">
                {product.oldPriceRub.toLocaleString("ru-RU")} ₽
              </div>
            )}
          </div>
          <button
            onClick={handleAdd}
            disabled={!product.inStock}
            className={clsx(
              "flex items-center justify-center w-10 h-10 rounded-xl transition-all duration-200 flex-shrink-0",
              added
                ? "bg-green-500 text-white scale-95"
                : product.inStock
                ? "bg-violet-600 text-white hover:bg-violet-700 hover:scale-105 active:scale-95"
                : "bg-gray-100 text-gray-300 cursor-not-allowed"
            )}
          >
            {added ? <Check size={18} /> : <ShoppingBag size={18} />}
          </button>
        </div>
      </div>
    </Link>
  );
}
