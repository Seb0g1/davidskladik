import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import clsx from "clsx";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const BRAND_GRADIENTS = [
  "from-violet-100 to-purple-200 text-violet-700",
  "from-pink-100 to-rose-200 text-rose-700",
  "from-amber-100 to-orange-200 text-amber-700",
  "from-blue-100 to-indigo-200 text-indigo-700",
  "from-teal-100 to-emerald-200 text-teal-700",
];

function brandColor(name: string) {
  const idx = (name?.charCodeAt(0) ?? 0) % BRAND_GRADIENTS.length;
  return BRAND_GRADIENTS[idx];
}

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
    setTimeout(() => setAdded(false), 1600);
  }

  const img = product.images[0] || "";
  const initials = (product.brand || product.name || "?").slice(0, 2).toUpperCase();
  const bgStyle = brandColor(product.brand || product.name || "");

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="group bg-white rounded-2xl overflow-hidden shadow-sm border border-gray-100 hover:border-violet-200 hover:shadow-lg transition-all duration-300 flex flex-col"
    >
      {/* Image */}
      <div className="relative aspect-square overflow-hidden bg-gray-50">
        {img ? (
          <img
            src={img}
            alt={product.name}
            className="w-full h-full object-contain p-3 group-hover:scale-105 transition-transform duration-500"
            loading="lazy"
          />
        ) : (
          <div className={`w-full h-full bg-gradient-to-br ${bgStyle} flex items-center justify-center`}>
            <span className="text-3xl font-display font-bold opacity-60">{initials}</span>
          </div>
        )}

        {!product.inStock && (
          <div className="absolute inset-0 bg-white/80 backdrop-blur-[1px] flex items-center justify-center">
            <span className="text-xs font-semibold text-gray-500 bg-white rounded-full px-3 py-1 shadow-sm">
              Нет в наличии
            </span>
          </div>
        )}
      </div>

      {/* Info */}
      <div className="p-3 flex flex-col flex-1">
        {product.brand && (
          <div className="text-[10px] font-bold uppercase tracking-widest text-violet-500 mb-1 truncate">
            {product.brand}
          </div>
        )}
        <div className="text-xs font-medium text-gray-800 line-clamp-2 flex-1 leading-snug">
          {product.name}
        </div>
        {product.volume && (
          <div className="text-[10px] text-gray-400 mt-1">{product.volume}</div>
        )}

        {/* Price + CTA */}
        <div className="flex items-center justify-between mt-3 gap-1">
          <div className="text-sm font-bold text-gray-900 leading-none">
            {product.priceRub.toLocaleString("ru-RU")} ₽
          </div>
          <button
            onClick={handleAdd}
            disabled={!product.inStock}
            className={clsx(
              "flex items-center justify-center w-8 h-8 rounded-xl transition-all duration-200 flex-shrink-0 text-xs",
              added
                ? "bg-emerald-500 text-white scale-90"
                : product.inStock
                ? "bg-violet-600 text-white hover:bg-violet-500 hover:scale-110 active:scale-95 shadow-sm shadow-violet-300"
                : "bg-gray-100 text-gray-300 cursor-not-allowed"
            )}
          >
            {added ? <Check size={14} strokeWidth={3} /> : <ShoppingBag size={14} />}
          </button>
        </div>
      </div>
    </Link>
  );
}
