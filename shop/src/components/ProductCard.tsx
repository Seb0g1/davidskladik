import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import clsx from "clsx";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const PLACEHOLDER_COLORS = [
  "from-violet-100 to-purple-50",
  "from-pink-100 to-rose-50",
  "from-amber-100 to-yellow-50",
  "from-blue-100 to-indigo-50",
  "from-teal-100 to-emerald-50",
];

function placeholderColor(str: string) {
  const n = (str?.charCodeAt(0) ?? 0) % PLACEHOLDER_COLORS.length;
  return PLACEHOLDER_COLORS[n];
}

interface Props {
  product: ShopProduct;
}

export default function ProductCard({ product }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const [imgError, setImgError] = useState(false);

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    if (!product.inStock) return;
    add(product);
    setAdded(true);
    setTimeout(() => setAdded(false), 1800);
  }

  const img = (!imgError && product.images[0]) || "";
  const gradient = placeholderColor(product.brand || product.name);
  const initial = (product.brand || product.name || "?")[0]?.toUpperCase();

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="group bg-white rounded-2xl overflow-hidden transition-all duration-300 hover:shadow-card-hover hover:-translate-y-0.5 block"
      style={{ boxShadow: "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03)" }}
    >
      {/* Image */}
      <div className={clsx("relative aspect-square overflow-hidden bg-gradient-to-br", img ? "bg-apple-gray-bg" : gradient)}>
        {img ? (
          <img
            src={img}
            alt={product.name}
            className="w-full h-full object-contain p-4 group-hover:scale-[1.04] transition-transform duration-500"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center">
            <span className="text-4xl font-bold text-black/10">{initial}</span>
          </div>
        )}

        {!product.inStock && (
          <div className="absolute inset-0 bg-white/70 backdrop-blur-[2px] flex items-center justify-center">
            <span className="text-xs font-medium text-apple-gray bg-white rounded-full px-3 py-1.5 shadow-sm">
              Нет в наличии
            </span>
          </div>
        )}

        {/* Cart button — shown on hover */}
        <div className={clsx(
          "absolute bottom-3 right-3 transition-all duration-200",
          product.inStock ? "opacity-0 group-hover:opacity-100 translate-y-1 group-hover:translate-y-0" : "hidden"
        )}>
          <button
            onClick={handleAdd}
            className={clsx(
              "w-9 h-9 rounded-xl flex items-center justify-center shadow-lg transition-all duration-200",
              added ? "bg-emerald-500 text-white" : "bg-violet-600 text-white hover:bg-violet-500 active:scale-90"
            )}
          >
            {added ? <Check size={15} strokeWidth={2.5} /> : <ShoppingBag size={15} />}
          </button>
        </div>
      </div>

      {/* Info */}
      <div className="p-3.5">
        {product.brand && (
          <p className="text-[11px] font-semibold text-apple-gray uppercase tracking-wider mb-1 truncate">
            {product.brand}
          </p>
        )}
        <p className="text-[13px] font-medium text-apple-black line-clamp-2 leading-snug">
          {product.name}
        </p>
        {product.volume && (
          <p className="text-[11px] text-apple-gray mt-0.5">{product.volume}</p>
        )}
        <div className="flex items-center justify-between mt-2.5">
          <span className="text-[15px] font-bold text-apple-black tracking-tight">
            {product.priceRub.toLocaleString("ru-RU")} ₽
          </span>
          {product.oldPriceRub && (
            <span className="text-[11px] text-apple-gray line-through">
              {product.oldPriceRub.toLocaleString("ru-RU")} ₽
            </span>
          )}
        </div>
      </div>
    </Link>
  );
}
