import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import clsx from "clsx";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const PLACEHOLDER_BG = [
  "#f5f3ff",
  "#fdf2f8",
  "#eff6ff",
  "#f0fdf4",
  "#fffbeb",
];

function placeholderBg(str: string) {
  const n = (str?.charCodeAt(0) ?? 0) % PLACEHOLDER_BG.length;
  return PLACEHOLDER_BG[n];
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
  const bg = placeholderBg(product.brand || product.name);
  const initial = (product.brand || product.name || "?")[0]?.toUpperCase();

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="group flex flex-col bg-white rounded-2xl overflow-hidden transition-all duration-300 hover:-translate-y-1 block"
      style={{
        border: "1px solid #f0f0f2",
        boxShadow: "0 1px 4px rgba(0,0,0,0.04)",
      }}
      onMouseEnter={e => {
        (e.currentTarget as HTMLElement).style.boxShadow = "0 12px 36px rgba(0,0,0,0.1)";
        (e.currentTarget as HTMLElement).style.borderColor = "#e8e8ec";
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLElement).style.boxShadow = "0 1px 4px rgba(0,0,0,0.04)";
        (e.currentTarget as HTMLElement).style.borderColor = "#f0f0f2";
      }}
    >
      {/* Image */}
      <div
        className="relative overflow-hidden"
        style={{ aspectRatio: "1/1", background: img ? "#f8f8fa" : bg }}
      >
        {img ? (
          <img
            src={img}
            alt={product.name}
            className="w-full h-full object-contain p-4 transition-transform duration-500 group-hover:scale-[1.05]"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center">
            <span className="text-3xl font-bold" style={{ color: "rgba(0,0,0,0.08)" }}>{initial}</span>
          </div>
        )}

        {!product.inStock && (
          <div className="absolute inset-0 flex items-center justify-center" style={{ background: "rgba(255,255,255,0.72)", backdropFilter: "blur(2px)" }}>
            <span className="text-[11px] font-semibold text-apple-gray bg-white rounded-full px-3 py-1.5 shadow-sm" style={{ border: "1px solid #e5e5e7" }}>
              Нет в наличии
            </span>
          </div>
        )}

        {/* Add to cart button */}
        {product.inStock && (
          <div className="absolute bottom-2.5 right-2.5 opacity-0 group-hover:opacity-100 translate-y-1.5 group-hover:translate-y-0 transition-all duration-200">
            <button
              onClick={handleAdd}
              className={clsx(
                "w-9 h-9 rounded-xl flex items-center justify-center shadow-lg transition-all duration-200 active:scale-90",
                added
                  ? "bg-emerald-500 text-white"
                  : "text-white hover:scale-105"
              )}
              style={added ? {} : { background: "linear-gradient(135deg, #7c3aed, #9333ea)", boxShadow: "0 4px 14px rgba(124,58,237,0.4)" }}
            >
              {added
                ? <Check size={14} strokeWidth={2.5} />
                : <ShoppingBag size={14} strokeWidth={2} />}
            </button>
          </div>
        )}
      </div>

      {/* Info */}
      <div className="flex flex-col flex-1 p-3.5 pt-3">
        {product.brand && (
          <p className="text-[10px] font-bold uppercase tracking-widest text-apple-gray mb-1 truncate">
            {product.brand}
          </p>
        )}
        <p className="text-[12.5px] font-medium text-apple-black line-clamp-2 leading-snug flex-1">
          {product.name}
        </p>
        {product.volume && (
          <p className="text-[10px] text-apple-gray mt-0.5">{product.volume}</p>
        )}
        <div className="flex items-center gap-2 mt-2.5">
          <span className="text-[14px] font-bold text-apple-black tracking-tight">
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
