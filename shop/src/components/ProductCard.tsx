import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const PLACEHOLDER_COLORS = ["#F3F0FA", "#FAF0F3", "#F0F3FA", "#F0FAF3", "#FAF7F0"];

function placeholderColor(str: string) {
  return PLACEHOLDER_COLORS[(str?.charCodeAt(0) ?? 0) % PLACEHOLDER_COLORS.length];
}

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product, showBrand = true }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const [imgError, setImgError] = useState(false);

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    if (!product.inStock || added) return;
    add(product);
    setAdded(true);
    setTimeout(() => setAdded(false), 1600);
  }

  const img = !imgError && product.images[0] ? product.images[0] : "";
  const initial = (product.brand || product.name || "?")[0]?.toUpperCase() ?? "?";

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="product-card group"
    >
      {/* Image area — 4:5 portrait */}
      <div
        className="relative w-full overflow-hidden"
        style={{
          aspectRatio: "4 / 5",
          background: img ? "#F8F8FA" : placeholderColor(product.brand || product.name),
        }}
      >
        {img ? (
          <img
            src={img}
            alt={product.name}
            className="w-full h-full object-contain p-3 transition-transform duration-500 group-hover:scale-[1.04]"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center select-none">
            <span className="text-5xl font-bold opacity-[0.08]">{initial}</span>
          </div>
        )}

        {/* Out of stock overlay */}
        {!product.inStock && (
          <div className="absolute inset-0 flex items-center justify-center" style={{ background: "rgba(255,255,255,0.7)", backdropFilter: "blur(2px)" }}>
            <span className="text-[11px] font-semibold text-[#888] bg-white border border-[#E8E8E8] rounded-full px-3 py-1.5 shadow-sm">
              Нет в наличии
            </span>
          </div>
        )}

        {/* Add to cart — always visible on mobile, hover on desktop */}
        {product.inStock && (
          <button
            onClick={handleAdd}
            className={`btn-cart absolute bottom-2.5 right-2.5 md:opacity-0 md:group-hover:opacity-100 md:translate-y-1.5 md:group-hover:translate-y-0 transition-all duration-200 ${added ? "added" : ""}`}
            aria-label="В корзину"
          >
            {added ? <Check size={14} strokeWidth={2.5} /> : <ShoppingBag size={14} strokeWidth={2} />}
          </button>
        )}
      </div>

      {/* Info */}
      <div className="flex flex-col flex-1 px-3 pb-3 pt-2.5 gap-0.5">
        {showBrand && product.brand && (
          <p className="text-[10px] font-bold uppercase tracking-widest text-[#ABABAB] line-clamp-1">
            {product.brand}
          </p>
        )}
        <p className="text-[12.5px] font-medium text-[#111] line-clamp-2 leading-[1.4] flex-1">
          {product.name}
        </p>
        {product.volume && (
          <p className="text-[11px] text-[#ABABAB] mt-0.5">{product.volume}</p>
        )}
        <div className="flex items-center gap-2 mt-2">
          <span className="text-[15px] font-bold text-[#111] tracking-tight">
            {product.priceRub.toLocaleString("ru-RU")} ₽
          </span>
          {product.oldPriceRub && (
            <span className="text-[11px] text-[#C0C0C0] line-through">
              {product.oldPriceRub.toLocaleString("ru-RU")} ₽
            </span>
          )}
        </div>
      </div>
    </Link>
  );
}
