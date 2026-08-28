import { useState } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const PLACEHOLDER_LETTERS = ["#1A1915","#191A15","#15191A","#1A1519","#191518"];
const placeholder = (s: string) => PLACEHOLDER_LETTERS[(s?.charCodeAt(0) ?? 0) % PLACEHOLDER_LETTERS.length];

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product, showBrand = true }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const [imgError, setImgError] = useState(false);

  const img = !imgError && product.images[0] ? product.images[0] : "";
  const initial = (product.brand || product.name || "?")[0]?.toUpperCase() ?? "?";

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    e.stopPropagation();
    if (!product.inStock || added) return;
    add(product);
    setAdded(true);
    setTimeout(() => setAdded(false), 1600);
  }

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="product-card group"
      style={{ textDecoration: "none" }}
    >
      {/* Image */}
      <div style={{
        aspectRatio: "4/5",
        background: img ? "var(--surface2)" : placeholder(product.brand || product.name),
        position: "relative", overflow: "hidden",
      }}>
        {img ? (
          <img
            src={img}
            alt={product.name}
            style={{ width: "100%", height: "100%", objectFit: "contain", padding: 12, transition: "transform 0.4s ease" }}
            className="group-hover:scale-105"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div style={{ width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span className="serif" style={{ fontSize: 38, fontWeight: 500, color: "rgba(244,239,230,0.08)", fontStyle: "italic" }}>{initial}</span>
          </div>
        )}

        {/* Out of stock */}
        {!product.inStock && (
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(14,13,11,0.62)", backdropFilter: "blur(3px)" }}>
            <span style={{ fontSize: 11, fontWeight: 500, color: "var(--muted)", background: "rgba(255,252,245,0.06)", border: "1px solid var(--border)", borderRadius: 100, padding: "5px 14px", letterSpacing: "0.04em" }}>
              Нет в наличии
            </span>
          </div>
        )}

        {/* Cart button */}
        {product.inStock && (
          <button
            onClick={handleAdd}
            className={`btn-cart ${added ? "added" : ""}`}
            style={{ position: "absolute", bottom: 10, right: 10 }}
            aria-label="В корзину"
          >
            {added ? <Check size={14} strokeWidth={2.5} /> : <ShoppingBag size={13} strokeWidth={1.7} />}
          </button>
        )}
      </div>

      {/* Info */}
      <div style={{ padding: "10px 13px 13px", display: "flex", flexDirection: "column", gap: 2 }}>
        {showBrand && product.brand && (
          <p style={{ fontSize: 9.5, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.1em", color: "var(--subtle)" }} className="line-clamp-1">
            {product.brand}
          </p>
        )}
        <p style={{ fontSize: 12.5, fontWeight: 400, color: "var(--text)", lineHeight: 1.45, flex: 1 }} className="line-clamp-2">
          {product.name}
        </p>
        {product.volume && (
          <p style={{ fontSize: 10.5, color: "var(--subtle)", marginTop: 1 }}>{product.volume}</p>
        )}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 7 }}>
          {(product.priceRub ?? 0) > 0 && (
            <span style={{ fontSize: 14, fontWeight: 600, color: "var(--accent)", letterSpacing: "-0.01em" }}>
              {product.priceRub.toLocaleString("ru-RU")} ₽
            </span>
          )}
          {product.oldPriceRub && product.oldPriceRub > 0 && (
            <span style={{ fontSize: 11, color: "var(--subtle)", textDecoration: "line-through" }}>
              {product.oldPriceRub.toLocaleString("ru-RU")} ₽
            </span>
          )}
        </div>
      </div>
    </Link>
  );
}
