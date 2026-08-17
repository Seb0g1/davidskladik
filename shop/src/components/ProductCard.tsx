import { useState, useCallback } from "react";
import { Link } from "react-router-dom";
import { ShoppingBag, Check } from "lucide-react";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";

const PLACEHOLDER_BG = ["rgba(109,40,217,0.12)","rgba(79,70,229,0.12)","rgba(124,58,237,0.1)","rgba(91,33,182,0.1)","rgba(167,139,250,0.08)"];
const placeholder = (s: string) => PLACEHOLDER_BG[(s?.charCodeAt(0) ?? 0) % PLACEHOLDER_BG.length];

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product, showBrand = true }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const [imgError, setImgError] = useState(false);
  const [tiltStyle, setTiltStyle] = useState<React.CSSProperties>({});

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

  const onMove = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const el = e.currentTarget;
    const rect = el.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width  - 0.5;
    const y = (e.clientY - rect.top)  / rect.height - 0.5;
    setTiltStyle({
      transform: `perspective(600px) rotateY(${x * 10}deg) rotateX(${-y * 10}deg) scale3d(1.015,1.015,1.015)`,
      transition: "transform 0.08s ease",
    });
    el.style.setProperty("--mx", `${(x + 0.5) * 100}%`);
    el.style.setProperty("--my", `${(y + 0.5) * 100}%`);
  }, []);

  const onLeave = useCallback(() => {
    setTiltStyle({ transform: "perspective(600px) rotateY(0) rotateX(0) scale3d(1,1,1)", transition: "transform 0.35s ease" });
  }, []);

  return (
    <Link
      to={`/product/${encodeURIComponent(product.offerId)}`}
      className="product-card-3d group"
      style={tiltStyle}
      onMouseMove={onMove}
      onMouseLeave={onLeave}
    >
      {/* Image */}
      <div style={{ aspectRatio: "4/5", background: img ? "rgba(255,255,255,0.03)" : placeholder(product.brand || product.name), position: "relative", overflow: "hidden" }}>
        {img ? (
          <img
            src={img}
            alt={product.name}
            style={{ width: "100%", height: "100%", objectFit: "contain", padding: 12, transition: "transform 0.5s ease" }}
            className="group-hover:scale-105"
            loading="lazy"
            onError={() => setImgError(true)}
          />
        ) : (
          <div style={{ width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ fontSize: 42, fontWeight: 800, color: "rgba(167,139,250,0.12)" }}>{initial}</span>
          </div>
        )}

        {/* Out of stock */}
        {!product.inStock && (
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(9,6,15,0.6)", backdropFilter: "blur(4px)" }}>
            <span style={{ fontSize: 11, fontWeight: 600, color: "var(--muted)", background: "rgba(255,255,255,0.06)", border: "1px solid var(--border)", borderRadius: 100, padding: "5px 14px" }}>
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
            {added ? <Check size={14} strokeWidth={2.5} /> : <ShoppingBag size={14} strokeWidth={1.8} />}
          </button>
        )}
      </div>

      {/* Info */}
      <div style={{ padding: "10px 13px 13px", display: "flex", flexDirection: "column", gap: 3 }}>
        {showBrand && product.brand && (
          <p style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.09em", color: "var(--subtle)" }} className="line-clamp-1">
            {product.brand}
          </p>
        )}
        <p style={{ fontSize: 12.5, fontWeight: 500, color: "var(--text)", lineHeight: 1.4, flex: 1 }} className="line-clamp-2">
          {product.name}
        </p>
        {product.volume && (
          <p style={{ fontSize: 11, color: "var(--subtle)", marginTop: 1 }}>{product.volume}</p>
        )}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 6 }}>
          {(product.priceRub ?? 0) > 0 && (
            <span style={{ fontSize: 15, fontWeight: 700, color: "var(--accent3)", letterSpacing: "-0.02em" }}>
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
