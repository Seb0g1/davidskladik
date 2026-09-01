import { useState } from "react";
import { Link } from "react-router-dom";
import { Check } from "lucide-react";
import type { ShopProduct } from "../types";
import { useCart } from "../CartContext";
import StarRating from "./StarRating";

const PLACEHOLDER_BG = ["#141414","#131313","#151515","#141313","#131415"];
const placeholder = (s: string) => PLACEHOLDER_BG[(s?.charCodeAt(0) ?? 0) % PLACEHOLDER_BG.length];

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product, showBrand = true }: Props) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const [img1Error, setImg1Error] = useState(false);
  const [img2Error, setImg2Error] = useState(false);

  const img  = !img1Error && product.images[0] ? product.images[0] : "";
  const img2 = !img2Error && product.images[1] ? product.images[1] : "";
  const hasFlip = !!(img && img2);
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
      className="product-card"
      style={{ textDecoration: "none" }}
    >
      {/* Image */}
      <div
        className={hasFlip ? "card-flip-scene" : undefined}
        style={{
          aspectRatio: "4/5",
          background: img ? "#141414" : placeholder(product.brand || product.name),
          position: "relative",
          overflow: hasFlip ? undefined : "hidden",
        }}
      >
        {hasFlip ? (
          <>
            {/* Back image (second photo) */}
            <div className="card-page-back">
              <img
                src={img2}
                alt=""
                style={{ width: "100%", height: "100%", objectFit: "contain", padding: 12 }}
                loading="lazy"
                onError={() => setImg2Error(true)}
              />
            </div>
            {/* Front image (first photo) — flips on hover */}
            <div className="card-page-front" style={{ background: "#141414" }}>
              <img
                src={img}
                alt={product.name}
                style={{ width: "100%", height: "100%", objectFit: "contain", padding: 12 }}
                loading="lazy"
                onError={() => setImg1Error(true)}
              />
            </div>
          </>
        ) : img ? (
          <img
            src={img}
            alt={product.name}
            style={{ width: "100%", height: "100%", objectFit: "contain", padding: 12, transition: "transform 0.5s cubic-bezier(0.16,1,0.3,1)" }}
            loading="lazy"
            onError={() => setImg1Error(true)}
          />
        ) : (
          <div style={{ width: "100%", height: "100%", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span className="serif" style={{ fontSize: 38, fontWeight: 300, color: "rgba(245,244,240,0.07)", fontStyle: "italic" }}>{initial}</span>
          </div>
        )}

        {/* Out of stock */}
        {!product.inStock && (
          <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(11,11,11,0.65)", backdropFilter: "blur(3px)", zIndex: 10 }}>
            <span style={{ fontSize: 10, fontWeight: 400, color: "var(--muted)", background: "rgba(255,255,255,0.05)", border: "1px solid var(--border)", borderRadius: 2, padding: "5px 14px", letterSpacing: "0.1em", textTransform: "uppercase" }}>
              Нет в наличии
            </span>
          </div>
        )}
      </div>

      {/* Info */}
      <div style={{ padding: "15px 15px 17px", display: "flex", flexDirection: "column", gap: 8 }}>
        {showBrand && product.brand && (
          <p style={{ margin: 0, fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: "var(--accent)" }} className="line-clamp-1">
            {product.brand}
          </p>
        )}
        <p style={{ margin: 0, fontSize: 13.5, lineHeight: 1.4, color: "var(--text)", minHeight: 38 }} className="line-clamp-2">
          {product.name}
        </p>
        {product.volume && (
          <p style={{ margin: 0, fontSize: 11.5, color: "#6f6c66" }}>{product.volume}</p>
        )}
        {(product.rating ?? 0) > 0 && (
          <StarRating rating={product.rating!} count={product.reviewCount} size={11} compact />
        )}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10, marginTop: 4 }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
            {(product.priceRub ?? 0) > 0 && (
              <span style={{ fontSize: 15, color: "var(--text)" }}>
                {product.priceRub.toLocaleString("ru-RU")} ₽
              </span>
            )}
            {product.oldPriceRub && product.oldPriceRub > 0 && (
              <span style={{ fontSize: 11, color: "#6f6c66", textDecoration: "line-through" }}>
                {product.oldPriceRub.toLocaleString("ru-RU")} ₽
              </span>
            )}
          </div>
          {product.inStock && (
            <button
              onClick={handleAdd}
              className={`btn-cart ${added ? "added" : ""}`}
              aria-label="В корзину"
            >
              {added ? <Check size={12} strokeWidth={2.5} /> : "В корзину"}
            </button>
          )}
        </div>
      </div>
    </Link>
  );
}
