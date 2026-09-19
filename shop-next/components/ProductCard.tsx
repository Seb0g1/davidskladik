"use client";
import { useState } from "react";
import Link from "next/link";
import type { ShopProduct } from "@/lib/types";
import { toProductSlug } from "@/lib/slug";
import AddToCartButton from "./ui/AddToCartButton";

const PLACEHOLDER_BG = ["#141414", "#131313", "#151515", "#141313", "#131415"];
const placeholder = (s: string) => PLACEHOLDER_BG[(s?.charCodeAt(0) ?? 0) % PLACEHOLDER_BG.length];

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product, showBrand = true }: Props) {
  const [imgError, setImgError] = useState(false);

  const img = !imgError && product.images[0] ? product.images[0] : null;

  const discount = product.oldPriceRub && product.oldPriceRub > product.priceRub
    ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100)
    : null;

  return (
    <Link href={`/product/${toProductSlug(product.name, product.offerId)}`} className="product-card" style={{ display: "flex", flexDirection: "column", textDecoration: "none" }}>
      {/* Image area */}
      <div style={{ position: "relative", paddingBottom: "100%", overflow: "hidden" }}>
        <div style={{ position: "absolute", inset: 0, background: img ? "radial-gradient(ellipse 85% 85% at 50% 46%, #ffffff 0%, #d5ccc0 50%, #0E0D0B 85%)" : placeholder(product.name) }}>
          {img && (
            // eslint-disable-next-line @next/next/no-img-element
            <img
              src={img}
              alt={product.brand ? `${product.name} ${product.brand}` : product.name}
              loading="lazy"
              onError={() => setImgError(true)}
              style={{ position: "absolute", inset: 0, width: "100%", height: "100%", objectFit: "contain", padding: 12, mixBlendMode: "multiply" }}
            />
          )}
        </div>

        {/* Badges */}
        <div style={{ position: "absolute", top: 8, left: 8, display: "flex", flexDirection: "column", gap: 4, zIndex: 2 }}>
          {!product.inStock && <span style={{ background: "rgba(0,0,0,0.7)", color: "rgba(245,244,240,0.7)", fontSize: 9, letterSpacing: "0.14em", textTransform: "uppercase", padding: "3px 7px", borderRadius: 2, border: "1px solid rgba(255,255,255,0.1)" }}>Нет в наличии</span>}
          {discount && <span style={{ background: "rgba(201,162,94,0.15)", color: "#e9d2a0", fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", padding: "3px 7px", borderRadius: 2, border: "1px solid rgba(201,162,94,0.3)" }}>−{discount}%</span>}
        </div>
      </div>

      {/* Info */}
      <div style={{ padding: "14px 14px 16px", display: "flex", flexDirection: "column", flex: 1, gap: 6, position: "relative", zIndex: 2 }}>
        {showBrand && product.brand && (
          <div style={{ fontSize: 9.5, letterSpacing: "0.22em", textTransform: "uppercase", color: "rgba(201,162,94,0.7)", fontWeight: 400, lineHeight: 1.2 }}>{product.brand}</div>
        )}
        <div style={{ fontSize: 13, fontWeight: 400, color: "#f5f4f0", lineHeight: 1.4, letterSpacing: "0.02em", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{product.name}</div>
        {product.volume && <div style={{ fontSize: 11, color: "rgba(245,244,240,0.35)", letterSpacing: "0.06em" }}>{product.volume}</div>}

        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: "auto", paddingTop: 10, gap: 8 }}>
          <div>
            <span suppressHydrationWarning style={{ fontSize: 16, fontWeight: 500, color: "#f5f4f0", letterSpacing: "-0.01em" }}>{product.priceRub.toLocaleString("ru-RU")} ₽</span>
            {product.oldPriceRub && <span suppressHydrationWarning style={{ fontSize: 11, color: "rgba(245,244,240,0.3)", textDecoration: "line-through", marginLeft: 6 }}>{product.oldPriceRub.toLocaleString("ru-RU")} ₽</span>}
          </div>
          {product.inStock && (
            <AddToCartButton product={product} />
          )}
        </div>
      </div>
    </Link>
  );
}
