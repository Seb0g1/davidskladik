"use client";
import { useRef, useState } from "react";
import { productImg, productImgSet } from "@/lib/img";
import Link from "next/link";
import type { ShopProduct } from "@/lib/types";
import { toProductSlug } from "@/lib/slug";
import AddToCartButton from "./ui/AddToCartButton";
import { useLivePrice } from "@/lib/live-prices";

// hover tint per card — cycles through the scent-family palette
const TINTS = ["#ffc8dc", "#a9dcff", "#e6f99b", "#ffd2a8", "#d9d2ff", "#fff0a0"];
const tintFor = (s: string) => TINTS[[...(s || "x")].reduce((a, c) => a + c.charCodeAt(0), 0) % TINTS.length];

interface Props {
  product: ShopProduct;
  showBrand?: boolean;
}

export default function ProductCard({ product: rendered, showBrand = true }: Props) {
  const product = useLivePrice(rendered); // ISR pages may carry an old price
  const [imgError, setImgError] = useState(false);
  const ref = useRef<HTMLAnchorElement>(null);

  const img = !imgError && product.images[0] ? product.images[0] : null;
  const discount = product.oldPriceRub && product.oldPriceRub > product.priceRub
    ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100)
    : null;

  // 3D tilt + glare position (pointer devices only)
  const onMove = (e: React.PointerEvent) => {
    if (e.pointerType !== "mouse") return;
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const x = (e.clientX - r.left) / r.width;
    const y = (e.clientY - r.top) / r.height;
    el.style.transform = `perspective(900px) rotateX(${(0.5 - y) * 7}deg) rotateY(${(x - 0.5) * 9}deg) translateY(-6px)`;
    el.style.setProperty("--gx", `${x * 100}%`);
    el.style.setProperty("--gy", `${y * 100}%`);
  };
  const onLeave = () => { if (ref.current) ref.current.style.transform = ""; };

  return (
    <Link
      ref={ref}
      href={`/product/${toProductSlug(product.name, product.offerId)}`}
      className="product-card"
      onPointerMove={onMove}
      onPointerLeave={onLeave}
      style={{ "--pc-tint": tintFor(product.brand || product.name) } as React.CSSProperties}
    >
      <div className="pc-media" style={{ position: "relative", paddingBottom: "112%", overflow: "hidden", margin: 8, borderRadius: 14 }}>
        {img ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={productImg(img, 320, 1.12)}
            srcSet={productImgSet(img, 320, 1.12)}
            alt={product.brand ? `${product.name} ${product.brand}` : product.name}
            loading="lazy"
            decoding="async"
            onError={() => setImgError(true)}
            style={{ position: "absolute", inset: 0, width: "100%", height: "100%", objectFit: "contain", mixBlendMode: "multiply" }}
          />
        ) : (
          <span style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "var(--font-display)", fontWeight: 800, fontSize: 44, color: "rgba(18,18,18,0.15)" }}>
            {(product.brand || product.name || "?")[0]}
          </span>
        )}

        <div style={{ position: "absolute", top: 10, left: 10, display: "flex", flexDirection: "column", gap: 5, zIndex: 2 }}>
          {!product.inStock && <span style={{ background: "#fff", color: "var(--muted)", fontSize: 11, fontWeight: 600, padding: "4px 9px", borderRadius: 999 }}>Нет в наличии</span>}
          {discount && <span style={{ background: "var(--pink)", color: "#fff", fontSize: 11, fontWeight: 700, padding: "4px 9px", borderRadius: 999 }}>−{discount}%</span>}
        </div>
      </div>

      <div style={{ padding: "6px 16px 16px", display: "flex", flexDirection: "column", flex: 1, gap: 5, position: "relative", zIndex: 2 }}>
        {showBrand && product.brand && (
          <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.06em", textTransform: "uppercase", color: "var(--accent)", lineHeight: 1.2 }}>{product.brand}</div>
        )}
        <div style={{ fontSize: 14.5, fontWeight: 500, color: "var(--ink)", lineHeight: 1.35, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{product.name}</div>
        {product.volume && <div style={{ fontSize: 12.5, color: "var(--muted)" }}>{product.volume}</div>}

        <div className="pc-footer" style={{ display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", marginTop: "auto", paddingTop: 10, gap: 8 }}>
          <div className="pc-price-wrap">
            <span suppressHydrationWarning style={{ fontFamily: "var(--font-display)", fontSize: 17, fontWeight: 700, color: "var(--ink)", letterSpacing: "-0.02em", whiteSpace: "nowrap" }}>{product.priceRub.toLocaleString("ru-RU")} ₽</span>
            {(product.oldPriceRub ?? 0) > 0 && <span suppressHydrationWarning title="Цена этого товара на маркетплейсах" style={{ fontSize: 12, color: "var(--subtle)", textDecoration: "line-through", marginLeft: 6 }}>{(product.oldPriceRub ?? 0).toLocaleString("ru-RU")} ₽</span>}
          </div>
          {product.inStock && <AddToCartButton product={product} />}
        </div>
      </div>
    </Link>
  );
}
