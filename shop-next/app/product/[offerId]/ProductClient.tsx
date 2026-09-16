"use client";
import { useState } from "react";
import Link from "next/link";
import { ShoppingBag, Check, ChevronRight, Star, Shield, Truck, RefreshCw, Minus, Plus, Share2 } from "lucide-react";
import { useCart } from "@/components/CartContext";
import type { ShopProduct, ShopSettings, MarketplaceReview, FragranceNotes } from "@/lib/types";
import ProductCard from "@/components/ProductCard";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
};

function StarRow({ rating, size = 14 }: { rating: number; size?: number }) {
  return (
    <div style={{ display: "flex", gap: 2 }}>
      {[1, 2, 3, 4, 5].map(i => (
        <Star key={i} size={size} fill={i <= Math.round(rating) ? S.accent : "none"} stroke={S.accent} strokeWidth={1.5} />
      ))}
    </div>
  );
}

interface Props {
  product: ShopProduct;
  settings: ShopSettings | null;
  initialReviews: MarketplaceReview[];
  initialAvgRating?: number;
  initialReviewCount?: number;
  fragranceNotes: FragranceNotes | null;
  relatedProducts: ShopProduct[];
}

export default function ProductClient({ product, settings, initialReviews, initialAvgRating, initialReviewCount, fragranceNotes, relatedProducts }: Props) {
  const { add } = useCart();
  const [qty, setQty] = useState(1);
  const [added, setAdded] = useState(false);
  const [activeImg, setActiveImg] = useState(0);
  const [imgErrors, setImgErrors] = useState(new Set<number>());
  const [copied, setCopied] = useState(false);

  const validImages = product.images.filter((_, i) => !imgErrors.has(i));
  const activeValidImg = validImages[activeImg] ?? validImages[0] ?? null;

  const freeDelivery = settings?.freeDeliveryFrom && product.priceRub >= settings.freeDeliveryFrom;

  function handleAdd() {
    add(product, qty);
    setAdded(true);
    setTimeout(() => setAdded(false), 2000);
  }

  function handleShare() {
    navigator.clipboard.writeText(window.location.href).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000); });
  }

  const reviews = initialReviews;
  const avgRating = initialAvgRating ?? product.rating ?? 0;
  const reviewCount = initialReviewCount ?? product.reviewCount ?? 0;

  return (
    <div style={{ background: S.surface, borderRadius: 24, overflow: "hidden", border: `1px solid ${S.border}` }}>
      <style>{`@media(min-width:768px){.product-layout{grid-template-columns:1fr 1fr!important;}}`}</style>
      <div className="product-layout" style={{ display: "grid", gridTemplateColumns: "1fr" }}>

        {/* Images */}
        <div style={{ background: S.surface2, padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column", gap: 16, borderRight: `1px solid ${S.border}` }}>
          <div style={{ aspectRatio: "1", borderRadius: 18, overflow: "hidden", background: "radial-gradient(ellipse 82% 82% at 50% 46%, #ffffff 0%, #d8cfc4 52%, #0E0D0B 84%)", display: "flex", alignItems: "center", justifyContent: "center", border: `1px solid ${S.border}`, position: "relative" }}>
            {activeValidImg ? (
              <>
                <img src={activeValidImg} alt={`${product.name} ${product.brand} купить`} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 8, mixBlendMode: "multiply" }} onError={() => setImgErrors(s => new Set(s).add(activeImg))} />
                <div style={{ position: "absolute", bottom: 16, right: 18, zIndex: 3, display: "flex", alignItems: "center", gap: 5, pointerEvents: "none", opacity: 0.28, mixBlendMode: "multiply" }}>
                  <img src="/favicon.svg" alt="" width={16} height={16} />
                  <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 600, fontSize: 14, color: "#6b5a3e" }}>Magic Vibes</span>
                </div>
              </>
            ) : <span style={{ fontSize: 80, color: S.subtle, opacity: 0.2 }}>{product.brand?.[0] ?? "?"}</span>}
          </div>

          {validImages.length > 1 && (
            <div style={{ display: "flex", gap: 8, overflowX: "auto" }}>
              {validImages.map((img, i) => (
                <button key={i} onClick={() => setActiveImg(i)} style={{ width: 60, height: 60, flexShrink: 0, borderRadius: 10, border: `1px solid ${i === activeImg ? S.accent : S.border}`, background: "radial-gradient(ellipse 85% 85% at center, #fff 0%, #ccc4b8 55%, #0E0D0B 88%)", overflow: "hidden", cursor: "pointer", padding: 0 }}>
                  <img src={img} alt={`${product.name} фото ${i + 1}`} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 4, mixBlendMode: "multiply" }} onError={() => setImgErrors(s => new Set(s).add(i))} />
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Info */}
        <div style={{ padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column", gap: 20 }}>
          <div>
            <div style={{ fontSize: 10, letterSpacing: "0.28em", textTransform: "uppercase", color: S.accent, marginBottom: 8 }}>{product.brand}</div>
            <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(24px,4vw,40px)", color: S.text, margin: "0 0 8px", lineHeight: 1.1 }}>{product.name}</h1>
            {product.volume && <div style={{ fontSize: 13, color: S.muted }}>{product.volume}</div>}
          </div>

          {/* Rating */}
          {avgRating > 0 && reviewCount > 0 && (
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <StarRow rating={avgRating} />
              <span style={{ fontSize: 13, color: S.muted }}>{avgRating.toFixed(1)} · {reviewCount} отзывов</span>
            </div>
          )}

          {/* Price */}
          <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
            <span style={{ fontSize: "clamp(28px,4vw,40px)", fontWeight: 600, color: S.text, letterSpacing: "-0.02em" }}>{product.priceRub.toLocaleString("ru-RU")} ₽</span>
            {product.oldPriceRub && <span style={{ fontSize: 18, color: S.subtle, textDecoration: "line-through" }}>{product.oldPriceRub.toLocaleString("ru-RU")} ₽</span>}
          </div>

          {/* Stock status */}
          <div style={{ fontSize: 13, color: product.inStock ? "#86efac" : "#fca5a5", display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: product.inStock ? "#86efac" : "#fca5a5", display: "inline-block" }} />
            {product.inStock ? `В наличии${product.stockQty > 0 && product.stockQty <= 5 ? ` · осталось ${product.stockQty} шт.` : ""}` : "Нет в наличии"}
          </div>

          {/* Qty + Add to cart */}
          {product.inStock && (
            <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
              <div style={{ display: "flex", alignItems: "center", border: `1px solid ${S.borderMd}`, borderRadius: 10, overflow: "hidden" }}>
                <button onClick={() => setQty(q => Math.max(1, q - 1))} style={{ width: 40, height: 48, background: "transparent", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><Minus size={14} /></button>
                <span style={{ minWidth: 32, textAlign: "center", fontSize: 15, color: S.text }}>{qty}</span>
                <button onClick={() => setQty(q => Math.min(product.stockQty || 99, q + 1))} style={{ width: 40, height: 48, background: "transparent", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><Plus size={14} /></button>
              </div>
              <button onClick={handleAdd} style={{ flex: 1, minWidth: 180, height: 48, background: added ? "rgba(100,180,100,0.15)" : "#f2efe6", color: added ? "#86efac" : "#14120f", border: "none", borderRadius: 10, fontSize: 13, fontWeight: 500, letterSpacing: "0.1em", textTransform: "uppercase", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 8, transition: "all 0.3s" }}>
                {added ? <><Check size={16} /> Добавлено</> : <><ShoppingBag size={16} /> В корзину · {(product.priceRub * qty).toLocaleString("ru-RU")} ₽</>}
              </button>
            </div>
          )}

          {/* Delivery info */}
          <div style={{ display: "flex", flexDirection: "column", gap: 8, padding: "16px", borderRadius: 12, border: `1px solid ${S.border}`, background: "rgba(255,255,255,0.02)" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: S.muted }}>
              <Truck size={15} style={{ color: S.accent, flexShrink: 0 }} />
              <span>{freeDelivery ? "Бесплатная доставка" : `Доставка ${(settings?.deliveryPriceRub ?? 350).toLocaleString("ru-RU")} ₽`} · {settings?.deliveryDaysMin ?? 1}–{settings?.deliveryDays ?? 5} дней</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: S.muted }}>
              <Shield size={15} style={{ color: S.accent, flexShrink: 0 }} />
              <span>Гарантия оригинала</span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: S.muted }}>
              <RefreshCw size={15} style={{ color: S.accent, flexShrink: 0 }} />
              <span>Возврат в течение 14 дней</span>
            </div>
          </div>

          {/* Description */}
          {product.description && (
            <div>
              <div style={{ fontSize: 11, letterSpacing: "0.2em", textTransform: "uppercase", color: S.accent, marginBottom: 10 }}>Описание</div>
              <p style={{ fontSize: 14, color: S.muted, lineHeight: 1.8, margin: 0 }}>{product.description.slice(0, 400)}</p>
            </div>
          )}

          {/* Fragrance notes */}
          {fragranceNotes && (fragranceNotes.topNotes.length > 0 || fragranceNotes.middleNotes.length > 0 || fragranceNotes.baseNotes.length > 0) && (
            <div>
              <div style={{ fontSize: 11, letterSpacing: "0.2em", textTransform: "uppercase", color: S.accent, marginBottom: 12 }}>Пирамида аромата</div>
              {[
                { label: "Верхние ноты", notes: fragranceNotes.topNotes },
                { label: "Сердце", notes: fragranceNotes.middleNotes },
                { label: "База", notes: fragranceNotes.baseNotes },
              ].filter(g => g.notes.length > 0).map(g => (
                <div key={g.label} style={{ marginBottom: 10 }}>
                  <div style={{ fontSize: 11, color: S.subtle, marginBottom: 4 }}>{g.label}</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {g.notes.map(n => <span key={n} style={{ fontSize: 12, padding: "4px 10px", borderRadius: 20, border: `1px solid ${S.border}`, color: S.muted }}>{n}</span>)}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Share */}
          <button onClick={handleShare} style={{ alignSelf: "flex-start", display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: S.muted, background: "none", border: "none", cursor: "pointer", padding: 0 }}>
            {copied ? <><Check size={13} /> Ссылка скопирована</> : <><Share2 size={13} /> Поделиться</>}
          </button>
        </div>
      </div>

      {/* Reviews section */}
      {reviews.length > 0 && (
        <div style={{ padding: "clamp(32px,4vw,56px)", borderTop: `1px solid ${S.border}` }}>
          <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 28, fontWeight: 300, color: S.text, margin: "0 0 24px" }}>Отзывы покупателей</h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {reviews.slice(0, 6).map(r => (
              <div key={r.id} style={{ padding: "18px 20px", borderRadius: 12, border: `1px solid ${S.border}`, background: "rgba(255,255,255,0.02)" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
                  <StarRow rating={r.rating} size={12} />
                  <span style={{ fontSize: 11, color: S.subtle }}>{r.author ?? "Покупатель"} · {r.source === "ozon" ? "Ozon" : "Яндекс Маркет"}</span>
                </div>
                {r.text && <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.7, margin: 0 }}>{r.text}</p>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Related products */}
      {relatedProducts.length > 0 && (
        <div style={{ padding: "clamp(32px,4vw,56px)", borderTop: `1px solid ${S.border}` }}>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 20 }}>
            <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 24, fontWeight: 300, color: S.text, margin: 0 }}>Похожие ароматы</h2>
            <Link href={`/catalog?brand=${encodeURIComponent(product.brand)}`} style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", color: S.muted, textDecoration: "none" }}>Все {product.brand} →</Link>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(180px,1fr))", gap: 12 }}>
            {relatedProducts.map(p => <ProductCard key={p.id} product={p} />)}
          </div>
        </div>
      )}
    </div>
  );
}
