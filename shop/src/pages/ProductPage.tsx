import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { ShoppingBag, Check, ChevronLeft, Star, Shield, Truck, RefreshCw, Minus, Plus, ChevronRight } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";

const S = {
  bg:      "#09060F",
  surface: "#130F1D",
  surface2:"#1C1630",
  border:  "rgba(255,255,255,0.07)",
  borderMd:"rgba(255,255,255,0.12)",
  text:    "#F0EBFF",
  muted:   "rgba(240,235,255,0.45)",
  subtle:  "rgba(240,235,255,0.2)",
  accent:  "#7C3AED",
  accent2: "#A78BFA",
  accent3: "#C4B5FD",
};

export default function ProductPage() {
  const { offerId } = useParams<{ offerId: string }>();
  const { add } = useCart();
  const [qty, setQty] = useState(1);
  const [activeImg, setActiveImg] = useState(0);
  const [added, setAdded] = useState(false);
  const [imgErrors, setImgErrors] = useState<Set<number>>(new Set());

  const { data: product, isLoading, error } = useQuery({
    queryKey: ["shop-product", offerId],
    queryFn: () => api.product(offerId!),
    enabled: !!offerId,
  });

  function handleAdd() {
    if (!product || !product.inStock) return;
    add(product, qty);
    setAdded(true);
    setTimeout(() => setAdded(false), 2000);
  }

  if (isLoading) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1100, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
          <div style={{ background: S.surface, borderRadius: 24, overflow: "hidden", border: `1px solid ${S.border}` }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr" }}>
              <div className="skeleton" style={{ aspectRatio: "1", minHeight: 400 }} />
              <div style={{ padding: 48, display: "flex", flexDirection: "column", gap: 20 }}>
                <div className="skeleton" style={{ height: 12, width: "30%" }} />
                <div className="skeleton" style={{ height: 32, width: "80%" }} />
                <div className="skeleton" style={{ height: 12, width: "40%" }} />
                <div className="skeleton" style={{ height: 48, borderRadius: 16, marginTop: 24 }} />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !product) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 56, marginBottom: 16 }}>😔</div>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: S.text, marginBottom: 8 }}>Товар не найден</h2>
          <Link to="/catalog" style={{ fontSize: 13, color: S.accent3, textDecoration: "none", fontWeight: 500 }}>
            ← Вернуться в каталог
          </Link>
        </div>
      </div>
    );
  }

  const validImages = product.images.filter((_, i) => !imgErrors.has(i));
  const activeValidImg = validImages[activeImg] || null;
  const discount = product.oldPriceRub ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100) : 0;

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "clamp(20px,3vw,48px) clamp(16px,4vw,32px)" }}>

        {/* Breadcrumb */}
        <nav style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: S.muted, marginBottom: 24, flexWrap: "wrap" }}>
          <Link to="/" style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
            onMouseEnter={e => (e.currentTarget.style.color = S.text)}
            onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>Главная</Link>
          <ChevronRight size={11} style={{ color: S.subtle }} />
          <Link to="/catalog" style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
            onMouseEnter={e => (e.currentTarget.style.color = S.text)}
            onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>Каталог</Link>
          {product.brand && (
            <>
              <ChevronRight size={11} style={{ color: S.subtle }} />
              <Link to={`/catalog?brand=${encodeURIComponent(product.brand)}`} style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
                onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>{product.brand}</Link>
            </>
          )}
          <ChevronRight size={11} style={{ color: S.subtle }} />
          <span style={{ color: S.text, maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{product.name}</span>
        </nav>

        <div style={{ background: S.surface, borderRadius: 24, overflow: "hidden", border: `1px solid ${S.border}` }}>
          <style>{`@media(min-width:768px){.product-layout{grid-template-columns:1fr 1fr!important;}}`}</style>
          <div className="product-layout" style={{ display: "grid", gridTemplateColumns: "1fr" }}>

            {/* Images */}
            <div style={{ background: S.surface2, padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column", gap: 16, borderRight: `1px solid ${S.border}` }}>
              <div style={{
                aspectRatio: "1", borderRadius: 18, overflow: "hidden", background: S.bg,
                display: "flex", alignItems: "center", justifyContent: "center",
                border: `1px solid ${S.border}`,
              }}>
                {activeValidImg
                  ? <img src={activeValidImg} alt={product.name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 24 }}
                      onError={() => setImgErrors((s) => new Set(s).add(activeImg))} />
                  : <span style={{ fontSize: 80, fontWeight: 800, color: S.subtle, opacity: 0.2 }}>{product.brand?.[0] ?? "?"}</span>
                }
              </div>
              {validImages.length > 1 && (
                <div style={{ display: "flex", gap: 8, overflowX: "auto", paddingBottom: 4 }}>
                  {product.images.map((img, i) => !imgErrors.has(i) && (
                    <button key={i} onClick={() => setActiveImg(i)} style={{
                      flexShrink: 0, width: 56, height: 56, borderRadius: 12, overflow: "hidden",
                      background: S.bg, border: `2px solid ${i === activeImg ? S.accent : S.border}`,
                      cursor: "pointer", transition: "border-color 0.15s ease",
                      boxShadow: i === activeImg ? "0 0 16px rgba(124,58,237,0.3)" : "none",
                    }}>
                      <img src={img} alt="" style={{ width: "100%", height: "100%", objectFit: "contain", padding: 4 }}
                        onError={() => setImgErrors((s) => new Set(s).add(i))} />
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Info */}
            <div style={{ padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column" }}>
              <Link to="/catalog" style={{
                display: "inline-flex", alignItems: "center", gap: 4, fontSize: 12, color: S.muted,
                textDecoration: "none", marginBottom: 20, transition: "color 0.15s ease", width: "fit-content",
              }}
                onMouseEnter={e => (e.currentTarget.style.color = S.accent3)}
                onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>
                <ChevronLeft size={14} /> Назад к каталогу
              </Link>

              {product.brand && (
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.12em", color: S.accent3, marginBottom: 10 }}>
                  {product.brand}
                </div>
              )}
              <h1 style={{ fontSize: "clamp(20px,2.5vw,28px)", fontWeight: 700, color: S.text, letterSpacing: "-0.035em", lineHeight: 1.2, marginBottom: 12 }}>
                {product.name}
              </h1>

              {product.volume && (
                <span style={{ display: "inline-block", fontSize: 12, color: S.muted, background: "rgba(255,255,255,0.06)", border: `1px solid ${S.border}`, borderRadius: 8, padding: "4px 12px", marginBottom: 16, alignSelf: "flex-start" }}>
                  {product.volume}
                </span>
              )}

              {product.rating && product.rating > 0 ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
                  <div style={{ display: "flex" }}>
                    {[1,2,3,4,5].map((s) => (
                      <Star key={s} size={14} style={{ color: s <= Math.round(product.rating!) ? "#fbbf24" : S.subtle, fill: s <= Math.round(product.rating!) ? "#fbbf24" : S.subtle }} />
                    ))}
                  </div>
                  <span style={{ fontSize: 13, fontWeight: 600, color: S.text }}>{product.rating.toFixed(1)}</span>
                  {product.reviewCount && <span style={{ fontSize: 12, color: S.muted }}>{product.reviewCount} отзывов</span>}
                </div>
              ) : null}

              {/* Price */}
              <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
                <span style={{ fontSize: "clamp(28px,3vw,40px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em" }}>
                  {product.priceRub.toLocaleString("ru-RU")} ₽
                </span>
                {product.oldPriceRub && (
                  <>
                    <span style={{ fontSize: 16, color: S.subtle, textDecoration: "line-through" }}>{product.oldPriceRub.toLocaleString("ru-RU")} ₽</span>
                    <span style={{ fontSize: 12, background: "rgba(239,68,68,0.12)", color: "#f87171", fontWeight: 700, padding: "3px 8px", borderRadius: 8 }}>−{discount}%</span>
                  </>
                )}
              </div>

              {/* Stock */}
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 24, fontSize: 13, fontWeight: 500, color: product.inStock ? "#4ade80" : S.muted }}>
                <div style={{ width: 8, height: 8, borderRadius: "50%", background: product.inStock ? "#4ade80" : S.subtle, boxShadow: product.inStock ? "0 0 8px #4ade80" : "none" }} />
                {product.inStock ? `В наличии${product.stockQty > 0 ? ` · ${product.stockQty} шт.` : ""}` : "Нет в наличии"}
              </div>

              {product.inStock && (
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
                  <div style={{ display: "flex", alignItems: "center", background: S.surface2, borderRadius: 14, border: `1px solid ${S.border}`, overflow: "hidden" }}>
                    <button onClick={() => setQty((q) => Math.max(1, q - 1))} style={{ padding: "12px 16px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                      onMouseLeave={e => (e.currentTarget.style.color = S.muted)}><Minus size={15} /></button>
                    <span style={{ padding: "12px 16px", fontSize: 15, fontWeight: 700, color: S.text, minWidth: 48, textAlign: "center" }}>{qty}</span>
                    <button onClick={() => setQty((q) => Math.min(product.stockQty || 99, q + 1))} style={{ padding: "12px 16px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                      onMouseLeave={e => (e.currentTarget.style.color = S.muted)}><Plus size={15} /></button>
                  </div>
                  <button onClick={handleAdd} className={added ? "" : "btn-primary"} style={{
                    flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                    padding: "14px 20px", borderRadius: 16, fontSize: 14, fontWeight: 700, border: "none", cursor: "pointer",
                    ...(added ? {
                      background: "rgba(74,222,128,0.15)", color: "#4ade80",
                      border: "1px solid rgba(74,222,128,0.25)", boxShadow: "none",
                    } : {}),
                    transition: "all 0.2s ease",
                  }}>
                    {added ? <><Check size={18} /> Добавлено!</> : <><ShoppingBag size={18} /> В корзину</>}
                  </button>
                </div>
              )}

              {/* Perks */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 10, marginBottom: 24 }}>
                {[
                  { icon: Truck,     label: "Доставка Ozon" },
                  { icon: Shield,    label: "100% оригинал" },
                  { icon: RefreshCw, label: "Возврат 14 дней" },
                ].map(({ icon: Icon, label }) => (
                  <div key={label} style={{
                    display: "flex", flexDirection: "column", alignItems: "center", gap: 8,
                    background: S.surface2, borderRadius: 14, padding: "12px 8px", textAlign: "center",
                    border: `1px solid ${S.border}`,
                  }}>
                    <Icon size={16} style={{ color: S.accent3 }} />
                    <span style={{ fontSize: 10.5, color: S.muted, fontWeight: 500, lineHeight: 1.3 }}>{label}</span>
                  </div>
                ))}
              </div>

              {product.description && (
                <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 20 }}>
                  <h3 style={{ fontSize: 13, fontWeight: 600, color: S.text, marginBottom: 10 }}>Описание</h3>
                  <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.7 }}>{product.description}</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
