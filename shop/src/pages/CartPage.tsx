import { Link } from "react-router-dom";
import { Trash2, Plus, Minus, ShoppingBag, ArrowRight, Package } from "lucide-react";
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
  accent3: "#C4B5FD",
};

export default function CartPage() {
  const { items, totalRub, remove, setQty } = useCart();

  if (!items.length) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", padding: "80px 20px" }}>
          <div style={{
            width: 72, height: 72, borderRadius: 24, display: "flex", alignItems: "center", justifyContent: "center",
            margin: "0 auto 24px", background: "rgba(124,58,237,0.1)", border: "1px solid rgba(124,58,237,0.2)",
          }}>
            <ShoppingBag size={32} style={{ color: S.accent3 }} strokeWidth={1.5} />
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: S.text, marginBottom: 8, letterSpacing: "-0.03em" }}>Корзина пуста</h2>
          <p style={{ fontSize: 14, color: S.muted, marginBottom: 32 }}>Добавьте товары из каталога</p>
          <Link to="/catalog" className="btn-primary" style={{ fontSize: 14, padding: "12px 28px" }}>
            Перейти в каталог <ArrowRight size={15} strokeWidth={2.5} />
          </Link>
        </div>
      </div>
    );
  }

  const delivery = totalRub >= 3000 ? 0 : 350;
  const total = totalRub + delivery;

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 1000, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
        <h1 style={{ fontSize: "clamp(22px,3vw,30px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em", marginBottom: 32 }}>
          Корзина
          <span style={{ fontSize: 16, fontWeight: 500, color: S.muted, marginLeft: 12 }}>
            · {items.reduce((s, i) => s + i.quantity, 0)} товара
          </span>
        </h1>

        <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 20 }}>
          <style>{`@media(min-width:768px){.cart-grid{grid-template-columns:1fr 340px!important;}}`}</style>
          <div className="cart-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 20, alignItems: "start" }}>

            {/* Items */}
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {items.map(({ product, quantity }) => (
                <div key={product.offerId} style={{
                  background: S.surface, borderRadius: 18, padding: "16px",
                  border: `1px solid ${S.border}`, display: "flex", gap: 14,
                  transition: "border-color 0.2s ease",
                }}>
                  <Link to={`/product/${encodeURIComponent(product.offerId)}`}
                    style={{ flexShrink: 0, width: 76, height: 76, borderRadius: 14, overflow: "hidden",
                      background: S.surface2, display: "flex", alignItems: "center", justifyContent: "center", textDecoration: "none" }}>
                    {product.images[0]
                      ? <img src={product.images[0]} alt={product.name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 6 }} />
                      : <span style={{ fontSize: 22, fontWeight: 700, color: S.subtle }}>{product.brand?.[0] ?? "?"}</span>
                    }
                  </Link>

                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.accent3, marginBottom: 3 }}>
                      {product.brand}
                    </div>
                    <Link to={`/product/${encodeURIComponent(product.offerId)}`}
                      style={{ fontSize: 13, fontWeight: 500, color: S.text, textDecoration: "none",
                        display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden", lineHeight: 1.4 }}>
                      {product.name}
                    </Link>
                    {product.volume && <div style={{ fontSize: 11, color: S.muted, marginTop: 2 }}>{product.volume}</div>}

                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 12 }}>
                      <div style={{ display: "flex", alignItems: "center", background: S.surface2, borderRadius: 12, overflow: "hidden", border: `1px solid ${S.border}` }}>
                        <button onClick={() => setQty(product.offerId, quantity - 1)}
                          style={{ padding: "8px 12px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s ease" }}
                          onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                          onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>
                          <Minus size={13} />
                        </button>
                        <span style={{ padding: "8px 12px", fontSize: 13, fontWeight: 700, color: S.text, minWidth: 36, textAlign: "center" }}>{quantity}</span>
                        <button onClick={() => setQty(product.offerId, Math.min(product.stockQty || 99, quantity + 1))}
                          style={{ padding: "8px 12px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s ease" }}
                          onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                          onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>
                          <Plus size={13} />
                        </button>
                      </div>

                      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                        <span style={{ fontSize: 16, fontWeight: 700, color: S.text }}>
                          {(product.priceRub * quantity).toLocaleString("ru-RU")} ₽
                        </span>
                        <button onClick={() => remove(product.offerId)}
                          style={{ padding: 8, background: "none", border: "none", color: S.subtle, cursor: "pointer", borderRadius: 10, transition: "all 0.15s ease" }}
                          onMouseEnter={e => { (e.currentTarget as HTMLElement).style.color = "#f87171"; (e.currentTarget as HTMLElement).style.background = "rgba(239,68,68,0.1)"; }}
                          onMouseLeave={e => { (e.currentTarget as HTMLElement).style.color = S.subtle; (e.currentTarget as HTMLElement).style.background = "transparent"; }}>
                          <Trash2 size={15} />
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Summary */}
            <div style={{ background: S.surface, borderRadius: 20, padding: 24, border: `1px solid ${S.border}`, position: "sticky", top: 80 }}>
              <h3 style={{ fontSize: 15, fontWeight: 700, color: S.text, marginBottom: 20 }}>Итого</h3>

              <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 20 }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: S.muted }}>
                  <span>Товары · {items.reduce((s, i) => s + i.quantity, 0)} шт.</span>
                  <span style={{ color: S.text, fontWeight: 500 }}>{totalRub.toLocaleString("ru-RU")} ₽</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: S.muted }}>
                  <span>Доставка Ozon</span>
                  {delivery === 0
                    ? <span style={{ color: "#4ade80", fontWeight: 600 }}>Бесплатно</span>
                    : <span style={{ color: S.text, fontWeight: 500 }}>{delivery} ₽</span>}
                </div>
                {delivery > 0 && (
                  <div style={{ fontSize: 11, color: "#f59e0b", background: "rgba(245,158,11,0.1)", borderRadius: 10, padding: "8px 12px", border: "1px solid rgba(245,158,11,0.15)" }}>
                    До бесплатной доставки: {(3000 - totalRub).toLocaleString("ru-RU")} ₽
                  </div>
                )}
              </div>

              <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 16, marginBottom: 20 }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 700, color: S.text }}>
                  <span>К оплате</span>
                  <span>{total.toLocaleString("ru-RU")} ₽</span>
                </div>
              </div>

              <Link to="/checkout" className="btn-primary" style={{ width: "100%", fontSize: 15, padding: "14px 0", justifyContent: "center" }}>
                Оформить заказ <ArrowRight size={16} />
              </Link>

              <div style={{ marginTop: 14, display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 11, color: S.subtle }}>
                <Package size={11} />
                <span>Доставка через Ozon · Оригинальная продукция</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
