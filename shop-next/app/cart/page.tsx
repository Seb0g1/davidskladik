"use client";
import { useState, useEffect } from "react";
import { productImg, productImgSet } from "@/lib/img";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Minus, Plus, Trash2, ShoppingBag, ArrowRight, Package } from "lucide-react";
import { useCart } from "@/components/CartContext";
import { OzonPayLogo } from "@/components/OzonPay";
import { ruPlural } from "@/lib/utils";
import { toProductSlug } from "@/lib/slug";

const S = {
  bg:      "var(--surface)",
  surface: "var(--surface)",
  surface2:"var(--surface)",
  border:  "rgba(var(--ink-rgb),0.056)",
  borderMd:"rgba(var(--ink-rgb),0.104)",
  text:    "var(--ink)",
  muted:   "rgba(var(--ink-rgb),0.55)",
  subtle:  "rgba(var(--ink-rgb),0.45)",
  accent:  "var(--accent)",
  accent3: "var(--accent2)",
};

export default function CartPage() {
  const { items, totalRub, remove, setQty, totalItems } = useCart();
  const router = useRouter();

  if (!items.length) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", padding: "80px 20px" }}>
          <div style={{
            width: 72, height: 72, borderRadius: 24, display: "flex", alignItems: "center", justifyContent: "center",
            margin: "0 auto 24px", background: "rgba(var(--accent-rgb),0.1)", border: "1px solid rgba(var(--accent-rgb),0.2)",
          }}>
            <ShoppingBag size={32} style={{ color: S.accent3 }} strokeWidth={1.5} />
          </div>
          <h2 style={{ fontSize: 22, fontWeight: 700, color: S.text, marginBottom: 8, letterSpacing: "-0.03em" }}>Корзина пуста</h2>
          <p style={{ fontSize: 14, color: S.muted, marginBottom: 32 }}>Добавьте товары из каталога</p>
          <Link href="/catalog" className="btn-primary" style={{ fontSize: 14, padding: "12px 28px", textDecoration: "none" }}>
            Перейти в каталог <ArrowRight size={15} strokeWidth={2.5} />
          </Link>
        </div>
      </div>
    );
  }

  // delivery tariff from the shop settings (davidsklad → Магазин → Настройки), final price at checkout
  // with the Ozon-tariff mode the price depends on the city → «от N ₽» here, exact price at checkout
  const [tariff, setTariff] = useState<{ price: number; freeFrom: number; byCity: boolean } | null>(null);
  const cartKey = items.map((i) => `${i.product.offerId}:${i.quantity}`).join(",");
  useEffect(() => {
    if (!cartKey) return;
    fetch(`${process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru"}/api/shop/checkout/delivery-mode`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity })), goodsRub: totalRub }),
    })
      .then((r) => r.json())
      .then((d) => setTariff({ price: Math.max(0, Number(d.needCity ? d.fromRub : d.deliveryRub) || 0), freeFrom: Math.max(0, Number(d.freeDeliveryFrom) || 0), byCity: Boolean(d.needCity) }))
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cartKey]);
  const deliveryRub = tariff?.price ?? 0;
  const toFree = tariff && deliveryRub > 0 && tariff.freeFrom > 0 ? tariff.freeFrom - totalRub : 0;
  const total = totalRub + deliveryRub;

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 1000, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
        <h1 style={{ fontSize: "clamp(22px,3vw,30px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em", marginBottom: 32 }}>
          Корзина
          <span style={{ fontSize: 16, fontWeight: 500, color: S.muted, marginLeft: 12 }}>
            · {totalItems} {ruPlural(totalItems, "товар", "товара", "товаров")}
          </span>
        </h1>

        <style>{`@media(min-width:768px){.cart-grid{grid-template-columns:1fr 340px!important;}}`}</style>
        <div className="cart-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 20, alignItems: "start" }}>

          {/* Items */}
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {items.map(({ product, quantity }) => (
              <div key={product.offerId} style={{
                background: S.surface, borderRadius: 18, padding: "16px",
                border: `1px solid ${S.border}`, display: "flex", gap: 14,
              }}>
                <Link href={`/product/${toProductSlug(product.name, product.offerId)}`}
                  style={{ flexShrink: 0, width: 76, height: 76, borderRadius: 14, overflow: "hidden",
                    background: S.surface2, display: "flex", alignItems: "center", justifyContent: "center", textDecoration: "none" }}>
                  {product.images[0]
                    // eslint-disable-next-line @next/next/no-img-element
                    ? <img src={productImg(product.images[0], 160)} alt={product.name} style={{ width: "100%", height: "100%", objectFit: "contain", background: "#fff" }} />
                    : <span style={{ fontSize: 22, fontWeight: 700, color: S.subtle }}>{product.brand?.[0] ?? "?"}</span>
                  }
                </Link>

                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.accent3, marginBottom: 3 }}>
                    {product.brand}
                  </div>
                  <Link href={`/product/${toProductSlug(product.name, product.offerId)}`}
                    style={{ fontSize: 13, fontWeight: 500, color: S.text, textDecoration: "none",
                      display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden", lineHeight: 1.4 } as React.CSSProperties}>
                    {product.name}
                  </Link>
                  {product.volume && <div style={{ fontSize: 11, color: S.muted, marginTop: 2 }}>{product.volume}</div>}

                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: 12 }}>
                    <div style={{ display: "flex", alignItems: "center", background: S.surface2, borderRadius: 12, overflow: "hidden", border: `1px solid ${S.border}` }}>
                      <button onClick={() => setQty(product.offerId, quantity - 1)}
                        style={{ padding: "8px 14px", minHeight: 44, background: "none", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center" }}>
                        <Minus size={13} />
                      </button>
                      <span style={{ padding: "8px 12px", fontSize: 13, fontWeight: 700, color: S.text, minWidth: 36, textAlign: "center", display: "flex", alignItems: "center", justifyContent: "center" }}>{quantity}</span>
                      <button onClick={() => setQty(product.offerId, Math.min(product.stockQty || 99, quantity + 1))}
                        style={{ padding: "8px 14px", minHeight: 44, background: "none", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center" }}>
                        <Plus size={13} />
                      </button>
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <span style={{ fontSize: 16, fontWeight: 700, color: S.text }}>
                        {(product.priceRub * quantity).toLocaleString("ru-RU")} ₽
                      </span>
                      <button onClick={() => remove(product.offerId)}
                        style={{ padding: 12, background: "none", border: "none", color: S.subtle, cursor: "pointer", borderRadius: 10 }}>
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
                <span style={{ color: S.text, fontWeight: 500 }}>{totalRub.toLocaleString("ru-RU")} ₽</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: S.muted }}>
                <span>Доставка</span>
                {!tariff ? <span>…</span> : deliveryRub > 0
                  ? <span style={{ color: S.text, fontWeight: 500 }}>{tariff.byCity ? "от " : ""}{deliveryRub.toLocaleString("ru-RU")} ₽</span>
                  : <span style={{ color: "var(--success)", fontWeight: 600 }}>Бесплатно</span>}
              </div>
              {toFree > 0 && (
                <div style={{ fontSize: 12, lineHeight: 1.45, color: S.muted, background: "rgba(var(--ink-rgb),0.04)", borderRadius: 12, padding: "8px 10px" }}>
                  Добавьте товаров ещё на <b style={{ color: S.text }}>{toFree.toLocaleString("ru-RU")} ₽</b> — доставка станет бесплатной
                </div>
              )}
            </div>

            <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 16, marginBottom: 20 }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 700, color: S.text }}>
                <span>К оплате</span>
                <span>{tariff?.byCity ? "от " : ""}{total.toLocaleString("ru-RU")} ₽</span>
              </div>
            </div>

            <button onClick={() => router.push("/checkout")} className="btn-primary" style={{ width: "100%", fontSize: 15, padding: "14px 0", justifyContent: "center", border: "none", cursor: "pointer" }}>
              Оформить заказ <ArrowRight size={16} />
            </button>

            <div style={{ marginTop: 14, display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 11, color: S.subtle }}>
              <Package size={11} />
              <span>Доставка через Ozon · Оригинальная продукция</span>
            </div>
            <div style={{ marginTop: 10, display: "flex", alignItems: "center", justifyContent: "center", gap: 8, fontSize: 11, color: S.muted }}>
              <OzonPayLogo height={16} />
              <span>Картой, СБП или в рассрочку</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
