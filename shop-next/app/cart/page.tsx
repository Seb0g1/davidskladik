"use client";
import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Minus, Plus, Trash2, ShoppingBag } from "lucide-react";
import { useCart } from "@/components/CartContext";

const S = {
  border: "rgba(255,252,245,0.08)",
  accent: "#C9A96E",
  muted: "rgba(244,239,230,0.48)",
  text: "#F4EFE6",
  surface: "#161512",
  surface2: "#1D1C18",
};

export default function CartPage() {
  const { items, setQty, remove, totalRub, totalItems } = useCart();
  const router = useRouter();

  if (items.length === 0) {
    return (
      <div style={{ minHeight: "60vh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", textAlign: "center", padding: 40 }}>
        <ShoppingBag size={64} style={{ color: "rgba(201,162,94,0.2)", marginBottom: 24 }} />
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: 36, color: S.text, margin: "0 0 12px" }}>Корзина пуста</h1>
        <p style={{ fontSize: 14, color: S.muted, marginBottom: 32 }}>Добавьте что-нибудь из каталога</p>
        <Link href="/catalog" className="btn-primary" style={{ textDecoration: "none" }}>Перейти в каталог</Link>
      </div>
    );
  }

  return (
    <div style={{ maxWidth: 900, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
      <nav style={{ fontSize: 12, color: S.muted, marginBottom: 24 }}>
        <Link href="/" style={{ color: S.muted, textDecoration: "none" }}>Главная</Link> /{" "}
        <span style={{ color: S.text }}>Корзина</span>
      </nav>

      <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,4vw,44px)", color: S.text, margin: "0 0 32px" }}>Корзина · {totalItems} {totalItems === 1 ? "товар" : totalItems < 5 ? "товара" : "товаров"}</h1>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 32 }} className="cart-layout">
        <style>{`@media(max-width:700px){.cart-layout{grid-template-columns:1fr!important;}}`}</style>

        {/* Items */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {items.map(item => {
            const img = item.product.images[0] ?? null;
            return (
              <div key={item.product.id} style={{ display: "flex", gap: 16, padding: "16px", border: `1px solid ${S.border}`, borderRadius: 12, background: S.surface2, alignItems: "center" }}>
                <div style={{ width: 72, height: 72, flexShrink: 0, borderRadius: 8, background: "radial-gradient(ellipse 85% 85% at center, #fff 0%, #ccc4b8 55%, #0E0D0B 88%)", overflow: "hidden" }}>
                  {img && <img src={img} alt={item.product.name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 6, mixBlendMode: "multiply" }} />}
                </div>

                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 9, letterSpacing: "0.2em", color: S.accent, marginBottom: 4 }}>{item.product.brand}</div>
                  <Link href={`/product/${encodeURIComponent(item.product.offerId)}`} style={{ fontSize: 13, color: S.text, textDecoration: "none", display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.product.name}</Link>
                  {item.product.volume && <div style={{ fontSize: 11, color: S.muted, marginTop: 2 }}>{item.product.volume}</div>}
                </div>

                <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
                  <div style={{ display: "flex", alignItems: "center", border: `1px solid ${S.border}`, borderRadius: 8 }}>
                    <button onClick={() => setQty(item.product.offerId, item.quantity - 1)} style={{ width: 32, height: 32, background: "none", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><Minus size={12} /></button>
                    <span style={{ width: 24, textAlign: "center", fontSize: 13, color: S.text }}>{item.quantity}</span>
                    <button onClick={() => setQty(item.product.offerId, item.quantity + 1)} style={{ width: 32, height: 32, background: "none", border: "none", color: S.muted, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}><Plus size={12} /></button>
                  </div>
                  <span style={{ fontSize: 14, fontWeight: 500, color: S.text, minWidth: 80, textAlign: "right" }}>{(item.product.priceRub * item.quantity).toLocaleString("ru-RU")} ₽</span>
                  <button onClick={() => remove(item.product.offerId)} style={{ background: "none", border: "none", color: "rgba(244,239,230,0.28)", cursor: "pointer", padding: 4 }}><Trash2 size={14} /></button>
                </div>
              </div>
            );
          })}
        </div>

        {/* Summary */}
        <div style={{ background: S.surface2, border: `1px solid ${S.border}`, borderRadius: 16, padding: "24px", height: "fit-content", position: "sticky", top: 80 }}>
          <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: 22, color: S.text, margin: "0 0 20px" }}>Итого</h2>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 14, color: S.muted, marginBottom: 10 }}>
            <span>{totalItems} {totalItems === 1 ? "товар" : totalItems < 5 ? "товара" : "товаров"}</span>
            <span>{totalRub.toLocaleString("ru-RU")} ₽</span>
          </div>
          <div style={{ borderTop: `1px solid ${S.border}`, margin: "16px 0", paddingTop: 16, display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 600, color: S.text }}>
            <span>Сумма</span>
            <span>{totalRub.toLocaleString("ru-RU")} ₽</span>
          </div>
          <button onClick={() => router.push("/checkout")} style={{ width: "100%", height: 48, background: "#f2efe6", color: "#14120f", border: "none", borderRadius: 10, fontSize: 13, fontWeight: 500, letterSpacing: "0.1em", textTransform: "uppercase", cursor: "pointer", marginTop: 8 }}>
            Оформить заказ
          </button>
          <Link href="/catalog" style={{ display: "block", textAlign: "center", marginTop: 14, fontSize: 12, color: S.muted, textDecoration: "none" }}>← Продолжить покупки</Link>
        </div>
      </div>
    </div>
  );
}
