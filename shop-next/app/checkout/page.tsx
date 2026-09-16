"use client";
import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Check, AlertCircle, MapPin } from "lucide-react";
import { useCart } from "@/components/CartContext";
import type { ShopOrderPayload } from "@/lib/types";

const S = {
  border: "rgba(255,252,245,0.08)",
  borderMd: "rgba(255,252,245,0.14)",
  accent: "#C9A96E",
  muted: "rgba(244,239,230,0.48)",
  text: "#F4EFE6",
  surface2: "#1D1C18",
};

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";

function Field({ label, name, type = "text", value, onChange, required, placeholder }: { label: string; name: string; type?: string; value: string; onChange: (v: string) => void; required?: boolean; placeholder?: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <label style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", color: "rgba(201,162,94,0.7)" }}>{label}{required && " *"}</label>
      <input type={type} name={name} value={value} onChange={e => onChange(e.target.value)} required={required} placeholder={placeholder} style={{ background: S.surface2, border: `1px solid ${S.borderMd}`, borderRadius: 8, padding: "10px 14px", color: S.text, fontSize: 14, outline: "none" }} />
    </div>
  );
}

export default function CheckoutPage() {
  const { items, totalRub, totalItems, clear } = useCart();
  const router = useRouter();

  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [address, setAddress] = useState("");
  const [city, setCity] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const [comment, setComment] = useState("");
  const [promoCode, setPromoCode] = useState("");
  const [deliveryType, setDeliveryType] = useState<"courier" | "pickup">("courier");
  const [paymentMethod, setPaymentMethod] = useState<"sbp" | "cash">("sbp");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (items.length === 0) router.replace("/cart");
  }, [items, router]);

  if (items.length === 0) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const payload: ShopOrderPayload = {
        items: items.map(i => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
        delivery: { type: deliveryType, firstName, lastName, phone, email, address, city, postalCode },
        comment: comment || undefined,
        paymentMethod,
        promoCode: promoCode || undefined,
      };
      const res = await fetch(`${BASE}/api/shop/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const json = await res.json().catch(() => ({}));
        throw new Error(json.error ?? `Ошибка ${res.status}`);
      }
      const data = await res.json();
      clear();
      if (data.paymentUrl) {
        window.location.href = data.paymentUrl;
      } else {
        router.push(`/orders?success=${data.orderId ?? "1"}`);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Не удалось оформить заказ");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ maxWidth: 960, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
      <nav style={{ fontSize: 12, color: S.muted, marginBottom: 24 }}>
        <Link href="/" style={{ color: S.muted, textDecoration: "none" }}>Главная</Link> /{" "}
        <Link href="/cart" style={{ color: S.muted, textDecoration: "none" }}>Корзина</Link> /{" "}
        <span style={{ color: S.text }}>Оформление</span>
      </nav>

      <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,4vw,44px)", color: S.text, margin: "0 0 32px" }}>Оформление заказа</h1>

      <form onSubmit={handleSubmit}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 32, alignItems: "start" }} className="checkout-layout">
          <style>{`@media(max-width:700px){.checkout-layout{grid-template-columns:1fr!important;}}`}</style>

          <div style={{ display: "flex", flexDirection: "column", gap: 28 }}>
            {/* Contact */}
            <section>
              <div style={{ fontSize: 11, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 18 }}>Контакты</div>
              <div style={{ display: "grid", gap: 14 }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                  <Field label="Имя" name="firstName" value={firstName} onChange={setFirstName} required />
                  <Field label="Фамилия" name="lastName" value={lastName} onChange={setLastName} />
                </div>
                <Field label="Телефон" name="phone" type="tel" value={phone} onChange={setPhone} required placeholder="+7 900 000-00-00" />
                <Field label="Email" name="email" type="email" value={email} onChange={setEmail} required placeholder="email@example.com" />
              </div>
            </section>

            {/* Delivery type */}
            <section>
              <div style={{ fontSize: 11, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 14 }}>Способ доставки</div>
              <div style={{ display: "flex", gap: 10 }}>
                {([["courier", "Курьер"], ["pickup", "ПВЗ Ozon"]] as const).map(([val, label]) => (
                  <button key={val} type="button" onClick={() => setDeliveryType(val)} style={{ flex: 1, padding: "12px", border: `1px solid ${deliveryType === val ? "rgba(201,162,94,0.5)" : S.border}`, borderRadius: 10, background: deliveryType === val ? "rgba(201,162,94,0.08)" : "transparent", color: deliveryType === val ? S.accent : S.muted, cursor: "pointer", fontSize: 13, display: "flex", alignItems: "center", justifyContent: "center", gap: 6 }}>
                    <MapPin size={14} />{label}
                  </button>
                ))}
              </div>
            </section>

            {/* Address */}
            <section>
              <div style={{ fontSize: 11, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 18 }}>Адрес {deliveryType === "pickup" ? "ПВЗ" : "доставки"}</div>
              <div style={{ display: "grid", gap: 14 }}>
                <Field label="Город" name="city" value={city} onChange={setCity} required />
                <Field label={deliveryType === "pickup" ? "Адрес ПВЗ" : "Улица, дом, квартира"} name="address" value={address} onChange={setAddress} required={deliveryType === "courier"} />
                {deliveryType === "courier" && <Field label="Индекс" name="postalCode" value={postalCode} onChange={setPostalCode} />}
              </div>
            </section>

            {/* Payment */}
            <section>
              <div style={{ fontSize: 11, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 14 }}>Способ оплаты</div>
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                {([["sbp", "💸 СБП"], ["cash", "💵 Наличными"]] as const).map(([val, label]) => (
                  <button key={val} type="button" onClick={() => setPaymentMethod(val)} style={{ padding: "10px 18px", border: `1px solid ${paymentMethod === val ? "rgba(201,162,94,0.5)" : S.border}`, borderRadius: 10, background: paymentMethod === val ? "rgba(201,162,94,0.08)" : "transparent", color: paymentMethod === val ? S.accent : S.muted, cursor: "pointer", fontSize: 13 }}>
                    {label}
                  </button>
                ))}
              </div>
            </section>

            {/* Comment + promo */}
            <section>
              <div style={{ display: "grid", gap: 14 }}>
                <div>
                  <label style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", color: "rgba(201,162,94,0.7)", display: "block", marginBottom: 6 }}>Комментарий</label>
                  <textarea value={comment} onChange={e => setComment(e.target.value)} rows={2} style={{ width: "100%", background: S.surface2, border: `1px solid ${S.borderMd}`, borderRadius: 8, padding: "10px 14px", color: S.text, fontSize: 14, outline: "none", resize: "vertical", boxSizing: "border-box" }} />
                </div>
                <Field label="Промокод" name="promoCode" value={promoCode} onChange={setPromoCode} placeholder="Если есть" />
              </div>
            </section>
          </div>

          {/* Order summary */}
          <div style={{ background: S.surface2, border: `1px solid ${S.border}`, borderRadius: 16, padding: "24px", position: "sticky", top: 80 }}>
            <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: 20, color: S.text, margin: "0 0 16px" }}>Ваш заказ</h2>
            {items.map(i => (
              <div key={i.product.id} style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 8, fontSize: 12, color: S.muted }}>
                <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{i.product.name} × {i.quantity}</span>
                <span style={{ flexShrink: 0 }}>{(i.product.priceRub * i.quantity).toLocaleString("ru-RU")} ₽</span>
              </div>
            ))}
            <div style={{ borderTop: `1px solid ${S.border}`, margin: "16px 0", paddingTop: 16, display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 600, color: S.text }}>
              <span>Итого</span>
              <span>{totalRub.toLocaleString("ru-RU")} ₽</span>
            </div>

            {error && (
              <div style={{ marginBottom: 12, padding: "12px", background: "rgba(239,68,68,0.08)", border: "1px solid rgba(239,68,68,0.3)", borderRadius: 8, fontSize: 12, color: "#fca5a5", display: "flex", gap: 8, alignItems: "flex-start" }}>
                <AlertCircle size={14} style={{ flexShrink: 0, marginTop: 1 }} />{error}
              </div>
            )}

            <button type="submit" disabled={loading} style={{ width: "100%", height: 48, background: loading ? "rgba(242,239,230,0.3)" : "#f2efe6", color: "#14120f", border: "none", borderRadius: 10, fontSize: 13, fontWeight: 500, letterSpacing: "0.1em", textTransform: "uppercase", cursor: loading ? "not-allowed" : "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
              {loading ? "Оформляем..." : <><Check size={15} /> Подтвердить заказ</>}
            </button>
            <p style={{ fontSize: 10, color: S.muted, textAlign: "center", marginTop: 10, lineHeight: 1.5, opacity: 0.7 }}>Нажимая кнопку, вы соглашаетесь с условиями продажи</p>
          </div>
        </div>
      </form>
    </div>
  );
}
