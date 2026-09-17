"use client";
import { useState, useEffect, useRef } from "react";
import dynamic from "next/dynamic";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Check, AlertCircle, MapPin, ChevronLeft, Loader2, Shield, Lock, Tag, X, ChevronRight } from "lucide-react";
import { useCart } from "@/components/CartContext";
import { useAuth } from "@/components/AuthContext";
import type { ShopOrderPayload } from "@/lib/types";
import type { PvzPoint } from "@/components/OzonPickupMap";

const OzonPickupMap = dynamic(() => import("@/components/OzonPickupMap"), { ssr: false });

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
  accent3: "#EDD9B0",
};

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";

function DarkField({
  label, required, textarea, ...props
}: { label: string; required?: boolean; textarea?: boolean } & React.InputHTMLAttributes<HTMLInputElement | HTMLTextAreaElement>) {
  const inputStyle: React.CSSProperties = {
    width: "100%", paddingLeft: 16, paddingRight: 16, paddingTop: 12, paddingBottom: 12,
    fontSize: 13, fontWeight: 500, fontFamily: "inherit",
    background: "rgba(255,255,255,0.05)", border: `1.5px solid ${S.border}`,
    borderRadius: 12, color: S.text, outline: "none", transition: "all 0.15s ease",
    resize: textarea ? "none" as const : undefined, boxSizing: "border-box" as const,
  };
  return (
    <div>
      <label style={{ display: "block", fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle, marginBottom: 6 }}>
        {label}{required && " *"}
      </label>
      {textarea
        ? <textarea {...(props as React.TextareaHTMLAttributes<HTMLTextAreaElement>)} rows={3} style={inputStyle}
            onFocus={e => { e.target.style.borderColor = "rgba(201,169,110,0.4)"; e.target.style.background = "rgba(255,255,255,0.08)"; }}
            onBlur={e => { e.target.style.borderColor = S.border; e.target.style.background = "rgba(255,255,255,0.05)"; }} />
        : <input {...(props as React.InputHTMLAttributes<HTMLInputElement>)} required={required} style={inputStyle}
            onFocus={e => { e.target.style.borderColor = "rgba(201,169,110,0.4)"; e.target.style.background = "rgba(255,255,255,0.08)"; }}
            onBlur={e => { e.target.style.borderColor = S.border; e.target.style.background = "rgba(255,255,255,0.05)"; }} />
      }
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ background: S.surface, borderRadius: 20, padding: "22px 24px", border: `1px solid ${S.border}` }}>
      <h3 style={{ fontSize: 14, fontWeight: 700, color: S.text, marginBottom: 18 }}>{title}</h3>
      {children}
    </div>
  );
}

export default function CheckoutPage() {
  const { items, totalRub, clear } = useCart();
  const { customer, token } = useAuth();
  const router = useRouter();

  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [city, setCity] = useState("");
  const [address, setAddress] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const [comment, setComment] = useState("");
  const [deliveryType, setDeliveryType] = useState<"courier" | "pickup">("pickup");
  const [paymentMethod, setPaymentMethod] = useState<"sbp" | "cash">("sbp");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pvzMapOpen, setPvzMapOpen] = useState(false);
  const [selectedPvz, setSelectedPvz] = useState<PvzPoint | null>(null);

  const [promoInput, setPromoInput] = useState("");
  const [promoValidating, setPromoValidating] = useState(false);
  const [promoResult, setPromoResult] = useState<{ valid: boolean; discountPct?: number; code?: string } | null>(null);
  const promoRef = useRef<HTMLInputElement>(null);

  // Pre-fill from auth
  useEffect(() => {
    if (customer) {
      if (customer.firstName) setFirstName(customer.firstName);
      if (customer.lastName) setLastName(customer.lastName ?? "");
      if (customer.email) setEmail(customer.email);
      if (customer.phone) setPhone(customer.phone ?? "");
    }
  }, [customer]);

  useEffect(() => {
    if (items.length === 0) router.replace("/cart");
  }, [items, router]);

  const refCode = typeof window !== "undefined" ? (localStorage.getItem("shopRefCode") || undefined) : undefined;
  const refDiscount = refCode ? Math.round(totalRub * 0.07) : 0;
  const promoDiscount = (promoResult?.valid && !refCode) ? Math.round(totalRub * (promoResult.discountPct ?? 0) / 100) : 0;
  const total = totalRub - refDiscount - promoDiscount;

  async function applyPromo() {
    const code = promoInput.trim().toUpperCase();
    if (!code) return;
    setPromoValidating(true);
    try {
      const r = await fetch(`${BASE}/api/shop/promo/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code }),
      });
      const data = await r.json();
      setPromoResult(data.valid ? { valid: true, discountPct: data.discountPct, code: data.code } : { valid: false });
    } catch {
      setPromoResult({ valid: false });
    } finally {
      setPromoValidating(false);
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!firstName || !phone || !email) return;
    setLoading(true);
    setError(null);
    try {
      const resolvedAddress = deliveryType === "pickup" && selectedPvz ? selectedPvz.address : address;
      const resolvedCity = deliveryType === "pickup" && selectedPvz ? (selectedPvz.city ?? city) : city;
      const payload: ShopOrderPayload = {
        items: items.map(i => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
        delivery: { type: deliveryType, firstName, lastName, phone, email, city: resolvedCity, address: resolvedAddress, postalCode, pvzId: deliveryType === "pickup" ? (selectedPvz?.id ?? undefined) : undefined },
        comment: comment || undefined,
        paymentMethod,
        promoCode: promoResult?.valid ? promoResult.code : undefined,
        refCode,
      };
      const res = await fetch(`${BASE}/api/shop/orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
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
        router.push(`/order-success?id=${data.id ?? data.orderId ?? ""}`);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Не удалось оформить заказ");
    } finally {
      setLoading(false);
    }
  }

  if (items.length === 0) return null;

  const valid = !!firstName && !!phone && !!email && (deliveryType === "courier" ? !!address : !!selectedPvz);

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <OzonPickupMap
        open={pvzMapOpen}
        onClose={() => setPvzMapOpen(false)}
        defaultCity={city}
        onSelect={(pvz: PvzPoint) => {
          setSelectedPvz(pvz);
          if (pvz.city) setCity(pvz.city);
          setPvzMapOpen(false);
        }}
      />
      <div style={{ maxWidth: 1000, margin: "0 auto", padding: "clamp(20px,3vw,48px) clamp(16px,4vw,32px)" }}>
        <Link href="/cart" style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 13, color: S.muted, textDecoration: "none", marginBottom: 24 }}
          onMouseEnter={e => (e.currentTarget.style.color = S.text)}
          onMouseLeave={e => (e.currentTarget.style.color = S.muted)}
        >
          <ChevronLeft size={15} /> Назад в корзину
        </Link>

        <h1 style={{ fontSize: "clamp(22px,3vw,30px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em", marginBottom: 32 }}>
          Оформление заказа
        </h1>

        <form onSubmit={handleSubmit}>
          <style>{`@media(min-width:1024px){.checkout-grid{grid-template-columns:1fr 340px!important;}}`}</style>
          <div className="checkout-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16, alignItems: "start" }}>

            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>

              {/* Contact */}
              <Section title="Контактные данные">
                <style>{`@media(min-width:560px){.contact-grid{grid-template-columns:1fr 1fr!important;}}`}</style>
                <div className="contact-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 12 }}>
                  <DarkField label="Имя" required value={firstName} onChange={e => setFirstName((e.target as HTMLInputElement).value)} placeholder="Иван" />
                  <DarkField label="Фамилия" value={lastName} onChange={e => setLastName((e.target as HTMLInputElement).value)} placeholder="Иванов" />
                  <DarkField label="Телефон" required type="tel" value={phone} onChange={e => setPhone((e.target as HTMLInputElement).value)} placeholder="+7 900 000-00-00" />
                  <DarkField label="Email" required type="email" value={email} onChange={e => setEmail((e.target as HTMLInputElement).value)} placeholder="ivan@mail.ru" />
                </div>
              </Section>

              {/* Delivery */}
              <Section title="Доставка">
                <div style={{ display: "flex", gap: 10, marginBottom: 16 }}>
                  {([["pickup", "ПВЗ Ozon"], ["courier", "Курьер"]] as const).map(([val, label]) => (
                    <button key={val} type="button" onClick={() => setDeliveryType(val)} style={{
                      flex: 1, padding: "12px", border: `1px solid ${deliveryType === val ? "rgba(201,169,110,0.5)" : S.border}`,
                      borderRadius: 12, background: deliveryType === val ? "rgba(201,169,110,0.08)" : "transparent",
                      color: deliveryType === val ? S.accent : S.muted, cursor: "pointer", fontSize: 13,
                      display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                    }}>
                      <MapPin size={14} />{label}
                    </button>
                  ))}
                </div>
                <div style={{ display: "grid", gap: 12 }}>
                  <DarkField label="Город" required value={city} onChange={e => setCity((e.target as HTMLInputElement).value)} placeholder="Москва" />
                  {deliveryType === "pickup" ? (
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle, marginBottom: 6 }}>
                        Пункт выдачи
                      </label>
                      {selectedPvz ? (
                        <div style={{ display: "flex", gap: 10, alignItems: "flex-start", padding: "12px 16px", background: "rgba(201,169,110,0.06)", border: "1.5px solid rgba(201,169,110,0.3)", borderRadius: 12 }}>
                          <MapPin size={15} style={{ color: S.accent, flexShrink: 0, marginTop: 2 }} />
                          <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{ fontSize: 13, fontWeight: 600, color: S.text }}>{selectedPvz.name}</div>
                            <div style={{ fontSize: 12, color: S.muted, marginTop: 2 }}>{selectedPvz.address}</div>
                          </div>
                          <button type="button" onClick={() => setPvzMapOpen(true)} style={{ fontSize: 11, color: S.accent, background: "none", border: "none", cursor: "pointer", flexShrink: 0, fontWeight: 600 }}>
                            Изменить
                          </button>
                        </div>
                      ) : (
                        <button type="button" onClick={() => setPvzMapOpen(true)} style={{
                          width: "100%", padding: "12px 16px", display: "flex", alignItems: "center", gap: 10,
                          background: "rgba(255,255,255,0.04)", border: `1.5px dashed ${S.border}`,
                          borderRadius: 12, cursor: "pointer", color: S.muted, fontSize: 13, fontFamily: "inherit",
                          transition: "border-color 0.15s, color 0.15s",
                        }}
                          onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,169,110,0.4)"; el.style.color = S.accent; }}
                          onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = S.border; el.style.color = S.muted; }}
                        >
                          <MapPin size={15} style={{ flexShrink: 0 }} />
                          Выбрать пункт выдачи на карте
                          <ChevronRight size={14} style={{ marginLeft: "auto" }} />
                        </button>
                      )}
                    </div>
                  ) : (
                    <DarkField label="Улица, дом, квартира" required value={address} onChange={e => setAddress((e.target as HTMLInputElement).value)} placeholder="ул. Ленина, д. 1, кв. 5" />
                  )}
                  {deliveryType === "courier" && <DarkField label="Индекс" value={postalCode} onChange={e => setPostalCode((e.target as HTMLInputElement).value)} placeholder="123456" />}
                </div>
              </Section>

              {/* Payment */}
              <Section title="Способ оплаты">
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                  {([["sbp", "💸 СБП"], ["cash", "💵 Наличными"]] as const).map(([val, label]) => (
                    <button key={val} type="button" onClick={() => setPaymentMethod(val)} style={{
                      padding: "10px 18px", border: `1px solid ${paymentMethod === val ? "rgba(201,169,110,0.5)" : S.border}`,
                      borderRadius: 12, background: paymentMethod === val ? "rgba(201,169,110,0.08)" : "transparent",
                      color: paymentMethod === val ? S.accent : S.muted, cursor: "pointer", fontSize: 13,
                    }}>
                      {label}
                    </button>
                  ))}
                </div>
              </Section>

              {/* Comment */}
              <Section title="Комментарий">
                <DarkField label="Пожелания к заказу" textarea value={comment} onChange={e => setComment((e.target as HTMLTextAreaElement).value)} placeholder="Необязательно..." />
              </Section>
            </div>

            {/* Order summary */}
            <div style={{ background: S.surface, borderRadius: 20, padding: 22, border: `1px solid ${S.border}`, position: "sticky", top: 80 }}>
              <h3 style={{ fontSize: 14, fontWeight: 700, color: S.text, marginBottom: 18 }}>Ваш заказ</h3>
              <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 16, maxHeight: 200, overflowY: "auto" }}>
                {items.map(({ product, quantity }) => (
                  <div key={product.offerId} style={{ display: "flex", gap: 8, fontSize: 12 }}>
                    <span style={{ flex: 1, color: S.muted, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden", lineHeight: 1.4 }}>{product.name}</span>
                    <span style={{ color: S.subtle, flexShrink: 0 }}>×{quantity}</span>
                    <span style={{ fontWeight: 600, color: S.text, flexShrink: 0 }}>{(product.priceRub * quantity).toLocaleString("ru-RU")} ₽</span>
                  </div>
                ))}
              </div>

              {/* Promo code */}
              {!refCode && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ display: "flex", gap: 8 }}>
                    <div style={{ position: "relative", flex: 1 }}>
                      <Tag size={13} style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: S.subtle, pointerEvents: "none" }} />
                      <input
                        ref={promoRef}
                        type="text"
                        value={promoInput}
                        onChange={e => { setPromoInput(e.target.value.toUpperCase()); setPromoResult(null); }}
                        onKeyDown={e => e.key === "Enter" && (e.preventDefault(), applyPromo())}
                        placeholder="Промокод"
                        maxLength={32}
                        disabled={promoResult?.valid}
                        style={{
                          width: "100%", paddingLeft: 34, paddingRight: 12, paddingTop: 10, paddingBottom: 10,
                          fontSize: 12, fontFamily: "monospace", letterSpacing: "0.08em", boxSizing: "border-box",
                          background: promoResult?.valid ? "rgba(74,222,128,0.06)" : "rgba(255,255,255,0.04)",
                          border: `1.5px solid ${promoResult === null ? S.border : promoResult.valid ? "rgba(74,222,128,0.4)" : "rgba(239,68,68,0.4)"}`,
                          borderRadius: 10, color: S.text, outline: "none",
                        }}
                      />
                    </div>
                    {promoResult?.valid
                      ? <button type="button" onClick={() => { setPromoResult(null); setPromoInput(""); }} style={{ background: "rgba(255,255,255,0.06)", border: `1px solid ${S.border}`, borderRadius: 8, padding: "9px 12px", color: S.muted, cursor: "pointer" }}><X size={13} /></button>
                      : <button type="button" onClick={applyPromo} disabled={!promoInput.trim() || promoValidating} style={{ background: S.accent, color: "#0E0D0B", border: "none", borderRadius: 8, padding: "9px 14px", fontSize: 12, fontWeight: 600, cursor: "pointer", opacity: (!promoInput.trim() || promoValidating) ? 0.5 : 1, flexShrink: 0 }}>
                          {promoValidating ? <Loader2 size={12} style={{ animation: "spin 1s linear infinite" }} /> : "Применить"}
                        </button>
                    }
                  </div>
                  {promoResult?.valid && <div style={{ display: "flex", alignItems: "center", gap: 5, marginTop: 6, fontSize: 12, color: "#4ade80" }}><Check size={12} /> {promoResult.code} — скидка {promoResult.discountPct}%</div>}
                  {promoResult !== null && !promoResult.valid && <div style={{ marginTop: 6, fontSize: 12, color: "#f87171" }}>Промокод не найден</div>}
                </div>
              )}

              <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 14, display: "flex", flexDirection: "column", gap: 8, marginBottom: 20 }}>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: S.muted }}>
                  <span>Товары</span>
                  <span style={{ color: S.text, fontWeight: 500 }}>{totalRub.toLocaleString("ru-RU")} ₽</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, color: S.muted }}>
                  <span>Доставка</span>
                  <span style={{ color: "#4ade80", fontWeight: 600 }}>Бесплатно</span>
                </div>
                {refDiscount > 0 && (
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
                    <span style={{ color: S.accent }}>Реферальная скидка −7%</span>
                    <span style={{ color: "#4ade80", fontWeight: 600 }}>−{refDiscount.toLocaleString("ru-RU")} ₽</span>
                  </div>
                )}
                {promoDiscount > 0 && (
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
                    <span style={{ color: S.accent }}>Промокод −{promoResult?.discountPct}%</span>
                    <span style={{ color: "#4ade80", fontWeight: 600 }}>−{promoDiscount.toLocaleString("ru-RU")} ₽</span>
                  </div>
                )}
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 700, color: S.text, paddingTop: 8, borderTop: `1px solid ${S.border}`, marginTop: 4 }}>
                  <span>К оплате</span>
                  <span>{total.toLocaleString("ru-RU")} ₽</span>
                </div>
              </div>

              {error && (
                <div style={{ background: "rgba(239,68,68,0.1)", color: "#f87171", fontSize: 12, padding: "10px 14px", borderRadius: 12, marginBottom: 14, border: "1px solid rgba(239,68,68,0.2)", display: "flex", gap: 8, alignItems: "flex-start" }}>
                  <AlertCircle size={14} style={{ flexShrink: 0, marginTop: 1 }} />{error}
                </div>
              )}

              <button type="submit" disabled={!valid || loading} style={{
                width: "100%", fontSize: 15, padding: "14px 0", display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                background: valid && !loading ? "#f2efe6" : "rgba(242,239,230,0.25)",
                color: "#14120f", border: "none", borderRadius: 12, fontWeight: 600, letterSpacing: "0.06em",
                cursor: valid && !loading ? "pointer" : "not-allowed", fontFamily: "inherit",
              }}>
                {loading
                  ? <><Loader2 size={16} style={{ animation: "spin 1s linear infinite" }} /> Оформляем...</>
                  : <><Lock size={14} /> Оплатить · {total.toLocaleString("ru-RU")} ₽</>
                }
              </button>
              <div style={{ marginTop: 12, display: "flex", alignItems: "center", justifyContent: "center", gap: 6, fontSize: 11, color: S.subtle }}>
                <Shield size={11} />
                <span>Безопасное оформление заказа</span>
              </div>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
