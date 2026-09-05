import { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useMutation } from "@tanstack/react-query";
import { ChevronLeft, Loader2, Shield, Lock, MapPin, ChevronRight } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import OzonPickupMap, { type PvzPoint } from "../components/OzonPickupMap";

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

interface FormData {
  firstName: string;
  lastName: string;
  phone: string;
  email: string;
  city: string;
  comment: string;
}

const INITIAL: FormData = { firstName: "", lastName: "", phone: "", email: "", city: "", comment: "" };

function DarkField({
  label, required, textarea, ...props
}: { label: string; required?: boolean; textarea?: boolean } & React.InputHTMLAttributes<HTMLInputElement | HTMLTextAreaElement>) {
  const inputStyle: React.CSSProperties = {
    width: "100%", paddingLeft: 16, paddingRight: 16, paddingTop: 12, paddingBottom: 12,
    fontSize: 13, fontWeight: 500, fontFamily: "inherit",
    background: "rgba(255,255,255,0.05)", border: `1.5px solid ${S.border}`,
    borderRadius: 12, color: S.text, outline: "none", transition: "all 0.15s ease",
    resize: textarea ? "none" as const : undefined,
  };
  return (
    <div>
      <label style={{ display: "block", fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.1em", color: S.subtle, marginBottom: 6 }}>
        {label}{required && " *"}
      </label>
      {textarea
        ? <textarea {...(props as React.TextareaHTMLAttributes<HTMLTextAreaElement>)} rows={3} style={inputStyle}
            onFocus={e => { (e.target as HTMLTextAreaElement).style.borderColor = "rgba(201,169,110,0.4)"; (e.target as HTMLTextAreaElement).style.background = "rgba(255,255,255,0.08)"; }}
            onBlur={e => { (e.target as HTMLTextAreaElement).style.borderColor = S.border; (e.target as HTMLTextAreaElement).style.background = "rgba(255,255,255,0.05)"; }} />
        : <input {...(props as React.InputHTMLAttributes<HTMLInputElement>)} required={required} style={inputStyle}
            onFocus={e => { (e.target as HTMLInputElement).style.borderColor = "rgba(201,169,110,0.4)"; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.08)"; }}
            onBlur={e => { (e.target as HTMLInputElement).style.borderColor = S.border; (e.target as HTMLInputElement).style.background = "rgba(255,255,255,0.05)"; }} />
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
  const navigate = useNavigate();
  const [pvzOpen, setPvzOpen] = useState(false);
  const [selectedPvz, setSelectedPvz] = useState<PvzPoint | null>(null);
  const [form, setForm] = useState<FormData>({
    ...INITIAL,
    firstName: customer?.firstName || "",
    lastName: customer?.lastName || "",
    email: customer?.email || "",
    phone: customer?.phone || "",
  });

  const deliveryCost = 0;
  const refCode = localStorage.getItem("shopRefCode") || undefined;
  const refDiscount = refCode ? Math.round(totalRub * 0.07) : 0;
  const total = totalRub + deliveryCost - refDiscount;

  const mutation = useMutation({
    mutationFn: () => api.createOrder({
      items: items.map((i) => ({ offerId: i.product.offerId, quantity: i.quantity, priceRub: i.product.priceRub })),
      delivery: {
        type: "pickup",
        firstName: form.firstName,
        lastName: form.lastName,
        phone: form.phone,
        email: form.email,
        city: selectedPvz?.city || form.city || "",
        address: selectedPvz?.address || "",
        pvzId: selectedPvz?.id,
      },
      comment: form.comment || undefined,
      refCode,
    }, token ?? undefined),
    onSuccess: (order) => {
      clear();
      if (order.paymentUrl) {
        window.location.href = order.paymentUrl;
      } else {
        navigate(`/order-success?id=${order.id}`);
      }
    },
  });

  function set(key: keyof FormData) {
    return (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
      setForm((f) => ({ ...f, [key]: e.target.value }));
  }

  const valid = form.firstName && form.phone && form.email && !!selectedPvz;

  if (!items.length) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", padding: 48 }}>
          <p style={{ color: S.muted, marginBottom: 16, fontSize: 14 }}>Корзина пуста</p>
          <Link to="/catalog" style={{ color: S.accent3, fontSize: 14, textDecoration: "none", fontWeight: 500 }}>В каталог</Link>
        </div>
      </div>
    );
  }

  return (
    <>
      <OzonPickupMap
        open={pvzOpen}
        onClose={() => setPvzOpen(false)}
        onSelect={pvz => { setSelectedPvz(pvz); setPvzOpen(false); }}
        defaultCity={form.city}
      />
      <div style={{ background: S.bg, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1000, margin: "0 auto", padding: "clamp(20px,3vw,48px) clamp(16px,4vw,32px)" }}>
          <Link to="/cart" style={{
            display: "inline-flex", alignItems: "center", gap: 6, fontSize: 13, color: S.muted,
            textDecoration: "none", marginBottom: 24, transition: "color 0.15s ease",
          }}
            onMouseEnter={e => (e.currentTarget.style.color = S.text)}
            onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>
            <ChevronLeft size={15} /> Назад в корзину
          </Link>

          <h1 style={{ fontSize: "clamp(22px,3vw,30px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em", marginBottom: 32 }}>
            Оформление заказа
          </h1>

          <style>{`@media(min-width:1024px){.checkout-grid{grid-template-columns:1fr 340px!important;}}`}</style>
          <div className="checkout-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16, alignItems: "start" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>

              {/* Contact */}
              <Section title="Контактные данные">
                <style>{`@media(min-width:560px){.contact-grid{grid-template-columns:1fr 1fr!important;}}`}</style>
                <div className="contact-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 12 }}>
                  <DarkField label="Имя" required value={form.firstName} onChange={set("firstName")} placeholder="Иван" />
                  <DarkField label="Фамилия" value={form.lastName} onChange={set("lastName")} placeholder="Иванов" />
                  <DarkField label="Телефон" required type="tel" value={form.phone} onChange={set("phone")} placeholder="+7 900 000-00-00" />
                  <DarkField label="Email" required type="email" value={form.email} onChange={set("email")} placeholder="ivan@mail.ru" />
                </div>
              </Section>

              {/* Pickup */}
              <Section title="Пункт выдачи Ozon">
                <button type="button" onClick={() => setPvzOpen(true)} style={{
                  width: "100%", display: "flex", alignItems: "center", gap: 14, padding: "14px 16px",
                  borderRadius: 14, border: `2px ${selectedPvz ? "solid" : "dashed"} ${selectedPvz ? "rgba(74,222,128,0.4)" : S.border}`,
                  background: selectedPvz ? "rgba(74,222,128,0.06)" : "rgba(255,255,255,0.02)",
                  cursor: "pointer", textAlign: "left", transition: "all 0.2s ease",
                }}
                  onMouseEnter={e => {
                    if (!selectedPvz) {
                      (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,169,110,0.4)";
                      (e.currentTarget as HTMLElement).style.background = "rgba(201,169,110,0.04)";
                    }
                  }}
                  onMouseLeave={e => {
                    if (!selectedPvz) {
                      (e.currentTarget as HTMLElement).style.borderColor = S.border;
                      (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.02)";
                    }
                  }}>
                  <div style={{
                    width: 38, height: 38, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                    background: selectedPvz ? "rgba(74,222,128,0.12)" : "rgba(255,106,0,0.12)",
                    border: `1px solid ${selectedPvz ? "rgba(74,222,128,0.2)" : "rgba(255,106,0,0.2)"}`,
                  }}>
                    <MapPin size={16} style={{ color: selectedPvz ? "#4ade80" : "#fb923c" }} />
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    {selectedPvz ? (
                      <>
                        <p style={{ fontSize: 12, fontWeight: 700, color: "#4ade80", marginBottom: 2 }}>Пункт выдачи выбран ✓</p>
                        <p style={{ fontSize: 12, color: S.muted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{selectedPvz.address}</p>
                      </>
                    ) : (
                      <>
                        <p style={{ fontSize: 13, fontWeight: 600, color: S.text }}>Выбрать пункт выдачи Ozon</p>
                        <p style={{ fontSize: 11, color: S.muted }}>Бесплатно — откроется карта с ПВЗ</p>
                      </>
                    )}
                  </div>
                  <ChevronRight size={15} style={{ color: S.subtle, flexShrink: 0 }} />
                </button>
              </Section>

              {/* Comment */}
              <Section title="Комментарий">
                <DarkField
                  label="Пожелания к заказу"
                  textarea
                  value={form.comment}
                  onChange={set("comment")}
                  placeholder="Необязательно..."
                />
              </Section>

            </div>

            {/* Summary */}
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
                    <span style={{ color: "#c9a25e" }}>Реферальная скидка −7%</span>
                    <span style={{ color: "#4ade80", fontWeight: 600 }}>−{refDiscount.toLocaleString("ru-RU")} ₽</span>
                  </div>
                )}
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 18, fontWeight: 700, color: S.text, paddingTop: 8, borderTop: `1px solid ${S.border}`, marginTop: 4 }}>
                  <span>Итого</span>
                  <span>{total.toLocaleString("ru-RU")} ₽</span>
                </div>
              </div>

              {mutation.error && (
                <div style={{ background: "rgba(239,68,68,0.1)", color: "#f87171", fontSize: 12, padding: "10px 14px", borderRadius: 12, marginBottom: 14, border: "1px solid rgba(239,68,68,0.2)" }}>
                  {(mutation.error as Error).message}
                </div>
              )}

              <button type="button" onClick={() => mutation.mutate()} disabled={!valid || mutation.isPending}
                className="btn-primary"
                style={{
                  width: "100%", fontSize: 15, padding: "14px 0", justifyContent: "center",
                  opacity: (!valid || mutation.isPending) ? 0.45 : 1,
                  cursor: (!valid || mutation.isPending) ? "not-allowed" : "pointer",
                  animation: (!valid || mutation.isPending) ? "none" : undefined,
                }}>
                {mutation.isPending
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
        </div>
      </div>
    </>
  );
}
