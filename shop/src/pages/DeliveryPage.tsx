import { Truck, CreditCard, RotateCcw, MapPin, Clock, Shield } from "lucide-react";

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
  accent2: "#D9BF8F",
};

function Section({ icon, title, children }: { icon: React.ReactNode; title: string; children: React.ReactNode }) {
  return (
    <div style={{ background: S.surface, borderRadius: 20, padding: "28px 28px 24px", border: `1px solid ${S.border}`, marginBottom: 16 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
        <div style={{ width: 40, height: 40, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(201,169,110,0.1)", border: `1px solid rgba(201,169,110,0.2)`, flexShrink: 0 }}>
          {icon}
        </div>
        <h2 style={{ fontSize: 17, fontWeight: 600, color: S.text, margin: 0 }}>{title}</h2>
      </div>
      <div style={{ fontSize: 14, color: S.muted, lineHeight: 1.8 }}>
        {children}
      </div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12, padding: "10px 0", borderBottom: `1px solid ${S.border}` }}>
      <span style={{ color: S.muted, fontSize: 14 }}>{label}</span>
      <span style={{ color: S.text, fontSize: 14, fontWeight: 500, textAlign: "right" }}>{value}</span>
    </div>
  );
}

export default function DeliveryPage() {
  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 800, margin: "0 auto", padding: "64px 24px 96px" }}>

        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 56 }}>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 10, marginBottom: 20, padding: "6px 16px", borderRadius: 999, border: `1px solid ${S.borderMd}`, background: "rgba(201,169,110,0.06)" }}>
            <Truck size={13} style={{ color: S.accent }} />
            <span style={{ fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", color: S.accent }}>Условия</span>
          </div>
          <h1 className="serif" style={{ fontSize: "clamp(26px,5vw,40px)", fontStyle: "italic", fontWeight: 600, color: S.text, margin: "0 0 16px" }}>
            Доставка и оплата
          </h1>
          <div style={{ width: 48, height: 1, background: S.accent, margin: "0 auto 16px" }} />
          <p style={{ fontSize: 15, color: S.muted, lineHeight: 1.7, maxWidth: 520, margin: "0 auto" }}>
            Мы доставляем оригинальную парфюмерию по всей России через инфраструктуру Ozon
          </p>
        </div>

        {/* Delivery */}
        <Section icon={<Truck size={18} style={{ color: S.accent }} />} title="Доставка">
          <div style={{ marginBottom: 20 }}>
            Все заказы отправляются через логистику <strong style={{ color: S.text }}>Ozon FBS</strong> (Fulfillment by Seller).
            Вы самостоятельно выбираете удобный пункт выдачи Ozon на карте при оформлении заказа.
          </div>
          <Row label="Способ доставки" value="Пункт выдачи Ozon (ПВЗ)" />
          <Row label="Стоимость" value="Бесплатно" />
          <Row label="Срок доставки" value="1–5 рабочих дней" />
          <Row label="Geography" value="По всей России" />
          <div style={{ marginTop: 16, padding: "14px 16px", borderRadius: 12, background: "rgba(201,169,110,0.06)", border: `1px solid rgba(201,169,110,0.15)` }}>
            <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <Clock size={14} style={{ color: S.accent, flexShrink: 0, marginTop: 2 }} />
              <span style={{ fontSize: 13 }}>
                После оформления заказа мы обрабатываем его в течение <strong style={{ color: S.accent2 }}>1 рабочего дня</strong> и передаём в службу доставки Ozon.
              </span>
            </div>
          </div>
        </Section>

        {/* Payment */}
        <Section icon={<CreditCard size={18} style={{ color: S.accent }} />} title="Оплата">
          <div style={{ marginBottom: 20 }}>
            Оплата производится онлайн через защищённую платёжную систему. Ваши платёжные данные не хранятся на наших серверах.
          </div>
          <Row label="Банковские карты" value="Visa, Mastercard, Мир" />
          <Row label="Онлайн-банкинг" value="СБП (Система быстрых платежей)" />
          <Row label="Валюта" value="Российский рубль (₽)" />
          <div style={{ marginTop: 16, padding: "14px 16px", borderRadius: 12, background: "rgba(74,222,128,0.05)", border: "1px solid rgba(74,222,128,0.15)" }}>
            <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <Shield size={14} style={{ color: "#4ade80", flexShrink: 0, marginTop: 2 }} />
              <span style={{ fontSize: 13 }}>
                Платёж защищён по протоколу <strong style={{ color: "#4ade80" }}>TLS/SSL</strong>. Данные карты не передаются магазину.
              </span>
            </div>
          </div>
        </Section>

        {/* Returns */}
        <Section icon={<RotateCcw size={18} style={{ color: S.accent }} />} title="Возврат и обмен">
          <div style={{ marginBottom: 20 }}>
            В соответствии с Законом РФ «О защите прав потребителей» (статья 26.1, 18) вы имеете право:
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 20 }}>
            {[
              { title: "14 дней — возврат товара надлежащего качества", desc: "Товар можно вернуть в течение 14 дней с момента получения, если он не был в употреблении, сохранены оригинальная упаковка, товарный вид и потребительские свойства." },
              { title: "Возврат некачественного товара", desc: "Если товар имеет дефекты, вы вправе потребовать замены, ремонта, соразмерного уменьшения цены или полного возврата средств." },
              { title: "Возврат средств", desc: "Деньги возвращаются на исходный способ оплаты в течение 10 рабочих дней после получения и проверки товара." },
            ].map(item => (
              <div key={item.title} style={{ padding: "14px 16px", borderRadius: 12, background: S.surface2, border: `1px solid ${S.border}` }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: S.accent2, marginBottom: 6 }}>{item.title}</div>
                <div style={{ fontSize: 13 }}>{item.desc}</div>
              </div>
            ))}
          </div>
          <div style={{ padding: "14px 16px", borderRadius: 12, background: "rgba(201,169,110,0.06)", border: `1px solid rgba(201,169,110,0.15)` }}>
            <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <MapPin size={14} style={{ color: S.accent, flexShrink: 0, marginTop: 2 }} />
              <span style={{ fontSize: 13 }}>
                Для оформления возврата напишите нам на{" "}
                <a href="mailto:info@magicvibes.ru" style={{ color: S.accent, textDecoration: "none" }}>info@magicvibes.ru</a>{" "}
                с темой «Возврат» и номером заказа. Мы свяжемся в течение 1 рабочего дня.
              </span>
            </div>
          </div>
        </Section>

        {/* Legal details */}
        <div style={{ background: S.surface, borderRadius: 20, padding: "24px 28px", border: `1px solid ${S.border}` }}>
          <h2 style={{ fontSize: 14, fontWeight: 700, color: S.muted, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 16 }}>Реквизиты продавца</h2>
          <div style={{ fontSize: 13, color: S.muted, lineHeight: 2 }}>
            <div><span style={{ color: S.subtle }}>Продавец: </span><span style={{ color: S.text }}>ИП Шальнев Давид Алиевич</span></div>
            <div><span style={{ color: S.subtle }}>ОГРНИП: </span><span style={{ color: S.text, fontFamily: "monospace" }}>323861700065205</span></div>
            <div><span style={{ color: S.subtle }}>ИНН: </span><span style={{ color: S.text, fontFamily: "monospace" }}>860203590860</span></div>
            <div><span style={{ color: S.subtle }}>Email: </span><a href="mailto:info@magicvibes.ru" style={{ color: S.accent, textDecoration: "none" }}>info@magicvibes.ru</a></div>
          </div>
          <div style={{ marginTop: 16, fontSize: 12, color: S.subtle, lineHeight: 1.7, borderTop: `1px solid ${S.border}`, paddingTop: 14 }}>
            Настоящая страница является публичной офертой в части условий доставки и возврата товаров,
            реализуемых ИП Шальнев Давид Алиевич. Совершая заказ, вы подтверждаете согласие с данными условиями.
          </div>
        </div>
      </div>
    </div>
  );
}
