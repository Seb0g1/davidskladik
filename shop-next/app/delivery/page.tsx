import type { Metadata } from "next";
import Link from "next/link";
import { fetchSettings } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

export const revalidate = 3600;

export const metadata: Metadata = {
  title: `Доставка и оплата | ${SITE_NAME}`,
  description: "Условия доставки и оплаты Magic Vibes: курьер, ПВЗ Ozon, Почта России. Доставка по всей России за 1–5 дней. Оригинальная парфюмерия с гарантией.",
  alternates: { canonical: "/delivery" },
  openGraph: { title: "Доставка и оплата — Magic Vibes", url: `${SITE_URL}/delivery` },
};

export default async function DeliveryPage() {
  let settings = null;
  try { settings = await fetchSettings(); } catch {}

  const deliveryDays = settings?.deliveryDays ?? 5;
  const deliveryDaysMin = settings?.deliveryDaysMin ?? 1;
  const deliveryPrice = settings?.deliveryPriceRub ?? 350;
  const freeFrom = settings?.freeDeliveryFrom;

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Доставка и оплата", url: "/delivery" },
  ]);

  const webPageLd = {
    "@context": "https://schema.org",
    "@type": "WebPage",
    name: "Доставка и оплата — Magic Vibes",
    url: `${SITE_URL}/delivery`,
    breadcrumb: breadcrumb,
    description: "Условия доставки и оплаты в интернет-магазине парфюмерии Magic Vibes",
  };

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(webPageLd) }} />

      <div style={{ maxWidth: 800, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link>
          {" / "}<span style={{ color: "#f5f4f0" }}>Доставка и оплата</span>
        </nav>

        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(32px,5vw,56px)", color: "#f5f4f0", margin: "0 0 40px" }}>Доставка и оплата</h1>

        <section style={{ marginBottom: 48 }}>
          <h2 style={{ fontSize: 13, letterSpacing: "0.2em", textTransform: "uppercase", color: "rgba(201,162,94,0.8)", marginBottom: 20, fontWeight: 500 }}>Доставка</h2>

          <div style={{ display: "grid", gap: 16 }}>
            {[
              {
                icon: "🚚",
                title: "Курьерская доставка",
                desc: `Доставляем курьером по всей России за ${deliveryDaysMin}–${deliveryDays} дней. Стоимость — ${deliveryPrice.toLocaleString("ru-RU")} ₽${freeFrom ? `. При заказе от ${freeFrom.toLocaleString("ru-RU")} ₽ — бесплатно.` : "."}`,
              },
              { icon: "📦", title: "Пункты выдачи Ozon", desc: "Более 30 000 пунктов выдачи Ozon по всей России. Срок — 1–4 дня после отправки." },
              { icon: "✉️", title: "Почта России", desc: "Доставка Почтой России в отдалённые регионы. Срок — 3–10 дней." },
            ].map(m => (
              <div key={m.title} style={{ padding: "20px 24px", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 12, background: "rgba(255,255,255,0.02)" }}>
                <div style={{ display: "flex", gap: 14, alignItems: "flex-start" }}>
                  <span style={{ fontSize: 24, flexShrink: 0 }}>{m.icon}</span>
                  <div>
                    <div style={{ fontSize: 14, fontWeight: 500, color: "#f5f4f0", marginBottom: 6 }}>{m.title}</div>
                    <div style={{ fontSize: 13, color: "rgba(245,244,240,0.5)", lineHeight: 1.7 }}>{m.desc}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section style={{ marginBottom: 48 }}>
          <h2 style={{ fontSize: 13, letterSpacing: "0.2em", textTransform: "uppercase", color: "rgba(201,162,94,0.8)", marginBottom: 20, fontWeight: 500 }}>Способы оплаты</h2>
          <div style={{ display: "grid", gap: 12 }}>
            {[
              { icon: "💳", title: "Банковские карты", desc: "Visa, Mastercard, Мир — онлайн через безопасную форму оплаты." },
              { icon: "📲", title: "СБП", desc: "Система быстрых платежей — перевод по номеру телефона без комиссии." },
              { icon: "💰", title: "Наличные при получении", desc: "Оплата курьеру или в пункте выдачи при получении заказа." },
            ].map(m => (
              <div key={m.title} style={{ padding: "16px 20px", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 10, display: "flex", gap: 12, alignItems: "center" }}>
                <span style={{ fontSize: 20 }}>{m.icon}</span>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 500, color: "#f5f4f0", marginBottom: 3 }}>{m.title}</div>
                  <div style={{ fontSize: 12, color: "rgba(245,244,240,0.45)" }}>{m.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section>
          <h2 style={{ fontSize: 13, letterSpacing: "0.2em", textTransform: "uppercase", color: "rgba(201,162,94,0.8)", marginBottom: 16, fontWeight: 500 }}>Возврат</h2>
          <div style={{ fontSize: 14, color: "rgba(245,244,240,0.55)", lineHeight: 1.9 }}>
            <p>Возврат товара возможен в течение <strong style={{ color: "#f5f4f0" }}>14 дней</strong> с момента получения заказа.</p>
            <p>Для возврата товар должен быть в оригинальной упаковке, без следов использования. Парфюмерия принимается к возврату только в запечатанном виде.</p>
            <p>Для оформления возврата напишите нам на <a href="mailto:info@magicvibes.ru" style={{ color: "rgba(201,162,94,0.8)", textDecoration: "none" }}>info@magicvibes.ru</a>.</p>
          </div>
        </section>
      </div>
    </>
  );
}
