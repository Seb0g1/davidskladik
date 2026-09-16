import type { Metadata } from "next";
import Link from "next/link";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";

export const metadata: Metadata = {
  title: `Вопросы и ответы | ${SITE_NAME}`,
  description: "Ответы на часто задаваемые вопросы о покупке парфюмерии в Magic Vibes: доставка, оплата, возврат, подлинность.",
  alternates: { canonical: "/faq" },
};

const FAQ_ITEMS = [
  { q: "Вы продаёте оригинальную парфюмерию?", a: "Да, все товары в Magic Vibes — сертифицированные оригиналы от официальных дистрибьюторов. Мы даём гарантию подлинности на каждый товар." },
  { q: "Как быстро осуществляется доставка?", a: "Доставка курьером — 1–5 рабочих дней по всей России. ПВЗ Ozon — 1–4 дня. Почта России — 3–10 дней в зависимости от региона." },
  { q: "Как оплатить заказ?", a: "Принимаем оплату картой (Visa, Mastercard, Мир), через СБП и наличными при получении." },
  { q: "Можно ли вернуть товар?", a: "Да, возврат возможен в течение 14 дней с момента получения. Товар должен быть в оригинальной запечатанной упаковке без следов использования." },
  { q: "Как узнать, подойдёт ли мне аромат?", a: "Воспользуйтесь нашим AI-подборщиком аромата — он поможет найти парфюм по вашим предпочтениям. Также смотрите пирамиды нот на страницах товаров и читайте отзывы покупателей." },
  { q: "Есть ли физический магазин?", a: "Пока нет. Magic Vibes — онлайн-магазин. Следите за нашим Telegram-каналом @magicvibes_ru — там мы публикуем новости и акции." },
  { q: "Делаете ли вы подарочную упаковку?", a: "Да, при оформлении заказа укажите в комментарии, что нужна подарочная упаковка. Стоимость — 150 ₽." },
];

export default function FaqPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "FAQ", url: "/faq" }]);
  const faqLd = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: FAQ_ITEMS.map(f => ({
      "@type": "Question",
      name: f.q,
      acceptedAnswer: { "@type": "Answer", text: f.a },
    })),
  };

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqLd) }} />

      <div style={{ maxWidth: 760, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>FAQ</span>
        </nav>
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,5vw,50px)", color: "#f5f4f0", margin: "0 0 40px" }}>Вопросы и ответы</h1>
        <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
          {FAQ_ITEMS.map((item, i) => (
            <details key={i} style={{ borderBottom: "1px solid rgba(255,255,255,0.07)", padding: "20px 0" }}>
              <summary style={{ fontSize: 15, fontWeight: 500, color: "#f5f4f0", cursor: "pointer", listStyle: "none", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                {item.q}
                <span style={{ fontSize: 20, color: "rgba(201,162,94,0.6)", flexShrink: 0, marginLeft: 12 }}>+</span>
              </summary>
              <p style={{ marginTop: 14, fontSize: 14, color: "rgba(245,244,240,0.55)", lineHeight: 1.8 }}>{item.a}</p>
            </details>
          ))}
        </div>
      </div>
    </>
  );
}
