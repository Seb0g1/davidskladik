import type { Metadata } from "next";
import Link from "next/link";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";

export const metadata: Metadata = {
  title: `Часто задаваемые вопросы | ${SITE_NAME}`,
  description: "Ответы на вопросы о доставке, оригинальности товаров, оплате и возврате в интернет-магазине Magic Vibes.",
  alternates: { canonical: "/faq" },
  openGraph: { title: "FAQ — Magic Vibes", url: `${SITE_URL}/faq` },
};

const C = {
  bg:      "#09090b",
  gold:    "#c9a25e",
  text:    "#f5f4f0",
  muted:   "#7d7a73",
  border:  "rgba(255,255,255,0.07)",
  surface: "rgba(255,255,255,0.03)",
};

const sections = [
  {
    title: "Доставка и упаковка",
    items: [
      { q: "Как упакован флакон при доставке?", a: "Каждый флакон упаковывается в защитную пузырчатую плёнку и укладывается в жёсткую картонную коробку. Стекло не соприкасается со стенками коробки." },
      { q: "Сколько идёт доставка?", a: "По Москве и СПб — 1–2 дня. По России — 3–7 рабочих дней в зависимости от региона." },
      { q: "Как выбрать пункт выдачи?", a: "В корзине можно выбрать пункт выдачи на карте — доступны Ozon и Wildberries pickup. Адрес выдачи указывается при оформлении." },
      { q: "Можно ли вернуть товар?", a: "Да, в течение 14 дней с момента получения, если флакон не открывался и упаковка сохранена. Свяжитесь с нами через чат или email." },
      { q: "Доставляете ли вы за рубеж?", a: "На данный момент доставка только по России и Беларуси." },
    ],
  },
  {
    title: "Оригинальность товара",
    items: [
      { q: "Как проверить оригинальность?", a: "На каждом флаконе есть серийный номер, который проверяется через официальный сайт бренда или приложение CheckFresh. Мы закупаем только у официальных дистрибьюторов." },
      { q: "Как отличить оригинал от подделки?", a: "Оригинал имеет чёткую полиграфию, правильное написание бренда, ровный цвет жидкости и соответствующий номер партии. Мы предоставляем сертификат при запросе." },
      { q: "Откуда вы берёте товар?", a: "Все ароматы закупаются у официальных европейских и российских дистрибьюторов брендов." },
      { q: "Есть ли гарантия подлинности?", a: "Да, на все товары распространяется гарантия подлинности. При сомнениях — возврат без вопросов." },
    ],
  },
  {
    title: "Оплата и возврат",
    items: [
      { q: "Какие способы оплаты доступны?", a: "Банковская карта (Visa, Mastercard, Мир), СБП (Система быстрых платежей), наличные при получении." },
      { q: "Можно ли оплатить частями?", a: "Рассрочка доступна через Ozon Pay при оформлении через наш маркетплейс." },
      { q: "Как оформить возврат?", a: "Напишите нам в поддержку с фото товара и описанием причины. Возврат оформляется в течение 3 рабочих дней." },
      { q: "Когда придут деньги после возврата?", a: "В течение 5–10 рабочих дней на карту, с которой была совершена оплата." },
    ],
  },
  {
    title: "О парфюмерии",
    items: [
      { q: "Как правильно хранить парфюм?", a: "В тёмном, прохладном месте вдали от прямых солнечных лучей и источников тепла. Не в ванной комнате — влажность разрушает молекулы аромата." },
      { q: "Какой срок годности у парфюма?", a: "Обычно 3–5 лет от даты изготовления. Дата указана на флаконе и упаковке (обозначение «PAO» — «Period After Opening»)." },
      { q: "В чём разница EDT и EDP?", a: "EDP (Eau de Parfum) содержит 15–20% ароматических масел и держится 6–8 часов. EDT (Eau de Toilette) — 8–12%, 4–6 часов. EDP богаче и стойче." },
      { q: "Как наносить парфюм правильно?", a: "На пульсирующие точки — запястья, шею, за ухом. Не растирать после нанесения: это разрушает верхние ноты." },
    ],
  },
];

const allItems = sections.flatMap(s => s.items);

export default function FaqPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "FAQ", url: "/faq" }]);
  const faqLd = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: allItems.map(f => ({
      "@type": "Question",
      name: f.q,
      acceptedAnswer: { "@type": "Answer", text: f.a },
    })),
  };

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqLd) }} />

      <div style={{ background: C.bg, minHeight: "100vh" }}>
        {/* Hero */}
        <div style={{ textAlign: "center", padding: "clamp(56px,8vw,100px) clamp(16px,4vw,32px) clamp(40px,5vw,64px)", borderBottom: `1px solid ${C.border}` }}>
          <p style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.22em", textTransform: "uppercase", color: C.gold, marginBottom: 18 }}>Поддержка</p>
          <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(32px,5vw,56px)", color: C.text, letterSpacing: "-0.02em", lineHeight: 1.15, marginBottom: 24 }}>
            Часто задаваемые вопросы
          </h1>
          <div style={{ width: 56, height: 1, background: C.gold, margin: "0 auto 20px", opacity: 0.7 }} />
          <p style={{ fontSize: 14, color: C.muted, lineHeight: 1.7, maxWidth: 480, margin: "0 auto" }}>
            Ответы на популярные вопросы о доставке, оплате и нашей продукции
          </p>
        </div>

        {/* Content */}
        <div style={{ maxWidth: 760, margin: "0 auto", padding: "clamp(40px,6vw,72px) clamp(16px,4vw,32px)" }}>
          <nav style={{ fontSize: 12, color: C.muted, marginBottom: 40 }}>
            <Link href="/" style={{ color: C.muted, textDecoration: "none" }}>Главная</Link>
            {" / "}<span style={{ color: C.text }}>FAQ</span>
          </nav>

          {sections.map(section => (
            <div key={section.title} style={{ marginBottom: 48 }}>
              <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: "clamp(20px,2.5vw,26px)", color: C.text, marginBottom: 4, letterSpacing: "-0.01em" }}>
                {section.title}
              </h2>
              <div style={{ width: 32, height: 1, background: C.gold, marginBottom: 24, opacity: 0.6 }} />

              {section.items.map(item => (
                <details key={item.q} style={{ borderBottom: `1px solid ${C.border}` }}>
                  <summary style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 16, padding: "20px 0", cursor: "pointer", listStyle: "none", WebkitAppearance: "none" }}>
                    <span style={{ fontSize: 15, fontWeight: 500, color: "rgba(245,244,240,0.85)", lineHeight: 1.45 }}>{item.q}</span>
                    <span style={{ flexShrink: 0, width: 22, height: 22, display: "flex", alignItems: "center", justifyContent: "center", color: C.gold }}>
                      <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                        <path d="M2 4.5L7 9.5L12 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </span>
                  </summary>
                  <p style={{ fontSize: 14, color: C.muted, lineHeight: 1.75, paddingBottom: 20, margin: 0 }}>{item.a}</p>
                </details>
              ))}
            </div>
          ))}

          {/* Contact CTA */}
          <div style={{ marginTop: 16, padding: "32px 36px", border: "1px solid rgba(201,162,94,0.2)", borderRadius: 4, background: "rgba(201,162,94,0.03)", textAlign: "center" }}>
            <p style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 20, color: C.text, marginBottom: 10 }}>Не нашли ответ?</p>
            <p style={{ fontSize: 13, color: C.muted, marginBottom: 20, lineHeight: 1.6 }}>Напишите нам — ответим в течение нескольких часов</p>
            <a href="mailto:info@magicvibes.ru" style={{ display: "inline-block", padding: "12px 28px", border: "1px solid rgba(201,162,94,0.45)", borderRadius: 2, fontSize: 12, fontWeight: 600, letterSpacing: "0.14em", textTransform: "uppercase", color: C.gold, textDecoration: "none" }}>
              Написать нам
            </a>
          </div>
        </div>
      </div>
    </>
  );
}
