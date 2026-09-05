import { useState } from "react";

const C = {
  bg:     "#09090b",
  gold:   "#c9a25e",
  text:   "#f5f4f0",
  muted:  "#7d7a73",
  border: "rgba(255,255,255,0.07)",
  surface:"rgba(255,255,255,0.03)",
};

interface FAQItem {
  q: string;
  a: string;
}

interface FAQSection {
  title: string;
  items: FAQItem[];
}

const sections: FAQSection[] = [
  {
    title: "Доставка и упаковка",
    items: [
      {
        q: "Как упакован флакон при доставке?",
        a: "Каждый флакон упаковывается в защитную пузырчатую плёнку и укладывается в жёсткую картонную коробку. Стекло не соприкасается со стенками коробки.",
      },
      {
        q: "Сколько идёт доставка?",
        a: "По Москве и СПб — 1–2 дня. По России — 3–7 рабочих дней в зависимости от региона.",
      },
      {
        q: "Как выбрать пункт выдачи?",
        a: "В корзине можно выбрать пункт выдачи на карте — доступны Ozon и Wildberries pickup. Адрес выдачи указывается при оформлении.",
      },
      {
        q: "Можно ли вернуть товар?",
        a: "Да, в течение 14 дней с момента получения, если флакон не открывался и упаковка сохранена. Свяжитесь с нами через чат или email.",
      },
      {
        q: "Доставляете ли вы за рубеж?",
        a: "На данный момент доставка только по России и Беларуси.",
      },
    ],
  },
  {
    title: "Оригинальность товара",
    items: [
      {
        q: "Как проверить оригинальность?",
        a: "На каждом флаконе есть серийный номер, который проверяется через официальный сайт бренда или приложение CheckFresh. Мы закупаем только у официальных дистрибьюторов.",
      },
      {
        q: "Как отличить оригинал от подделки?",
        a: "Оригинал имеет чёткую полиграфию, правильное написание бренда, ровный цвет жидкости и соответствующий номер партии. Мы предоставляем сертификат при запросе.",
      },
      {
        q: "Откуда вы берёте товар?",
        a: "Все ароматы закупаются у официальных европейских и российских дистрибьюторов брендов.",
      },
      {
        q: "Есть ли гарантия?",
        a: "Да, на все товары распространяется гарантия подлинности. При сомнениях — возврат без вопросов.",
      },
    ],
  },
  {
    title: "Оплата и возврат",
    items: [
      {
        q: "Какие способы оплаты доступны?",
        a: "Банковская карта, СБП (Система быстрых платежей), наличные при получении.",
      },
      {
        q: "Можно ли оплатить частями?",
        a: "Рассрочка доступна через Ozon Pay при оформлении через наш маркетплейс.",
      },
      {
        q: "Как оформить возврат?",
        a: "Напишите нам в поддержку с фото товара и описанием причины. Возврат оформляется в течение 3 рабочих дней.",
      },
      {
        q: "Когда придут деньги после возврата?",
        a: "В течение 5–10 рабочих дней на карту, с которой была совершена оплата.",
      },
    ],
  },
  {
    title: "О парфюмерии",
    items: [
      {
        q: "Как правильно хранить парфюм?",
        a: "В тёмном, прохладном месте вдали от прямых солнечных лучей и источников тепла. Не в ванной комнате — влажность разрушает молекулы аромата.",
      },
      {
        q: "Какой срок годности у парфюма?",
        a: "Обычно 3–5 лет от даты изготовления. Дата указана на флаконе и упаковке (обозначение «PAO» — «Period After Opening»).",
      },
      {
        q: "В чём разница EDT и EDP?",
        a: "EDP (Eau de Parfum) содержит 15–20% ароматических масел и держится 6–8 часов. EDT (Eau de Toilette) — 8–12%, 4–6 часов. EDP богаче и стойче.",
      },
      {
        q: "Как наносить парфюм правильно?",
        a: "На пульсирующие точки — запястья, шею, за ухом. Не растирать после нанесения: это разрушает верхние ноты.",
      },
    ],
  },
];

function AccordionItem({ item }: { item: FAQItem }) {
  const [open, setOpen] = useState(false);

  return (
    <div
      style={{
        borderBottom: `1px solid ${C.border}`,
      }}
    >
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 16,
          padding: "20px 0",
          background: "none",
          border: "none",
          cursor: "pointer",
          textAlign: "left",
          fontFamily: "inherit",
        }}
      >
        <span
          style={{
            fontSize: 15,
            fontWeight: 500,
            color: open ? C.text : "rgba(245,244,240,0.85)",
            lineHeight: 1.45,
            transition: "color 0.2s",
          }}
        >
          {item.q}
        </span>
        <span
          style={{
            flexShrink: 0,
            width: 22,
            height: 22,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: C.gold,
            transform: open ? "rotate(180deg)" : "rotate(0deg)",
            transition: "transform 0.25s ease",
          }}
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M2 4.5L7 9.5L12 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </span>
      </button>

      <div
        style={{
          overflow: "hidden",
          maxHeight: open ? 400 : 0,
          transition: "max-height 0.3s ease",
        }}
      >
        <p
          style={{
            fontSize: 14,
            color: C.muted,
            lineHeight: 1.75,
            paddingBottom: 20,
            margin: 0,
          }}
        >
          {item.a}
        </p>
      </div>
    </div>
  );
}

function FAQSection({ section }: { section: FAQSection }) {
  return (
    <div style={{ marginBottom: 48 }}>
      <h2
        style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic",
          fontWeight: 500,
          fontSize: "clamp(20px, 2.5vw, 26px)",
          color: C.text,
          marginBottom: 4,
          letterSpacing: "-0.01em",
        }}
      >
        {section.title}
      </h2>
      <div
        style={{
          width: 32,
          height: 1,
          background: C.gold,
          marginBottom: 24,
          opacity: 0.6,
        }}
      />
      <div>
        {section.items.map((item) => (
          <AccordionItem key={item.q} item={item} />
        ))}
      </div>
    </div>
  );
}

export default function FAQPage() {
  return (
    <div style={{ background: C.bg, minHeight: "100vh" }}>
      {/* Hero */}
      <div
        style={{
          textAlign: "center",
          padding: "clamp(56px, 8vw, 100px) clamp(16px, 4vw, 32px) clamp(40px, 5vw, 64px)",
          borderBottom: `1px solid ${C.border}`,
        }}
      >
        <p
          style={{
            fontSize: 11,
            fontWeight: 600,
            letterSpacing: "0.22em",
            textTransform: "uppercase",
            color: C.gold,
            marginBottom: 18,
          }}
        >
          Поддержка
        </p>
        <h1
          style={{
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: "italic",
            fontWeight: 400,
            fontSize: "clamp(32px, 5vw, 56px)",
            color: C.text,
            letterSpacing: "-0.02em",
            lineHeight: 1.15,
            marginBottom: 24,
          }}
        >
          Часто задаваемые вопросы
        </h1>
        <div
          style={{
            width: 56,
            height: 1,
            background: C.gold,
            margin: "0 auto",
            opacity: 0.7,
          }}
        />
        <p
          style={{
            fontSize: 14,
            color: C.muted,
            lineHeight: 1.7,
            marginTop: 20,
            maxWidth: 480,
            margin: "20px auto 0",
          }}
        >
          Ответы на популярные вопросы о доставке, оплате и нашей продукции
        </p>
      </div>

      {/* Content */}
      <div
        style={{
          maxWidth: 760,
          margin: "0 auto",
          padding: "clamp(40px, 6vw, 72px) clamp(16px, 4vw, 32px)",
        }}
      >
        {sections.map((section) => (
          <FAQSection key={section.title} section={section} />
        ))}

        {/* Contact CTA */}
        <div
          style={{
            marginTop: 16,
            padding: "32px 36px",
            border: `1px solid rgba(201,162,94,0.2)`,
            borderRadius: 4,
            background: "rgba(201,162,94,0.03)",
            textAlign: "center",
          }}
        >
          <p
            style={{
              fontFamily: "'Cormorant Garamond', Georgia, serif",
              fontStyle: "italic",
              fontSize: 20,
              color: C.text,
              marginBottom: 10,
            }}
          >
            Не нашли ответ?
          </p>
          <p style={{ fontSize: 13, color: C.muted, marginBottom: 20, lineHeight: 1.6 }}>
            Напишите нам — ответим в течение нескольких часов
          </p>
          <a
            href="mailto:info@magicvibes.ru"
            style={{
              display: "inline-block",
              padding: "12px 28px",
              border: `1px solid rgba(201,162,94,0.45)`,
              borderRadius: 2,
              fontSize: 12,
              fontWeight: 600,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              color: C.gold,
              textDecoration: "none",
              transition: "background 0.2s, color 0.2s",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(201,162,94,0.1)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "transparent";
            }}
          >
            Написать нам
          </a>
        </div>
      </div>
    </div>
  );
}
