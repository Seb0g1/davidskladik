import type { Metadata } from "next";
import Link from "next/link";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";

export const metadata: Metadata = {
  title: `Гарантия и возврат | ${SITE_NAME}`,
  description: "Гарантия подлинности парфюмерии в Magic Vibes. Возврат в течение 14 дней. Все товары оригинальные.",
  alternates: { canonical: "/warranty" },
};

export default function WarrantyPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "Гарантия", url: "/warranty" }]);
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <div style={{ maxWidth: 800, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>Гарантия</span>
        </nav>
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,5vw,50px)", color: "#f5f4f0", margin: "0 0 32px" }}>Гарантия и возврат</h1>
        {[
          { title: "100% оригинальная продукция", text: "Все товары в Magic Vibes — сертифицированные оригиналы от официальных дистрибьюторов. Мы не торгуем подделками и тестерами без упаковки." },
          { title: "Возврат 14 дней", text: "Вы можете вернуть товар в течение 14 дней с момента получения. Товар должен быть в оригинальной упаковке и не использовался. Парфюм принимается к возврату только в запечатанном виде." },
          { title: "Как оформить возврат", text: "Напишите нам на info@magicvibes.ru или в Telegram @magicvibes_ru, укажите номер заказа и причину возврата. Мы свяжемся с вами в течение 1 рабочего дня." },
          { title: "Обмен", text: "Обмен товара возможен при наличии аналогичного. Для оформления обращайтесь в поддержку." },
        ].map(s => (
          <div key={s.title} style={{ marginBottom: 32 }}>
            <h2 style={{ fontSize: 15, fontWeight: 500, color: "#f5f4f0", marginBottom: 10, letterSpacing: "0.02em" }}>{s.title}</h2>
            <p style={{ fontSize: 14, color: "rgba(245,244,240,0.55)", lineHeight: 1.85 }}>{s.text}</p>
          </div>
        ))}
      </div>
    </>
  );
}
