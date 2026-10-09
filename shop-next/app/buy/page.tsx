import type { Metadata } from "next";
import { STORE_LINKS } from "@/lib/buy";
import { SITE_URL } from "@/lib/seo";
import { BuyCard, BuyShell } from "./BuyCard";

export const metadata: Metadata = {
  title: "Где купить",
  description: "Оригинальная парфюмерия Magic Vibes: на сайте magicvibes.ru, на Яндекс Маркете и на Ozon.",
  alternates: { canonical: "/buy" },
  robots: { index: false, follow: true },
  openGraph: {
    title: "Где купить — Magic Vibes",
    description: "Сайт, Яндекс Маркет или Ozon — выберите, где удобнее.",
    url: `${SITE_URL}/buy`,
  },
};

export default function BuyPage() {
  return (
    <BuyShell>
      <div style={{ textAlign: "center", marginBottom: 28 }}>
        <span className="buy-kicker">Magic Vibes</span>
        <h1 className="buy-h1">Где купить</h1>
        <p style={{ margin: 0, color: "var(--muted)", fontSize: 15, lineHeight: 1.5 }}>
          Одни и те же оригинальные ароматы — выберите, где вам удобнее
        </p>
      </div>
      <div style={{ display: "grid", gap: 14 }}>
        <BuyCard
          channel="site" href="/catalog" badge="Официальный сайт"
          title="magicvibes.ru" subtitle="Оплата Ozon Pay · доставка 1–5 дней · промокоды"
          cta="Купить на сайте"
        />
        <BuyCard
          channel="yandex" href={STORE_LINKS.yandex}
          title="Яндекс Маркет" subtitle="Наш магазин на Маркете · пункты выдачи и курьер"
          cta="Купить в Яндекс Маркете"
        />
        <BuyCard
          channel="ozon" href={STORE_LINKS.ozon}
          title="Ozon" subtitle="Наш магазин на Ozon · пункты выдачи Ozon"
          cta="Купить на Ozon"
        />
      </div>
      <p style={{ textAlign: "center", color: "var(--subtle)", fontSize: 13, marginTop: 24 }}>
        Новинки и акции — в Telegram{" "}
        <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" style={{ color: "var(--accent)", fontWeight: 600 }}>@magicvibes_ru</a>
      </p>
    </BuyShell>
  );
}
