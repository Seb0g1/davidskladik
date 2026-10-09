// Home page SEO block: keyword copy for «парфюмерия купить», crawlable links to every landing
// and the top brands, FAQ with FAQPage structured data. Server component.
import Link from "next/link";
import { fetchBrands } from "@/lib/api";
import { COLLECTION_DEFS, brandHref, collectionHref } from "@/lib/landings";

export const HOME_FAQ: [string, string][] = [
  ["Где купить оригинальную парфюмерию?", "В интернет-магазине Magic Vibes: мы продаём только оригинальную парфюмерию в заводской упаковке и по запросу предоставляем документы о происхождении товара. Рейтинг магазина — 4.9 из 5 на Ozon."],
  ["Сколько стоит доставка парфюмерии?", "Стоимость рассчитывается при оформлении заказа по выбранному способу: пункт выдачи Ozon, СДЭК, Яндекс Доставки или курьер. От определённой суммы заказа доставка бесплатная."],
  ["Как быстро доставят духи?", "Передаём заказ в доставку в течение 1 рабочего дня после оплаты. Доставка по России занимает 1–5 рабочих дней, по Москве возможна доставка курьером в день заказа."],
  ["Как оплатить заказ?", "Онлайн через Ozon Pay: картой любого российского банка, через СБП, Ozon Картой или в рассрочку."],
  ["Можно ли сначала попробовать аромат?", "Да. В каталоге есть пробники, отливанты и миниатюры многих ароматов — удобно познакомиться с парфюмом перед покупкой полноразмерного флакона."],
  ["Можно ли вернуть парфюмерию?", "Да, в течение 7 дней после получения, если флакон не вскрывался и сохранена заводская упаковка. Подробнее — в условиях покупки."],
];

const PERFUME_SLUGS = ["zhenskaya", "muzhskaya", "uniseks", "nishevaya", "elitnaya", "arabskaya", "parfyumernaya-voda", "tualetnaya-voda", "dukhi", "probniki", "miniatyury", "nabory"];

export default async function SeoHub() {
  const brands = await fetchBrands().catch(() => [] as { name: string; count: number }[]);
  const top = brands
    .filter((b) => b.count >= 20 && b.name.length <= 30)
    .sort((a, b) => b.count - a.count)
    .slice(0, 48)
    .sort((a, b) => a.name.localeCompare(b.name, "ru"));
  const main = PERFUME_SLUGS.map((s) => COLLECTION_DEFS.find((c) => c.slug === s)).filter((c) => c !== undefined);
  const other = COLLECTION_DEFS.filter((c) => !PERFUME_SLUGS.includes(c.slug));
  const faqLd = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: HOME_FAQ.map(([q, a]) => ({ "@type": "Question", name: q, acceptedAnswer: { "@type": "Answer", text: a } })),
  };

  return (
    <section className="mv-seo mv-cv" aria-labelledby="mv-seo-title">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqLd) }} />
      <div className="mv-seo-wrap">
        <div className="mv-seo-text">
          <h2 id="mv-seo-title">Парфюмерия — купить оригинальные духи в интернет-магазине Magic Vibes</h2>
          <p>
            Magic Vibes — интернет-магазин оригинальной парфюмерии с доставкой по всей России. В каталоге более 22 000 ароматов
            от {brands.length > 100 ? `${Math.floor(brands.length / 100) * 100}+` : "сотен"} брендов: женские и мужские духи, парфюмерная и туалетная вода, нишевая,
            элитная и арабская парфюмерия, пробники, миниатюры и подарочные наборы.
          </p>
          <p>
            Мы продаём только оригинальную продукцию в заводской упаковке: Chanel, Dior, Tom Ford, Creed, Kilian, Byredo, Montale,
            Amouage, Xerjoff, Lattafa и многие другие парфюмерные дома. Каждый заказ проверяем перед отправкой и передаём в доставку
            в течение рабочего дня — курьером или в пункт выдачи Ozon, СДЭК и Яндекс Доставки за 1–5 дней.
          </p>
          <p>
            Не знаете, какой аромат выбрать? Воспользуйтесь <Link href="/find">AI-подбором аромата</Link> по настроению и любимым нотам,
            почитайте <Link href="/guide/women">гид по женским</Link> и <Link href="/guide/men">мужским ароматам</Link> или начните
            с <Link href="/collections/probniki">пробников и отливантов</Link>.
          </p>
        </div>

        <nav className="mv-seo-links" aria-label="Разделы каталога">
          <div>
            <h3>Парфюмерия</h3>
            <ul>{main.map((c) => <li key={c.slug}><Link href={collectionHref(c)}>{c.h1}</Link></li>)}</ul>
          </div>
          <div>
            <h3>Ароматы и категории</h3>
            <ul>{other.map((c) => <li key={c.slug}><Link href={collectionHref(c)}>{c.h1}</Link></li>)}</ul>
          </div>
          {top.length > 0 && (
            <div className="mv-seo-brands">
              <h3>Популярные бренды</h3>
              <ul>{top.map((b) => <li key={b.name}><Link href={brandHref(b.name)} prefetch={false}>{b.name}</Link></li>)}</ul>
              <Link href="/brands" className="mv-seo-all">Все бренды →</Link>
            </div>
          )}
        </nav>

        <div className="mv-land-faq">
          <h2>Частые вопросы</h2>
          {HOME_FAQ.map(([q, a]) => (
            <details key={q}>
              <summary>{q}</summary>
              <p>{a}</p>
            </details>
          ))}
        </div>
      </div>
    </section>
  );
}
