// Server-rendered SEO landing (brand / collection): hero, intro, product grid with real links,
// pagination, FAQ, related links + ItemList / FAQPage / BreadcrumbList structured data.
import Link from "next/link";
import ProductCard from "@/components/ProductCard";
import type { ShopProduct } from "@/lib/types";
import { SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import { toProductSlug } from "@/lib/slug";

export interface LandingProps {
  path: string;            // "/brand/montale"
  kicker: string;          // "Бренд" / "Коллекция"
  h1: string;
  script?: string;
  intro: string;
  products: ShopProduct[];
  total: number;
  page: number;
  pageSize: number;
  catalogHref: string;     // full catalog with filters
  faq: [string, string][];
  related?: { label: string; href: string }[];
  crumbs: { name: string; url: string }[];
}

const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

export default function LandingPage(p: LandingProps) {
  const pages = Math.max(1, Math.ceil(p.total / p.pageSize));
  const prices = p.products.map((x) => x.priceRub).filter((x) => x > 0);
  const minPrice = prices.length ? Math.min(...prices) : 0;
  const pageHref = (n: number) => (n <= 1 ? p.path : `${p.path}?page=${n}`);

  const itemList = {
    "@context": "https://schema.org",
    "@type": "ItemList",
    name: p.h1,
    numberOfItems: p.total,
    itemListElement: p.products.map((x, i) => ({
      "@type": "ListItem",
      position: (p.page - 1) * p.pageSize + i + 1,
      url: `${SITE_URL}/product/${toProductSlug(x.name, x.offerId)}`,
      name: x.name,
    })),
  };
  const faqLd = p.faq.length ? {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: p.faq.map(([q, a]) => ({ "@type": "Question", name: q, acceptedAnswer: { "@type": "Answer", text: a } })),
  } : null;

  return (
    <div className="mv-land">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumbJsonLd(p.crumbs)) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(itemList) }} />
      {faqLd && <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(faqLd) }} />}

      <div className="mv-land-wrap">
        <nav className="mv-legal-crumbs" aria-label="Навигация">
          {p.crumbs.map((c, i) => i < p.crumbs.length - 1
            ? <span key={c.url}><Link href={c.url}>{c.name}</Link> / </span>
            : <span key={c.url}>{c.name}</span>)}
        </nav>

        <header className="mv-land-hero">
          <span className="mv-land-kicker">✦ {p.kicker}</span>
          <h1 className="mv-land-title">{p.h1}</h1>
          {p.script && <div className="mv-legal-script">{p.script}</div>}
          <p className="mv-land-intro">{p.intro}</p>
          <div className="mv-land-stats">
            <span><b>{p.total.toLocaleString("ru-RU")}</b> {p.total % 10 === 1 && p.total % 100 !== 11 ? "товар" : "товаров"}</span>
            {minPrice > 0 && <span>от <b>{rub(minPrice)}</b></span>}
            <span>доставка <b>1–5 дней</b></span>
            <span><b>100%</b> оригинал</span>
          </div>
        </header>

        {p.products.length ? (
          <div className="product-grid mv-land-grid">
            {p.products.map((x) => <ProductCard key={x.offerId} product={x} />)}
          </div>
        ) : (
          <div className="mv-acc-empty"><b>Сейчас нет в наличии</b><span>Загляните позже или посмотрите весь каталог.</span></div>
        )}

        <div className="mv-land-pager">
          {p.page > 1 && <Link rel="prev" href={pageHref(p.page - 1)} className="mv-acc-btn ghost">← Назад</Link>}
          {pages > 1 && <span className="mv-land-pageinfo">Страница {p.page} из {pages}</span>}
          {p.page < pages && <Link rel="next" href={pageHref(p.page + 1)} className="mv-acc-btn">Ещё товары →</Link>}
          <Link href={p.catalogHref} className="mv-acc-btn ghost">Фильтры и сортировка</Link>
        </div>

        {p.related && p.related.length > 0 && (
          <section className="mv-land-related">
            <h2>Смотрите также</h2>
            <div>{p.related.map((r) => <Link key={r.href} href={r.href} className="chip">{r.label}</Link>)}</div>
          </section>
        )}

        {p.faq.length > 0 && (
          <section className="mv-land-faq">
            <h2>Вопросы и ответы</h2>
            {p.faq.map(([q, a]) => (
              <details key={q}>
                <summary>{q}</summary>
                <p>{a}</p>
              </details>
            ))}
          </section>
        )}
      </div>
    </div>
  );
}
