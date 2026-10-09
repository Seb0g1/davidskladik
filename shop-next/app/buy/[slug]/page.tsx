import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchBuyCompare } from "@/lib/api";
import { STORE_LINKS, CHANNEL_LABEL, type BuyChannel, type BuyCompare } from "@/lib/buy";
import { parseSlugForOfferId, toProductSlug } from "@/lib/slug";
import { productImg } from "@/lib/img";
import { SITE_URL } from "@/lib/seo";
import { BuyCard, BuyShell } from "../BuyCard";

interface Props { params: Promise<{ slug: string }> }

export const revalidate = 120;

const rub = (n: number) => `${Math.round(n).toLocaleString("ru-RU")} ₽`;

async function load(slug: string): Promise<BuyCompare | null> {
  try { return await fetchBuyCompare(parseSlugForOfferId(slug)); } catch { return null; }
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  const d = await load(slug);
  if (!d) return { title: "Товар не найден", robots: { index: false } };
  const prices = Object.values(d.offers).filter((o) => o && o.inStock && o.price).map((o) => o!.price!);
  const from = prices.length ? Math.min(...prices) : d.product.priceRub;
  const title = `${d.product.name} — где купить`;
  const description = `На сайте magicvibes.ru — ${rub(d.product.priceRub)}. Также в нашем магазине на Яндекс Маркете и Ozon. Оригинал.`;
  return {
    title,
    description,
    robots: { index: false, follow: true },
    alternates: { canonical: `/product/${toProductSlug(d.product.name, d.product.offerId)}` },
    openGraph: {
      title: `${d.product.name} — от ${rub(from)}`,
      description,
      url: `${SITE_URL}/buy/${slug}`,
      images: d.product.image ? [{ url: d.product.image }] : [],
    },
  };
}

type Row = { channel: BuyChannel; price: number | null; url: string; inStock: boolean };

export default async function BuyProductPage({ params }: Props) {
  const { slug } = await params;
  const d = await load(slug);
  if (!d) notFound();
  const { product, offers } = d;
  const productHref = `/product/${toProductSlug(product.name, product.offerId)}`;

  const rows: Row[] = (["site", "yandex", "ozon"] as BuyChannel[]).map((channel) => {
    const o = offers[channel];
    if (channel === "site") return { channel, price: o?.price ?? null, url: productHref, inStock: Boolean(o?.inStock) };
    // Not listed on this marketplace → the shop's storefront there.
    return o ? { channel, price: o.price, url: o.url || STORE_LINKS[channel], inStock: o.inStock } : { channel, price: null, url: STORE_LINKS[channel], inStock: false };
  });

  const priced = rows.filter((r) => r.inStock && r.price);
  const min = priced.length ? Math.min(...priced.map((r) => r.price!)) : null;
  const max = priced.length ? Math.max(...priced.map((r) => r.price!)) : null;
  // On a tie the site wins — it is our own shop.
  const best = min == null ? null : (priced.find((r) => r.channel === "site" && r.price === min) ?? priced.find((r) => r.price === min))!.channel;
  const saving = min != null && max != null && priced.length > 1 ? max - min : 0;
  const compared = priced.length > 1; // «Выгоднее» only when at least two exact buyer prices are known

  rows.sort((a, b) => {
    const rank = (r: Row) => (r.channel === "site" ? 0 : r.inStock ? 1 : 2);
    return rank(a) - rank(b) || (a.price ?? Infinity) - (b.price ?? Infinity);
  });

  const cta: Record<BuyChannel, string> = { site: "Купить на сайте", yandex: "Купить в Яндекс Маркете", ozon: "Купить на Ozon" };
  const seePrice: Record<BuyChannel, string> = { site: "Купить на сайте", yandex: "Смотреть цену на Маркете", ozon: "Смотреть цену на Ozon" };
  const listed = (r: Row) => r.channel === "site" || Boolean(offers[r.channel]);

  return (
    <BuyShell>
      <div className="buy-product">
        {product.image && (
          // eslint-disable-next-line @next/next/no-img-element
          <img src={productImg(product.image, 480)} alt={product.name} className="buy-product-img" />
        )}
        <div style={{ minWidth: 0 }}>
          <span className="buy-kicker" style={{ marginBottom: 6 }}>{product.brand || "Magic Vibes"}</span>
          <h1 className="buy-h2">{product.name}</h1>
          <div style={{ fontSize: 13, color: "var(--muted)", marginTop: 6 }}>
            {[product.volume, "оригинал"].filter(Boolean).join(" · ")}
          </div>
        </div>
      </div>

      <h2 className="buy-section">{compared ? "Сравните цены" : "Где купить"}{compared && saving > 0 && <span className="buy-save">разница до {rub(saving)}</span>}</h2>

      <div style={{ display: "grid", gap: 14 }}>
        {rows.map((r) => {
          const isBest = compared && r.channel === best;
          const diff = !isBest && min != null && r.price && r.inStock ? r.price - min : 0;
          return (
            <BuyCard
              key={r.channel}
              channel={r.channel}
              href={r.url}
              title={CHANNEL_LABEL[r.channel]}
              subtitle={
                r.channel === "site" ? (r.inStock ? "Официальный магазин · доставка 1–5 дней" : "Сейчас нет в наличии")
                : !listed(r) ? "Этого аромата там нет — откроем наш магазин"
                : !r.inStock ? "Сейчас нет в наличии"
                : r.channel === "ozon" ? "В наличии · цена со скидками Ozon — на площадке" : "В наличии · цена со скидками Маркета — на площадке"
              }
              badge={isBest ? "Выгоднее всего" : r.channel === "site" && r.inStock ? "Официальный сайт" : undefined}
              badgeTone={isBest ? "pink" : "dark"}
              price={r.price ? rub(r.price) : undefined}
              priceNote={diff > 0 ? `+${rub(diff)}` : undefined}
              dim={listed(r) && !r.inStock}
              cta={!listed(r) ? `Магазин на ${r.channel === "ozon" ? "Ozon" : "Маркете"}` : !r.inStock ? "Открыть" : r.price ? cta[r.channel] : seePrice[r.channel]}
            />
          );
        })}
      </div>

      <p style={{ color: "var(--subtle)", fontSize: 12, lineHeight: 1.5, marginTop: 18 }}>
        На Яндекс Маркете и Ozon цену формируют сами площадки — со своими скидками (Ozon Карта, баллы Плюса),
        поэтому смотрите её по кнопке.
      </p>
      <p style={{ textAlign: "center", fontSize: 14, marginTop: 20 }}>
        <Link href="/buy" style={{ color: "var(--accent)", fontWeight: 600 }}>Все наши магазины →</Link>
      </p>
    </BuyShell>
  );
}
