import type { Metadata } from "next";
import Link from "next/link";
import { Quote, Star } from "lucide-react";
import { fetchPopular, fetchAromaMesyatsa, fetchReviews, fetchNews, fetchBanners } from "@/lib/api";
import { SITE_URL, SITE_NAME } from "@/lib/seo";
import ProductCard from "@/components/ProductCard";
import BannerSlider from "@/components/BannerSlider";
import BrandGallery from "@/components/BrandGallery";
import ScentQuiz from "@/components/ScentQuiz";
import UnboxingSection from "@/components/UnboxingSection";
import HeroClient from "@/components/HeroClient";
import CityTopsTicker from "@/components/CityTopsTicker";
import ContestSection from "@/components/ContestSection";
import PageEffects from "@/components/PageEffects";

export const revalidate = 300;

export const metadata: Metadata = {
  title: `${SITE_NAME} — Оригинальный парфюм с доставкой по России`,
  description: "Купить оригинальную парфюмерию в Magic Vibes. 22 000+ ароматов: Chanel, Dior, Tom Ford, Montale, Creed, Byredo. Нишевая, арабская, женская и мужская парфюмерия. Доставка по России 1–5 дней. Рейтинг 4.9 на Ozon.",
  alternates: { canonical: "/" },
  openGraph: { title: `${SITE_NAME} — Оригинальный парфюм`, url: SITE_URL },
};

const STORE_SCHEMA = {
  "@context": "https://schema.org",
  "@type": "Store",
  name: SITE_NAME,
  url: SITE_URL,
  image: `${SITE_URL}/og-image.jpg`,
  description: "Оригинальная парфюмерия мировых брендов с доставкой по России",
  priceRange: "₽₽",
  openingHours: "Mo-Su 00:00-24:00",
  address: { "@type": "PostalAddress", addressCountry: "RU" },
  aggregateRating: { "@type": "AggregateRating", ratingValue: "4.9", reviewCount: "1240", bestRating: "5" },
};

export default async function HomePage() {
  const [popular, aroma, reviews, news, banners] = await Promise.allSettled([
    fetchPopular(12),
    fetchAromaMesyatsa(),
    fetchReviews(6),
    fetchNews(3),
    fetchBanners(),
  ]);

  const popularProducts = popular.status === "fulfilled" ? popular.value.products : [];
  const aromaProduct = aroma.status === "fulfilled" ? aroma.value.product : null;
  const aromaNote = aroma.status === "fulfilled" ? aroma.value.note : "";
  const aromaValidUntil = aroma.status === "fulfilled" ? aroma.value.validUntil : null;
  const reviewsList = reviews.status === "fulfilled" ? reviews.value.reviews : [];
  const newsList = news.status === "fulfilled" ? news.value.posts : [];
  const bannersList = banners.status === "fulfilled" ? banners.value : [];

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(STORE_SCHEMA) }} />
      <PageEffects />

      {/* ════════ BANNERS ════════ */}
      <section style={{ padding: "clamp(16px,3vw,32px) clamp(18px,4vw,56px) 0" }}>
        <BannerSlider banners={bannersList} />
      </section>

      {/* ════════ HERO (client — WebGL + effects) ════════ */}
      <HeroClient />

      {/* ════════ CITY TICKER ════════ */}
      <CityTopsTicker />

      {/* ════════ STATS ════════ */}
      <section className="reveal-section" style={{ borderTop: "1px solid rgba(255,255,255,0.06)", borderBottom: "1px solid rgba(255,255,255,0.06)", margin: "0 clamp(18px,4vw,56px)" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(180px,1fr))" }}>
          {[
            { value: "22 000+", label: "Ароматов в наличии" },
            { value: "100%",    label: "Гарантия оригинала" },
            { value: "1–5 дн.", label: "Доставка по России" },
            { value: "4.9",     label: "Рейтинг на Ozon" },
          ].map(({ value, label }, i) => (
            <div key={label} style={{ padding: "34px 26px", borderRight: i < 3 ? "1px solid rgba(255,255,255,0.06)" : undefined }}>
              <p style={{ margin: 0, fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 40, lineHeight: 1, color: "#f5f4f0" }}>{value}</p>
              <p style={{ margin: "10px 0 0", fontSize: 12.5, letterSpacing: "0.08em", color: "#7d7a73" }}>{label}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ════════ SCENT QUIZ ════════ */}
      <div className="reveal-section"><ScentQuiz /></div>

      {/* ════════ GIFT CTA ════════ */}
      <section style={{ margin: "clamp(40px,6vw,80px) 0", padding: "0 clamp(18px,4vw,56px)" }}>
        <Link href="/gift" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "clamp(20px,3vw,36px) clamp(24px,4vw,48px)", background: "linear-gradient(135deg, #1a1408 0%, #111113 60%)", border: "1px solid rgba(201,162,94,0.3)", borderRadius: 3, textDecoration: "none", gap: 16, flexWrap: "wrap" }}>
          <div>
            <p style={{ fontSize: 10, letterSpacing: "0.26em", textTransform: "uppercase", color: "#c9a25e", margin: "0 0 6px" }}>Идеальный подарок</p>
            <p style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontSize: "clamp(22px,3vw,34px)", fontStyle: "italic", fontWeight: 300, color: "#f5f4f0", margin: 0, lineHeight: 1.15 }}>Собери подарочный набор</p>
          </div>
          <span style={{ fontSize: 13, color: "#c9a25e", letterSpacing: "0.08em" }}>Выбрать аромат →</span>
        </Link>
      </section>

      {/* ════════ HOW IT WORKS ════════ */}
      <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
        <p className="eyebrow" style={{ marginBottom: "clamp(20px,3vw,38px)" }}>Как мы работаем</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: 18 }}>
          {[
            { n: "01", title: "Подбираем аромат",        body: "Расскажите о предпочтениях — подберём парфюм по нотам, стойкости и поводу из 22 000 позиций.", cta: null },
            { n: "02", title: "Проверяем оригинал",      body: "Каждый флакон проходит проверку подлинности перед отправкой — только оригинал, без компромиссов.", cta: null },
            { n: "03", title: "Упаковываем как подарок", body: "Фирменная упаковка и защита: заказ выглядит дороже, чем в бутике.", cta: null },
            { n: "04", title: "Доставляем через Ozon",   body: "1–5 дней в любой город России, бесплатно до пункта выдачи рядом с домом.", cta: "/catalog" },
          ].map(({ n, title, body, cta }) => (
            <article key={n} style={{ display: "flex", flexDirection: "column", justifyContent: "space-between", minHeight: "clamp(260px,28vh,340px)", padding: "clamp(22px,3vw,36px)", border: cta ? "1px solid rgba(201,162,94,0.24)" : "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: cta ? "linear-gradient(150deg,#17140f 0%,#0d0d0d 70%)" : "linear-gradient(150deg,#131110 0%,#0d0d0d 68%)" }}>
              <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 28, color: "#c9a25e" }}>{n}</span>
              <div>
                <h3 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: "0 0 12px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(22px,2.6vw,36px)", lineHeight: 1.1, color: "#f5f4f0" }}>{title}</h3>
                <p style={{ margin: 0, fontSize: 13.5, lineHeight: 1.7, color: "#8b8880" }}>{body}</p>
                {cta && <Link href={cta} className="btn-primary" style={{ display: "inline-flex", marginTop: 22 }}>Выбрать аромат</Link>}
              </div>
            </article>
          ))}
        </div>
      </section>

      {/* ════════ POPULAR PRODUCTS ════════ */}
      {popularProducts.length > 0 && (
        <section style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0" }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 34 }}>
            <div>
              <p className="eyebrow" style={{ marginBottom: 10 }}>Популярное</p>
              <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Хиты сезона</h2>
            </div>
            <Link href="/catalog" className="btn-ghost">Смотреть всё →</Link>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(200px,1fr))", gap: 18 }}>
            {popularProducts.map(p => <div key={p.id} data-mv-card="1"><ProductCard product={p} /></div>)}
          </div>
        </section>
      )}

      {/* ════════ WHY US ════════ */}
      <section style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0", textAlign: "center" }}>
        <p className="eyebrow" style={{ marginBottom: 10 }}>Почему мы</p>
        <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: "0 0 44px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Ваш выбор</h2>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(230px,1fr))", gap: 1, background: "rgba(255,255,255,0.07)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, textAlign: "left", maxWidth: 1180, margin: "0 auto" }}>
          {[
            { n: "01", title: "100% оригинал",      text: "Прямые поставки от авторизованных дистрибьюторов, сертификат на каждый бренд." },
            { n: "02", title: "Доставка по России",  text: "Через Ozon за 1–5 дней в любой город. Удобные пункты выдачи рядом с домом." },
            { n: "03", title: "4.9 на Ozon",         text: "Тысячи довольных покупателей и рейтинг 4.9 из 5 на маркетплейсе." },
            { n: "04", title: "Широкий выбор",       text: "Более 22 000 ароматов от 200+ брендов — от масс-маркета до нишевой парфюмерии." },
          ].map(({ n, title, text }) => (
            <div key={n} style={{ background: "#0e0e0e", padding: "30px 26px" }}>
              <p style={{ margin: "0 0 14px", fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 26, color: "#c9a25e" }}>{n}</p>
              <p style={{ margin: "0 0 10px", fontSize: 15, color: "#f5f4f0" }}>{title}</p>
              <p style={{ margin: 0, fontSize: 13, lineHeight: 1.6, color: "#7d7a73" }}>{text}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ════════ PROMO CARDS ════════ */}
      <section style={{ padding: "clamp(56px,8vw,104px) clamp(18px,4vw,56px) 0" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))", gap: 18 }}>
          {[
            { tag: "Для близких", title: "Подарите аромат", body: "Подарочные наборы от мировых парфюмерных домов в фирменной упаковке.", href: "/catalog?category=sets", cta: "Выбрать набор →" },
            { tag: "Эксклюзив", title: "Духи Parfum", body: "Редкие ароматы высочайшей концентрации от нишевых парфюмерных домов.", href: "/catalog?category=parfum", cta: "Смотреть →" },
          ].map(({ tag, title, body, href, cta }) => (
            <div key={tag} style={{ padding: "clamp(30px,4vw,52px)", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "linear-gradient(140deg,#131110 0%,#0d0d0d 70%)" }}>
              <p className="eyebrow" style={{ marginBottom: 14 }}>{tag}</p>
              <h3 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: "0 0 14px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(28px,3.2vw,40px)", color: "#f5f4f0" }}>{title}</h3>
              <p style={{ margin: "0 0 24px", maxWidth: "34ch", fontSize: 13.5, lineHeight: 1.6, color: "#7d7a73" }}>{body}</p>
              <Link href={href} style={{ fontSize: 12.5, letterSpacing: "0.14em", textTransform: "uppercase", color: "var(--accent)", textDecoration: "none" }}>{cta}</Link>
            </div>
          ))}
        </div>
      </section>

      {/* ════════ BRAND GALLERY ════════ */}
      <section className="reveal-section" style={{ marginTop: "clamp(56px,8vw,104px)", padding: "clamp(32px,4vw,52px) 0", borderTop: "1px solid rgba(255,255,255,0.06)", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>
        <p style={{ margin: "0 0 28px", textAlign: "center", fontSize: 10, letterSpacing: "0.34em", textTransform: "uppercase", color: "#5d5a54" }}>Мировые парфюмерные дома</p>
        <BrandGallery />
      </section>

      {/* ════════ CONTEST ════════ */}
      <ContestSection />

      {/* ════════ AROMA OF THE MONTH ════════ */}
      {aromaProduct && (
        <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 28 }}>
            <div style={{ flex: 1, height: 1, background: "linear-gradient(90deg, transparent, rgba(201,162,94,0.4))" }} />
            <span style={{ fontSize: 10, letterSpacing: "0.32em", textTransform: "uppercase", color: "#c9a25e" }}>Аромат месяца</span>
            <div style={{ flex: 1, height: 1, background: "linear-gradient(270deg, transparent, rgba(201,162,94,0.4))" }} />
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(280px,1fr))", gap: "clamp(24px,4vw,56px)", alignItems: "center", background: "linear-gradient(135deg, rgba(20,16,8,0.7) 0%, rgba(14,13,11,0.6) 100%)", border: "1px solid rgba(201,162,94,0.2)", borderRadius: 4, padding: "clamp(28px,4vw,52px)" }}>
            <div style={{ display: "flex", justifyContent: "center" }}>
              <div style={{ width: "clamp(180px,30vw,280px)", height: "clamp(180px,30vw,280px)", background: "#141210", borderRadius: 2, border: "1px solid rgba(201,162,94,0.1)", display: "flex", alignItems: "center", justifyContent: "center", padding: 20, boxShadow: "0 24px 64px rgba(0,0,0,0.6)" }}>
                {aromaProduct.images[0] ? <img src={aromaProduct.images[0]} alt={aromaProduct.name} style={{ width: "100%", height: "100%", objectFit: "contain" }} /> : <span style={{ fontSize: 48, color: "rgba(201,162,94,0.1)", fontStyle: "italic" }}>{(aromaProduct.brand || "?")[0]}</span>}
              </div>
            </div>
            <div>
              {aromaProduct.brand && <p style={{ margin: "0 0 6px", fontSize: 11, letterSpacing: "0.22em", textTransform: "uppercase", color: "#c9a25e" }}>{aromaProduct.brand}</p>}
              <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: "0 0 16px", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,3.6vw,48px)", lineHeight: 1.05, color: "#f5f4f0" }}>{aromaProduct.name}</h2>
              {aromaNote && <p style={{ margin: "0 0 20px", fontSize: "clamp(13px,1.4vw,15px)", color: "rgba(242,237,230,0.6)", lineHeight: 1.8, maxWidth: "42ch" }}>{aromaNote}</p>}
              <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 14, marginBottom: 24 }}>
                {(aromaProduct.priceRub ?? 0) > 0 && <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 32, color: "#f5f4f0" }}>{aromaProduct.priceRub.toLocaleString("ru-RU")} ₽</span>}
                {aromaValidUntil && new Date(aromaValidUntil) > new Date() && (
                  <span style={{ fontSize: 11, color: "#c9a25e", background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.2)", borderRadius: 2, padding: "4px 10px" }}>
                    ещё {Math.ceil((new Date(aromaValidUntil).getTime() - Date.now()) / 86400000)} дн.
                  </span>
                )}
              </div>
              <Link href={`/product/${encodeURIComponent(aromaProduct.offerId)}`} style={{ display: "inline-flex", alignItems: "center", gap: 10, padding: "13px 28px", borderRadius: 2, background: "linear-gradient(135deg, rgba(201,162,94,0.18), rgba(201,162,94,0.08))", border: "1px solid rgba(201,162,94,0.4)", color: "#c9a25e", fontSize: 13, fontWeight: 600, letterSpacing: "0.06em", textDecoration: "none", textTransform: "uppercase" }}>
                Открыть аромат
              </Link>
            </div>
          </div>
        </section>
      )}

      {/* ════════ REVIEWS ════════ */}
      {reviewsList.length > 0 && (
        <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
          <p className="eyebrow" style={{ marginBottom: 10 }}>Отзывы покупателей</p>
          <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: "0 0 34px", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Что говорят клиенты</h2>
          <div style={{ overflowX: "auto", scrollbarWidth: "none" as const }}>
            <div style={{ display: "flex", gap: 18, width: "max-content", paddingBottom: 4 }}>
              {reviewsList.map((r) => (
                <div key={r.id} style={{ width: 300, flexShrink: 0, padding: "24px 22px", border: "1px solid rgba(255,255,255,0.07)", borderRadius: 3, background: "#0e0e0e" }}>
                  <Quote size={16} style={{ color: "#c9a25e", opacity: 0.4, marginBottom: 12 }} />
                  <div style={{ display: "flex", gap: 2, marginBottom: 12 }}>
                    {Array.from({ length: 5 }).map((_, i) => (
                      <Star key={i} size={11} fill={i < r.rating ? "#c9a25e" : "none"} stroke={i < r.rating ? "#c9a25e" : "rgba(255,255,255,0.2)"} strokeWidth={1.5} />
                    ))}
                  </div>
                  {r.productName && <p style={{ margin: "0 0 8px", fontSize: 9.5, letterSpacing: "0.1em", textTransform: "uppercase", color: "#6f6c66" }}>{r.productName}</p>}
                  <p style={{ margin: "0 0 16px", fontSize: 13, color: "#8b8880", lineHeight: 1.7 }}>{r.text.length > 220 ? r.text.slice(0, 220) + "…" : r.text}</p>
                  <p style={{ margin: 0, fontSize: 12, color: "var(--text)" }}>{r.author ?? "Покупатель"}</p>
                  <p style={{ margin: "3px 0 0", fontSize: 10, color: "#6f6c66" }}>{new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}</p>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {/* ════════ NEWS ════════ */}
      {newsList.length > 0 && (
        <section style={{ padding: "clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0" }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 28 }}>
            <div>
              <p className="eyebrow" style={{ marginBottom: 10 }}>Новости</p>
              <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", margin: 0, fontStyle: "italic", fontWeight: 400, fontSize: "clamp(34px,4.4vw,56px)", lineHeight: 1, color: "#f5f4f0" }}>Magic Vibes</h2>
            </div>
            <Link href="/news" className="btn-ghost">Все новости →</Link>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))", gap: 18 }}>
            {newsList.map((post) => (
              <a key={post.id} href="https://t.me/magicvibes_ru" target="_blank" rel="noreferrer" style={{ borderRadius: 3, border: "1px solid rgba(255,255,255,0.07)", background: "#0e0e0e", overflow: "hidden", textDecoration: "none", display: "flex", flexDirection: "column" }}>
                {post.photoUrl && (
                  <div style={{ width: "100%", aspectRatio: "16/9", overflow: "hidden", background: "#141414" }}>
                    <img src={post.photoUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover" }} loading="lazy" />
                  </div>
                )}
                <div style={{ padding: "18px 18px 20px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
                  <p style={{ margin: 0, fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "#6f6c66" }}>{new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}</p>
                  <p style={{ margin: 0, fontSize: 13.5, color: "#8b8880", lineHeight: 1.65, flex: 1 }}>{post.text.replace(/#\S+/g, "").trim().slice(0, 220)}{post.text.length > 220 && "…"}</p>
                  <span style={{ fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--accent)" }}>Читать в Telegram →</span>
                </div>
              </a>
            ))}
          </div>
        </section>
      )}

      {/* ════════ UNBOXINGS ════════ */}
      <UnboxingSection />
    </>
  );
}
