import type { Metadata } from "next";
import Link from "next/link";
import Image from "next/image";
import { Quote, Star, ArrowUpRight, Sparkles, Gift, Map, FlaskConical, ShieldCheck, Truck } from "lucide-react";
import { fetchPopular, fetchAromaMesyatsa, fetchReviews, fetchNews, fetchBanners, fetchContests } from "@/lib/api";
import { SITE_URL, SITE_NAME } from "@/lib/seo";
import { getFeatures } from "@/lib/features";
import { toProductSlug } from "@/lib/slug";
import ProductCard from "@/components/ProductCard";
import BannerSlider from "@/components/BannerSlider";
import BrandGallery from "@/components/BrandGallery";
import ScentQuiz from "@/components/ScentQuiz";
import UnboxingSection from "@/components/UnboxingSection";
import CityTopsTicker from "@/components/CityTopsTicker";
import ContestSection from "@/components/ContestSection";
import PageEffects from "@/components/PageEffects";
import FlaconStory from "@/components/FlaconStory";
import CollectionsGallery from "@/components/CollectionsGallery";
import CountUp from "@/components/CountUp";

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
  description: "Оригинальная парфюмерия мировых брендов с доставкой по России",
  priceRange: "₽₽",
  openingHours: "Mo-Su 00:00-24:00",
  address: { "@type": "PostalAddress", addressCountry: "RU" },
  aggregateRating: { "@type": "AggregateRating", ratingValue: "4.9", reviewCount: "1240", bestRating: "5" },
};

/* Scent-family story blocks; the flacon changes liquid + label for each. */
const FAMILIES = [
  { key: "floral",   bg: "var(--c-floral)",   word: "цветы",  title: "Цветочные",  script: "роза, пион, жасмин",     side: "right" as const,
    text: "Самое большое семейство: от нежного пиона до густой туберозы. Для свиданий, весны и хорошего настроения.",
    chips: [["Женская", "/collections/zhenskaya"], ["Нишевая", "/collections/nishevaya"], ["Миниатюры", "/collections/miniatyury"]], href: "/collections/tsvetochnye" },
  { key: "fresh",    bg: "var(--c-fresh)",    word: "бриз",   title: "Свежие",     script: "море, цитрус, зелень",   side: "left" as const,
    text: "Прохладные, прозрачные и бодрящие — для жары, спорта и офиса. Их не бывает слишком много.",
    chips: [["Цитрусовые", "/collections/tsitrusovye"], ["Фужерные", "/collections/fuzhernye"], ["Унисекс", "/collections/uniseks"]], href: "/collections/svezhie" },
  { key: "woody",    bg: "var(--c-woody)",    word: "дерево", title: "Древесные",  script: "кедр, сандал, ветивер",  side: "right" as const,
    text: "Тёплые и уверенные. Основа мужского гардероба, осени и вечеров, когда хочется спокойной силы.",
    chips: [["Мужская", "/collections/muzhskaya"], ["Шипровые", "/collections/shiprovye"], ["Мускусные", "/collections/muskusnye"]], href: "/collections/drevesnye" },
  { key: "oriental", bg: "var(--c-oriental)", word: "уд",     title: "Восточные",  script: "амбра, уд, ваниль",      side: "left" as const,
    text: "Густые, сладкие, пряные. Арабская парфюмерия с шлейфом, который запоминают.",
    chips: [["Арабская", "/collections/arabskaya"], ["Элитная", "/collections/elitnaya"], ["Духи Parfum", "/collections/dukhi"]], href: "/collections/vostochnye" },
];

const SERVICES = [
  { icon: Sparkles,     title: "AI-подбор",           text: "Опишите настроение — подберём аромат из 22 000.",       href: "/find",                      tag: "за минуту" },
  { icon: Gift,         title: "Конструктор подарка", text: "Для кого, повод, бюджет — и готовый подарок.",           href: "/gift",                      tag: "без ошибок" },
  { icon: Gift,         title: "Подарочные наборы",   text: "Аромат с лосьоном, гелем или миниатюрами — готовый подарок.", href: "/collections/nabory",    tag: "с любовью" },
  { icon: FlaskConical, title: "Пробники",            text: "Отливанты и миниатюры, чтобы попробовать перед флаконом.", href: "/collections/probniki", tag: "от 2 мл" },
  { icon: Map,          title: "Карта ароматов",      text: "Путешествие по парфюмерным традициям мира.",             href: "/world",                     tag: "интерактив" },
  { icon: ShieldCheck,  title: "Гарантия оригинала",  text: "Прямые поставки и проверка каждого флакона.",            href: "/warranty",                  tag: "100%" },
  { icon: Truck,        title: "Доставка через Ozon", text: "1–5 дней до пункта выдачи рядом с домом.",               href: "/delivery",                  tag: "по России" },
];

const MARQUEE = ["100% оригинал", "Доставка 1–5 дней", "Оплата Ozon Pay", "22 000+ ароматов", "Рейтинг 4.9 на Ozon", "Подарочная упаковка"];

const HIDE = JSON.stringify({ hide: true });

function Marquee({ items, bg = "var(--ink)" }: { items: string[]; bg?: string }) {
  const row = [...items, ...items];
  return (
    <div className="mv-marquee" style={{ background: bg }} aria-hidden="true">
      <div className="mv-marquee-track">
        {row.map((t, i) => <span key={i} className="mv-marquee-item">{t}</span>)}
      </div>
    </div>
  );
}

export default async function HomePage() {
  const features = await getFeatures();
  const [popular, aroma, reviews, news, banners, contests] = await Promise.allSettled([
    fetchPopular(12),
    fetchAromaMesyatsa(),
    fetchReviews(6),
    fetchNews(3),
    fetchBanners(),
    fetchContests(),
  ]);

  const popularProducts = popular.status === "fulfilled" ? popular.value.products : [];
  const aromaProduct = aroma.status === "fulfilled" ? aroma.value.product : null;
  const aromaNote = aroma.status === "fulfilled" ? aroma.value.note : "";
  const aromaValidUntil = aroma.status === "fulfilled" ? aroma.value.validUntil : null;
  const reviewsList = reviews.status === "fulfilled" ? reviews.value.reviews : [];
  const newsList = news.status === "fulfilled" ? news.value.posts : [];
  const bannersList = banners.status === "fulfilled" ? banners.value : [];
  const contestsList = contests.status === "fulfilled" ? contests.value : [];

  const pad = "0 clamp(18px,4vw,56px)";

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(STORE_SCHEMA) }} />
      <PageEffects />
      <FlaconStory />

      {/* ════════ HERO ════════ */}
      <section
        className="mv-stage mv-staged"
        data-flacon={JSON.stringify({ x: 0.3, y: 0.02, ry: -0.35, rz: 0.06, s: 1, family: "floral", my: -0.24 })}
        style={{ background: "var(--lime)", minHeight: "100svh", display: "flex", alignItems: "center" }}
      >
        <span className="bg-word" style={{ left: "-2vw", bottom: "-4vw", color: "var(--ink)", opacity: 0.08 }}>магия</span>
        <div className="mv-stage-inner" style={{ width: "100%" }}>
          <div style={{ maxWidth: 680 }}>
            <span className="eyebrow anim-slide-up">✦ Оригинальная парфюмерия</span>
            <h1 className="h-mega anim-slide-up" style={{ marginTop: 22, animationDelay: "0.08s", fontSize: "clamp(40px,6.2vw,104px)" }}>
              Магия,<br />которую<br />чувствуешь
              <span className="h-script">22 000 оригинальных ароматов</span>
            </h1>
            <p className="anim-slide-up" style={{ margin: "26px 0 0", maxWidth: "44ch", fontSize: "clamp(15px,1.4vw,18px)", lineHeight: 1.6, color: "rgba(18,18,18,0.75)", animationDelay: "0.16s" }}>
              Chanel, Dior, Tom Ford, Creed, Byredo, Montale и ещё 200+ брендов. Проверяем каждый флакон и доставляем через Ozon за 1–5 дней.
            </p>
            <div className="anim-slide-up" style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 30, animationDelay: "0.24s" }}>
              <Link href="/catalog" className="btn-primary">Смотреть каталог <ArrowUpRight size={18} /></Link>
              <Link href="/find" className="btn-ghost"><Sparkles size={16} /> Подобрать с AI</Link>
            </div>
            <div className="anim-slide-up mv-hero-stats" style={{ animationDelay: "0.32s" }}>
              {[
                { v: <CountUp to={22000} suffix="+" />, l: "ароматов" },
                { v: "4.9", l: "рейтинг на Ozon" },
                { v: "1–5", l: "дней доставка" },
              ].map((s) => (
                <div key={s.l}>
                  <div style={{ fontFamily: "var(--font-display)", fontWeight: 700, fontSize: "clamp(24px,3vw,40px)", letterSpacing: "-0.03em", lineHeight: 1 }}>{s.v}</div>
                  <div style={{ fontSize: 13, color: "rgba(18,18,18,0.65)", marginTop: 6 }}>{s.l}</div>
                </div>
              ))}
            </div>
          </div>
          {/* static bottle when WebGL / motion is off */}
          <Image className="mv-flacon-fallback" src="/brand/shots/flacon-cutout.png?v=3" alt="Флакон Magic Vibes" width={500} height={600} priority
            style={{ position: "absolute", right: "clamp(0px,6vw,120px)", top: "50%", transform: "translateY(-45%)", width: "min(38vw, 460px)", height: "auto" }} />
        </div>
      </section>

      <Marquee items={MARQUEE} />

      {/* ════════ PROMO BANNERS (admin) ════════ */}
      {bannersList.some((b) => b.active) && (
        <section data-flacon={HIDE} style={{ padding: "clamp(28px,4vw,48px) clamp(18px,4vw,56px) 0", maxWidth: 1460, margin: "0 auto" }}>
          <BannerSlider banners={bannersList} />
        </section>
      )}

      {/* ════════ SCENT FAMILIES — flacon travels & changes ════════ */}
      {FAMILIES.map((f) => {
        const bottleRight = f.side === "right";
        return (
          <section key={f.key} className="mv-stage mv-staged"
            data-flacon={JSON.stringify({ x: bottleRight ? 0.24 : -0.24, y: 0, ry: bottleRight ? -0.45 : 0.45, rz: bottleRight ? 0.08 : -0.08, s: 1, family: f.key })}
            style={{ background: f.bg, minHeight: "92svh", display: "flex", alignItems: "center" }}>
            <span className="bg-word" style={{ [bottleRight ? "right" : "left"]: "-1vw", top: "8%", color: "#fff", opacity: 0.55 }}>{f.word}</span>
            <div className="mv-stage-inner reveal-section" style={{ width: "100%", display: "flex", justifyContent: bottleRight ? "flex-start" : "flex-end" }}>
              <div style={{ maxWidth: 560 }}>
                <span className="eyebrow">Семейство ароматов</span>
                <h2 className="h-mega" style={{ marginTop: 18 }}>{f.title}<span className="h-script">{f.script}</span></h2>
                <p style={{ margin: "22px 0 0", fontSize: "clamp(15px,1.4vw,18px)", lineHeight: 1.6, color: "rgba(18,18,18,0.78)" }}>{f.text}</p>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginTop: 24 }}>
                  {f.chips.map(([l, h]) => <Link key={h} href={h} className="chip">{l}</Link>)}
                </div>
                <Link href={f.href} className="btn-primary" style={{ marginTop: 28 }}>Смотреть {f.title.toLowerCase()} <ArrowUpRight size={18} /></Link>
              </div>
            </div>
            <Image className="mv-flacon-fallback" src={`/brand/shots/${f.key === "floral" ? "her" : f.key === "woody" ? "him" : f.key}.jpg?v=2`} alt="" width={420} height={525}
              style={{ position: "absolute", [bottleRight ? "right" : "left"]: "6vw", top: "50%", transform: "translateY(-50%)", width: "min(30vw, 380px)", height: "auto", borderRadius: 28 }} />
          </section>
        );
      })}

      {/* ════════ everything below until "how we work" keeps the flacon away ════════ */}
      <div data-flacon={HIDE} style={{ height: 1 }} />

      <CityTopsTicker />

      {/* ════════ COLLECTIONS — horizontal scroll gallery ════════ */}
      <CollectionsGallery />

      {/* ════════ POPULAR PRODUCTS ════════ */}
      {popularProducts.length > 0 && (
        <section className="mv-cv" style={{ padding: `clamp(40px,6vw,80px) clamp(18px,4vw,56px) 0`, maxWidth: 1460, margin: "0 auto" }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 34 }}>
            <div>
              <span className="eyebrow">Популярное</span>
              <h2 className="h-section" style={{ marginTop: 16 }}>Хиты сезона<span className="h-script">то, что берут чаще всего</span></h2>
            </div>
            <Link href="/catalog" className="btn-ghost">Смотреть всё <ArrowUpRight size={16} /></Link>
          </div>
          <div className="product-grid">
            {popularProducts.map(p => <div key={p.id} data-mv-card="1"><ProductCard product={p} /></div>)}
          </div>
        </section>
      )}

      <div data-flacon={HIDE} style={{ height: 1 }} />

      {/* ════════ HOW WE WORK — flacon returns ════════ */}
      <section className="mv-stage mv-staged" data-flacon={JSON.stringify({ x: 0.32, y: 0.04, ry: -0.6, rz: 0.12, s: 0.9, family: "gift" })}
        style={{ marginTop: "clamp(60px,8vw,120px)" }}>
        <div className="mv-stage-inner reveal-section">
          <h2 className="h-mega" style={{ color: "var(--danger)", maxWidth: 640, fontSize: "clamp(40px,6vw,96px)" }}>Четыре шага<span className="h-script" style={{ color: "var(--ink)" }}>и аромат у вас дома</span></h2>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(240px,1fr))", gap: "36px 48px", marginTop: 48, maxWidth: 760 }}>
            {[
              { n: "01", t: "Подбираем аромат", b: "По нотам, стойкости и поводу — сами, через AI или с консультантом." },
              { n: "02", t: "Проверяем оригинал", b: "Каждый флакон проходит проверку подлинности перед отправкой." },
              { n: "03", t: "Упаковываем как подарок", b: "Фирменная упаковка: заказ выглядит дороже, чем в бутике." },
              { n: "04", t: "Доставляем через Ozon", b: "1–5 дней в любой город России до пункта выдачи рядом с домом." },
            ].map((s) => (
              <div key={s.n}>
                <div style={{ fontFamily: "var(--font-display)", fontWeight: 800, fontSize: "clamp(40px,5vw,64px)", color: "var(--danger)", lineHeight: 1, letterSpacing: "-0.04em" }}>{s.n}</div>
                <h3 style={{ fontFamily: "var(--font-display)", fontWeight: 700, textTransform: "uppercase", fontSize: 18, letterSpacing: "-0.01em", margin: "14px 0 8px" }}>{s.t}</h3>
                <p style={{ margin: 0, fontSize: 15, lineHeight: 1.6, color: "var(--muted)" }}>{s.b}</p>
              </div>
            ))}
          </div>
          <Link href="/catalog" className="btn-primary" style={{ marginTop: 44 }}>Выбрать аромат <ArrowUpRight size={18} /></Link>
        </div>
      </section>

      <div data-flacon={HIDE} style={{ height: 1 }} />

      <Marquee items={["Оригинал", "Нишевая", "Арабская", "Пробники", "Наборы", "Новинки", "AI-подбор"]} />

      {/* ════════ SERVICES (ink) ════════ */}
      <section className="mv-cv" style={{ background: "var(--ink)", color: "var(--paper)" }}>
        <div style={{ maxWidth: 1360, margin: "0 auto", padding: "clamp(70px,9vw,130px) clamp(18px,4vw,56px)" }}>
          <h2 className="h-mega" style={{ color: "var(--paper)" }}>Всё для аромата<span className="h-script" style={{ color: "var(--lime)" }}>и немного магии</span></h2>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(280px,1fr))", marginTop: 52, borderTop: "1px solid rgba(245,242,236,0.14)" }}>
            {SERVICES.filter((sv) => features.giftBuilder ? sv.href !== "/collections/nabory" : sv.href !== "/gift").map(({ icon: Icon, title, text, href, tag }) => (
              <Link key={href} href={href} className="mv-service">
                <Icon size={26} color="var(--lime)" />
                <span style={{ display: "block", fontFamily: "var(--font-display)", fontWeight: 700, textTransform: "uppercase", fontSize: 19, margin: "18px 0 8px", color: "var(--paper)" }}>{title}</span>
                <span style={{ display: "block", fontSize: 15, lineHeight: 1.55, color: "rgba(245,242,236,0.62)" }}>{text}</span>
                <span style={{ display: "inline-block", marginTop: 16, fontSize: 13, fontWeight: 700, color: "var(--lime)" }}>{tag} →</span>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ════════ SCENT QUIZ ════════ */}
      <div className="reveal-section"><ScentQuiz /></div>

      {/* ════════ AROMA OF THE MONTH ════════ */}
      {aromaProduct && (
        <section className="mv-cv" style={{ background: "var(--pink)", color: "#fff", position: "relative", overflow: "hidden", marginTop: "clamp(40px,6vw,80px)" }}>
          <span aria-hidden="true" style={{ position: "absolute", right: "-2vw", top: "-6vw", fontFamily: "var(--font-display)", fontWeight: 800, fontSize: "clamp(160px,30vw,460px)", lineHeight: 1, color: "rgba(255,255,255,0.14)", letterSpacing: "-0.06em" }}>№1</span>
          <div style={{ position: "relative", maxWidth: 1360, margin: "0 auto", padding: "clamp(70px,9vw,120px) clamp(18px,4vw,56px)", display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(280px,1fr))", gap: "clamp(28px,5vw,72px)", alignItems: "center" }}>
            <div>
              <span className="eyebrow" style={{ background: "#fff", color: "var(--ink)" }}>Аромат месяца</span>
              {aromaProduct.brand && <p style={{ margin: "22px 0 6px", fontSize: 14, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>{aromaProduct.brand}</p>}
              <h2 className="h-section" style={{ color: "#fff", fontSize: "clamp(24px,2.8vw,42px)", lineHeight: 1.05 }}>{aromaProduct.name}</h2>
              {aromaNote && <p style={{ margin: "20px 0 0", fontSize: "clamp(15px,1.4vw,17px)", lineHeight: 1.7, maxWidth: "46ch", color: "rgba(255,255,255,0.9)" }}>{aromaNote}</p>}
              <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: 14, margin: "26px 0 28px" }}>
                {(aromaProduct.priceRub ?? 0) > 0 && <span style={{ fontFamily: "var(--font-display)", fontWeight: 700, fontSize: 34 }}>{aromaProduct.priceRub.toLocaleString("ru-RU")} ₽</span>}
                {aromaValidUntil && new Date(aromaValidUntil) > new Date() && (
                  <span style={{ fontSize: 13, fontWeight: 600, background: "rgba(255,255,255,0.18)", borderRadius: 999, padding: "6px 12px" }}>
                    ещё {Math.ceil((new Date(aromaValidUntil).getTime() - Date.now()) / 86400000)} дн.
                  </span>
                )}
              </div>
              <Link href={`/product/${toProductSlug(aromaProduct.name, aromaProduct.offerId)}`} className="btn-primary">Открыть аромат <ArrowUpRight size={18} /></Link>
            </div>
            <div style={{ position: "relative", aspectRatio: "1", maxWidth: 460, width: "100%", justifySelf: "center", background: "#fff", borderRadius: 40, boxShadow: "0 40px 80px -40px rgba(0,0,0,0.45)", transform: "rotate(3deg)" }}>
              {aromaProduct.images[0]
                ? <Image src={aromaProduct.images[0]} alt={aromaProduct.name} fill sizes="460px" style={{ objectFit: "contain", padding: 36 }} />
                : <span style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 80, fontFamily: "var(--font-display)", color: "var(--pink)" }}>{(aromaProduct.brand || "?")[0]}</span>}
            </div>
          </div>
        </section>
      )}

      {/* ════════ CONTEST ════════ */}
      <ContestSection contests={contestsList} />

      {/* ════════ BRANDS ════════ */}
      <section className="mv-cv reveal-section" style={{ marginTop: "clamp(56px,8vw,104px)", padding: "clamp(32px,4vw,52px) 0" }}>
        <p style={{ margin: "0 0 28px", textAlign: "center" }}><span className="eyebrow soft">Мировые парфюмерные дома</span></p>
        <BrandGallery />
      </section>

      {/* ════════ REVIEWS ════════ */}
      {reviewsList.length > 0 && (
        <section className="mv-cv" style={{ padding: `clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0`, maxWidth: 1460, margin: "0 auto" }}>
          <span className="eyebrow">Отзывы покупателей</span>
          <h2 className="h-section" style={{ margin: "16px 0 34px" }}>Что говорят<span className="h-script">4.9 из 5 на Ozon</span></h2>
          <div className="scroll-x">
            <div style={{ display: "flex", gap: 18, width: "max-content", paddingBottom: 6 }}>
              {reviewsList.map((r, i) => (
                <div key={r.id} style={{ width: 320, flexShrink: 0, padding: "26px 24px", borderRadius: "var(--r-lg)", background: ["#fff", "var(--c-floral)", "var(--c-fresh)", "var(--lime)"][i % 4] }}>
                  <Quote size={22} style={{ color: "var(--ink)", opacity: 0.25, marginBottom: 12 }} />
                  <div style={{ display: "flex", gap: 3, marginBottom: 12 }}>
                    {Array.from({ length: 5 }).map((_, k) => (
                      <Star key={k} size={14} fill={k < r.rating ? "var(--ink)" : "none"} stroke="var(--ink)" strokeWidth={1.5} />
                    ))}
                  </div>
                  {r.productName && <p style={{ margin: "0 0 8px", fontSize: 11, fontWeight: 700, letterSpacing: "0.06em", textTransform: "uppercase", color: "rgba(18,18,18,0.55)" }}>{r.productName}</p>}
                  <p style={{ margin: "0 0 16px", fontSize: 15, lineHeight: 1.6 }}>{r.text.length > 220 ? r.text.slice(0, 220) + "…" : r.text}</p>
                  <p style={{ margin: 0, fontFamily: "var(--font-script)", fontSize: 22 }}>{r.author ?? "Покупатель"}</p>
                  <p style={{ margin: "2px 0 0", fontSize: 12, color: "rgba(18,18,18,0.55)" }}>{new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}</p>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {/* ════════ NEWS ════════ */}
      {newsList.length > 0 && (
        <section className="mv-cv" style={{ padding: `clamp(56px,8vw,96px) clamp(18px,4vw,56px) 0`, maxWidth: 1460, margin: "0 auto" }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 24, flexWrap: "wrap", marginBottom: 28 }}>
            <div>
              <span className="eyebrow">Новости</span>
              <h2 className="h-section" style={{ marginTop: 16 }}>Magic Vibes<span className="h-script">из нашего Telegram</span></h2>
            </div>
            <Link href="/news" className="btn-ghost">Все новости <ArrowUpRight size={16} /></Link>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))", gap: 18 }}>
            {newsList.map((post) => (
              <a key={post.id} href="https://t.me/magicvibes_ru" target="_blank" rel="noreferrer" className="product-card" style={{ color: "var(--ink)" }}>
                {post.photoUrl && (
                  <div style={{ width: "100%", aspectRatio: "16/9", overflow: "hidden", background: "var(--surface2)", position: "relative" }}>
                    <Image src={post.photoUrl} alt="" fill sizes="(max-width: 768px) 100vw, 400px" style={{ objectFit: "cover" }} unoptimized={!post.photoUrl.includes("davidsklad.ru")} />
                  </div>
                )}
                <div style={{ padding: "18px 20px 22px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
                  <p style={{ margin: 0, fontSize: 12, fontWeight: 600, color: "var(--muted)" }}>{new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}</p>
                  <p style={{ margin: 0, fontSize: 15, lineHeight: 1.6, flex: 1 }}>{post.text.replace(/#\S+/g, "").trim().slice(0, 220)}{post.text.length > 220 && "…"}</p>
                  <span style={{ fontSize: 13, fontWeight: 700, color: "var(--accent)" }}>Читать в Telegram →</span>
                </div>
              </a>
            ))}
          </div>
        </section>
      )}

      {/* ════════ UNBOXINGS ════════ */}
      <UnboxingSection />

      <div data-flacon={HIDE} style={{ height: 1 }} />

      {/* ════════ FINAL CTA (electric) ════════ */}
      <section className="mv-stage mv-staged" data-flacon={JSON.stringify({ x: 0.26, y: 0.02, ry: -0.3, rz: -0.08, s: 1, family: "niche" })}
        style={{ background: "var(--c-electric)", color: "#fff", marginTop: "clamp(60px,8vw,120px)", minHeight: "86svh", display: "flex", alignItems: "center" }}>
        <div className="mv-stage-inner reveal-section" style={{ width: "100%" }}>
          <h2 className="h-mega" style={{ color: "#fff", maxWidth: 760 }}>Найди свой аромат<span className="h-script" style={{ color: "var(--lime)" }}>за одну минуту</span></h2>
          <p style={{ margin: "24px 0 0", maxWidth: "42ch", fontSize: "clamp(15px,1.4vw,18px)", lineHeight: 1.6, color: "rgba(255,255,255,0.85)" }}>
            Расскажите, что любите — AI подберёт ароматы из каталога и объяснит, почему они вам подойдут.
          </p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 30 }}>
            <Link href="/find" className="btn-primary on-dark"><Sparkles size={18} /> Подобрать аромат</Link>
            <Link href="/catalog" className="btn-ghost" style={{ borderColor: "#fff", color: "#fff" }}>В каталог</Link>
          </div>
        </div>
        <Image className="mv-flacon-fallback" src="/brand/shots/niche.jpg?v=3" alt="" width={420} height={525}
          style={{ position: "absolute", right: "6vw", top: "50%", transform: "translateY(-50%)", width: "min(30vw, 380px)", height: "auto", borderRadius: 28 }} />
      </section>
    </>
  );
}
