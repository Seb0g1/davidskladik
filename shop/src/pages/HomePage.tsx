import { useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { ArrowRight, BadgeCheck, Truck, Star, Newspaper, Quote } from "lucide-react";
import { api } from "../api";
import type { AutoCategory } from "../types";
import ProductCard from "../components/ProductCard";

/* ── Reveal on scroll ──────────────────────────────────────────── */
function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll(".reveal-section");
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) {
          e.target.classList.add("is-visible");
          io.unobserve(e.target);
        }
      });
    }, { threshold: 0.08 });
    els.forEach((el) => io.observe(el));
    return () => io.disconnect();
  }, []);
}

const CAT_LABELS: Record<string, string> = {
  parfum: "Духи", edp: "Парфюмерная вода", edt: "Туалетная вода",
  sets: "Наборы", testers: "Тестеры", body: "Уход", home: "Для дома", deo: "Дезодоранты",
};
const CAT_ROMAN: Record<string, string> = {
  parfum: "I", edp: "II", edt: "III", sets: "IV", testers: "V", body: "VI", home: "VII", deo: "VIII",
};

const BRANDS = ["Chanel","Dior","Tom Ford","Hermès","Byredo","Jo Malone","Creed","Guerlain","Givenchy","Prada","Valentino","Burberry","Versace","Hugo Boss","Montale","Kilian","YSL","Bvlgari","Lancôme","Moschino"];

function CatCard({ cat, index }: { cat: AutoCategory; index: number }) {
  return (
    <Link
      to={`/catalog?category=${cat.slug}`}
      style={{
        display: "flex", flexDirection: "column", gap: 10, textDecoration: "none",
        width: 140, padding: "20px 16px 18px",
        borderRadius: 10, border: "1px solid var(--border)",
        background: "var(--surface)",
        transition: "border-color 0.2s, transform 0.2s, box-shadow 0.2s",
        animation: `slideUp 0.45s cubic-bezier(0.22,1,0.36,1) ${index * 0.04}s both`,
        flexShrink: 0,
      }}
      onMouseEnter={e => {
        const el = e.currentTarget as HTMLElement;
        el.style.borderColor = "var(--accent)";
        el.style.transform = "translateY(-2px)";
        el.style.boxShadow = "0 8px 28px rgba(0,0,0,0.35)";
      }}
      onMouseLeave={e => {
        const el = e.currentTarget as HTMLElement;
        el.style.borderColor = "var(--border)";
        el.style.transform = "translateY(0)";
        el.style.boxShadow = "none";
      }}
    >
      <span style={{ fontSize: 11, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.06em", fontFamily: "inherit" }}>
        {CAT_ROMAN[cat.slug] ?? String(index + 1).padStart(2, "0")}
      </span>
      <p style={{ fontSize: 13, fontWeight: 500, color: "var(--text)", lineHeight: 1.35 }}>
        {CAT_LABELS[cat.slug] || cat.label}
      </p>
      <p style={{ fontSize: 10.5, color: "var(--subtle)", marginTop: "auto" }}>
        {cat.count.toLocaleString("ru-RU")} поз.
      </p>
    </Link>
  );
}

function CardSkeleton() {
  return (
    <div className="product-card" style={{ width: 168, flexShrink: 0 }}>
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div style={{ padding: "10px 12px 12px" }}>
        <div className="skeleton" style={{ height: 8, width: "40%", marginBottom: 6 }} />
        <div className="skeleton" style={{ height: 11, marginBottom: 4 }} />
        <div className="skeleton" style={{ height: 11, width: "70%", marginBottom: 8 }} />
        <div className="skeleton" style={{ height: 14, width: "50%" }} />
      </div>
    </div>
  );
}

const doubled = [...BRANDS, ...BRANDS];

export default function HomePage() {
  useReveal();

  const { data: catalog, isLoading } = useQuery({
    queryKey: ["shop-home"],
    queryFn: () => api.catalog({ pageSize: 12, sort: "name" }),
  });
  const { data: autoCategories } = useQuery({
    queryKey: ["shop-auto-categories"],
    queryFn: () => api.autoCategories(),
    staleTime: 5 * 60_000,
  });
  const { data: newsData } = useQuery({
    queryKey: ["shop-news"],
    queryFn: () => api.news(6),
    staleTime: 5 * 60_000,
  });
  const { data: reviewsData } = useQuery({
    queryKey: ["shop-reviews"],
    queryFn: () => api.reviews(8),
    staleTime: 5 * 60_000,
  });

  return (
    <div style={{ background: "var(--bg)", color: "var(--text)", minHeight: "100vh" }}>

      {/* ════════════════════ HERO ════════════════════ */}
      <section style={{ position: "relative", minHeight: "92svh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "0 clamp(20px,5vw,60px)", textAlign: "center" }}>
        {/* Subtle warm gradient vignette */}
        <div style={{ position: "absolute", inset: 0, background: "radial-gradient(ellipse 80% 60% at 50% 40%, rgba(201,169,110,0.04) 0%, transparent 70%)", pointerEvents: "none" }} />

        <div style={{ position: "relative", zIndex: 1, maxWidth: 860, width: "100%" }}>
          {/* Eyebrow */}
          <p className="anim-fade-in" style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase", color: "var(--accent)", marginBottom: 28, animationDelay: "0.1s" }}>
            Оригинальная парфюмерия
          </p>

          {/* Main headline */}
          <h1 className="serif anim-slide-up" style={{
            fontSize: "clamp(56px, 13vw, 160px)",
            fontWeight: 500,
            lineHeight: 0.9,
            letterSpacing: "-0.02em",
            marginBottom: 0,
            color: "var(--text)",
            animationDelay: "0.15s",
            fontStyle: "italic",
          }}>
            Magic<br />Vibes
          </h1>

          {/* Thin rule */}
          <div className="anim-fade-in" style={{ width: 48, height: 1, background: "var(--accent)", margin: "32px auto", animationDelay: "0.3s" }} />

          <p className="anim-slide-up" style={{ fontSize: "clamp(14px,1.8vw,17px)", color: "var(--muted)", maxWidth: 400, margin: "0 auto 40px", lineHeight: 1.7, animationDelay: "0.35s", fontWeight: 400 }}>
            Мировые ароматы с доставкой по России через Ozon
          </p>

          <div className="anim-slide-up" style={{ display: "flex", gap: 12, justifyContent: "center", flexWrap: "wrap", animationDelay: "0.45s" }}>
            <Link to="/catalog" className="btn-primary">
              Каталог ароматов
            </Link>
            <Link to="/brands" className="btn-ghost">
              Все бренды <ArrowRight size={14} strokeWidth={2} />
            </Link>
          </div>
        </div>

        {/* Scroll hint */}
        <div style={{ position: "absolute", bottom: 28, left: "50%", transform: "translateX(-50%)", display: "flex", flexDirection: "column", alignItems: "center", gap: 6 }}>
          <div style={{ width: 1, height: 36, background: "linear-gradient(to bottom, transparent, var(--border-md))" }} />
        </div>
      </section>

      {/* ════════════════════ STATS BAND ════════════════════ */}
      <section className="reveal-section">
        <div className="section-divider" />
        <div style={{ maxWidth: 1200, margin: "0 auto" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)" }}>
            {[
              { value: "22 000+", label: "Ароматов в наличии" },
              { value: "100%",    label: "Гарантия оригинала" },
              { value: "1–5 дн.", label: "Доставка по России" },
              { value: "4.9",     label: "Рейтинг на Ozon" },
            ].map(({ value, label }, i) => (
              <div key={label} style={{
                padding: "clamp(28px,4vw,48px) clamp(20px,4vw,52px)",
                borderRight: i % 2 === 0 ? "1px solid var(--border)" : undefined,
                borderBottom: i < 2 ? "1px solid var(--border)" : undefined,
              }}>
                <div className="serif" style={{ fontSize: "clamp(32px,4.5vw,56px)", fontWeight: 500, color: "var(--text)", letterSpacing: "-0.02em", lineHeight: 1, marginBottom: 8, fontStyle: "italic" }}>
                  {value}
                </div>
                <div style={{ fontSize: 12, color: "var(--muted)", letterSpacing: "0.04em", fontWeight: 500 }}>
                  {label}
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="section-divider" />
      </section>

      {/* ════════════════════ CATEGORIES ════════════════════ */}
      {autoCategories && autoCategories.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) 0" }}>
          <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)", marginBottom: 28 }}>
            <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
              <div>
                <p style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.16em", textTransform: "uppercase", color: "var(--accent)", marginBottom: 10 }}>
                  Коллекция
                </p>
                <h2 className="serif" style={{ fontSize: "clamp(26px,3.5vw,44px)", fontWeight: 500, letterSpacing: "-0.01em", lineHeight: 1.05, fontStyle: "italic" }}>
                  Категории
                </h2>
              </div>
              <Link to="/catalog" className="btn-ghost" style={{ flexShrink: 0 }}>
                Все товары <ArrowRight size={14} strokeWidth={2} />
              </Link>
            </div>
          </div>
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)", paddingBottom: 12 }}>
            <div style={{ display: "flex", gap: 10, width: "max-content" }}>
              {autoCategories.map((cat, i) => <CatCard key={cat.slug} cat={cat} index={i} />)}
            </div>
          </div>
        </section>
      )}

      {/* ════════════════════ PRODUCTS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "0 0 clamp(48px,6vw,80px)" }}>
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)", marginBottom: 24 }}>
          <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
            <div>
              <p style={{ fontSize: 10, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 10 }}>Популярное</p>
              <h2 className="serif" style={{ fontSize: "clamp(24px,3.5vw,40px)", fontWeight: 500, letterSpacing: "-0.01em", fontStyle: "italic" }}>Хиты сезона</h2>
            </div>
            <Link to="/catalog" className="btn-ghost" style={{ flexShrink: 0 }}>Смотреть всё <ArrowRight size={13} strokeWidth={2} /></Link>
          </div>
        </div>

        {isLoading ? (
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "flex", gap: 14, width: "max-content" }}>
              {Array.from({ length: 8 }).map((_, i) => <CardSkeleton key={i} />)}
            </div>
          </div>
        ) : (
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "flex", gap: 14, width: "max-content" }}>
              {catalog?.products.map((p) => (
                <div key={p.offerId} style={{ width: 168, flexShrink: 0 }}>
                  <ProductCard product={p} />
                </div>
              ))}
            </div>
          </div>
        )}
      </section>

      {/* ════════════════════ FEATURES ════════════════════ */}
      <section className="reveal-section">
        <div className="section-divider" />
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(48px,6vw,80px) clamp(16px,4vw,48px)" }}>
          <p style={{ fontSize: 10, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 10, textAlign: "center" }}>Почему мы</p>
          <h2 className="serif" style={{ fontSize: "clamp(26px,3.5vw,44px)", fontWeight: 500, letterSpacing: "-0.01em", fontStyle: "italic", textAlign: "center", marginBottom: 40 }}>
            Ваш выбор
          </h2>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 1, border: "1px solid var(--border)", borderRadius: 10, overflow: "hidden" }}>
            {[
              { icon: <BadgeCheck size={18} />, title: "100% оригинал", text: "Прямые поставки от авторизованных дистрибьюторов. Сертификаты на каждый бренд." },
              { icon: <Truck size={18} />, title: "Доставка по России", text: "Через Ozon за 1–5 дней в любой город страны. Удобные пункты выдачи." },
              { icon: <Star size={18} />, title: "4.9 на Ozon", text: "Тысячи довольных покупателей. Рейтинг 4.9 из 5 звёзд на маркетплейсе." },
              { icon: <ArrowRight size={18} />, title: "Широкий выбор", text: "Более 22 000 ароматов от 200+ брендов — от масс-маркета до нишевой парфюмерии." },
            ].map(({ icon, title, text }) => (
              <div key={title} style={{
                padding: "32px 28px", background: "var(--surface)",
                borderRight: "1px solid var(--border)", borderBottom: "1px solid var(--border)",
              }}>
                <div style={{ width: 36, height: 36, display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 18, color: "var(--accent)" }}>
                  {icon}
                </div>
                <h3 style={{ fontSize: 14, fontWeight: 600, marginBottom: 10, color: "var(--text)", letterSpacing: "0.01em" }}>{title}</h3>
                <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.7 }}>{text}</p>
              </div>
            ))}
          </div>
        </div>
        <div className="section-divider" />
      </section>

      {/* ════════════════════ PROMO CARDS ════════════════════ */}
      <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) clamp(16px,4vw,48px)", maxWidth: 1200, margin: "0 auto" }}>
        <div style={{ display: "grid", gap: 14 }}>
          <style>{`@media(min-width:640px){.promo-grid{grid-template-columns:1fr 1fr!important;}}`}</style>
          <div className="promo-grid" style={{ display: "grid", gridTemplateColumns: "1fr", gap: 14 }}>
            <Link to="/catalog?category=sets" style={{
              padding: "clamp(28px,4vw,44px)", minHeight: 220, display: "flex", flexDirection: "column",
              justifyContent: "space-between", borderRadius: 10, border: "1px solid var(--border)",
              background: "var(--surface)", textDecoration: "none", overflow: "hidden",
              transition: "border-color 0.2s, transform 0.2s, box-shadow 0.2s",
            }}
              onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "var(--border-md)"; el.style.transform = "translateY(-2px)"; el.style.boxShadow = "0 12px 40px rgba(0,0,0,0.4)"; }}
              onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "var(--border)"; el.style.transform = "none"; el.style.boxShadow = "none"; }}
            >
              <div>
                <p style={{ fontSize: 10, fontWeight: 600, color: "var(--subtle)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 14 }}>Для близких</p>
                <h3 className="serif" style={{ fontSize: "clamp(22px,3vw,34px)", fontWeight: 500, letterSpacing: "-0.01em", lineHeight: 1.1, marginBottom: 12, color: "var(--text)", fontStyle: "italic" }}>Подарите аромат</h3>
                <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.65, maxWidth: 280 }}>Подарочные наборы от мировых парфюмерных домов</p>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 500, color: "var(--accent)", marginTop: 24 }}>
                Выбрать набор <ArrowRight size={13} strokeWidth={2} />
              </div>
            </Link>

            <Link to="/catalog?category=parfum" style={{
              padding: "clamp(28px,4vw,44px)", minHeight: 220, display: "flex", flexDirection: "column",
              justifyContent: "space-between", borderRadius: 10,
              border: "1px solid rgba(201,169,110,0.18)", background: "rgba(201,169,110,0.04)",
              textDecoration: "none", overflow: "hidden",
              transition: "border-color 0.2s, transform 0.2s, box-shadow 0.2s",
            }}
              onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,169,110,0.35)"; el.style.transform = "translateY(-2px)"; el.style.boxShadow = "0 12px 40px rgba(201,169,110,0.08)"; }}
              onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,169,110,0.18)"; el.style.transform = "none"; el.style.boxShadow = "none"; }}
            >
              <div>
                <p style={{ fontSize: 10, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.12em", textTransform: "uppercase", marginBottom: 14 }}>Эксклюзив</p>
                <h3 className="serif" style={{ fontSize: "clamp(22px,3vw,34px)", fontWeight: 500, letterSpacing: "-0.01em", lineHeight: 1.1, marginBottom: 12, color: "var(--text)", fontStyle: "italic" }}>Духи Parfum</h3>
                <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.65, maxWidth: 280 }}>Редкие ароматы высочайшей концентрации от нишевых домов</p>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 500, color: "var(--accent2)", marginTop: 24 }}>
                Смотреть <ArrowRight size={13} strokeWidth={2} />
              </div>
            </Link>
          </div>
        </div>
      </section>

      {/* ════════════════════ BRANDS MARQUEE ════════════════════ */}
      <section className="reveal-section" style={{ overflow: "hidden", paddingBlock: "clamp(24px,3vw,40px)" }}>
        <div className="section-divider" />
        <p style={{ textAlign: "center", fontSize: 10, fontWeight: 600, color: "var(--subtle)", letterSpacing: "0.18em", textTransform: "uppercase", padding: "24px 0 18px" }}>
          Мировые парфюмерные дома
        </p>
        <div style={{ position: "relative" }}>
          <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to right, var(--bg), transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 80, background: "linear-gradient(to left, var(--bg), transparent)", zIndex: 10, pointerEvents: "none" }} />
          <div className="animate-marquee">
            {doubled.map((brand, i) => (
              <Link key={i} to={`/catalog?brand=${encodeURIComponent(brand)}`}
                style={{ display: "inline-flex", alignItems: "center", marginInline: 20, fontSize: 12.5, fontWeight: 500, color: "var(--subtle)", textDecoration: "none", whiteSpace: "nowrap", transition: "color 0.15s ease", letterSpacing: "0.05em" }}
                onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
                onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
              >
                {brand}
                <span style={{ marginLeft: 20, color: "var(--border-md)" }}>·</span>
              </Link>
            ))}
          </div>
        </div>
        <div className="section-divider" style={{ marginTop: 24 }} />
      </section>

      {/* ════════════════════ REVIEWS ════════════════════ */}
      {reviewsData && reviewsData.reviews.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) 0" }}>
          <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)", marginBottom: 32 }}>
            <p style={{ fontSize: 10, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 10 }}>
              Отзывы покупателей
            </p>
            <h2 className="serif" style={{ fontSize: "clamp(26px,3.5vw,44px)", fontWeight: 500, letterSpacing: "-0.01em", lineHeight: 1.05, fontStyle: "italic" }}>
              Что говорят клиенты
            </h2>
          </div>
          <div className="scroll-x" style={{ padding: "0 clamp(16px,4vw,48px)", paddingBottom: 16 }}>
            <div style={{ display: "flex", gap: 14, width: "max-content" }}>
              {reviewsData.reviews.map((r) => (
                <div key={r.id} style={{
                  width: 280, flexShrink: 0, padding: "22px 20px",
                  borderRadius: 10, border: "1px solid var(--border)",
                  background: "var(--surface)",
                }}>
                  <Quote size={16} style={{ color: "var(--accent)", opacity: 0.4, marginBottom: 12 }} />
                  <div style={{ display: "flex", gap: 2, marginBottom: 12 }}>
                    {Array.from({ length: 5 }).map((_, i) => (
                      <Star key={i} size={11} fill={i < r.rating ? "var(--accent)" : "none"} stroke={i < r.rating ? "var(--accent)" : "var(--border-md)"} strokeWidth={1.5} />
                    ))}
                  </div>
                  {r.productName && (
                    <p style={{ fontSize: 9.5, fontWeight: 600, color: "var(--subtle)", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>{r.productName}</p>
                  )}
                  <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.7, marginBottom: 16 }}>
                    {r.text.length > 200 ? r.text.slice(0, 200) + "…" : r.text}
                  </p>
                  <p style={{ fontSize: 12, fontWeight: 600, color: "var(--text)" }}>{r.author ?? "Покупатель"}</p>
                  <p style={{ fontSize: 10, color: "var(--subtle)", marginTop: 3 }}>
                    {new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
                  </p>
                </div>
              ))}
            </div>
          </div>
          <div className="section-divider" style={{ marginTop: 32 }} />
        </section>
      )}

      {/* ════════════════════ NEWS ════════════════════ */}
      {newsData && newsData.posts.length > 0 && (
        <section className="reveal-section" style={{ padding: "clamp(48px,6vw,80px) 0" }}>
          <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)", marginBottom: 28 }}>
            <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
              <div>
                <p style={{ fontSize: 10, fontWeight: 600, color: "var(--accent)", letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 10, display: "flex", alignItems: "center", gap: 6 }}>
                  <Newspaper size={12} />
                  Новости
                </p>
                <h2 className="serif" style={{ fontSize: "clamp(26px,3.5vw,44px)", fontWeight: 500, letterSpacing: "-0.01em", lineHeight: 1.05, fontStyle: "italic" }}>
                  Новости Magic Vibes
                </h2>
              </div>
              <Link to="/news" className="btn-ghost" style={{ flexShrink: 0, textDecoration: "none" }}>
                Все новости <ArrowRight size={13} strokeWidth={2} />
              </Link>
            </div>
          </div>
          <div style={{ maxWidth: 1200, margin: "0 auto", padding: "0 clamp(16px,4vw,48px)" }}>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 14 }}>
              {newsData.posts.map((post) => (
                <a key={post.id} href="https://t.me/magicvibes_ru" target="_blank" rel="noreferrer"
                  style={{
                    borderRadius: 10, border: "1px solid var(--border)", background: "var(--surface)",
                    overflow: "hidden", textDecoration: "none", display: "flex", flexDirection: "column",
                    transition: "border-color 0.2s, transform 0.2s",
                  }}
                  onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "var(--border-md)"; el.style.transform = "translateY(-2px)"; }}
                  onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "var(--border)"; el.style.transform = "none"; }}
                >
                  {post.photoUrl && (
                    <div style={{ width: "100%", aspectRatio: "16/9", overflow: "hidden", background: "var(--surface2)" }}>
                      <img src={post.photoUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover" }} loading="lazy" />
                    </div>
                  )}
                  <div style={{ padding: "18px 18px 20px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
                    <p style={{ fontSize: 10, fontWeight: 600, color: "var(--subtle)", letterSpacing: "0.1em", textTransform: "uppercase" }}>
                      {new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long" })}
                    </p>
                    <p style={{ fontSize: 13.5, color: "var(--muted)", lineHeight: 1.65, flex: 1 }}>
                      {post.text.replace(/#\S+/g, "").trim().slice(0, 220)}
                      {post.text.length > 220 && "…"}
                    </p>
                    <span style={{ fontSize: 12, fontWeight: 500, color: "var(--accent)", display: "flex", alignItems: "center", gap: 5 }}>
                      Читать в Telegram <ArrowRight size={12} strokeWidth={2} />
                    </span>
                  </div>
                </a>
              ))}
            </div>
          </div>
        </section>
      )}

    </div>
  );
}
