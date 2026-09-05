import { useState, useEffect, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import { useMutation, useQuery } from "@tanstack/react-query";
import { ShoppingBag, Check, ChevronLeft, Star, Shield, Truck, RefreshCw, Minus, Plus, ChevronRight, Share2, Link2, Users, X, Bell, MessageSquare, ThumbsUp, Send } from "lucide-react";
import { api } from "../api";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import type { ShopReview, MarketplaceReview } from "../types";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent2: "#D9BF8F",
  accent3: "#EDD9B0",
};

function viewerCount(offerId: string, rating: number | null | undefined): number {
  let h = 0;
  for (let i = 0; i < offerId.length; i++) h = (h * 31 + offerId.charCodeAt(i)) >>> 0;
  const minute = Math.floor(Date.now() / 60000);
  const base = (h + minute) % 7;
  if ((rating ?? 0) >= 4.5) return base + 3;
  if ((rating ?? 0) >= 4) return (base % 3) + 2;
  return (base % 2) + 1;
}

export default function ProductPage() {
  const { offerId } = useParams<{ offerId: string }>();
  const { add } = useCart();
  const [qty, setQty] = useState(1);
  const [activeImg, setActiveImg] = useState(0);
  const [added, setAdded] = useState(false);
  const [cartPopup, setCartPopup] = useState(false);
  const [imgErrors, setImgErrors] = useState<Set<number>>(new Set());
  const [viewers, setViewers] = useState(0);
  const [alertEmail, setAlertEmail] = useState("");
  const [alertSent, setAlertSent] = useState(false);
  const [shareCopied, setShareCopied] = useState(false);
  const [shareOpen, setShareOpen] = useState(false);
  const shareRef = useRef<HTMLDivElement>(null);

  const { customer, token } = useAuth();
  const [reviewText, setReviewText] = useState("");
  const [reviewRating, setReviewRating] = useState(5);
  const [reviewHover, setReviewHover] = useState(0);
  const [reviewOpen, setReviewOpen] = useState(false);
  const [reviewSent, setReviewSent] = useState(false);

  const { data: product, isLoading, error } = useQuery({
    queryKey: ["shop-product", offerId],
    queryFn: () => api.product(offerId!),
    enabled: !!offerId,
  });

  const { data: related } = useQuery({
    queryKey: ["shop-related", product?.brand],
    queryFn: () => api.catalog({ brand: product!.brand!, pageSize: 8, inStock: true }),
    enabled: !!product?.brand,
    staleTime: 5 * 60_000,
  });
  const relatedItems = (related?.products ?? [])
    .filter((p) => p.offerId !== offerId)
    .slice(0, 3);

  const reviewsQuery = useQuery({
    queryKey: ["shop-reviews", offerId],
    queryFn: () => api.reviews(20, offerId),
    enabled: !!offerId,
    staleTime: 2 * 60_000,
  });
  const productReviews: ShopReview[] = reviewsQuery.data?.reviews ?? [];

  const mpReviewsQuery = useQuery({
    queryKey: ["shop-mp-reviews", offerId],
    queryFn: () => api.marketplaceReviews(offerId!),
    enabled: !!offerId,
    staleTime: 30 * 60_000,
  });
  const mpReviews: MarketplaceReview[] = mpReviewsQuery.data?.reviews ?? [];
  const mpAvgRating = mpReviewsQuery.data?.avgRating ?? 0;
  const mpReviewCount = mpReviewsQuery.data?.reviewCount ?? 0;

  const reviewMutation = useMutation({
    mutationFn: () => api.postReview({
      offerId: offerId!,
      productName: product?.name,
      productImg: product?.images[0],
      rating: reviewRating,
      text: reviewText,
    }, token!),
    onSuccess: () => { setReviewSent(true); setReviewOpen(false); reviewsQuery.refetch(); },
  });

  useEffect(() => {
    if (!product) return;
    const ld: Record<string, unknown> = {
      "@context": "https://schema.org/",
      "@type": "Product",
      name: product.name,
      brand: { "@type": "Brand", name: product.brand },
      description: product.description ? product.description.slice(0, 200) : undefined,
      image: product.images,
      offers: {
        "@type": "Offer",
        priceCurrency: "RUB",
        price: product.priceRub,
        availability: product.inStock
          ? "https://schema.org/InStock"
          : "https://schema.org/OutOfStock",
        url: `https://magicvibes.ru/product/${product.offerId}`,
      },
    };
    if (product.rating && product.reviewCount) {
      ld.aggregateRating = {
        "@type": "AggregateRating",
        ratingValue: product.rating,
        reviewCount: product.reviewCount,
      };
    }
    const script = document.createElement("script");
    script.type = "application/ld+json";
    script.id = "product-ld-json";
    script.textContent = JSON.stringify(ld);
    document.head.appendChild(script);
    return () => {
      const existing = document.getElementById("product-ld-json");
      if (existing) existing.remove();
    };
  }, [product]);

  useEffect(() => {
    if (!shareOpen) return;
    const fn = (e: MouseEvent) => {
      if (shareRef.current && !shareRef.current.contains(e.target as Node)) setShareOpen(false);
    };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, [shareOpen]);

  useEffect(() => {
    if (!product) return;
    setViewers(viewerCount(offerId!, product.rating));
    const id = setInterval(() => setViewers(viewerCount(offerId!, product.rating)), 60_000);
    return () => clearInterval(id);
  }, [offerId, product]);

  const alertMutation = useMutation({
    mutationFn: () => api.stockAlert(offerId!, alertEmail),
    onSuccess: () => setAlertSent(true),
  });

  function handleAdd() {
    if (!product || !product.inStock) return;
    add(product, qty);
    setAdded(true);
    setCartPopup(true);
    setTimeout(() => setAdded(false), 2000);
  }

  if (isLoading) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1100, margin: "0 auto", padding: "clamp(24px,4vw,48px) clamp(16px,4vw,32px)" }}>
          <div style={{ background: S.surface, borderRadius: 24, overflow: "hidden", border: `1px solid ${S.border}` }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr" }}>
              <div className="skeleton" style={{ aspectRatio: "1", minHeight: 400 }} />
              <div style={{ padding: 48, display: "flex", flexDirection: "column", gap: 20 }}>
                <div className="skeleton" style={{ height: 12, width: "30%" }} />
                <div className="skeleton" style={{ height: 32, width: "80%" }} />
                <div className="skeleton" style={{ height: 12, width: "40%" }} />
                <div className="skeleton" style={{ height: 48, borderRadius: 16, marginTop: 24 }} />
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !product) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 56, marginBottom: 16 }}>😔</div>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: S.text, marginBottom: 8 }}>Товар не найден</h2>
          <Link to="/catalog" style={{ fontSize: 13, color: S.accent3, textDecoration: "none", fontWeight: 500 }}>
            ← Вернуться в каталог
          </Link>
        </div>
      </div>
    );
  }

  const validImages = product.images.filter((_, i) => !imgErrors.has(i));
  const activeValidImg = validImages[activeImg] || null;
  const discount = product.oldPriceRub ? Math.round((1 - product.priceRub / product.oldPriceRub) * 100) : 0;
  const pageUrl = typeof window !== "undefined" ? window.location.href : "";
  const shareText = `${product.name} — ${product.priceRub.toLocaleString("ru-RU")} ₽`;

  function copyShareLink() {
    navigator.clipboard.writeText(pageUrl).then(() => {
      setShareCopied(true);
      setTimeout(() => setShareCopied(false), 2000);
    });
  }

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "clamp(20px,3vw,48px) clamp(16px,4vw,32px)" }}>

        {/* Breadcrumb */}
        <nav style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: S.muted, marginBottom: 24, flexWrap: "wrap" }}>
          <Link to="/" style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
            onMouseEnter={e => (e.currentTarget.style.color = S.text)}
            onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>Главная</Link>
          <ChevronRight size={11} style={{ color: S.subtle }} />
          <Link to="/catalog" style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
            onMouseEnter={e => (e.currentTarget.style.color = S.text)}
            onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>Каталог</Link>
          {product.brand && (
            <>
              <ChevronRight size={11} style={{ color: S.subtle }} />
              <Link to={`/catalog?brand=${encodeURIComponent(product.brand)}`} style={{ color: S.muted, textDecoration: "none", transition: "color 0.15s" }}
                onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>{product.brand}</Link>
            </>
          )}
          <ChevronRight size={11} style={{ color: S.subtle }} />
          <span style={{ color: S.text, maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{product.name}</span>
        </nav>

        <div style={{ background: S.surface, borderRadius: 24, overflow: "hidden", border: `1px solid ${S.border}` }}>
          <style>{`@media(min-width:768px){.product-layout{grid-template-columns:1fr 1fr!important;}}`}</style>
          <div className="product-layout" style={{ display: "grid", gridTemplateColumns: "1fr" }}>

            {/* Images */}
            <div style={{ background: S.surface2, padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column", gap: 16, borderRight: `1px solid ${S.border}` }}>
              <div style={{
                aspectRatio: "1", borderRadius: 18, overflow: "hidden", background: S.bg,
                display: "flex", alignItems: "center", justifyContent: "center",
                border: `1px solid ${S.border}`,
              }}>
                {activeValidImg
                  ? <img src={activeValidImg} alt={product.name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 24 }}
                      onError={() => setImgErrors((s) => new Set(s).add(activeImg))} />
                  : <span style={{ fontSize: 80, fontWeight: 800, color: S.subtle, opacity: 0.2 }}>{product.brand?.[0] ?? "?"}</span>
                }
              </div>
              {validImages.length > 1 && (
                <div style={{ display: "flex", gap: 8, overflowX: "auto", paddingBottom: 4 }}>
                  {product.images.map((img, i) => !imgErrors.has(i) && (
                    <button key={i} onClick={() => setActiveImg(i)} style={{
                      flexShrink: 0, width: 56, height: 56, borderRadius: 12, overflow: "hidden",
                      background: S.bg, border: `2px solid ${i === activeImg ? S.accent : S.border}`,
                      cursor: "pointer", transition: "border-color 0.15s ease",
                      boxShadow: i === activeImg ? "0 0 16px rgba(201,169,110,0.25)" : "none",
                    }}>
                      <img src={img} alt="" style={{ width: "100%", height: "100%", objectFit: "contain", padding: 4 }}
                        onError={() => setImgErrors((s) => new Set(s).add(i))} />
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Info */}
            <div style={{ padding: "clamp(24px,4vw,48px)", display: "flex", flexDirection: "column" }}>
              <Link to="/catalog" style={{
                display: "inline-flex", alignItems: "center", gap: 4, fontSize: 12, color: S.muted,
                textDecoration: "none", marginBottom: 20, transition: "color 0.15s ease", width: "fit-content",
              }}
                onMouseEnter={e => (e.currentTarget.style.color = S.accent3)}
                onMouseLeave={e => (e.currentTarget.style.color = S.muted)}>
                <ChevronLeft size={14} /> Назад к каталогу
              </Link>

              {product.brand && (
                <div style={{ fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.12em", color: S.accent3, marginBottom: 10 }}>
                  {product.brand}
                </div>
              )}
              <h1 style={{ fontSize: "clamp(20px,2.5vw,28px)", fontWeight: 700, color: S.text, letterSpacing: "-0.035em", lineHeight: 1.2, marginBottom: 12 }}>
                {product.name}
              </h1>

              {product.volume && (
                <span style={{ display: "inline-block", fontSize: 12, color: S.muted, background: "rgba(255,255,255,0.06)", border: `1px solid ${S.border}`, borderRadius: 8, padding: "4px 12px", marginBottom: 16, alignSelf: "flex-start" }}>
                  {product.volume}
                </span>
              )}

              {(mpAvgRating > 0 || (product.rating && product.rating > 0)) ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
                  <div style={{ display: "flex" }}>
                    {[1,2,3,4,5].map((s) => {
                      const r = mpAvgRating || product.rating!;
                      return <Star key={s} size={14} style={{ color: s <= Math.round(r) ? "#C9A96E" : S.subtle, fill: s <= Math.round(r) ? "#C9A96E" : S.subtle }} />;
                    })}
                  </div>
                  <span style={{ fontSize: 13, fontWeight: 600, color: S.accent3 }}>
                    {(mpAvgRating || product.rating!).toFixed(1)}
                  </span>
                  <span style={{ fontSize: 12, color: S.muted }}>
                    {mpReviewCount > 0 ? `${mpReviewCount} на Ozon` : `${product.reviewCount} отзывов`}
                  </span>
                </div>
              ) : null}

              {/* Price */}
              <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
                <span style={{ fontSize: "clamp(28px,3vw,40px)", fontWeight: 700, color: S.text, letterSpacing: "-0.04em" }}>
                  {product.priceRub.toLocaleString("ru-RU")} ₽
                </span>
                {product.oldPriceRub && (
                  <>
                    <span style={{ fontSize: 16, color: S.subtle, textDecoration: "line-through" }}>{product.oldPriceRub.toLocaleString("ru-RU")} ₽</span>
                    <span style={{ fontSize: 12, background: "rgba(239,68,68,0.12)", color: "#f87171", fontWeight: 700, padding: "3px 8px", borderRadius: 8 }}>−{discount}%</span>
                  </>
                )}
              </div>

              {/* Stock + viewers */}
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8, marginBottom: 24 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, fontWeight: 500, color: product.inStock ? "#4ade80" : S.muted }}>
                  <div style={{ width: 8, height: 8, borderRadius: "50%", background: product.inStock ? "#4ade80" : S.subtle, boxShadow: product.inStock ? "0 0 8px #4ade80" : "none" }} />
                  {product.inStock ? `В наличии${product.stockQty > 0 ? ` · ${product.stockQty} шт.` : ""}` : "Нет в наличии"}
                </div>
                {product.inStock && viewers > 1 ? (
                  <div style={{ display: "flex", alignItems: "center", gap: 5, fontSize: 11, color: S.muted }}>
                    <Users size={11} style={{ color: S.accent3 }} />
                    <span>{viewers} смотрят</span>
                  </div>
                ) : null}
              </div>

              {!product.inStock && (
                <div style={{ marginBottom: 24 }}>
                  {alertSent ? (
                    <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "12px 16px", background: "rgba(74,222,128,0.08)", border: "1px solid rgba(74,222,128,0.2)", borderRadius: 14, fontSize: 13, color: "#4ade80" }}>
                      <Check size={15} /> Вы в списке ожидания! Уведомим на {alertEmail}
                    </div>
                  ) : (
                    <div style={{ display: "flex", gap: 8 }}>
                      <input
                        type="email"
                        value={alertEmail}
                        onChange={(e) => setAlertEmail(e.target.value)}
                        placeholder="Ваш email для уведомления"
                        style={{
                          flex: 1, padding: "12px 14px", background: S.surface2,
                          border: `1.5px solid ${S.border}`, borderRadius: 12, fontSize: 13,
                          color: S.text, fontFamily: "inherit", outline: "none",
                        }}
                        onFocus={e => (e.target.style.borderColor = "rgba(201,169,110,0.4)")}
                        onBlur={e => (e.target.style.borderColor = S.border)}
                        onKeyDown={(e) => { if (e.key === "Enter" && alertEmail) alertMutation.mutate(); }}
                      />
                      <button
                        onClick={() => alertMutation.mutate()}
                        disabled={!alertEmail || alertMutation.isPending}
                        style={{
                          display: "flex", alignItems: "center", gap: 6, padding: "12px 18px",
                          background: "rgba(201,169,110,0.1)", border: "1px solid rgba(201,169,110,0.25)",
                          borderRadius: 12, fontSize: 13, fontWeight: 600, color: S.accent3,
                          cursor: "pointer", flexShrink: 0, fontFamily: "inherit",
                          opacity: (!alertEmail || alertMutation.isPending) ? 0.5 : 1,
                        }}
                      >
                        <Bell size={14} /> Уведомить
                      </button>
                    </div>
                  )}
                  {alertMutation.error ? <div style={{ fontSize: 12, color: "#f87171", marginTop: 6 }}>{(alertMutation.error as Error).message}</div> : null}
                </div>
              )}

              {product.inStock && (
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
                  <div style={{ display: "flex", alignItems: "center", background: S.surface2, borderRadius: 14, border: `1px solid ${S.border}`, overflow: "hidden" }}>
                    <button onClick={() => setQty((q) => Math.max(1, q - 1))} style={{ padding: "12px 16px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                      onMouseLeave={e => (e.currentTarget.style.color = S.muted)}><Minus size={15} /></button>
                    <span style={{ padding: "12px 16px", fontSize: 15, fontWeight: 700, color: S.text, minWidth: 48, textAlign: "center" }}>{qty}</span>
                    <button onClick={() => setQty((q) => Math.min(product.stockQty || 99, q + 1))} style={{ padding: "12px 16px", background: "none", border: "none", color: S.muted, cursor: "pointer", transition: "color 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.color = S.text)}
                      onMouseLeave={e => (e.currentTarget.style.color = S.muted)}><Plus size={15} /></button>
                  </div>
                  <button onClick={handleAdd} className={added ? "" : "btn-primary"} style={{
                    flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                    padding: "14px 20px", borderRadius: 16, fontSize: 14, fontWeight: 700, border: "none", cursor: "pointer",
                    ...(added ? {
                      background: "rgba(74,222,128,0.15)", color: "#4ade80",
                      border: "1px solid rgba(74,222,128,0.25)", boxShadow: "none",
                    } : {}),
                    transition: "all 0.2s ease",
                  }}>
                    {added ? <><Check size={18} /> Добавлено!</> : <><ShoppingBag size={18} /> В корзину</>}
                  </button>
                </div>
              )}

              {/* Share popup */}
              <div ref={shareRef} style={{ position: "relative", marginBottom: 20, display: "inline-block" }}>
                <button
                  onClick={() => setShareOpen((o) => !o)}
                  style={{
                    display: "inline-flex", alignItems: "center", gap: 8,
                    padding: "9px 18px", borderRadius: 2, cursor: "pointer",
                    background: shareOpen ? "rgba(201,162,94,0.07)" : "transparent",
                    border: `1px solid ${shareOpen ? "rgba(201,162,94,0.5)" : S.border}`,
                    color: shareOpen ? S.accent3 : S.muted, fontSize: 12, fontFamily: "inherit",
                    letterSpacing: "0.12em", textTransform: "uppercase",
                    transition: "border-color 0.25s, color 0.25s, background 0.25s",
                  }}
                  onMouseEnter={e => { if (!shareOpen) { e.currentTarget.style.borderColor = "rgba(201,162,94,0.45)"; e.currentTarget.style.color = S.text; } }}
                  onMouseLeave={e => { if (!shareOpen) { e.currentTarget.style.borderColor = S.border; e.currentTarget.style.color = S.muted; } }}
                >
                  <Share2 size={13} strokeWidth={1.6} />
                  Поделиться
                </button>

                {shareOpen && (
                  <div style={{
                    position: "absolute", bottom: "calc(100% + 10px)", left: 0, zIndex: 60,
                    background: "#1a1815", borderRadius: 3, border: `1px solid ${S.borderMd}`,
                    boxShadow: "0 20px 60px rgba(0,0,0,0.8)", padding: "6px", minWidth: 220,
                  }}>
                    {/* Telegram */}
                    <a
                      href={`https://t.me/share/url?url=${encodeURIComponent(pageUrl)}&text=${encodeURIComponent(shareText)}`}
                      target="_blank" rel="noopener noreferrer"
                      onClick={() => setShareOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 2, textDecoration: "none", color: S.text, fontSize: 13, transition: "background 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.05)")}
                      onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                    >
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="12" fill="#26A5E4"/><path d="M5.5 11.8l11-4.3c.5-.2.9.1.8.6l-1.9 8.8c-.1.6-.5.7-1 .5l-2.8-2-1.3 1.3c-.1.1-.3.2-.5.2l.2-2.8 5.1-4.6c.2-.2 0-.3-.3-.1l-6.4 4-2.7-.9c-.6-.2-.6-.6.1-.9z" fill="white"/></svg>
                      Telegram
                    </a>

                    {/* VKontakte */}
                    <a
                      href={`https://vk.com/share.php?url=${encodeURIComponent(pageUrl)}&title=${encodeURIComponent(shareText)}`}
                      target="_blank" rel="noopener noreferrer"
                      onClick={() => setShareOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 2, textDecoration: "none", color: S.text, fontSize: 13, transition: "background 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.05)")}
                      onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                    >
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><rect width="24" height="24" rx="4" fill="#4C75A3"/><path d="M12.7 16.5h1.2s.3 0 .5-.3l.2-.5s.2-1.5.9-1.7c.6-.2 1.4 1.4 2.2 2 .6.4 1.1.3 1.1.3l2.2-.1s1.2-.1.6-1c0 0-.4-.8-2.1-2.3-1.7-1.5-1.5-1.3.6-4 1.3-1.8 1.8-2.9 1.7-3.3-.1-.4-1.1-.3-1.1-.3h-2.5s-.2 0-.3.1l-.2.3s-.5 1.4-1.2 2.6c-1.4 2.4-2 2.5-2.2 2.4-.5-.4-.4-1.4-.4-2.2 0-2.4.4-3.4-.7-3.6-.4-.1-.6-.1-1.6-.1-1.3 0-2.3.1-2.9.4-.4.2-.7.6-.5.6.2 0 .8.1 1 .5.3.5.3 1.7.3 1.7s.2 2.8-.4 3.2c-.4.3-1-.4-2.2-2.5-.7-1.2-1.2-2.5-1.2-2.5l-.2-.3s-.1-.1-.3-.2H4.6s-.3 0-.4.1c-.1.2 0 .5 0 .5s2 4.7 4.2 7.1c2 2.2 4.3 2 4.3 2z" fill="white"/></svg>
                      ВКонтакте
                    </a>

                    {/* WhatsApp */}
                    <a
                      href={`https://wa.me/?text=${encodeURIComponent(shareText + " " + pageUrl)}`}
                      target="_blank" rel="noopener noreferrer"
                      onClick={() => setShareOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 2, textDecoration: "none", color: S.text, fontSize: 13, transition: "background 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.05)")}
                      onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                    >
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="12" fill="#25D366"/><path d="M17 14.4c-.3-.1-1.7-.9-2-.9-.2 0-.4.1-.6.4l-.7.8c-.1.2-.3.2-.6.1-1.4-.6-2.4-1.5-3.1-2.7-.2-.4-.1-.6.1-.8l.5-.6c.1-.2.2-.4.1-.6-.1-.2-.6-1.6-.9-2.2-.3-.5-.5-.5-.7-.5-.5 0-.9.1-1.2.4-1 1.1-.9 2.5.3 4 1.2 1.5 3.4 3 5.9 3.7.4.1.8.2 1.2.2.7 0 1.4-.2 1.9-.7.4-.4.5-.9.4-1.3-.1-.2-.3-.3-.6-.3z" fill="white"/></svg>
                      WhatsApp
                    </a>

                    {/* Odnoklassniki */}
                    <a
                      href={`https://connect.ok.ru/offer?url=${encodeURIComponent(pageUrl)}&title=${encodeURIComponent(shareText)}`}
                      target="_blank" rel="noopener noreferrer"
                      onClick={() => setShareOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 2, textDecoration: "none", color: S.text, fontSize: 13, transition: "background 0.15s" }}
                      onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.05)")}
                      onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                    >
                      <svg width="18" height="18" viewBox="0 0 24 24" fill="none"><circle cx="12" cy="12" r="12" fill="#ED812B"/><path d="M12 6.5a2.5 2.5 0 100 5 2.5 2.5 0 000-5zm0 3.5a1 1 0 110-2 1 1 0 010 2zm4 3.2c-.7.5-1.5.8-2.4.9l2.1 2.1c.3.3.3.8 0 1.1-.3.3-.8.3-1.1 0L12 14.8l-2.6 2.5c-.3.3-.8.3-1.1 0-.3-.3-.3-.8 0-1.1l2.1-2.1c-.9-.1-1.7-.4-2.4-.9-.4-.3-.5-.8-.2-1.2.3-.4.8-.5 1.2-.2.9.6 2 1 3.1 1s2.1-.3 3.1-1c.4-.3.9-.2 1.2.2.2.4.1.9-.3 1.2z" fill="white"/></svg>
                      Одноклассники
                    </a>

                    <div style={{ height: 1, background: S.border, margin: "4px 0" }} />

                    {/* Copy link */}
                    <button
                      onClick={() => { copyShareLink(); setShareOpen(false); }}
                      style={{ display: "flex", alignItems: "center", gap: 12, padding: "10px 12px", borderRadius: 2, background: "transparent", border: "none", cursor: "pointer", color: shareCopied ? "#4ade80" : S.text, fontSize: 13, width: "100%", textAlign: "left", transition: "background 0.15s, color 0.2s", fontFamily: "inherit" }}
                      onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.05)")}
                      onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                    >
                      <Link2 size={18} style={{ color: shareCopied ? "#4ade80" : S.muted, flexShrink: 0, transition: "color 0.2s" }} />
                      {shareCopied ? "Ссылка скопирована!" : "Копировать ссылку"}
                    </button>
                  </div>
                )}
              </div>

              {/* Perks */}
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 10, marginBottom: 24 }}>
                {[
                  { icon: Truck,     label: "Доставка Ozon" },
                  { icon: Shield,    label: "100% оригинал" },
                  { icon: RefreshCw, label: "Возврат 14 дней" },
                ].map(({ icon: Icon, label }) => (
                  <div key={label} style={{
                    display: "flex", flexDirection: "column", alignItems: "center", gap: 8,
                    background: S.surface2, borderRadius: 14, padding: "12px 8px", textAlign: "center",
                    border: `1px solid ${S.border}`,
                  }}>
                    <Icon size={16} style={{ color: S.accent3 }} />
                    <span style={{ fontSize: 10.5, color: S.muted, fontWeight: 500, lineHeight: 1.3 }}>{label}</span>
                  </div>
                ))}
              </div>

              {product.description && (
                <div style={{ borderTop: `1px solid ${S.border}`, paddingTop: 20 }}>
                  <h3 style={{ fontSize: 13, fontWeight: 600, color: S.text, marginBottom: 10 }}>Описание</h3>
                  <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.7 }}>{product.description}</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Reviews */}
        <div style={{ marginTop: 32 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20, gap: 12, flexWrap: "wrap" }}>
            <div>
              <h2 style={{ fontSize: 16, fontWeight: 700, color: S.text, letterSpacing: "-0.03em", marginBottom: 4 }}>
                Отзывы{(productReviews.length + mpReviews.length) > 0 && (
                  <span style={{ fontSize: 13, color: S.muted, fontWeight: 400 }}> ({productReviews.length + mpReviews.length})</span>
                )}
              </h2>
              {/* Show Ozon rating prominently */}
              {mpAvgRating > 0 && (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                  <div style={{ display: "flex", gap: 2 }}>
                    {[1,2,3,4,5].map((s) => <Star key={s} size={14} style={{ color: s <= Math.round(mpAvgRating) ? "#C9A96E" : S.subtle, fill: s <= Math.round(mpAvgRating) ? "#C9A96E" : S.subtle }} />)}
                  </div>
                  <span style={{ fontSize: 13, fontWeight: 600, color: S.accent3 }}>{mpAvgRating.toFixed(1)}</span>
                  <span style={{ fontSize: 12, color: S.muted }}>{mpReviewCount} {mpReviewCount === 1 ? "отзыв" : mpReviewCount >= 2 && mpReviewCount <= 4 ? "отзыва" : "отзывов"} на Ozon</span>
                </div>
              )}
              {productReviews.length > 0 && mpAvgRating === 0 && (() => {
                const avg = productReviews.reduce((s, r) => s + r.rating, 0) / productReviews.length;
                return (
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    {[1,2,3,4,5].map((s) => <Star key={s} size={13} style={{ color: s <= Math.round(avg) ? "#fbbf24" : S.subtle, fill: s <= Math.round(avg) ? "#fbbf24" : S.subtle }} />)}
                    <span style={{ fontSize: 12, color: S.muted }}>{avg.toFixed(1)} средняя оценка</span>
                  </div>
                );
              })()}
            </div>
            {customer && !reviewSent && (
              <button onClick={() => setReviewOpen((o) => !o)} style={{
                display: "flex", alignItems: "center", gap: 6, padding: "9px 16px",
                background: reviewOpen ? "rgba(201,169,110,0.12)" : "rgba(255,255,255,0.06)",
                border: `1px solid ${reviewOpen ? "rgba(201,169,110,0.3)" : S.border}`,
                borderRadius: 12, fontSize: 13, fontWeight: 500, color: reviewOpen ? S.accent3 : S.muted,
                cursor: "pointer", transition: "all 0.15s", fontFamily: "inherit",
              }}>
                <MessageSquare size={14} /> Написать отзыв
              </button>
            )}
          </div>

          {/* Write review form */}
          {reviewOpen && customer && !reviewSent && (
            <div style={{ background: S.surface, borderRadius: 18, padding: 24, border: `1px solid ${S.border}`, marginBottom: 20 }}>
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 12, color: S.muted, marginBottom: 8 }}>Ваша оценка</div>
                <div style={{ display: "flex", gap: 6 }}>
                  {[1,2,3,4,5].map((s) => (
                    <button key={s} type="button"
                      onClick={() => setReviewRating(s)}
                      onMouseEnter={() => setReviewHover(s)}
                      onMouseLeave={() => setReviewHover(0)}
                      style={{ background: "none", border: "none", cursor: "pointer", padding: 2 }}>
                      <Star size={24} style={{
                        color: s <= (reviewHover || reviewRating) ? "#fbbf24" : S.subtle,
                        fill: s <= (reviewHover || reviewRating) ? "#fbbf24" : S.subtle,
                        transition: "color 0.1s",
                      }} />
                    </button>
                  ))}
                </div>
              </div>
              <textarea
                value={reviewText}
                onChange={(e) => setReviewText(e.target.value)}
                placeholder="Расскажите о товаре — запах, стойкость, упаковка..."
                rows={4}
                style={{
                  width: "100%", padding: "12px 14px", background: S.surface2,
                  border: `1.5px solid ${S.border}`, borderRadius: 12, fontSize: 13,
                  color: S.text, fontFamily: "inherit", outline: "none", resize: "vertical",
                  lineHeight: 1.6, boxSizing: "border-box",
                }}
                onFocus={e => (e.target.style.borderColor = "rgba(201,169,110,0.4)")}
                onBlur={e => (e.target.style.borderColor = S.border)}
              />
              {reviewMutation.error && (
                <div style={{ fontSize: 12, color: "#f87171", marginTop: 6 }}>{(reviewMutation.error as Error).message}</div>
              )}
              <div style={{ display: "flex", gap: 10, marginTop: 12 }}>
                <button onClick={() => reviewMutation.mutate()} disabled={reviewText.trim().length < 10 || reviewMutation.isPending}
                  style={{
                    flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                    padding: "11px 20px", background: S.accent, color: "#0E0D0B",
                    border: "none", borderRadius: 12, fontSize: 13, fontWeight: 700,
                    cursor: "pointer", opacity: (reviewText.trim().length < 10 || reviewMutation.isPending) ? 0.5 : 1,
                    fontFamily: "inherit",
                  }}>
                  <Send size={14} /> {reviewMutation.isPending ? "Отправка…" : "Опубликовать отзыв"}
                </button>
                <button onClick={() => setReviewOpen(false)} style={{
                  padding: "11px 14px", background: "none", border: `1px solid ${S.border}`,
                  borderRadius: 12, color: S.muted, cursor: "pointer",
                }}>
                  <X size={14} />
                </button>
              </div>
            </div>
          )}

          {reviewSent && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "12px 16px", background: "rgba(74,222,128,0.08)", border: "1px solid rgba(74,222,128,0.2)", borderRadius: 14, fontSize: 13, color: "#4ade80", marginBottom: 20 }}>
              <Check size={15} /> Спасибо! Ваш отзыв отправлен на проверку.
            </div>
          )}

          {productReviews.length === 0 && mpReviews.length === 0 && !reviewOpen && (
            <div style={{ textAlign: "center", padding: "40px 24px", background: S.surface, borderRadius: 18, border: `1px solid ${S.border}` }}>
              <ThumbsUp size={32} style={{ color: S.subtle, marginBottom: 12 }} />
              <p style={{ fontSize: 14, color: S.muted, marginBottom: 6 }}>Отзывов пока нет</p>
              <p style={{ fontSize: 12, color: S.subtle }}>
                {customer ? "Будьте первым — поделитесь впечатлениями!" : "Войдите, чтобы оставить отзыв"}
              </p>
            </div>
          )}

          {/* Marketplace reviews (Ozon) */}
          {mpReviews.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 12 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: S.muted }}>С маркетплейса</span>
                <div style={{ flex: 1, height: 1, background: S.border }} />
                <span style={{ fontSize: 10, color: S.subtle }}>Ozon</span>
              </div>
              {mpReviews.map((r) => (
                <div key={r.id} style={{ background: S.surface, borderRadius: 16, padding: "16px 20px", border: `1px solid ${S.border}` }}>
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 10 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      <div style={{ width: 36, height: 36, borderRadius: "50%", background: "rgba(0,143,255,0.1)", border: "1px solid rgba(0,143,255,0.2)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: "#60b3ff" }}>{(r.author || "П")[0].toUpperCase()}</span>
                      </div>
                      <div>
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span style={{ fontSize: 13, fontWeight: 600, color: S.text }}>{r.author}</span>
                          <span style={{ fontSize: 9, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "#60b3ff", background: "rgba(0,143,255,0.1)", borderRadius: 4, padding: "1px 5px" }}>Ozon</span>
                        </div>
                        <div style={{ fontSize: 11, color: S.muted }}>{new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}</div>
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 2, flexShrink: 0 }}>
                      {[1,2,3,4,5].map((s) => <Star key={s} size={12} style={{ color: s <= r.rating ? "#C9A96E" : S.subtle, fill: s <= r.rating ? "#C9A96E" : S.subtle }} />)}
                    </div>
                  </div>
                  <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.65, margin: 0 }}>{r.text}</p>
                  {r.advantages && (
                    <div style={{ marginTop: 8, display: "flex", gap: 6, alignItems: "flex-start" }}>
                      <span style={{ fontSize: 10, color: "#4ade80", fontWeight: 600, flexShrink: 0, marginTop: 1 }}>+</span>
                      <span style={{ fontSize: 12, color: "rgba(74,222,128,0.8)", lineHeight: 1.5 }}>{r.advantages}</span>
                    </div>
                  )}
                  {r.disadvantages && (
                    <div style={{ marginTop: 4, display: "flex", gap: 6, alignItems: "flex-start" }}>
                      <span style={{ fontSize: 10, color: "#f87171", fontWeight: 600, flexShrink: 0, marginTop: 1 }}>−</span>
                      <span style={{ fontSize: 12, color: "rgba(248,113,113,0.8)", lineHeight: 1.5 }}>{r.disadvantages}</span>
                    </div>
                  )}
                  {r.photos && r.photos.length > 0 && (
                    <div style={{ display: "flex", gap: 6, marginTop: 10, flexWrap: "wrap" }}>
                      {r.photos.slice(0, 4).map((url, i) => (
                        <img key={i} src={url} alt="" loading="lazy" style={{ width: 64, height: 64, objectFit: "cover", borderRadius: 8, border: `1px solid ${S.border}` }} />
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Our own site reviews */}
          {productReviews.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              {mpReviews.length > 0 && (
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                  <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.1em", textTransform: "uppercase", color: S.muted }}>На нашем сайте</span>
                  <div style={{ flex: 1, height: 1, background: S.border }} />
                </div>
              )}
              {productReviews.map((r) => (
                <div key={r.id} style={{ background: S.surface, borderRadius: 16, padding: "16px 20px", border: `1px solid ${S.border}` }}>
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 10 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                      <div style={{ width: 36, height: 36, borderRadius: "50%", background: "rgba(201,169,110,0.12)", border: `1px solid rgba(201,169,110,0.2)`, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                        <span style={{ fontSize: 13, fontWeight: 700, color: S.accent3 }}>{(r.author || "П")[0].toUpperCase()}</span>
                      </div>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: S.text }}>{r.author || "Покупатель"}</div>
                        <div style={{ fontSize: 11, color: S.muted }}>{new Date(r.createdAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}</div>
                      </div>
                    </div>
                    <div style={{ display: "flex", gap: 2, flexShrink: 0 }}>
                      {[1,2,3,4,5].map((s) => <Star key={s} size={12} style={{ color: s <= r.rating ? "#fbbf24" : S.subtle, fill: s <= r.rating ? "#fbbf24" : S.subtle }} />)}
                    </div>
                  </div>
                  <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.65, margin: 0 }}>{r.text}</p>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Related products */}
        {relatedItems.length > 0 && (
          <div style={{ marginTop: 32 }}>
            <h2 style={{ fontSize: 16, fontWeight: 700, color: S.text, letterSpacing: "-0.03em", marginBottom: 16 }}>
              С этим берут
            </h2>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12 }}>
              {relatedItems.map((p) => (
                <Link key={p.offerId} to={`/product/${encodeURIComponent(p.offerId)}`} style={{
                  display: "flex", gap: 12, padding: 14, textDecoration: "none",
                  background: S.surface, borderRadius: 16, border: `1px solid ${S.border}`,
                  transition: "border-color 0.15s",
                }}
                  onMouseEnter={e => (e.currentTarget.style.borderColor = "rgba(201,169,110,0.25)")}
                  onMouseLeave={e => (e.currentTarget.style.borderColor = S.border)}>
                  <div style={{ width: 52, height: 52, borderRadius: 10, overflow: "hidden", flexShrink: 0, background: S.surface2, display: "flex", alignItems: "center", justifyContent: "center" }}>
                    {p.images[0]
                      ? <img src={p.images[0]} alt={p.name} style={{ width: "100%", height: "100%", objectFit: "contain", padding: 4 }} />
                      : <span style={{ fontSize: 18, fontWeight: 700, color: S.subtle }}>{p.brand?.[0] ?? "?"}</span>}
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 11, color: S.accent3, fontWeight: 600, marginBottom: 2 }}>{p.brand}</div>
                    <div style={{ fontSize: 12, color: S.text, lineHeight: 1.3, overflow: "hidden", textOverflow: "ellipsis", display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical" }}>{p.name}</div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: S.text, marginTop: 4 }}>{p.priceRub.toLocaleString("ru-RU")} ₽</div>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Add-to-cart popup */}
      {cartPopup && product && (
        <div style={{
          position: "fixed", bottom: 0, left: 0, right: 0, zIndex: 100,
          padding: "16px clamp(16px,4vw,32px)",
          background: S.surface, borderTop: `1px solid ${S.border}`,
          boxShadow: "0 -8px 32px rgba(0,0,0,0.4)",
          animation: "slideUp 0.25s ease",
        }}>
          <style>{`@keyframes slideUp{from{transform:translateY(100%)}to{transform:translateY(0)}}`}</style>
          <div style={{ maxWidth: 680, margin: "0 auto", display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1, minWidth: 0 }}>
              <div style={{ width: 40, height: 40, borderRadius: 10, overflow: "hidden", flexShrink: 0, background: S.surface2 }}>
                {product.images[0] && <img src={product.images[0]} alt="" style={{ width: "100%", height: "100%", objectFit: "contain", padding: 3 }} />}
              </div>
              <div style={{ minWidth: 0 }}>
                <div style={{ fontSize: 12, color: "#4ade80", fontWeight: 600 }}>Добавлено в корзину</div>
                <div style={{ fontSize: 12, color: S.muted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{product.name}</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 10, flexShrink: 0 }}>
              <Link to="/cart" className="btn-primary" style={{ fontSize: 13, padding: "10px 18px" }}>
                Перейти в корзину →
              </Link>
              <button onClick={() => setCartPopup(false)} style={{ padding: "10px 12px", background: "none", border: `1px solid ${S.border}`, borderRadius: 12, color: S.muted, cursor: "pointer" }}>
                <X size={14} />
              </button>
            </div>
          </div>
          {relatedItems.length > 0 && (
            <div style={{ maxWidth: 680, margin: "12px auto 0", display: "flex", gap: 8, overflowX: "auto" }}>
              <span style={{ fontSize: 11, color: S.muted, flexShrink: 0, lineHeight: "32px" }}>С этим берут:</span>
              {relatedItems.map((p) => (
                <Link key={p.offerId} to={`/product/${encodeURIComponent(p.offerId)}`} onClick={() => setCartPopup(false)} style={{
                  flexShrink: 0, display: "flex", alignItems: "center", gap: 8, padding: "6px 10px",
                  background: S.surface2, borderRadius: 10, border: `1px solid ${S.border}`,
                  textDecoration: "none", fontSize: 12, color: S.text, whiteSpace: "nowrap",
                }}>
                  {p.images[0] && <img src={p.images[0]} alt="" style={{ width: 24, height: 24, objectFit: "contain", borderRadius: 4 }} />}
                  {p.priceRub.toLocaleString("ru-RU")} ₽
                </Link>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
