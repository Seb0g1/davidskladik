import { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { ChevronLeft, ChevronRight, Copy, Check } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import type { ShopBanner } from "../types";

const HOLIDAY_PRESETS_CLIENT: Record<string, { emoji: string; name: string }> = {
  halloween:    { emoji: "🎃", name: "Хэллоуин" },
  black_friday: { emoji: "🛒", name: "Чёрная пятница" },
  new_year:     { emoji: "🎄", name: "Новый год" },
  valentine:    { emoji: "💝", name: "День влюблённых" },
  defender_day: { emoji: "🛡️", name: "23 февраля" },
  womens_day:   { emoji: "🌷", name: "8 марта" },
  may_day:      { emoji: "🌸", name: "Майские" },
  "11_11":      { emoji: "🔥", name: "11.11" },
};

const FALLBACK_BANNERS: ShopBanner[] = [
  {
    id: "fallback-1",
    imageUrl: "",
    title: "Magic Vibes",
    subtitle: "Парфюмерия мирового класса с доставкой по России",
    linkUrl: "/catalog",
    linkText: "Смотреть каталог",
    active: true,
    order: 0,
  },
];

function CountdownTimer({ endDate }: { endDate: string }) {
  const getTimeLeft = () => {
    const diff = new Date(endDate).getTime() - Date.now();
    if (diff <= 0) return null;
    const d = Math.floor(diff / 86400000);
    const h = Math.floor((diff % 86400000) / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    const s = Math.floor((diff % 60000) / 1000);
    return { d, h, m, s };
  };

  const [t, setT] = useState(getTimeLeft());
  useEffect(() => {
    const id = setInterval(() => setT(getTimeLeft()), 1000);
    return () => clearInterval(id);
  }, [endDate]);

  if (!t) return null;

  const pad = (n: number) => String(n).padStart(2, "0");
  const parts = t.d > 0
    ? [{ val: t.d, label: "дн" }, { val: t.h, label: "ч" }, { val: t.m, label: "мин" }]
    : [{ val: t.h, label: "ч" }, { val: t.m, label: "мин" }, { val: t.s, label: "сек" }];

  return (
    <div style={{
      position: "absolute", bottom: 18, right: 18, zIndex: 10,
      display: "flex", alignItems: "center", gap: 6,
      background: "rgba(0,0,0,0.6)", backdropFilter: "blur(8px)",
      border: "1px solid rgba(201,162,94,0.35)", borderRadius: 4,
      padding: "8px 14px",
    }}>
      <span style={{ fontSize: 9, letterSpacing: "0.18em", textTransform: "uppercase", color: "#c9a25e", marginRight: 4 }}>Осталось</span>
      {parts.map(({ val, label }, i) => (
        <span key={label} style={{ display: "flex", alignItems: "baseline", gap: 2 }}>
          {i > 0 && <span style={{ color: "rgba(201,162,94,0.4)", marginRight: 2 }}>:</span>}
          <span style={{ fontFamily: "Georgia,serif", fontStyle: "italic", fontSize: 22, color: "#f2ede6", lineHeight: 1, minWidth: 26, textAlign: "center" }}>{pad(val)}</span>
          <span style={{ fontSize: 9, color: "#7d7a73", letterSpacing: "0.1em" }}>{label}</span>
        </span>
      ))}
    </div>
  );
}

function PromoCodeBadge({ code, accent = "#c9a25e" }: { code: string; accent?: string }) {
  const [copied, setCopied] = useState(false);
  function handleCopy() {
    navigator.clipboard.writeText(code).catch(() => {});
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }
  return (
    <div
      onClick={handleCopy}
      style={{
        display: "inline-flex", alignItems: "center", gap: 8, cursor: "pointer",
        background: "rgba(0,0,0,0.45)", backdropFilter: "blur(10px)",
        border: `1px solid ${accent}55`, borderRadius: 4, padding: "7px 14px",
        transition: "border-color 0.2s",
        userSelect: "none",
      }}
      title="Нажмите, чтобы скопировать"
    >
      <span style={{ fontSize: 9, letterSpacing: "0.2em", color: accent, textTransform: "uppercase" }}>Промокод</span>
      <span style={{ fontFamily: "monospace", fontSize: 15, fontWeight: 700, color: "#f2ede6", letterSpacing: "0.12em" }}>{code}</span>
      {copied
        ? <Check size={12} color="#4ade80" />
        : <Copy size={12} color={accent} style={{ opacity: 0.7 }} />}
    </div>
  );
}

// ── Holiday banner visual themes ──────────────────────────────────────────────
const HOLIDAY_THEMES: Record<string, {
  bg: string; aura: string; accent: string; particleColor: string; textGlow: string;
}> = {
  halloween: {
    bg: "radial-gradient(ellipse 100% 100% at 50% 0%, #1a0533 0%, #0f0318 45%, #1a0800 100%)",
    aura: "radial-gradient(ellipse 60% 55% at 50% 40%, rgba(255,107,0,0.18) 0%, transparent 70%)",
    accent: "#ff6b00", particleColor: "rgba(160,60,220,0.6)", textGlow: "0 0 40px rgba(255,107,0,0.4)",
  },
  black_friday: {
    bg: "linear-gradient(135deg, #050505 0%, #0d0d0d 60%, #0a0800 100%)",
    aura: "radial-gradient(ellipse 70% 60% at 50% 50%, rgba(201,162,94,0.12) 0%, transparent 70%)",
    accent: "#c9a25e", particleColor: "rgba(201,162,94,0.5)", textGlow: "0 0 40px rgba(201,162,94,0.35)",
  },
  new_year: {
    bg: "radial-gradient(ellipse 100% 120% at 50% 0%, #061528 0%, #051208 45%, #0a0a18 100%)",
    aura: "radial-gradient(ellipse 65% 55% at 50% 40%, rgba(100,200,120,0.15) 0%, transparent 70%)",
    accent: "#4ade80", particleColor: "rgba(201,162,94,0.7)", textGlow: "0 0 40px rgba(100,200,120,0.3)",
  },
  valentine: {
    bg: "radial-gradient(ellipse 100% 110% at 50% 0%, #2d0018 0%, #1a0008 50%, #1a0012 100%)",
    aura: "radial-gradient(ellipse 60% 55% at 50% 40%, rgba(220,50,100,0.2) 0%, transparent 70%)",
    accent: "#e8547a", particleColor: "rgba(220,100,140,0.5)", textGlow: "0 0 40px rgba(220,50,100,0.4)",
  },
  defender_day: {
    bg: "linear-gradient(150deg, #070e1c 0%, #101c0a 50%, #080f18 100%)",
    aura: "radial-gradient(ellipse 60% 55% at 50% 40%, rgba(50,100,200,0.15) 0%, transparent 70%)",
    accent: "#6b9de8", particleColor: "rgba(100,160,255,0.4)", textGlow: "0 0 40px rgba(50,100,200,0.35)",
  },
  womens_day: {
    bg: "radial-gradient(ellipse 100% 110% at 50% 0%, #220014 0%, #160008 50%, #1a0010 100%)",
    aura: "radial-gradient(ellipse 65% 55% at 50% 40%, rgba(220,90,160,0.2) 0%, transparent 70%)",
    accent: "#e87ab8", particleColor: "rgba(230,140,190,0.55)", textGlow: "0 0 40px rgba(220,90,160,0.4)",
  },
  may_day: {
    bg: "radial-gradient(ellipse 100% 110% at 50% 0%, #061508 0%, #0a1e06 50%, #080f08 100%)",
    aura: "radial-gradient(ellipse 65% 55% at 50% 40%, rgba(100,200,80,0.18) 0%, transparent 70%)",
    accent: "#7ade6a", particleColor: "rgba(120,220,100,0.5)", textGlow: "0 0 40px rgba(100,200,80,0.3)",
  },
  "11_11": {
    bg: "linear-gradient(135deg, #0a0000 0%, #1a0000 50%, #0a0500 100%)",
    aura: "radial-gradient(ellipse 65% 55% at 50% 40%, rgba(255,60,30,0.18) 0%, transparent 70%)",
    accent: "#ff4520", particleColor: "rgba(255,80,40,0.55)", textGlow: "0 0 40px rgba(255,60,30,0.35)",
  },
};

function HolidayBannerArt({ banner }: { banner: ShopBanner }) {
  const key = banner.holidayKey || "";
  const theme = HOLIDAY_THEMES[key] || HOLIDAY_THEMES.black_friday;

  return (
    <div style={{
      width: "100%", height: "100%",
      background: theme.bg,
      display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
      textAlign: "center", padding: "24px 32px", position: "relative", overflow: "hidden",
    }}>
      {/* Aura glow */}
      <div style={{ position: "absolute", inset: 0, background: theme.aura, pointerEvents: "none" }} />

      {/* Decorative border shimmer */}
      <div style={{
        position: "absolute", inset: 0, pointerEvents: "none",
        boxShadow: `inset 0 0 60px rgba(0,0,0,0.6), inset 0 1px 0 rgba(255,255,255,0.05)`,
      }} />

      {/* Content */}
      <div style={{ position: "relative", zIndex: 2, maxWidth: 560 }}>
        {banner.title && (
          <h2 style={{
            fontFamily: "'Cormorant Garamond', Georgia, serif",
            fontStyle: "italic", fontWeight: 300,
            fontSize: "clamp(28px, 5vw, 56px)",
            color: "#f8f2ea", lineHeight: 1.05, marginBottom: 10,
            textShadow: theme.textGlow,
          }}>
            {banner.title}
          </h2>
        )}
        {banner.subtitle && (
          <p style={{
            fontSize: "clamp(13px, 1.6vw, 16px)", color: "rgba(242,237,230,0.65)",
            lineHeight: 1.6, marginBottom: 20, maxWidth: 42 + "ch", margin: "0 auto 20px",
          }}>
            {banner.subtitle}
          </p>
        )}

        {/* Promo code badge */}
        {banner.promoCode && (
          <div style={{ marginBottom: 20 }}>
            <PromoCodeBadge code={banner.promoCode} accent={theme.accent} />
          </div>
        )}

        {banner.linkUrl && (
          <div>
            <Link
              to={banner.linkUrl}
              style={{
                display: "inline-block",
                background: theme.accent,
                color: "#09090b",
                fontWeight: 600,
                fontSize: 13,
                letterSpacing: "0.06em",
                padding: "11px 28px",
                borderRadius: 3,
                textDecoration: "none",
                transition: "opacity 0.2s",
              }}
            >
              {banner.linkText || "Смотреть"}
            </Link>
          </div>
        )}
      </div>
    </div>
  );
}

function GradientBanner({ banner }: { banner: ShopBanner }) {
  if (banner.holidayKey) return <HolidayBannerArt banner={banner} />;
  return (
    <div style={{
      width: "100%", height: "100%",
      background: "linear-gradient(135deg, #1a0a28 0%, #0f1428 50%, #1a0818 100%)",
      display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
      textAlign: "center", padding: "24px 32px",
    }}>
      {banner.title && (
        <h2 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic", fontSize: "clamp(26px,5vw,52px)",
          color: "#f2ede6", marginBottom: 10,
        }}>
          {banner.title}
        </h2>
      )}
      {banner.subtitle && (
        <p style={{ fontSize: 15, color: "rgba(242,237,230,0.6)", marginBottom: 20 }}>{banner.subtitle}</p>
      )}
      {banner.promoCode && (
        <div style={{ marginBottom: 20 }}>
          <PromoCodeBadge code={banner.promoCode} />
        </div>
      )}
      {banner.linkUrl && (
        <Link
          to={banner.linkUrl}
          style={{
            background: "#c9a25e", color: "#09090b", fontWeight: 600, fontSize: 13,
            padding: "10px 26px", borderRadius: 3, textDecoration: "none",
          }}
        >
          {banner.linkText || "Подробнее"}
        </Link>
      )}
    </div>
  );
}

export default function BannerSlider() {
  const { data } = useQuery({
    queryKey: ["shop-banners"],
    queryFn: () => api.banners(),
    staleTime: 5 * 60 * 1000,
  });

  const banners = (data ?? FALLBACK_BANNERS).filter((b) => b.active);
  const [idx, setIdx] = useState(0);

  const prev = useCallback(() => setIdx((i) => (i - 1 + banners.length) % banners.length), [banners.length]);
  const next = useCallback(() => setIdx((i) => (i + 1) % banners.length), [banners.length]);

  useEffect(() => {
    if (banners.length <= 1) return;
    const t = setInterval(next, 5000);
    return () => clearInterval(t);
  }, [banners.length, next]);

  if (!banners.length) return null;

  const banner = banners[idx];

  const holidayTheme = banner.holidayKey ? HOLIDAY_THEMES[banner.holidayKey] : null;

  return (
    <div style={{ position: "relative", width: "100%", height: "clamp(200px, 40vw, 480px)", overflow: "hidden", borderRadius: 16, background: "#0d0d0f" }}>
      {/* Image or themed gradient */}
      <div key={idx} className="absolute inset-0 slide-in">
        {banner.imageUrl ? (
          <img src={banner.imageUrl} alt={banner.title ?? ""} style={{ width: "100%", height: "100%", objectFit: "cover" }} />
        ) : (
          <GradientBanner banner={banner} />
        )}
      </div>

      {/* Holiday badge — top left */}
      {banner.holidayKey && (
        <div style={{
          position: "absolute", top: 16, left: 16, zIndex: 10,
          background: "rgba(0,0,0,0.5)", backdropFilter: "blur(8px)",
          border: `1px solid ${holidayTheme?.accent ?? "#c9a25e"}44`,
          borderRadius: 3, padding: "5px 12px",
          display: "flex", alignItems: "center", gap: 7,
        }}>
          <span style={{ fontSize: 13 }}>{HOLIDAY_PRESETS_CLIENT[banner.holidayKey]?.emoji ?? "✦"}</span>
          <span style={{ fontSize: 10, letterSpacing: "0.14em", color: holidayTheme?.accent ?? "#c9a25e", textTransform: "uppercase" }}>
            {HOLIDAY_PRESETS_CLIENT[banner.holidayKey]?.name ?? "Акция"}
          </span>
        </div>
      )}

      {/* «Аромат месяца» badge (non-holiday) */}
      {!banner.holidayKey && (banner.title?.toLowerCase().includes('месяц') || banner.subtitle?.toLowerCase().includes('месяц')) && (
        <div style={{
          position: "absolute", top: 16, left: 16, zIndex: 10,
          background: "rgba(201,162,94,0.12)", border: "1px solid rgba(201,162,94,0.4)",
          borderRadius: 3, padding: "6px 14px",
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic", fontSize: 13, color: "#c9a25e", letterSpacing: "0.04em",
          display: "flex", alignItems: "center", gap: 8,
        }}>
          <span style={{ fontSize: 10 }}>✦</span>
          Аромат месяца
        </div>
      )}

      {/* Overlay with text (when image exists) */}
      {banner.imageUrl && (banner.title || banner.linkUrl) && (
        <div style={{
          position: "absolute", inset: 0, zIndex: 5,
          background: "linear-gradient(to right, rgba(0,0,0,0.65) 0%, rgba(0,0,0,0.3) 50%, transparent 100%)",
          display: "flex", flexDirection: "column", justifyContent: "flex-end",
          padding: "clamp(20px,5vw,48px)",
        }}>
          {banner.title && (
            <h2 style={{
              fontFamily: "'Cormorant Garamond', Georgia, serif",
              fontStyle: "italic", fontWeight: 300,
              fontSize: "clamp(22px,4vw,48px)", color: "#f8f2ea", marginBottom: 6,
            }}>{banner.title}</h2>
          )}
          {banner.subtitle && (
            <p style={{ fontSize: "clamp(13px,1.4vw,16px)", color: "rgba(255,255,255,0.7)", marginBottom: 16 }}>{banner.subtitle}</p>
          )}
          {/* Promo code on image banners */}
          {banner.promoCode && (
            <div style={{ marginBottom: 14, alignSelf: "flex-start" }}>
              <PromoCodeBadge code={banner.promoCode} />
            </div>
          )}
          {banner.linkUrl && (
            <div>
              <Link
                to={banner.linkUrl}
                style={{
                  display: "inline-block", background: "#c9a25e", color: "#09090b",
                  fontWeight: 600, fontSize: 13, letterSpacing: "0.06em",
                  padding: "10px 26px", borderRadius: 3, textDecoration: "none",
                }}
              >
                {banner.linkText || "Смотреть"}
              </Link>
            </div>
          )}
        </div>
      )}

      {/* Countdown timer */}
      {banner.endDate && new Date(banner.endDate).getTime() > Date.now() && (
        <CountdownTimer endDate={banner.endDate} />
      )}

      {/* Controls */}
      {banners.length > 1 && (
        <>
          {[{ dir: "prev", fn: prev, side: "left" }, { dir: "next", fn: next, side: "right" }].map(({ dir, fn, side }) => (
            <button
              key={dir}
              onClick={fn}
              style={{
                position: "absolute", [side]: 12, top: "50%", transform: "translateY(-50%)",
                zIndex: 10, background: "rgba(255,255,255,0.12)", backdropFilter: "blur(8px)",
                border: "1px solid rgba(255,255,255,0.15)", borderRadius: "50%",
                width: 36, height: 36, display: "flex", alignItems: "center", justifyContent: "center",
                cursor: "pointer", color: "#f2ede6", transition: "background 0.2s",
              }}
            >
              {dir === "prev" ? <ChevronLeft size={18} /> : <ChevronRight size={18} />}
            </button>
          ))}

          {/* Dots */}
          <div style={{
            position: "absolute", bottom: 14, left: "50%", transform: "translateX(-50%)",
            display: "flex", gap: 6, zIndex: 10,
          }}>
            {banners.map((_, i) => (
              <button
                key={i}
                onClick={() => setIdx(i)}
                style={{
                  borderRadius: 4, border: "none", cursor: "pointer",
                  width: i === idx ? 24 : 8, height: 8,
                  background: i === idx ? "#f2ede6" : "rgba(242,237,230,0.35)",
                  transition: "all 0.3s",
                }}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
