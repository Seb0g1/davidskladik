import { useState, useEffect, useCallback } from "react";
import { Link } from "react-router-dom";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import type { ShopBanner } from "../types";
import clsx from "clsx";

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

function GradientBanner({ banner }: { banner: ShopBanner }) {
  const GRADIENTS = [
    "from-violet-900 via-purple-800 to-pink-700",
    "from-brand-dark via-violet-900 to-violet-700",
    "from-rose-900 via-pink-800 to-violet-700",
  ];
  const g = GRADIENTS[Math.abs(banner.id.charCodeAt(0)) % GRADIENTS.length];
  return (
    <div className={clsx("w-full h-full bg-gradient-to-br", g, "flex flex-col items-center justify-center text-white text-center px-8")}>
      {banner.title && (
        <h2 className="font-display text-4xl md:text-6xl font-bold mb-3 drop-shadow-lg">{banner.title}</h2>
      )}
      {banner.subtitle && (
        <p className="text-lg md:text-xl text-white/80 mb-8 max-w-lg">{banner.subtitle}</p>
      )}
      {banner.linkUrl && (
        <Link
          to={banner.linkUrl}
          className="bg-white text-violet-700 font-semibold px-8 py-3 rounded-xl hover:bg-violet-50 transition-colors text-sm shadow-lg"
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

  return (
    <div className="relative w-full h-72 md:h-96 lg:h-[480px] overflow-hidden rounded-2xl bg-gray-100">
      {/* Image or gradient */}
      <div key={idx} className="absolute inset-0 slide-in">
        {banner.imageUrl ? (
          <img src={banner.imageUrl} alt={banner.title ?? ""} className="w-full h-full object-cover" />
        ) : (
          <GradientBanner banner={banner} />
        )}
      </div>

      {/* «Аромат месяца» badge */}
      {(banner.title?.toLowerCase().includes('месяц') || banner.subtitle?.toLowerCase().includes('месяц')) && (
        <div style={{
          position: 'absolute', top: 18, left: 18, zIndex: 10,
          background: 'rgba(201,162,94,0.12)',
          border: '1px solid rgba(201,162,94,0.4)',
          borderRadius: 3,
          padding: '6px 14px',
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: 'italic', fontSize: 13,
          color: '#c9a25e', letterSpacing: '0.04em',
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <span style={{ fontSize: 10 }}>✦</span>
          Аромат месяца
        </div>
      )}

      {/* Overlay with text (when image exists) */}
      {banner.imageUrl && (banner.title || banner.linkUrl) && (
        <div className="absolute inset-0 bg-gradient-to-r from-black/60 via-black/30 to-transparent flex flex-col justify-end p-8 md:p-12">
          {banner.title && (
            <h2 className="font-display text-3xl md:text-5xl font-bold text-white mb-2 drop-shadow">{banner.title}</h2>
          )}
          {banner.subtitle && (
            <p className="text-white/80 text-base md:text-lg mb-6">{banner.subtitle}</p>
          )}
          {banner.linkUrl && (
            <Link
              to={banner.linkUrl}
              className="inline-flex w-fit bg-white text-violet-700 font-semibold px-6 py-2.5 rounded-xl hover:bg-violet-50 transition-colors text-sm"
            >
              {banner.linkText || "Смотреть"}
            </Link>
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
          <button
            onClick={prev}
            className="absolute left-3 top-1/2 -translate-y-1/2 bg-white/90 hover:bg-white text-gray-800 rounded-full p-2 shadow-lg transition-all hover:scale-110"
          >
            <ChevronLeft size={20} />
          </button>
          <button
            onClick={next}
            className="absolute right-3 top-1/2 -translate-y-1/2 bg-white/90 hover:bg-white text-gray-800 rounded-full p-2 shadow-lg transition-all hover:scale-110"
          >
            <ChevronRight size={20} />
          </button>

          {/* Dots */}
          <div className="absolute bottom-4 left-1/2 -translate-x-1/2 flex gap-1.5">
            {banners.map((_, i) => (
              <button
                key={i}
                onClick={() => setIdx(i)}
                className={clsx(
                  "rounded-full transition-all duration-300",
                  i === idx ? "bg-white w-6 h-2" : "bg-white/50 w-2 h-2"
                )}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
