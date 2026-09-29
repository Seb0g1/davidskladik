"use client";
import { useEffect, useState } from "react";
import { readCookieConsent, type CookieConsent } from "@/lib/legal";

const ORG = "199272438950";

// The Yandex widget sets Yandex cookies → load it only after «Принять все»; otherwise a plain link.
export default function YandexRatingBadge() {
  const [all, setAll] = useState(false);
  useEffect(() => {
    setAll(readCookieConsent()?.all === true);
    const on = (e: Event) => setAll((e as CustomEvent<CookieConsent>).detail?.all === true);
    window.addEventListener("mv-cookie-consent", on);
    return () => window.removeEventListener("mv-cookie-consent", on);
  }, []);
  if (!all) {
    return (
      <a className="mv-yrating-link" href={`https://yandex.ru/maps/org/${ORG}/reviews/`} target="_blank" rel="noopener noreferrer">
        ★ Отзывы на Яндексе
      </a>
    );
  }
  return (
    // white Yandex widget can't be styled inside (cross-origin) → inverted by CSS into the dark footer
    <span className="mv-yrating">
      <iframe src={`https://yandex.ru/sprav/widget/rating-badge/${ORG}?type=rating`} width={150} height={50} frameBorder={0} loading="lazy" title="Рейтинг Magic Vibes на Яндексе" />
    </span>
  );
}
