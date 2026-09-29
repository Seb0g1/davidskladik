"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { readCookieConsent, saveCookieConsent } from "@/lib/legal";

// First-visit cookie notice: «Принять все» / «Только необходимые». Reopened from /cookies.
export default function CookieBanner() {
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (!readCookieConsent()) setOpen(true);
    const reopen = () => setOpen(true);
    window.addEventListener("mv-cookie-settings", reopen);
    return () => window.removeEventListener("mv-cookie-settings", reopen);
  }, []);
  if (!open) return null;
  const choose = (all: boolean) => { saveCookieConsent(all); setOpen(false); };
  return (
    <div className="mv-cookie" role="dialog" aria-live="polite" aria-label="Файлы cookie">
      <p>
        Мы используем cookie и хранилище браузера, чтобы работали корзина и вход в кабинет, а с вашего согласия — Яндекс Метрику и виджет отзывов.
        Подробнее — в <Link prefetch={false} href="/cookies">политике cookie</Link> и <Link prefetch={false} href="/privacy">политике обработки данных</Link>.
      </p>
      <div className="mv-cookie-btns">
        <button type="button" className="ghost" onClick={() => choose(false)}>Только необходимые</button>
        <button type="button" onClick={() => choose(true)}>Принять все</button>
      </div>
    </div>
  );
}
