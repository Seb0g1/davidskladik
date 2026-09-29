"use client";
import { useEffect, useRef, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import { readCookieConsent, type CookieConsent } from "@/lib/legal";
import { YM_IDS } from "@/lib/metrika";

type YmFn = ((...a: unknown[]) => void) & { a?: unknown[][]; l?: number };

function loadTag() {
  const w = window as unknown as { ym?: YmFn; dataLayer?: unknown[] };
  if (w.ym) return;
  // official snippet, rewritten: queue calls until tag.js arrives
  const q: YmFn = (...args: unknown[]) => { (q.a = q.a || []).push(args); };
  q.l = Date.now();
  w.ym = q;
  w.dataLayer = w.dataLayer || [];
  const s = document.createElement("script");
  s.async = true;
  s.src = "https://mc.yandex.ru/metrika/tag.js";
  document.head.appendChild(s);
  for (const id of YM_IDS) {
    w.ym(id, "init", { defer: true, clickmap: true, trackLinks: true, accurateTrackBounce: true, webvisor: true, ecommerce: "dataLayer" });
  }
}

export default function YandexMetrika() {
  const [on, setOn] = useState(false);
  const pathname = usePathname();
  const search = useSearchParams();
  const first = useRef(true);

  useEffect(() => {
    if (readCookieConsent()?.all) setOn(true);
    const h = (e: Event) => { if ((e as CustomEvent<CookieConsent>).detail?.all) setOn(true); };
    window.addEventListener("mv-cookie-consent", h);
    return () => window.removeEventListener("mv-cookie-consent", h);
  }, []);

  // defer:true → every page view (first one included) is sent as a hit from here
  useEffect(() => {
    if (!on) return;
    loadTag();
    const w = window as unknown as { ym: YmFn };
    const url = window.location.href;
    const referer = first.current ? document.referrer : undefined;
    first.current = false;
    for (const id of YM_IDS) w.ym(id, "hit", url, { referer, title: document.title });
  }, [on, pathname, search]);

  return null;
}
