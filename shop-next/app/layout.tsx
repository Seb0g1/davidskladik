import type { Metadata } from "next";
import "./fonts.css";
import "./globals.css";
import Providers from "@/components/Providers";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import SupportChatWidget from "@/components/SupportChatWidget";
import PopupPromo from "@/components/PopupPromo";
import CookieBanner from "@/components/CookieBanner";
import YandexMetrika from "@/components/YandexMetrika";
import { Suspense } from "react";
import { SITE_NAME, SITE_URL, DEFAULT_DESC } from "@/lib/seo";
import { SELLER } from "@/lib/legal";

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: {
    default: `${SITE_NAME} — Оригинальный парфюм с доставкой по России`,
    template: `%s | ${SITE_NAME}`,
  },
  description: DEFAULT_DESC,
  keywords: "парфюмерия купить, духи купить, интернет-магазин парфюмерии, оригинальная парфюмерия, парфюм с доставкой по России",
  authors: [{ name: SITE_NAME }],
  creator: SITE_NAME,
  applicationName: SITE_NAME,
  openGraph: {
    siteName: SITE_NAME,
    locale: "ru_RU",
    type: "website",
  },
  twitter: { card: "summary_large_image" },
  verification: {
    google: "FrAqJndC1BsTne8nIROKxHunibKIcVbSl2K8M0eLg-A",
    other: { "yandex-verification": ["5ca88817e8595104"] },
  },
  manifest: "/manifest.webmanifest",
  robots: { index: true, follow: true, googleBot: { index: true, follow: true, "max-snippet": -1, "max-image-preview": "large" } },
  other: {
    "geo.region": "RU",
    "geo.placename": "Россия",
  },
};

// OnlineStore is an Organization subtype: Google takes name/logo for the knowledge panel and the result's site name
const ORG_SCHEMA = {
  "@context": "https://schema.org",
  "@type": "OnlineStore",
  "@id": `${SITE_URL}/#organization`,
  name: SITE_NAME,
  alternateName: ["Мэджик Вайбс", "MagicVibes", "magicvibes.ru"],
  legalName: SELLER.short,
  url: SITE_URL,
  logo: { "@type": "ImageObject", url: `${SITE_URL}/icon-512.png`, width: 512, height: 512 },
  image: `${SITE_URL}/brand/logo/logo-mark-dark.png`,
  description: "Интернет-магазин оригинальной парфюмерии мировых брендов с доставкой по России",
  taxID: SELLER.inn,
  areaServed: { "@type": "Country", name: "Россия" },
  currenciesAccepted: "RUB",
  paymentAccepted: "Банковская карта, СБП, Ozon Pay, рассрочка",
  contactPoint: { "@type": "ContactPoint", contactType: "customer service", email: "noreply@magicvibes.ru", availableLanguage: "Russian", areaServed: "RU" },
  sameAs: ["https://t.me/magicvibes_ru"],
};

const WEBSITE_SCHEMA = {
  "@context": "https://schema.org",
  "@type": "WebSite",
  "@id": `${SITE_URL}/#website`,
  name: SITE_NAME,
  alternateName: ["Magic Vibes — интернет-магазин парфюмерии", "magicvibes.ru"],
  url: `${SITE_URL}/`,
  inLanguage: "ru-RU",
  publisher: { "@id": `${SITE_URL}/#organization` },
  potentialAction: { "@type": "SearchAction", target: `${SITE_URL}/catalog?q={search_term_string}`, "query-input": "required name=search_term_string" },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ru">
      <head>
        {/* one logo everywhere: ico for Yandex/old clients, 512 PNG (multiple of 48) for Google, SVG for browsers */}
        <link rel="icon" href="/favicon.ico" sizes="48x48" />
        <link rel="icon" type="image/png" sizes="512x512" href="/favicon.png" />
        <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
        <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png" />
        <meta name="theme-color" content="#f5f2ec" />
        {/* fonts are self-hosted (app/fonts.css); preload the two cyrillic files the first screen needs */}
        <link rel="preload" href="/fonts/c86556796966.woff2" as="font" type="font/woff2" crossOrigin="anonymous" />
        <link rel="preload" href="/fonts/4c756272dc0a.woff2" as="font" type="font/woff2" crossOrigin="anonymous" />
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(ORG_SCHEMA) }} />
        <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(WEBSITE_SCHEMA) }} />
      </head>
      <body suppressHydrationWarning>
        <Providers>
          <div className="min-h-screen flex flex-col page-root md:pb-0">
            <Header />
            <main className="flex-1">{children}</main>
            <Footer />
          </div>
          <SupportChatWidget />
          <PopupPromo />
          <CookieBanner />
          <Suspense fallback={null}><YandexMetrika /></Suspense>
        </Providers>
      </body>
    </html>
  );
}
