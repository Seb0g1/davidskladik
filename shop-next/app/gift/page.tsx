import type { Metadata } from "next";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import GiftClient from "./GiftClient";

export const metadata: Metadata = {
  title: `Подарочный конфигуратор | ${SITE_NAME}`,
  description: "Собери идеальный подарок: выбери аромат, упаковку и персональную открытку. Фирменная подарочная упаковка Magic Vibes с доставкой по России.",
  alternates: { canonical: "/gift" },
  openGraph: { title: "Подарочный конфигуратор — Magic Vibes", url: `${SITE_URL}/gift` },
};

export default function GiftPage() {
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "Подарок", url: "/gift" }]);
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <GiftClient />
    </>
  );
}
