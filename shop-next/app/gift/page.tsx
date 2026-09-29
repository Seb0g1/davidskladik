import type { Metadata } from "next";
import { SITE_NAME, SITE_URL, breadcrumbJsonLd } from "@/lib/seo";
import { notFound } from "next/navigation";
import GiftClient from "./GiftClient";
import { getFeatures } from "@/lib/features";

export const metadata: Metadata = {
  title: `Подарочный конфигуратор`,
  description: "Собери идеальный подарок: выбери аромат, упаковку и персональную открытку. Фирменная подарочная упаковка Magic Vibes с доставкой по России.",
  alternates: { canonical: "/gift" },
  openGraph: { title: "Подарочный конфигуратор — Magic Vibes", url: `${SITE_URL}/gift` },
};

export default async function GiftPage() {
  // «Собрать подарок» is switched off in the admin until it's ready
  if (!(await getFeatures()).giftBuilder) notFound();
  const breadcrumb = breadcrumbJsonLd([{ name: "Главная", url: "/" }, { name: "Подарок", url: "/gift" }]);
  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <GiftClient />
    </>
  );
}
