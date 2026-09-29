import type { Metadata } from "next";
import Link from "next/link";
import { SITE_URL } from "@/lib/seo";
import { SELLER } from "@/lib/legal";
import LegalDoc, { legalContacts, type LegalSection } from "@/components/LegalDoc";

export const revalidate = 3600;

export const metadata: Metadata = {
  title: "Согласие на получение рекламной рассылки",
  description: `Согласие на получение рекламных и информационных писем Magic Vibes (ст. 18 38-ФЗ «О рекламе»).`,
  alternates: { canonical: "/consent-ads" },
  openGraph: { title: "Согласие на рассылку | Magic Vibes", url: `${SITE_URL}/consent-ads` },
};

export default async function ConsentAdsPage() {
  const { email, phone } = await legalContacts();
  const sections: LegalSection[] = [
    { id: "what", title: "Что я разрешаю", items: [
      <>Я даю {SELLER.name} (ОГРНИП {SELLER.ogrnip}, далее — «Продавец») согласие на получение рекламы и информационных сообщений о товарах, акциях, промокодах, новинках и подборках ароматов на указанный мной адрес электронной почты (ч. 1 ст. 18 Федерального закона от 13.03.2006 № 38-ФЗ «О рекламе»).</>,
      <>Согласие даётся отдельной отметкой «Хочу получать скидки и подборки» при оформлении заказа или при подписке на Сайте {SELLER.site} (форма промокода, тест ароматов). Оно не является условием покупки: заказ можно оформить без него.</>,
      <>Для рассылки я также соглашаюсь на обработку моего email, имени и истории заказов в целях подбора предложений, на условиях <Link href="/privacy">Политики обработки персональных данных</Link>.</>,
    ]},
    { id: "letters", title: "Какие письма", items: [
      <>Промокод на первый заказ, подборки ароматов, просьба оценить купленный товар со скидкой на следующий заказ, «аромат месяца» и новинки. Письма приходят с адреса домена magicvibes.ru, как правило не чаще нескольких раз в месяц.</>,
      <>Письма о статусе заказа, чеки и коды входа рекламой не являются и приходят независимо от этого согласия.</>,
    ]},
    { id: "revoke", title: "Как отказаться", items: [
      <>Отписаться можно в любой момент — по ссылке «Отписаться» в любом письме или написав на {email}. После отказа рекламные письма больше не отправляются.</>,
      <>Согласие действует до отзыва.</>,
    ]},
  ];
  return <LegalDoc path="/consent-ads" title="Согласие на получение рекламы" script="только если вы сами захотели" sections={sections} email={email} phone={phone} />;
}
