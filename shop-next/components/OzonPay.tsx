/* eslint-disable @next/next/no-img-element */
// Ozon Pay brand assets (official logo from Ozon Bank's brand kit).
// Guidelines: button colour #005BFF, logo placed to the left of the button text,
// never smaller than other payment marks next to it.

export const OZON_PAY_BLUE = "#005BFF";

/**
 * Official Ozon Pay logo on a transparent background.
 * `color` — brand blue, for dark/light surfaces; `white` — for the #005BFF pay button
 * ("pay" is cut out of the pill, so the button colour shows through, as in Ozon's guidelines).
 */
export function OzonPayLogo({ height = 28, variant = "color" }: { height?: number; variant?: "color" | "white" }) {
  return (
    <img
      src={variant === "white" ? "/ozon-pay-logo-white.png" : "/ozon-pay-logo.png"}
      alt="Ozon Pay"
      height={height}
      style={{ height, width: "auto", display: "block", flexShrink: 0 }}
    />
  );
}

/** The payment methods Ozon Pay offers to the buyer — one source of truth for all pages. */
export const OZON_PAY_METHODS = [
  "Банковские карты любых российских банков (Мир, Visa, Mastercard)",
  "СБП — Система быстрых платежей",
  "Ozon Карта — в один клик, если карта сохранена на Ozon",
  "Ozon Рассрочка",
];

export const OZON_PAY_FAQ =
  "Сервис позволяет принимать безналичную оплату на сайте, в приложении или через соцсети — с карт любых российских банков и через СБП. " +
  "Покупатели Ozon, которые сохранили карты на маркетплейсе, могут оплачивать покупки в один клик, не вводя номер карты ещё раз.";
