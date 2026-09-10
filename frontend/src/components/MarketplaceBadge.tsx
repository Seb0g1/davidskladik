const BASE = import.meta.env.BASE_URL.replace(/\/$/, "");

const MARKETPLACE_ICONS: Record<string, string> = {
  ozon: `${BASE}/icons/ozon.svg`,
  yandex: `${BASE}/icons/yandex-market.svg`,
  wb: "",
  avito: `${BASE}/icons/avito.svg`,
};

const MARKETPLACE_LABELS: Record<string, string> = {
  ozon: "Ozon",
  yandex: "Яндекс",
  wb: "WB",
  avito: "Avito",
};

function normalizeKey(marketplace: string): string {
  const lower = marketplace.toLowerCase();
  if (lower.includes("ozon")) return "ozon";
  if (lower.includes("yandex") || lower.includes("яндекс")) return "yandex";
  if (lower === "wb" || lower.includes("wildberries")) return "wb";
  if (lower.includes("avito")) return "avito";
  return lower;
}

export function MarketplaceBadge({ marketplace, muted, className, showLabel }: {
  marketplace: string;
  muted?: boolean;
  className?: string;
  showLabel?: boolean;
}) {
  const key = normalizeKey(marketplace);
  const icon = MARKETPLACE_ICONS[key];
  const label = MARKETPLACE_LABELS[key] || marketplace.toUpperCase();
  const showText = showLabel || !icon;
  return (
    <span
      className={`market-badge market-${key}${muted ? " muted" : ""}${icon && !showText ? " icon-only" : ""}${className ? ` ${className}` : ""}`}
      title={label}
    >
      {icon ? <img src={icon} alt={label} width={16} height={16} /> : null}
      {showText ? label : null}
    </span>
  );
}
