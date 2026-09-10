const MARKETPLACE_ICONS: Record<string, string> = {
  ozon: "/icons/ozon.svg",
  yandex: "/icons/yandex-market.svg",
  wb: "",
  avito: "/icons/avito.svg",
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
  if (lower.includes("yandex")) return "yandex";
  if (lower === "wb" || lower.includes("wildberries")) return "wb";
  if (lower.includes("avito")) return "avito";
  return lower;
}

export function MarketplaceBadge({ marketplace, muted, className }: { marketplace: string; muted?: boolean; className?: string }) {
  const key = normalizeKey(marketplace);
  const icon = MARKETPLACE_ICONS[key];
  const label = MARKETPLACE_LABELS[key] || marketplace.toUpperCase();
  return (
    <span className={`market-badge market-${key}${muted ? " muted" : ""}${className ? ` ${className}` : ""}`}>
      {icon ? <img src={icon} alt="" width={16} height={16} /> : null}
      {label}
    </span>
  );
}
