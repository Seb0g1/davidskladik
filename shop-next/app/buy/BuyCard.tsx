import Link from "next/link";
import { ArrowUpRight } from "lucide-react";
import type { BuyChannel } from "@/lib/buy";

// Channel colours: our lime for the site, the marketplaces' own brand colours so buyers recognise them.
export const CHANNEL_THEME: Record<BuyChannel, { bg: string; fg: string; sub: string; mark: string }> = {
  site:   { bg: "var(--lime)", fg: "#121212", sub: "rgba(18,18,18,0.62)", mark: "MV" },
  yandex: { bg: "#ffdc2e",     fg: "#121212", sub: "rgba(18,18,18,0.62)", mark: "Я" },
  ozon:   { bg: "#005bff",     fg: "#ffffff", sub: "rgba(255,255,255,0.75)", mark: "O" },
};

export function BuyCard({ channel, href, title, subtitle, badge, badgeTone = "dark", price, priceNote, dim, cta }: {
  channel: BuyChannel;
  href: string;
  title: string;
  subtitle?: string;
  badge?: string;
  badgeTone?: "dark" | "pink";
  price?: string;
  priceNote?: string;
  dim?: boolean;
  cta: string;
}) {
  const th = CHANNEL_THEME[channel];
  const external = !href.startsWith("/");
  const body = (
    <>
      {badge && (
        <span className="buy-badge" style={{
          background: badgeTone === "pink" ? "var(--pink)" : "#121212",
          color: badgeTone === "pink" ? "#fff" : "var(--lime)",
        }}>{badge}</span>
      )}
      <span className="buy-row">
        <span className="buy-mark" style={{ background: channel === "ozon" ? "#fff" : "#121212", color: channel === "ozon" ? "#005bff" : th.bg === "var(--lime)" ? "#d9f84a" : th.bg }}>
          {th.mark}
        </span>
        <span className="buy-text">
          <span className="buy-title">{title}</span>
          {subtitle && <span className="buy-sub" style={{ color: th.sub }}>{subtitle}</span>}
        </span>
        {price && (
          <span className="buy-price">
            <b>{price}</b>
            {priceNote && <span style={{ color: th.sub }}>{priceNote}</span>}
          </span>
        )}
      </span>
      <span className="buy-cta" style={{ background: channel === "ozon" ? "#fff" : "#121212", color: channel === "ozon" ? "#005bff" : "#fff" }}>
        {cta} <ArrowUpRight size={17} />
      </span>
    </>
  );
  const style: React.CSSProperties = { background: th.bg, color: th.fg, opacity: dim ? 0.55 : 1 };
  return external
    ? <a href={href} target="_blank" rel="noopener noreferrer" className="buy-card" style={style}>{body}</a>
    : <Link href={href} className="buy-card" style={style}>{body}</Link>;
}

export function BuyShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="buy-root" style={{ background: "var(--bg)", minHeight: "100vh" }}>
      <div style={{ maxWidth: 560, margin: "0 auto", padding: "40px 16px 120px" }}>{children}</div>
    </div>
  );
}
