// Magic Vibes logo: the "orb" mark (the flacon's sphere cap with a spark of magic) + wordmark.
interface Props {
  variant?: "full" | "mark";
  tone?: "ink" | "paper";
  height?: number;
}

export function LogoMark({ size = 32, tone = "ink" }: { size?: number; tone?: "ink" | "paper" }) {
  const orb = tone === "ink" ? "#121212" : "#f5f2ec";
  const shine = tone === "ink" ? "rgba(255,255,255,0.35)" : "rgba(18,18,18,0.18)";
  return (
    <svg width={size} height={size} viewBox="0 0 40 40" aria-hidden="true" style={{ display: "block", flexShrink: 0 }}>
      <circle cx="20" cy="20" r="19" fill={orb} />
      <ellipse cx="13.5" cy="12" rx="5.5" ry="3.2" transform="rotate(-35 13.5 12)" fill={shine} />
      <path d="M24 9.5c.9 5.3 3.2 7.6 8.5 8.5-5.3.9-7.6 3.2-8.5 8.5-.9-5.3-3.2-7.6-8.5-8.5 5.3-.9 7.6-3.2 8.5-8.5Z" fill="#d9f84a" />
      <circle cx="14" cy="27.5" r="2.4" fill="#ff3d7f" />
    </svg>
  );
}

export default function Logo({ variant = "full", tone = "ink", height = 30 }: Props) {
  const color = tone === "ink" ? "#121212" : "#f5f2ec";
  if (variant === "mark") return <LogoMark size={height} tone={tone} />;
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: height * 0.3, color, lineHeight: 1 }}>
      <LogoMark size={height} tone={tone} />
      <span style={{ fontFamily: "var(--font-display)", fontWeight: 800, fontSize: height * 0.56, letterSpacing: "-0.03em", textTransform: "uppercase", whiteSpace: "nowrap" }}>
        Magic<span style={{ fontFamily: "var(--font-script)", fontWeight: 400, textTransform: "none", color: "#ff3d7f", fontSize: height * 0.78, letterSpacing: 0, margin: "0 0.08em 0 0.12em", position: "relative", top: "0.06em" }}>vibes</span>
      </span>
    </span>
  );
}
