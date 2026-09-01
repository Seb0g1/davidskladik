interface Props {
  rating: number;
  count?: number;
  size?: number;
  showCount?: boolean;
  compact?: boolean;
}

export default function StarRating({ rating, count, size = 12, showCount = true, compact = false }: Props) {
  if (!rating || rating <= 0) return null;

  const full = Math.floor(rating);
  const half = rating - full >= 0.3 && rating - full < 0.8;
  const empty = 5 - full - (half ? 1 : 0);

  const star = (type: "full" | "half" | "empty", i: number) => (
    <svg key={i} width={size} height={size} viewBox="0 0 24 24" fill="none" style={{ flexShrink: 0 }}>
      {type === "full" && (
        <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"
          fill="#C9A96E" stroke="none" />
      )}
      {type === "half" && (
        <>
          <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77V2z"
            fill="#C9A96E" stroke="none" />
          <path d="M12 2v15.77L5.82 21.02 7 14.14 2 9.27l6.91-1.01L12 2z"
            fill="rgba(201,169,110,0.2)" stroke="none" />
        </>
      )}
      {type === "empty" && (
        <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"
          fill="rgba(201,169,110,0.18)" stroke="none" />
      )}
    </svg>
  );

  return (
    <div style={{ display: "inline-flex", alignItems: "center", gap: compact ? 3 : 4 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 1 }}>
        {Array.from({ length: full }, (_, i) => star("full", i))}
        {half && star("half", full)}
        {Array.from({ length: empty }, (_, i) => star("empty", full + (half ? 1 : 0) + i))}
      </div>
      {showCount && count !== undefined && count > 0 && (
        <span style={{ fontSize: size - 2, color: "rgba(244,239,230,0.4)", fontWeight: 400 }}>
          {compact ? count : `${rating.toFixed(1)} (${count})`}
        </span>
      )}
      {showCount && (count === undefined || count === 0) && rating > 0 && (
        <span style={{ fontSize: size - 2, color: "rgba(244,239,230,0.4)" }}>
          {rating.toFixed(1)}
        </span>
      )}
    </div>
  );
}
