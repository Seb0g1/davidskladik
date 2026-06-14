// Loading skeletons (PLAN-HARDENING.md 5.1): shimmer placeholders that mirror the real
// layout, instead of a bare spinner + "Загружаю…" text. Respects prefers-reduced-motion
// via CSS (.skeleton animation is disabled there).

export function Skeleton({
  width = "100%",
  height = 14,
  radius = 8,
  className = "",
}: {
  width?: number | string;
  height?: number | string;
  radius?: number;
  className?: string;
}) {
  return <span className={`skeleton ${className}`.trim()} style={{ width, height, borderRadius: radius }} aria-hidden="true" />;
}

// Mirrors the catalog product-group rows so the page does not jump when data arrives.
export function CatalogSkeleton({ rows = 8 }: { rows?: number }) {
  return (
    <div className="catalog-skeleton" role="status" aria-busy="true" aria-label="Загрузка каталога">
      {Array.from({ length: rows }).map((_, index) => (
        <div className="catalog-skeleton-row" key={index}>
          <Skeleton width={44} height={44} radius={10} />
          <div className="catalog-skeleton-lines">
            <Skeleton width={`${55 + ((index * 7) % 30)}%`} height={13} />
            <Skeleton width={`${30 + ((index * 5) % 20)}%`} height={11} />
          </div>
          <Skeleton width={72} height={24} radius={999} />
        </div>
      ))}
    </div>
  );
}
