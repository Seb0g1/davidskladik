import type { ReactNode } from "react";

export function Stat({ label, value, icon, tone, trend, delta, onClick, selected }: { label: string; value: unknown; icon?: ReactNode; tone?: "accent" | "success" | "warn" | ""; trend?: ReactNode; delta?: string; onClick?: () => void; selected?: boolean }) {
  const className = `stat stat-card${tone ? ` stat-${tone}` : ""}${onClick ? " is-clickable" : ""}${selected ? " is-selected" : ""}`;
  const body = (
    <>
      {icon ? <span className="stat-icon">{icon}</span> : null}
      <div className="stat-body">
        <span title={label}>{label}</span>
        <strong>{String(value ?? "-")}</strong>
        {delta ? <small>{delta}</small> : null}
      </div>
      {trend ? <div className="stat-trend">{trend}</div> : null}
    </>
  );
  // Кликабельная плитка работает как фильтр (например, «Изменения» на складе).
  if (onClick) {
    return <button type="button" className={className} onClick={onClick} aria-pressed={Boolean(selected)} title={selected ? "Снять фильтр" : `Показать: ${label}`}>{body}</button>;
  }
  return <div className={className}>{body}</div>;
}
