import type { ReactNode } from "react";

export function Stat({ label, value, icon, tone, trend, delta }: { label: string; value: unknown; icon?: ReactNode; tone?: "accent" | "success" | "warn" | ""; trend?: ReactNode; delta?: string }) {
  return (
    <div className={`stat stat-card${tone ? ` stat-${tone}` : ""}`}>
      {icon ? <span className="stat-icon">{icon}</span> : null}
      <div className="stat-body">
        <span>{label}</span>
        <strong>{String(value ?? "-")}</strong>
        {delta ? <small>{delta}</small> : null}
      </div>
      {trend ? <div className="stat-trend">{trend}</div> : null}
    </div>
  );
}
