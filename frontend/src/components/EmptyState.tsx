import { PackageSearch } from "lucide-react";
import { ReactNode } from "react";

interface EmptyStateProps {
  icon?: ReactNode;
  title: string;
  description?: string;
  action?: ReactNode;
}

export function EmptyState({ icon, title, description, action }: EmptyStateProps) {
  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
      justifyContent: "center",
      padding: "48px 24px",
      gap: 12,
      color: "var(--muted)",
      textAlign: "center",
    }}>
      <div style={{ opacity: 0.4, marginBottom: 4 }}>
        {icon ?? <PackageSearch size={40} />}
      </div>
      <div style={{ fontSize: 15, fontWeight: 600, color: "var(--text-secondary, #c8d6e8)" }}>{title}</div>
      {description && <div style={{ fontSize: 13, maxWidth: 360 }}>{description}</div>}
      {action && <div style={{ marginTop: 8 }}>{action}</div>}
    </div>
  );
}
