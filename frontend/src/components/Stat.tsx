export function Stat({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{String(value ?? "-")}</strong>
    </div>
  );
}
