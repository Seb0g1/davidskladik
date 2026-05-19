export function DiagnosticValue({ label, value, tone }: { label: string; value: unknown; tone?: string }) {
  const text = value === true ? "да" : value === false ? "нет" : String(value ?? "-");
  return (
    <div className={`diagnostic-value ${tone || ""}`}>
      <span>{label}</span>
      <strong>{text}</strong>
    </div>
  );
}
