// Yandex Metrika — loaded only after «Принять все» in the cookie banner (152-ФЗ).
// Counter 112580580 «Magic Vibes» (chosen by the owner; 112292511 is no longer installed).
export const YM_IDS = (process.env.NEXT_PUBLIC_YM_IDS || "112580580")
  .split(",").map((s) => Number(s.trim())).filter(Boolean);

type Ym = (id: number, method: string, ...args: unknown[]) => void;
const ym = (): Ym | null => (typeof window !== "undefined" && (window as unknown as { ym?: Ym }).ym) || null;

/** JS goal (create the same identifier in Metrika → Цели → «JavaScript-событие») */
export function ymGoal(name: string, params?: Record<string, unknown>) {
  const f = ym();
  if (f) YM_IDS.forEach((id) => f(id, "reachGoal", name, params));
}
