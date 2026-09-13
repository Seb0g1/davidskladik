/**
 * Russian pluralization: chooses the correct form based on the number.
 * ruPlural(1, "аромат", "аромата", "ароматов") → "аромат"
 * ruPlural(3, "аромат", "аромата", "ароматов") → "аромата"
 * ruPlural(11,"аромат", "аромата", "ароматов") → "ароматов"
 */
export function ruPlural(n: number, one: string, few: string, many: string): string {
  const abs = Math.abs(Math.floor(n)) % 100;
  const rem = abs % 10;
  if (abs >= 11 && abs <= 19) return many;
  if (rem === 1) return one;
  if (rem >= 2 && rem <= 4) return few;
  return many;
}
