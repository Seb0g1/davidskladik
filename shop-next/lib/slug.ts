const TRANSLIT: Record<string, string> = {
  'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
  'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
  'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
  'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'shch',
  'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
};

export function toProductSlug(name: string, offerId: string): string {
  const nameSlug = name
    .toLowerCase()
    .split('')
    .map(c => TRANSLIT[c] ?? c)
    .join('')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 80)
    .replace(/-+$/, '');

  // offerIds may contain URL-reserved chars (e.g. "#YV000011#") — encode, or the # turns into a fragment → 404
  return `${nameSlug}--${encodeURIComponent(offerId)}`;
}

export function parseSlugForOfferId(slug: string): string {
  const idx = slug.lastIndexOf('--');
  if (idx === -1) return decodeURIComponent(slug); // backward compat with old plain-offerId URLs
  const raw = slug.slice(idx + 2);
  try { return decodeURIComponent(raw); } catch { return raw; }
}
