// URLs for cleaned product photos served by app/img/route.ts.
const PROCESSABLE = /^https:\/\/([^/]+\.)?(ozone\.ru|ozon\.ru|ozonusercontent\.com|yandex\.net)\//;

export function productImg(url: string | undefined | null, width: number, ratio: 1 | 1.12 = 1): string {
  if (!url) return "";
  if (!PROCESSABLE.test(url)) return url;
  return `/img?u=${encodeURIComponent(url)}&w=${width}&r=${ratio}`;
}

/** srcSet for 1x / 2x screens. */
export function productImgSet(url: string | undefined | null, width: number, ratio: 1 | 1.12 = 1): string | undefined {
  if (!url || !PROCESSABLE.test(url)) return undefined;
  return `${productImg(url, width, ratio)} 1x, ${productImg(url, width * 2, ratio)} 2x`;
}
