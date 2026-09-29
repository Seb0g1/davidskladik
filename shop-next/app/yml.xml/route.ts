// /yml.xml — Yandex YML product feed (Вебмастер → Фиды → тип «Товары», Яндекс Бизнес / Директ).
// Built and cached by the API (/api/shop/yml.xml); served here from the shop's own domain.
const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

export const revalidate = 3600;

export async function GET() {
  // the feed is well above the 2 MB Next data-cache limit; the API caches it for an hour
  const r = await fetch(`${API}/yml.xml`, { cache: "no-store" });
  if (!r.ok) return new Response("Feed unavailable", { status: 503 });
  return new Response(await r.text(), {
    headers: { "Content-Type": "application/xml; charset=utf-8", "Cache-Control": "public, max-age=1800, s-maxage=3600" },
  });
}
