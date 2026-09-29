// Dostavista callbacks (кабинет: magicvibes.ru/callback/dostavista/orders и /deliveries) → API.
// Тело передаётся как есть (text/plain), чтобы API проверил подпись X-DV-Signature по исходным байтам.
const API = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";

export async function POST(req: Request) {
  const raw = await req.text();
  try {
    const r = await fetch(`${API}/api/shop/callback/dostavista`, {
      method: "POST",
      headers: { "Content-Type": "text/plain; charset=utf-8", "X-DV-Signature": req.headers.get("x-dv-signature") || "" },
      body: raw,
      cache: "no-store",
    });
    return new Response(await r.text(), { status: r.status, headers: { "Content-Type": "application/json" } });
  } catch {
    // Достависта повторит callback, если ответ не 200
    return new Response(JSON.stringify({ ok: false }), { status: 502, headers: { "Content-Type": "application/json" } });
  }
}

export function GET() {
  return new Response("ok");
}
