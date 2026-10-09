import { revalidatePath } from "next/cache";
import { timingSafeEqual } from "crypto";

// Called by the warehouse API (shopRepriceStorefront in server/parts/02d-shop-api-routes.js) after the
// shop markup changes: drops every ISR page and fetch cache, so HTML, meta and JSON-LD carry the new
// prices. Both sides read SHOP_REVALIDATE_SECRET.
export const dynamic = "force-dynamic";

function sameSecret(a: string, b: string) {
  const x = Buffer.from(a), y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

export async function POST(request: Request) {
  const secret = process.env.SHOP_REVALIDATE_SECRET || "";
  const given = request.headers.get("x-revalidate-secret") || "";
  if (!secret || !sameSecret(given, secret)) {
    return Response.json({ ok: false }, { status: 401 });
  }
  revalidatePath("/", "layout");
  return Response.json({ ok: true, revalidated: true, at: new Date().toISOString() });
}
