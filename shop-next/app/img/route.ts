// GET /img?u=<marketplace photo url>&w=<width>&r=<ratio>
// Cleaned product photo (see lib/product-image.ts), cached on disk under
// .next/cache/mv-img. Any failure falls back to a redirect to the original.
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { cleanProductImage } from "@/lib/product-image";

export const runtime = "nodejs";

const WIDTHS = [160, 320, 480, 640, 960, 1200];
const RATIOS = ["1", "1.12"];
const HOSTS = [/\.ozone\.ru$/, /\.ozon\.ru$/, /\.ozonusercontent\.com$/, /\.yandex\.net$/, /^davidsklad\.ru$/];
const CACHE_DIR = path.join(process.cwd(), ".next", "cache", "mv-img");
const inflight = new Map<string, Promise<Buffer>>();

function allowed(u: URL) {
  return u.protocol === "https:" && HOSTS.some((re) => re.test(u.hostname));
}

async function render(src: string, w: number, r: number, file: string): Promise<Buffer> {
  const res = await fetch(src, { signal: AbortSignal.timeout(12000), headers: { "User-Agent": "MagicVibesImage/1.0" } });
  if (!res.ok) throw new Error(`source ${res.status}`);
  const input = Buffer.from(await res.arrayBuffer());
  if (input.length > 15 * 1024 * 1024) throw new Error("source too large");
  const out = await cleanProductImage(input, w, r);
  await fs.mkdir(CACHE_DIR, { recursive: true });
  await fs.writeFile(file, out).catch(() => {});
  return out;
}

export async function GET(req: Request) {
  const sp = new URL(req.url).searchParams;
  const raw = sp.get("u") || "";
  let src: URL;
  try { src = new URL(raw); } catch { return new Response("bad url", { status: 400 }); }
  if (!allowed(src)) return new Response("host not allowed", { status: 400 });

  const wReq = Number(sp.get("w")) || 480;
  const w = WIDTHS.find((x) => x >= wReq) ?? WIDTHS[WIDTHS.length - 1];
  const r = Number(RATIOS.includes(sp.get("r") || "") ? sp.get("r") : "1");

  const key = createHash("sha1").update(`v1|${src.href}|${w}|${r}`).digest("hex");
  const file = path.join(CACHE_DIR, `${key}.webp`);
  const headers = {
    "Content-Type": "image/webp",
    "Cache-Control": "public, max-age=2592000, stale-while-revalidate=604800",
  };

  try {
    return new Response(new Uint8Array(await fs.readFile(file)), { headers });
  } catch { /* not cached yet */ }

  try {
    let job = inflight.get(key);
    if (!job) {
      job = render(src.href, w, r, file).finally(() => inflight.delete(key));
      inflight.set(key, job);
    }
    return new Response(new Uint8Array(await job), { headers });
  } catch {
    return Response.redirect(src.href, 302);
  }
}
