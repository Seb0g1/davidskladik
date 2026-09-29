// robots.txt as a route: Next's MetadataRoute.Robots cannot emit Yandex's Clean-param.
import { SITE_URL } from "@/lib/seo";

export const dynamic = "force-static";

const PRIVATE = ["/checkout", "/track/", "/callback/", "/cart", "/account", "/orders", "/order-success", "/order-failed", "/admin", "/login", "/studio", "/api/", "/*?*sort=", "/*?*inStock=", "/*?*view="];

export function GET() {
  const disallow = PRIVATE.map((p) => `Disallow: ${p}`).join("\n");
  const body = `User-agent: *
Allow: /
${disallow}

User-agent: Yandex
Allow: /
${disallow}
Clean-param: utm_source&utm_medium&utm_campaign&utm_content&utm_term&yclid&gclid&fbclid&ref&from /
Clean-param: sort&view&inStock /catalog

Sitemap: ${SITE_URL}/sitemap.xml
`;
  return new Response(body, { headers: { "Content-Type": "text/plain; charset=utf-8" } });
}
