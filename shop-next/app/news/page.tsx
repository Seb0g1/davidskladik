import type { Metadata } from "next";
import Link from "next/link";
import { fetchNews } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

export const revalidate = 1800;

export const metadata: Metadata = {
  title: `Новости | ${SITE_NAME}`,
  description: "Новости и акции Magic Vibes — интернет-магазина оригинальной парфюмерии.",
  alternates: { canonical: "/news" },
};

export default async function NewsPage() {
  let posts: Awaited<ReturnType<typeof fetchNews>>["posts"] = [];
  try { const d = await fetchNews(24); posts = d.posts; } catch {}

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Новости", url: "/news" },
  ]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <div style={{ maxWidth: 800, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>Новости</span>
        </nav>
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,5vw,50px)", color: "#f5f4f0", margin: "0 0 36px" }}>Новости и акции</h1>

        {posts.length === 0 ? (
          <p style={{ color: "rgba(245,244,240,0.3)", textAlign: "center", padding: 60 }}>Новостей пока нет</p>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
            {posts.map(post => (
              <div key={post.id} style={{ border: "1px solid rgba(255,255,255,0.07)", borderRadius: 14, padding: "20px 24px", display: "flex", gap: 20 }}>
                {post.photoUrl && (
                  <div style={{ width: 80, height: 80, flexShrink: 0, borderRadius: 10, overflow: "hidden" }}>
                    <img src={post.photoUrl} alt="" style={{ width: "100%", height: "100%", objectFit: "cover" }} />
                  </div>
                )}
                <div style={{ flex: 1 }}>
                  <time style={{ fontSize: 11, color: "rgba(245,244,240,0.35)", display: "block", marginBottom: 10 }}>
                    {new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
                  </time>
                  <p style={{ fontSize: 14, color: "rgba(245,244,240,0.65)", lineHeight: 1.7, margin: 0, whiteSpace: "pre-wrap" }}>{post.text}</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </>
  );
}
