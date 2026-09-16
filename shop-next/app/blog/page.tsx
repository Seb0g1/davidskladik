import type { Metadata } from "next";
import Link from "next/link";
import { fetchBlog } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

export const revalidate = 600;

export const metadata: Metadata = {
  title: `Блог о парфюмерии | ${SITE_NAME}`,
  description: "Статьи о парфюмерии: гиды по выбору аромата, обзоры брендов, нотные пирамиды, тренды и советы от Magic Vibes.",
  alternates: { canonical: "/blog" },
  openGraph: { title: "Блог о парфюмерии — Magic Vibes", url: `${SITE_URL}/blog` },
};

export default async function BlogPage() {
  let posts: Awaited<ReturnType<typeof fetchBlog>>["posts"] = [];
  try { const d = await fetchBlog({ pageSize: 20 }); posts = d.posts; } catch {}

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Блог", url: "/blog" },
  ]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <div style={{ maxWidth: 960, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> / <span style={{ color: "#f5f4f0" }}>Блог</span>
        </nav>

        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(32px,5vw,56px)", color: "#f5f4f0", margin: "0 0 40px" }}>Блог о парфюмерии</h1>

        {posts.length === 0 ? (
          <p style={{ color: "rgba(245,244,240,0.35)", fontSize: 15, textAlign: "center", padding: 60 }}>Статьи скоро появятся</p>
        ) : (
          <div style={{ display: "grid", gap: 24, gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))" }}>
            {posts.map(post => (
              <Link key={post.id} href={`/blog/${post.slug}`} style={{ border: "1px solid rgba(255,255,255,0.07)", borderRadius: 14, overflow: "hidden", textDecoration: "none", display: "flex", flexDirection: "column" }}>
                {post.coverUrl && (
                  <div style={{ aspectRatio: "16/9", overflow: "hidden" }}>
                    <img src={post.coverUrl} alt={post.title} loading="lazy" style={{ width: "100%", height: "100%", objectFit: "cover" }} />
                  </div>
                )}
                <div style={{ padding: "20px 20px 24px", flex: 1, display: "flex", flexDirection: "column", gap: 10 }}>
                  {post.tags.length > 0 && (
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {post.tags.slice(0, 3).map(t => <span key={t} style={{ fontSize: 10, padding: "3px 8px", borderRadius: 20, border: "1px solid rgba(201,162,94,0.25)", color: "rgba(201,162,94,0.7)", letterSpacing: "0.1em" }}>{t}</span>)}
                    </div>
                  )}
                  <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 20, fontWeight: 300, color: "#f5f4f0", margin: 0, lineHeight: 1.3 }}>{post.title}</h2>
                  {post.excerpt && <p style={{ fontSize: 13, color: "rgba(245,244,240,0.45)", margin: 0, lineHeight: 1.7, display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{post.excerpt}</p>}
                  <span style={{ fontSize: 11, color: "rgba(201,162,94,0.6)", marginTop: "auto", letterSpacing: "0.12em" }}>Читать →</span>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </>
  );
}
