import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { fetchBlogPost } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

interface Props {
  params: Promise<{ slug: string }>;
}

export const revalidate = 600;

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { slug } = await params;
  try {
    const { post } = await fetchBlogPost(decodeURIComponent(slug));
    return {
      title: `${post.title} | ${SITE_NAME}`,
      description: post.excerpt ?? post.title,
      alternates: { canonical: `/blog/${post.slug}` },
      openGraph: {
        title: post.title,
        description: post.excerpt ?? undefined,
        url: `${SITE_URL}/blog/${post.slug}`,
        type: "article",
        images: post.coverUrl ? [{ url: post.coverUrl, alt: post.title }] : [],
      },
    };
  } catch {
    return { title: "Статья не найдена" };
  }
}

export default async function BlogPostPage({ params }: Props) {
  const { slug } = await params;
  let post: Awaited<ReturnType<typeof fetchBlogPost>>["post"];
  try {
    const data = await fetchBlogPost(decodeURIComponent(slug));
    post = data.post;
  } catch {
    notFound();
  }

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Блог", url: "/blog" },
    { name: post.title, url: `/blog/${post.slug}` },
  ]);

  const articleLd = {
    "@context": "https://schema.org",
    "@type": "Article",
    headline: post.title,
    description: post.excerpt ?? undefined,
    image: post.coverUrl ?? undefined,
    datePublished: post.publishedAt ?? post.createdAt,
    publisher: { "@type": "Organization", name: SITE_NAME, logo: `${SITE_URL}/favicon.svg` },
    url: `${SITE_URL}/blog/${post.slug}`,
  };

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(articleLd) }} />

      <article style={{ maxWidth: 760, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
        <nav style={{ fontSize: 12, color: "rgba(245,244,240,0.45)", marginBottom: 24 }}>
          <Link href="/" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Главная</Link> /{" "}
          <Link href="/blog" style={{ color: "rgba(245,244,240,0.45)", textDecoration: "none" }}>Блог</Link> /{" "}
          <span style={{ color: "#f5f4f0" }}>{post.title}</span>
        </nav>

        {post.tags.length > 0 && (
          <div style={{ display: "flex", gap: 6, marginBottom: 20, flexWrap: "wrap" }}>
            {post.tags.map(t => <span key={t} style={{ fontSize: 10, padding: "3px 10px", borderRadius: 20, border: "1px solid rgba(201,162,94,0.25)", color: "rgba(201,162,94,0.7)", letterSpacing: "0.1em" }}>{t}</span>)}
          </div>
        )}

        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,5vw,52px)", color: "#f5f4f0", margin: "0 0 24px", lineHeight: 1.1 }}>{post.title}</h1>

        {(post.publishedAt || post.createdAt) && (
          <time dateTime={post.publishedAt ?? post.createdAt} style={{ fontSize: 12, color: "rgba(245,244,240,0.35)", display: "block", marginBottom: 32 }}>
            {new Date(post.publishedAt ?? post.createdAt!).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
          </time>
        )}

        {post.coverUrl && (
          <div style={{ borderRadius: 16, overflow: "hidden", marginBottom: 36, aspectRatio: "16/9" }}>
            <img src={post.coverUrl} alt={post.title} style={{ width: "100%", height: "100%", objectFit: "cover" }} />
          </div>
        )}

        {post.content ? (
          <div className="blog-content" style={{ fontSize: 15, lineHeight: 1.9, color: "rgba(245,244,240,0.7)" }} dangerouslySetInnerHTML={{ __html: post.content }} />
        ) : post.excerpt ? (
          <p style={{ fontSize: 15, lineHeight: 1.9, color: "rgba(245,244,240,0.7)", margin: 0 }}>{post.excerpt}</p>
        ) : null}

        <div style={{ marginTop: 48, paddingTop: 24, borderTop: "1px solid rgba(255,255,255,0.07)" }}>
          <Link href="/blog" style={{ fontSize: 12, color: "rgba(201,162,94,0.7)", textDecoration: "none", letterSpacing: "0.1em" }}>← Все статьи</Link>
        </div>
      </article>
    </>
  );
}
