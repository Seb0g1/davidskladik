import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft } from "lucide-react";
import { api } from "../api";

const S = {
  bg:     "#09090b",
  surface:"#111113",
  border: "rgba(255,255,255,0.07)",
  gold:   "#c9a25e",
  goldDim:"rgba(201,162,94,0.18)",
  text:   "#f2ede6",
  muted:  "#7d7a73",
};

export default function BlogPostPage() {
  const { slug } = useParams<{ slug: string }>();

  const { data, isLoading, isError } = useQuery({
    queryKey: ["shop-blog-post", slug],
    queryFn: () => api.blogPost(slug!),
    staleTime: 10 * 60_000,
    enabled: Boolean(slug),
  });

  const post = data?.post;

  if (isLoading) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ width: 32, height: 32, borderRadius: "50%", border: `2px solid ${S.goldDim}`, borderTopColor: S.gold, animation: "spin 0.8s linear infinite" }} />
      </div>
    );
  }

  if (isError || !post) {
    return (
      <div style={{ background: S.bg, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
        <div style={{ textAlign: "center" }}>
          <p style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontSize: 24, color: S.muted, marginBottom: 20 }}>
            Статья не найдена
          </p>
          <Link to="/blog" style={{ color: S.gold, fontSize: 14, textDecoration: "none" }}>← В блог</Link>
        </div>
      </div>
    );
  }

  const date = post.publishedAt
    ? new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })
    : "";

  return (
    <div style={{ background: S.bg, minHeight: "100vh", padding: "40px 0 80px" }}>
      <div style={{ maxWidth: 760, margin: "0 auto", padding: "0 clamp(18px,5vw,40px)" }}>

        {/* Back link */}
        <Link to="/blog" style={{ display: "inline-flex", alignItems: "center", gap: 6, color: S.muted, textDecoration: "none", fontSize: 13, marginBottom: 32 }}>
          <ChevronLeft size={16} />
          Все статьи
        </Link>

        {/* Tags */}
        {post.tags?.length > 0 && (
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 16 }}>
            {post.tags.map((tag) => (
              <span key={tag} style={{
                fontSize: 9, letterSpacing: "0.16em", textTransform: "uppercase",
                padding: "3px 8px", borderRadius: 2,
                border: `1px solid ${S.goldDim}`, color: S.gold, background: "rgba(201,162,94,0.06)",
              }}>
                {tag}
              </span>
            ))}
          </div>
        )}

        {/* Title */}
        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300,
          fontSize: "clamp(28px,5vw,52px)", color: S.text, lineHeight: 1.1, marginBottom: 16,
        }}>
          {post.title}
        </h1>

        {/* Date */}
        {date && (
          <p style={{ fontSize: 12, color: S.muted, marginBottom: 32 }}>{date}</p>
        )}

        {/* Divider */}
        <div style={{ height: 1, background: "linear-gradient(90deg, transparent, rgba(201,162,94,0.4), transparent)", marginBottom: 36 }} />

        {/* Cover image */}
        {post.coverUrl && (
          <div style={{ borderRadius: 4, overflow: "hidden", marginBottom: 36, border: `1px solid ${S.border}` }}>
            <img src={post.coverUrl} alt={post.title} style={{ width: "100%", display: "block", maxHeight: 440, objectFit: "cover" }} />
          </div>
        )}

        {/* Excerpt */}
        {post.excerpt && (
          <p style={{
            fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic",
            fontSize: 18, color: "rgba(242,237,230,0.75)", lineHeight: 1.65,
            marginBottom: 28, borderLeft: `2px solid ${S.gold}`, paddingLeft: 16,
          }}>
            {post.excerpt}
          </p>
        )}

        {/* Content — render as HTML if contains tags, otherwise plain paragraphs */}
        {post.content && (
          <div
            style={{ color: "rgba(242,237,230,0.82)", fontSize: 15, lineHeight: 1.8 }}
            className="blog-content"
            dangerouslySetInnerHTML={{ __html: post.content }}
          />
        )}

        {/* Footer */}
        <div style={{ marginTop: 56, paddingTop: 24, borderTop: `1px solid ${S.border}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <Link to="/blog" style={{ display: "inline-flex", alignItems: "center", gap: 6, color: S.muted, textDecoration: "none", fontSize: 13 }}>
            <ChevronLeft size={16} />
            Все статьи
          </Link>
          <Link to="/catalog" style={{ fontSize: 13, color: S.gold, textDecoration: "none", fontStyle: "italic" }}>
            Перейти в каталог →
          </Link>
        </div>
      </div>
    </div>
  );
}
