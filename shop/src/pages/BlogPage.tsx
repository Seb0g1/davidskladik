import { useState } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import type { BlogPost } from "../types";

const S = {
  bg:      "#09090b",
  surface: "#111113",
  border:  "rgba(255,255,255,0.07)",
  gold:    "#c9a25e",
  goldDim: "rgba(201,162,94,0.18)",
  text:    "#f2ede6",
  muted:   "#7d7a73",
};

function PostCard({ post }: { post: BlogPost }) {
  const date = post.publishedAt
    ? new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })
    : "";

  return (
    <Link
      to={`/blog/${post.slug}`}
      style={{ textDecoration: "none", display: "block" }}
    >
      <article style={{
        border: `1px solid ${S.border}`, borderRadius: 4, overflow: "hidden",
        background: S.surface, transition: "border-color 0.3s, transform 0.4s cubic-bezier(.16,1,.3,1)",
        cursor: "pointer",
      }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.35)";
          (e.currentTarget as HTMLElement).style.transform = "translateY(-3px)";
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLElement).style.borderColor = S.border;
          (e.currentTarget as HTMLElement).style.transform = "none";
        }}
      >
        {post.coverUrl && (
          <div style={{ height: 200, overflow: "hidden" }}>
            <img
              src={post.coverUrl}
              alt={post.title}
              style={{ width: "100%", height: "100%", objectFit: "cover", display: "block", transition: "transform 0.5s ease" }}
              onMouseEnter={(e) => (e.currentTarget.style.transform = "scale(1.04)")}
              onMouseLeave={(e) => (e.currentTarget.style.transform = "none")}
            />
          </div>
        )}
        <div style={{ padding: "22px 24px 24px" }}>
          {post.tags?.length > 0 && (
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 10 }}>
              {post.tags.slice(0, 3).map((tag) => (
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
          <h2 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 400, fontSize: 20, color: S.text, lineHeight: 1.3, marginBottom: 10 }}>
            {post.title}
          </h2>
          {post.excerpt && (
            <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.7, marginBottom: 14, display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical", overflow: "hidden" }}>
              {post.excerpt}
            </p>
          )}
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            {date && <span style={{ fontSize: 11, color: S.muted }}>{date}</span>}
            <span style={{ fontSize: 12, color: S.gold, fontStyle: "italic" }}>Читать →</span>
          </div>
        </div>
      </article>
    </Link>
  );
}

export default function BlogPage() {
  const [tag, setTag] = useState("");
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 9;

  const { data, isLoading } = useQuery({
    queryKey: ["shop-blog", tag, page],
    queryFn: () => api.blog({ page, pageSize: PAGE_SIZE, tag: tag || undefined }),
    staleTime: 5 * 60_000,
  });

  const posts = data?.posts || [];
  const total = data?.total || 0;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  // Collect all tags from loaded posts for filter
  const allTags = [...new Set(posts.flatMap((p) => p.tags || []))];

  return (
    <div style={{ background: S.bg, minHeight: "100vh", padding: "48px 0 80px" }}>
      <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 clamp(18px,5vw,60px)" }}>

        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 48 }}>
          <p style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontSize: 13, color: S.gold, letterSpacing: "0.04em", marginBottom: 12 }}>
            Мир ароматов
          </p>
          <h1 style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(36px,6vw,72px)", color: S.text, lineHeight: 0.9, marginBottom: 20 }}>
            Блог Magic Vibes
          </h1>
          <div style={{ width: "min(300px,50%)", height: 1, margin: "0 auto", background: "linear-gradient(90deg, transparent, rgba(201,162,94,0.6), transparent)" }} />
        </div>

        {/* Tag filter */}
        {allTags.length > 0 && (
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 32, justifyContent: "center" }}>
            <button
              onClick={() => { setTag(""); setPage(1); }}
              style={{
                fontSize: 10, letterSpacing: "0.16em", textTransform: "uppercase",
                padding: "5px 14px", borderRadius: 2, cursor: "pointer",
                border: `1px solid ${tag === "" ? S.gold : S.border}`,
                background: tag === "" ? "rgba(201,162,94,0.1)" : "transparent",
                color: tag === "" ? S.gold : S.muted,
              }}
            >
              Все
            </button>
            {allTags.map((t) => (
              <button
                key={t}
                onClick={() => { setTag(t); setPage(1); }}
                style={{
                  fontSize: 10, letterSpacing: "0.16em", textTransform: "uppercase",
                  padding: "5px 14px", borderRadius: 2, cursor: "pointer",
                  border: `1px solid ${tag === t ? S.gold : S.border}`,
                  background: tag === t ? "rgba(201,162,94,0.1)" : "transparent",
                  color: tag === t ? S.gold : S.muted,
                }}
              >
                {t}
              </button>
            ))}
          </div>
        )}

        {/* Grid */}
        {isLoading ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 16 }}>
            {[1, 2, 3].map((i) => (
              <div key={i} style={{ height: 360, background: S.surface, borderRadius: 4, border: `1px solid ${S.border}`, animation: "pulse 1.5s ease-in-out infinite" }} />
            ))}
          </div>
        ) : posts.length === 0 ? (
          <div style={{ textAlign: "center", padding: "80px 0" }}>
            <p style={{ fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontSize: 22, color: S.muted }}>
              {tag ? `Нет статей с тегом «${tag}»` : "Статьи скоро появятся"}
            </p>
          </div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 16 }}>
            {posts.map((post) => <PostCard key={post.id} post={post} />)}
          </div>
        )}

        {/* Pagination */}
        {totalPages > 1 && (
          <div style={{ display: "flex", justifyContent: "center", gap: 8, marginTop: 48 }}>
            {Array.from({ length: totalPages }, (_, i) => i + 1).map((p) => (
              <button
                key={p}
                onClick={() => setPage(p)}
                style={{
                  width: 36, height: 36, borderRadius: 2, cursor: "pointer",
                  border: `1px solid ${p === page ? S.gold : S.border}`,
                  background: p === page ? "rgba(201,162,94,0.12)" : "transparent",
                  color: p === page ? S.gold : S.muted,
                  fontSize: 13, fontFamily: "'Cormorant Garamond', Georgia, serif",
                }}
              >
                {p}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
