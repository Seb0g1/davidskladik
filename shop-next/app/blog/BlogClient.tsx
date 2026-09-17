"use client";
import { useState } from "react";
import Link from "next/link";
import Image from "next/image";
import { useQuery } from "@tanstack/react-query";
import type { BlogPost } from "@/lib/types";

const S = {
  bg:      "#09090b",
  surface: "#111113",
  border:  "rgba(255,255,255,0.07)",
  gold:    "#c9a25e",
  goldDim: "rgba(201,162,94,0.18)",
  text:    "#f2ede6",
  muted:   "#7d7a73",
};

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";
const PAGE_SIZE = 9;

async function fetchBlogClient(params: { page: number; tag?: string }): Promise<{ posts: BlogPost[]; total: number }> {
  const sp = new URLSearchParams({ page: String(params.page), pageSize: String(PAGE_SIZE) });
  if (params.tag) sp.set("tag", params.tag);
  const res = await fetch(`${BASE}/api/shop/blog?${sp}`);
  if (!res.ok) throw new Error("Ошибка загрузки");
  return res.json();
}

function PostCard({ post }: { post: BlogPost }) {
  const date = post.publishedAt
    ? new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })
    : "";
  return (
    <Link href={`/blog/${post.slug}`} style={{ textDecoration: "none", display: "block" }}>
      <article style={{ border: `1px solid ${S.border}`, borderRadius: 4, overflow: "hidden", background: S.surface, transition: "border-color 0.3s, transform 0.4s cubic-bezier(.16,1,.3,1)", cursor: "pointer" }}
        onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,162,94,0.35)"; el.style.transform = "translateY(-3px)"; }}
        onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = S.border; el.style.transform = "none"; }}
      >
        {post.coverUrl && (
          <div style={{ height: 200, overflow: "hidden", position: "relative" }}>
            <Image src={post.coverUrl} alt={post.title} fill
              sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 400px"
              style={{ objectFit: "cover", transition: "transform 0.5s ease" }}
              onMouseEnter={e => (e.currentTarget.style.transform = "scale(1.04)")}
              onMouseLeave={e => (e.currentTarget.style.transform = "none")} />
          </div>
        )}
        <div style={{ padding: "22px 24px 24px" }}>
          {post.tags?.length > 0 && (
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 10 }}>
              {post.tags.slice(0, 3).map(tag => (
                <span key={tag} style={{ fontSize: 9, letterSpacing: "0.16em", textTransform: "uppercase", padding: "3px 8px", borderRadius: 2, border: `1px solid ${S.goldDim}`, color: S.gold, background: "rgba(201,162,94,0.06)" }}>{tag}</span>
              ))}
            </div>
          )}
          <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 400, fontSize: 20, color: S.text, lineHeight: 1.3, marginBottom: 10 }}>{post.title}</h2>
          {post.excerpt && (
            <p style={{ fontSize: 13, color: S.muted, lineHeight: 1.7, marginBottom: 14, display: "-webkit-box", WebkitLineClamp: 3, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{post.excerpt}</p>
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

function SkeletonCard() {
  return (
    <div style={{ height: 360, background: S.surface, borderRadius: 4, border: `1px solid ${S.border}`, animation: "pulse 1.5s ease-in-out infinite" }} />
  );
}

interface Props {
  initialPosts: BlogPost[];
  initialTotal: number;
  initialTags: string[];
}

export default function BlogClient({ initialPosts, initialTotal, initialTags }: Props) {
  const [tag, setTag] = useState("");
  const [page, setPage] = useState(1);

  const { data, isFetching } = useQuery({
    queryKey: ["shop-blog", tag, page],
    queryFn: () => fetchBlogClient({ page, tag: tag || undefined }),
    initialData: !tag && page === 1 ? { posts: initialPosts, total: initialTotal } : undefined,
    staleTime: 5 * 60_000,
  });

  const posts = data?.posts ?? [];
  const total = data?.total ?? 0;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const allTags = initialTags.length > 0 ? initialTags : [...new Set(posts.flatMap(p => p.tags ?? []))];

  return (
    <>
      {/* Tag filter */}
      {allTags.length > 0 && (
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 32, justifyContent: "center" }}>
          {["", ...allTags].map(t => (
            <button key={t || "__all"} onClick={() => { setTag(t); setPage(1); }} style={{
              fontSize: 10, letterSpacing: "0.16em", textTransform: "uppercase",
              padding: "5px 14px", borderRadius: 2, cursor: "pointer",
              border: `1px solid ${tag === t ? S.gold : S.border}`,
              background: tag === t ? "rgba(201,162,94,0.1)" : "transparent",
              color: tag === t ? S.gold : S.muted, fontFamily: "inherit",
            }}>
              {t || "Все"}
            </button>
          ))}
        </div>
      )}

      {/* Grid */}
      {isFetching ? (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 16 }}>
          {[1, 2, 3].map(i => <SkeletonCard key={i} />)}
        </div>
      ) : posts.length === 0 ? (
        <div style={{ textAlign: "center", padding: "80px 0" }}>
          <p style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontSize: 22, color: S.muted }}>
            {tag ? `Нет статей с тегом «${tag}»` : "Статьи скоро появятся"}
          </p>
        </div>
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 16 }}>
          {posts.map(post => <PostCard key={post.id} post={post} />)}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", gap: 8, marginTop: 48 }}>
          {Array.from({ length: totalPages }, (_, i) => i + 1).map(p => (
            <button key={p} onClick={() => setPage(p)} style={{
              width: 36, height: 36, borderRadius: 2, cursor: "pointer",
              border: `1px solid ${p === page ? S.gold : S.border}`,
              background: p === page ? "rgba(201,162,94,0.12)" : "transparent",
              color: p === page ? S.gold : S.muted,
              fontSize: 13, fontFamily: "'Cormorant Garamond',Georgia,serif",
            }}>{p}</button>
          ))}
        </div>
      )}
    </>
  );
}
