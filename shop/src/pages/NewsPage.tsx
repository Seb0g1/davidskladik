import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Newspaper, Calendar } from "lucide-react";

const S = {
  bg:      "#0E0D0B",
  surface: "#161512",
  surface2:"#1D1C18",
  border:  "rgba(255,252,245,0.07)",
  borderMd:"rgba(255,252,245,0.13)",
  text:    "#F4EFE6",
  muted:   "rgba(244,239,230,0.48)",
  subtle:  "rgba(244,239,230,0.22)",
  accent:  "#C9A96E",
  accent2: "#D9BF8F",
  accent3: "#EDD9B0",
};

interface NewsPost {
  id: string;
  text: string | null;
  photoUrl: string | null;
  publishedAt: string;
}

function formatDate(iso: string) {
  return new Date(iso).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" });
}

function cleanText(raw: string | null): string {
  if (!raw) return "";
  return raw.replace(/#\S+/g, "").trim();
}

export default function NewsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["shop-news"],
    queryFn: () => api.news(12),
    staleTime: 5 * 60_000,
  });

  const posts: NewsPost[] = (data as { posts?: NewsPost[] } | undefined)?.posts || [];

  return (
    <div style={{ background: S.bg, minHeight: "100vh" }}>
      <div style={{ maxWidth: 800, margin: "0 auto", padding: "64px 24px 96px" }}>

        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 64 }}>
          <div style={{ display: "inline-flex", alignItems: "center", gap: 10, marginBottom: 20, padding: "6px 16px", borderRadius: 999, border: `1px solid ${S.borderMd}`, background: "rgba(201,169,110,0.06)" }}>
            <Newspaper size={13} style={{ color: S.accent }} />
            <span style={{ fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", color: S.accent }}>Новости</span>
          </div>
          <h1 className="serif" style={{ fontSize: "clamp(28px,5vw,42px)", fontStyle: "italic", fontWeight: 600, color: S.text, margin: "0 0 16px" }}>
            Magic Vibes
          </h1>
          <div style={{ width: 48, height: 1, background: S.accent, margin: "0 auto 16px" }} />
          <p style={{ fontSize: 15, color: S.muted, lineHeight: 1.7 }}>
            Акции, новинки и события из нашего Telegram-канала
          </p>
        </div>

        {/* Loading */}
        {isLoading && (
          <div style={{ display: "flex", flexDirection: "column", gap: 24 }}>
            {Array.from({ length: 3 }).map((_, i) => (
              <div key={i} style={{ borderRadius: 16, border: `1px solid ${S.border}`, background: S.surface, overflow: "hidden", opacity: 0.5 }}>
                <div style={{ height: 200, background: S.surface2 }} />
                <div style={{ padding: "24px 28px" }}>
                  <div style={{ height: 14, width: "30%", background: S.surface2, borderRadius: 4, marginBottom: 12 }} />
                  <div style={{ height: 16, width: "90%", background: S.surface2, borderRadius: 4, marginBottom: 8 }} />
                  <div style={{ height: 16, width: "70%", background: S.surface2, borderRadius: 4 }} />
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Empty */}
        {!isLoading && !posts.length && (
          <div style={{ textAlign: "center", padding: "64px 0", color: S.muted }}>
            <Newspaper size={40} style={{ margin: "0 auto 16px", opacity: 0.3 }} />
            <p>Новостей пока нет. Подпишитесь на наш Telegram-канал!</p>
            <a
              href="https://t.me/magicvibes_ru"
              target="_blank"
              rel="noopener noreferrer"
              className="btn-primary"
              style={{ display: "inline-flex", marginTop: 20 }}
            >
              Перейти в канал
            </a>
          </div>
        )}

        {/* Posts */}
        {posts.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>
            {posts.map((post) => (
              <article
                key={post.id}
                style={{
                  borderRadius: 16,
                  border: `1px solid ${S.border}`,
                  background: S.surface,
                  overflow: "hidden",
                  transition: "border-color 0.2s",
                }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = S.borderMd; }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = S.border; }}
              >
                {post.photoUrl && (
                  <div style={{ width: "100%", maxHeight: 360, overflow: "hidden" }}>
                    <img
                      src={post.photoUrl}
                      alt=""
                      loading="lazy"
                      style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}
                    />
                  </div>
                )}
                <div style={{ padding: "24px 28px" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                    <Calendar size={13} style={{ color: S.subtle }} />
                    <span style={{ fontSize: 12, color: S.subtle }}>{formatDate(post.publishedAt)}</span>
                  </div>
                  <p style={{ fontSize: 15, color: S.text, lineHeight: 1.8, margin: 0, whiteSpace: "pre-wrap" }}>
                    {cleanText(post.text)}
                  </p>
                </div>
              </article>
            ))}
          </div>
        )}

        {/* Telegram CTA */}
        {posts.length > 0 && (
          <div style={{ textAlign: "center", marginTop: 56, padding: "40px 24px", borderRadius: 20, border: `1px solid ${S.border}`, background: S.surface }}>
            <p style={{ color: S.muted, marginBottom: 16, fontSize: 14 }}>Следите за обновлениями в Telegram</p>
            <a
              href="https://t.me/magicvibes_ru"
              target="_blank"
              rel="noopener noreferrer"
              className="btn-primary"
              style={{ display: "inline-flex" }}
            >
              @magicvibes_ru
            </a>
          </div>
        )}
      </div>
    </div>
  );
}
