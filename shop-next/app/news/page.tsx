import type { Metadata } from "next";
import Link from "next/link";
import Image from "next/image";
import { Newspaper, Calendar } from "lucide-react";
import { fetchNews } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";

export const revalidate = 1800;

export const metadata: Metadata = {
  title: `Новости Magic Vibes — новинки парфюмерии и акции | ${SITE_NAME}`,
  description: "Последние новости Magic Vibes: новинки парфюмерии, специальные предложения, скидки и акции. Будьте в курсе лучших ароматов сезона.",
  alternates: { canonical: "/news" },
  openGraph: { title: "Новости — Magic Vibes", url: `${SITE_URL}/news` },
  keywords: "новости парфюмерия, новинки духов, скидки парфюм, акции интернет-магазин",
};

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

      <div style={{ background: S.bg, minHeight: "100vh" }}>
        <div style={{ maxWidth: 800, margin: "0 auto", padding: "64px clamp(18px,4vw,40px) 96px" }}>

          {/* Breadcrumb */}
          <nav style={{ fontSize: 12, color: S.muted, marginBottom: 40 }}>
            <Link href="/" style={{ color: S.muted, textDecoration: "none" }}>Главная</Link>
            {" / "}<span style={{ color: S.text }}>Новости</span>
          </nav>

          {/* Header */}
          <div style={{ textAlign: "center", marginBottom: 64 }}>
            <div style={{ display: "inline-flex", alignItems: "center", gap: 10, marginBottom: 20, padding: "6px 16px", borderRadius: 999, border: `1px solid ${S.borderMd}`, background: "rgba(201,169,110,0.06)" }}>
              <Newspaper size={13} style={{ color: S.accent }} />
              <span style={{ fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", color: S.accent }}>Новости</span>
            </div>
            <h1 style={{
              fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 600,
              fontSize: "clamp(28px,5vw,42px)", color: S.text, margin: "0 0 16px",
            }}>Magic Vibes</h1>
            <div style={{ width: 48, height: 1, background: S.accent, margin: "0 auto 16px" }} />
            <p style={{ fontSize: 15, color: S.muted, lineHeight: 1.7 }}>
              Акции, новинки и события из нашего Telegram-канала
            </p>
          </div>

          {posts.length === 0 ? (
            <div style={{ textAlign: "center", padding: "64px 0", color: S.muted }}>
              <Newspaper size={40} style={{ margin: "0 auto 16px", opacity: 0.3 }} />
              <p style={{ marginBottom: 20 }}>Новостей пока нет. Подпишитесь на наш Telegram-канал!</p>
              <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="btn-primary" style={{ display: "inline-flex" }}>
                Перейти в канал
              </a>
            </div>
          ) : (
            <>
              <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>
                {posts.map((post) => (
                  <article key={post.id} style={{
                    borderRadius: 16, border: `1px solid ${S.border}`,
                    background: S.surface, overflow: "hidden",
                    transition: "border-color 0.2s",
                  }}
                    onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = S.borderMd; }}
                    onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = S.border; }}
                  >
                    {post.photoUrl && (
                      <div style={{ width: "100%", aspectRatio: "16/9", overflow: "hidden", position: "relative" }}>
                        <Image src={post.photoUrl} alt="" fill sizes="(max-width: 768px) 100vw, 680px" style={{ objectFit: "cover" }} />
                      </div>
                    )}
                    <div style={{ padding: "24px 28px" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                        <Calendar size={13} style={{ color: S.subtle }} />
                        <time style={{ fontSize: 12, color: S.subtle }}>
                          {new Date(post.publishedAt).toLocaleDateString("ru-RU", { day: "numeric", month: "long", year: "numeric" })}
                        </time>
                      </div>
                      <p style={{ fontSize: 15, color: S.text, lineHeight: 1.8, margin: 0, whiteSpace: "pre-wrap" }}>
                        {post.text?.replace(/#\S+/g, "").trim()}
                      </p>
                    </div>
                  </article>
                ))}
              </div>

              {/* Telegram CTA */}
              <div style={{ textAlign: "center", marginTop: 56, padding: "40px 24px", borderRadius: 20, border: `1px solid ${S.border}`, background: S.surface }}>
                <p style={{ color: S.muted, marginBottom: 16, fontSize: 14 }}>Следите за обновлениями в Telegram</p>
                <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="btn-primary" style={{ display: "inline-flex" }}>
                  @magicvibes_ru
                </a>
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}
