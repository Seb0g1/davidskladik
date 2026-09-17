import type { Metadata } from "next";
import Link from "next/link";
import { BookOpen } from "lucide-react";
import { fetchBlog } from "@/lib/api";
import { breadcrumbJsonLd, SITE_URL, SITE_NAME } from "@/lib/seo";
import BlogClient from "./BlogClient";

export const revalidate = 600;

export const metadata: Metadata = {
  title: `Блог о парфюмерии | ${SITE_NAME}`,
  description: "Статьи о парфюмерии: гиды по выбору аромата, обзоры брендов, нотные пирамиды, тренды и советы от Magic Vibes.",
  alternates: { canonical: "/blog" },
  openGraph: { title: "Блог о парфюмерии — Magic Vibes", url: `${SITE_URL}/blog` },
  keywords: "блог парфюмерия, статьи о духах, обзоры ароматов, гид по парфюму",
};

const S = {
  bg:     "#09090b",
  border: "rgba(255,255,255,0.07)",
  text:   "#f2ede6",
  muted:  "rgba(242,237,230,0.45)",
  gold:   "#c9a25e",
};

export default async function BlogPage() {
  let initialPosts: Awaited<ReturnType<typeof fetchBlog>>["posts"] = [];
  let initialTotal = 0;
  try {
    const d = await fetchBlog({ page: 1, pageSize: 9 });
    initialPosts = d.posts;
    initialTotal = d.total;
  } catch {}

  const allTags = [...new Set(initialPosts.flatMap(p => p.tags ?? []))];

  const breadcrumb = breadcrumbJsonLd([
    { name: "Главная", url: "/" },
    { name: "Блог", url: "/blog" },
  ]);

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(breadcrumb) }} />

      <div style={{ background: S.bg, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1080, margin: "0 auto", padding: "clamp(24px,3vw,56px) clamp(18px,4vw,56px)" }}>
          {/* Breadcrumb */}
          <nav style={{ fontSize: 12, color: S.muted, marginBottom: 40 }}>
            <Link href="/" style={{ color: S.muted, textDecoration: "none" }}>Главная</Link>
            {" / "}<span style={{ color: S.text }}>Блог</span>
          </nav>

          {/* Header */}
          <div style={{ textAlign: "center", marginBottom: 56 }}>
            <div style={{ display: "inline-flex", alignItems: "center", gap: 10, marginBottom: 20, padding: "6px 18px", borderRadius: 999, border: `1px solid rgba(201,162,94,0.22)`, background: "rgba(201,162,94,0.05)" }}>
              <BookOpen size={13} style={{ color: S.gold }} />
              <span style={{ fontSize: 11, letterSpacing: "0.14em", textTransform: "uppercase", color: S.gold }}>Журнал</span>
            </div>
            <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 400, fontSize: "clamp(32px,5vw,52px)", color: S.text, margin: "0 0 14px", lineHeight: 1.15 }}>
              Блог о парфюмерии
            </h1>
            <div style={{ width: 48, height: 1, background: S.gold, margin: "0 auto 14px" }} />
            <p style={{ fontSize: 15, color: S.muted, lineHeight: 1.7, maxWidth: 480, margin: "0 auto" }}>
              Гиды, обзоры, нотные пирамиды и советы от команды Magic Vibes
            </p>
          </div>

          <BlogClient
            initialPosts={initialPosts}
            initialTotal={initialTotal}
            initialTags={allTags}
          />
        </div>
      </div>
    </>
  );
}
