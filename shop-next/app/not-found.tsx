import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Страница не найдена | Magic Vibes",
  robots: { index: false, follow: false },
};

export default function NotFound() {
  return (
    <div style={{ minHeight: "60vh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", textAlign: "center", padding: "60px 24px" }}>
      <div style={{ fontFamily: "var(--font-display)", fontWeight: 300, fontSize: "clamp(80px,14vw,160px)", color: "rgba(var(--accent-rgb),0.18)", lineHeight: 1, marginBottom: 24 }}>404</div>
      <h1 style={{ fontFamily: "var(--font-display)", fontWeight: 300, fontSize: "clamp(24px,4vw,40px)", color: "var(--ink)", margin: "0 0 16px" }}>Страница не найдена</h1>
      <p style={{ fontSize: 14, color: "rgba(var(--ink-rgb),0.46)", maxWidth: 400, lineHeight: 1.7, marginBottom: 36 }}>Возможно, страница была перемещена или удалена. Вернитесь на главную или перейдите в каталог.</p>
      <div style={{ display: "flex", gap: 12 }}>
        <Link href="/" className="btn-primary" style={{ textDecoration: "none" }}>На главную</Link>
        <Link href="/catalog" className="btn-ghost" style={{ textDecoration: "none" }}>Каталог</Link>
      </div>
    </div>
  );
}
