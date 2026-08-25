import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import ProductCard from "../components/ProductCard";
import { Link } from "react-router-dom";
import { ArrowLeft } from "lucide-react";

function CardSkeleton() {
  return (
    <div className="product-card">
      <div className="skeleton" style={{ aspectRatio: "4/5" }} />
      <div style={{ padding: "10px 12px 12px" }}>
        <div className="skeleton" style={{ height: 8, width: "40%", marginBottom: 6 }} />
        <div className="skeleton" style={{ height: 11, marginBottom: 4 }} />
        <div className="skeleton" style={{ height: 11, width: "70%", marginBottom: 8 }} />
        <div className="skeleton" style={{ height: 14, width: "50%" }} />
      </div>
    </div>
  );
}

export default function NewProductsPage() {
  const { data, isLoading } = useQuery({
    queryKey: ["shop-new"],
    queryFn: () => api.newProducts(),
    staleTime: 5 * 60_000,
  });

  const products = data?.products ?? [];

  return (
    <div style={{ background: "var(--bg)", minHeight: "100vh", paddingBottom: 80 }}>
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(28px,4vw,48px) clamp(16px,4vw,48px)" }}>

        {/* Back */}
        <Link to="/" style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 13, color: "var(--muted)", textDecoration: "none", marginBottom: 32, transition: "color 0.15s" }}
          onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
          onMouseLeave={e => (e.currentTarget.style.color = "var(--muted)")}
        >
          <ArrowLeft size={14} strokeWidth={1.7} /> Главная
        </Link>

        {/* Header */}
        <div style={{ marginBottom: 40 }}>
          <p style={{ fontSize: 10, fontWeight: 600, letterSpacing: "0.18em", textTransform: "uppercase", color: "var(--accent)", marginBottom: 10 }}>
            Свежие поступления
          </p>
          <h1 className="serif" style={{ fontSize: "clamp(36px,5vw,64px)", fontWeight: 500, letterSpacing: "-0.02em", lineHeight: 1, fontStyle: "italic", color: "var(--text)" }}>
            Новинки
          </h1>
          {!isLoading && products.length > 0 && (
            <p style={{ fontSize: 13, color: "var(--muted)", marginTop: 12 }}>
              {products.length} товаров за последние 14 дней
            </p>
          )}
        </div>

        {/* Divider */}
        <div style={{ height: 1, background: "var(--border)", marginBottom: 32 }} />

        {/* Grid */}
        {isLoading ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 14 }}>
            {Array.from({ length: 16 }).map((_, i) => <CardSkeleton key={i} />)}
          </div>
        ) : products.length === 0 ? (
          <div style={{ textAlign: "center", padding: "80px 0" }}>
            <p className="serif" style={{ fontSize: 28, fontWeight: 500, color: "var(--muted)", fontStyle: "italic", marginBottom: 12 }}>Пока пусто</p>
            <p style={{ fontSize: 14, color: "var(--subtle)" }}>Новинки появятся в ближайшее время</p>
            <Link to="/catalog" className="btn-ghost" style={{ marginTop: 24, textDecoration: "none", display: "inline-flex" }}>
              Смотреть каталог
            </Link>
          </div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 14 }}>
            {products.map((p) => (
              <ProductCard key={p.offerId} product={p} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
