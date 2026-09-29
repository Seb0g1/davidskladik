"use client";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { XCircle, ShoppingCart, RefreshCw } from "lucide-react";
import { Suspense } from "react";

function OrderFailedContent() {
  const params = useSearchParams();
  const orderId = params.get("id");

  return (
    <div style={{ background: "var(--surface)", minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ textAlign: "center", padding: "64px 24px", maxWidth: 420, width: "100%" }}>
        <div style={{
          width: 80, height: 80, borderRadius: 28, display: "flex", alignItems: "center", justifyContent: "center",
          margin: "0 auto 28px", background: "rgba(var(--danger-rgb),0.1)", border: "1px solid rgba(var(--danger-rgb),0.2)",
          boxShadow: "0 0 40px rgba(var(--danger-rgb),0.1)",
        }}>
          <XCircle size={40} style={{ color: "var(--danger)" }} strokeWidth={1.5} />
        </div>

        <h1 style={{ fontSize: 26, fontWeight: 700, color: "var(--ink)", letterSpacing: "-0.04em", marginBottom: 10 }}>
          Оплата не прошла
        </h1>
        {orderId && (
          <p style={{ fontSize: 13, color: "rgba(var(--ink-rgb),0.57)", marginBottom: 6 }}>
            Заказ <span style={{ fontFamily: "monospace", fontWeight: 700, color: "var(--accent)" }}>{orderId}</span> сохранён
          </p>
        )}
        <p style={{ fontSize: 13, color: "rgba(var(--ink-rgb),0.52)", lineHeight: 1.7, marginBottom: 40 }}>
          Платёж был отклонён или отменён.<br />
          Попробуйте ещё раз — заказ не потерян.
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 10, alignItems: "center" }}>
          <Link href="/checkout" style={{
            display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%",
            padding: "13px 32px", borderRadius: 14,
            background: "var(--accent)", color: "var(--surface)",
            textDecoration: "none", fontSize: 14, fontWeight: 600, letterSpacing: "0.06em",
          }}>
            <RefreshCw size={16} /> Попробовать снова
          </Link>
          <Link href="/cart" style={{
            display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%",
            padding: "11px 24px", borderRadius: 14,
            background: "rgba(var(--ink-rgb),0.032)", border: "1px solid rgba(var(--ink-rgb),0.064)", color: "var(--ink)",
            textDecoration: "none", fontSize: 13,
          }}>
            <ShoppingCart size={16} /> В корзину
          </Link>
        </div>
      </div>
    </div>
  );
}

export default function OrderFailedPage() {
  return (
    <Suspense fallback={<div style={{ background: "var(--surface)", minHeight: "100vh" }} />}>
      <OrderFailedContent />
    </Suspense>
  );
}
