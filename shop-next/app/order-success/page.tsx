"use client";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { CheckCircle, ShoppingBag, Package } from "lucide-react";
import { Suspense } from "react";

function OrderSuccessContent() {
  const params = useSearchParams();
  const orderId = params.get("id");

  return (
    <div style={{ background: "#0E0D0B", minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ textAlign: "center", padding: "64px 24px", maxWidth: 420, width: "100%" }}>
        <div style={{
          width: 80, height: 80, borderRadius: 28, display: "flex", alignItems: "center", justifyContent: "center",
          margin: "0 auto 28px", background: "rgba(74,222,128,0.1)", border: "1px solid rgba(74,222,128,0.2)",
        }}>
          <CheckCircle size={40} style={{ color: "#4ade80" }} strokeWidth={1.5} />
        </div>

        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontSize: 26, fontWeight: 500, color: "#F4EFE6",
          letterSpacing: "-0.01em", marginBottom: 10, fontStyle: "italic",
        }}>
          Заказ оформлен
        </h1>
        {orderId && (
          <p style={{ fontSize: 13, color: "rgba(244,239,230,0.5)", marginBottom: 6 }}>
            Номер заказа:{" "}
            <span style={{ fontFamily: "monospace", fontWeight: 700, color: "#C9A96E" }}>{orderId}</span>
          </p>
        )}
        <p style={{ fontSize: 13, color: "rgba(244,239,230,0.45)", lineHeight: 1.7, marginBottom: 40 }}>
          Подтверждение придёт на вашу почту.<br />
          Доставка через Ozon, 1–5 рабочих дней.
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 10, alignItems: "center" }}>
          <Link href="/catalog" style={{
            display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%",
            padding: "13px 32px", borderRadius: 3,
            background: "#C9A96E", color: "#0E0D0B",
            textDecoration: "none", fontSize: 14, fontWeight: 600, letterSpacing: "0.06em",
          }}>
            <ShoppingBag size={16} /> Продолжить покупки
          </Link>
          <Link href="/orders" style={{
            display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%",
            padding: "11px 24px", borderRadius: 3,
            background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)", color: "#F4EFE6",
            textDecoration: "none", fontSize: 13,
          }}>
            <Package size={14} /> Мои заказы
          </Link>
        </div>
      </div>
    </div>
  );
}

export default function OrderSuccessPage() {
  return (
    <Suspense fallback={<div style={{ background: "#0E0D0B", minHeight: "100vh" }} />}>
      <OrderSuccessContent />
    </Suspense>
  );
}
