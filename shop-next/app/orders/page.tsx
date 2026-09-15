"use client";
import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { Check } from "lucide-react";
import { useAuth } from "@/components/AuthContext";
import AccountPage from "@/app/account/page";

function OrdersContent() {
  const sp = useSearchParams();
  const { customer } = useAuth();
  const success = sp.get("success");

  if (success) {
    return (
      <div style={{ minHeight: "60vh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", textAlign: "center", padding: 40 }}>
        <div style={{ width: 72, height: 72, borderRadius: "50%", background: "rgba(134,239,172,0.12)", border: "1px solid rgba(134,239,172,0.3)", display: "flex", alignItems: "center", justifyContent: "center", marginBottom: 24 }}>
          <Check size={32} style={{ color: "#86efac" }} />
        </div>
        <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(28px,4vw,44px)", color: "#f5f4f0", margin: "0 0 16px" }}>Заказ оформлен!</h1>
        <p style={{ fontSize: 14, color: "rgba(245,244,240,0.5)", maxWidth: 380, lineHeight: 1.7, marginBottom: 36 }}>Мы получили ваш заказ и свяжемся с вами в ближайшее время для подтверждения.</p>
        <div style={{ display: "flex", gap: 12 }}>
          <Link href="/" className="btn-primary" style={{ textDecoration: "none" }}>На главную</Link>
          <Link href="/catalog" className="btn-ghost" style={{ textDecoration: "none" }}>Каталог</Link>
        </div>
      </div>
    );
  }

  if (customer) {
    return <AccountPage />;
  }

  return (
    <div style={{ maxWidth: 700, margin: "0 auto", padding: "clamp(24px,3vw,48px) clamp(18px,4vw,56px)" }}>
      <h1 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: 40, color: "#f5f4f0", margin: "0 0 24px" }}>Мои заказы</h1>
      <p style={{ color: "rgba(245,244,240,0.4)", fontSize: 14 }}>История заказов доступна после авторизации.</p>
      <div style={{ marginTop: 24 }}>
        <Link href="/catalog" className="btn-primary" style={{ textDecoration: "none" }}>Перейти в каталог</Link>
      </div>
    </div>
  );
}

export default function OrdersPage() {
  return (
    <Suspense fallback={null}>
      <OrdersContent />
    </Suspense>
  );
}
