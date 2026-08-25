import { useSearchParams, Link } from "react-router-dom";
import { XCircle, ShoppingCart, RefreshCw } from "lucide-react";

export default function OrderFailedPage() {
  const [params] = useSearchParams();
  const orderId = params.get("id");

  return (
    <div style={{ background: "#0E0D0B", minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center" }}>
      <div style={{ textAlign: "center", padding: "64px 24px", maxWidth: 420, width: "100%" }}>
        <div style={{
          width: 80, height: 80, borderRadius: 28, display: "flex", alignItems: "center", justifyContent: "center",
          margin: "0 auto 28px", background: "rgba(239,68,68,0.1)", border: "1px solid rgba(239,68,68,0.2)",
          boxShadow: "0 0 40px rgba(239,68,68,0.1)",
        }}>
          <XCircle size={40} style={{ color: "#f87171" }} strokeWidth={1.5} />
        </div>

        <h1 style={{ fontSize: 26, fontWeight: 700, color: "#F4EFE6", letterSpacing: "-0.04em", marginBottom: 10 }}>
          Оплата не прошла
        </h1>
        {orderId && (
          <p style={{ fontSize: 13, color: "rgba(244,239,230,0.5)", marginBottom: 6 }}>
            Заказ <span style={{ fontFamily: "monospace", fontWeight: 700, color: "#C9A96E" }}>{orderId}</span> сохранён
          </p>
        )}
        <p style={{ fontSize: 13, color: "rgba(244,239,230,0.45)", lineHeight: 1.7, marginBottom: 40 }}>
          Платёж был отклонён или отменён.<br />
          Попробуйте ещё раз — заказ не потерян.
        </p>

        <div style={{ display: "flex", flexDirection: "column", gap: 10, alignItems: "center" }}>
          <Link to="/checkout" className="btn-primary" style={{ fontSize: 14, padding: "13px 32px", width: "100%", justifyContent: "center" }}>
            <RefreshCw size={16} /> Попробовать снова
          </Link>
          <Link to="/cart" className="btn-ghost" style={{ fontSize: 13, padding: "11px 24px", width: "100%", justifyContent: "center" }}>
            <ShoppingCart size={16} /> В корзину
          </Link>
        </div>
      </div>
    </div>
  );
}
