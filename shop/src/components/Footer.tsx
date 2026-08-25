import { Link } from "react-router-dom";
import { Send } from "lucide-react";

export default function Footer() {
  return (
    <footer style={{ background: "var(--surface)", borderTop: "1px solid var(--border)", marginTop: 64 }}>
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(40px,5vw,64px) clamp(16px,4vw,48px)" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "clamp(32px,4vw,56px)", paddingBottom: "clamp(32px,4vw,48px)", borderBottom: "1px solid var(--border)" }}>

          {/* Brand */}
          <div style={{ gridColumn: "1 / -1", maxWidth: 260 }}>
            <Link to="/" style={{ textDecoration: "none" }}>
              <span className="serif" style={{ fontSize: 20, fontWeight: 500, color: "var(--text)", fontStyle: "italic", letterSpacing: "0.01em" }}>
                Magic Vibes
              </span>
            </Link>
            <p style={{ fontSize: 13, color: "var(--muted)", lineHeight: 1.7, marginTop: 14 }}>
              Оригинальная парфюмерия мировых брендов с доставкой по всей России
            </p>
            <a href="mailto:info@magicvibes.ru" style={{ fontSize: 12, color: "var(--subtle)", marginTop: 12, display: "block", textDecoration: "none", transition: "color 0.15s" }}
              onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
              onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
            >
              info@magicvibes.ru
            </a>
          </div>

          {/* Catalog */}
          <div>
            <h4 style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.12em", textTransform: "uppercase", color: "var(--text)", marginBottom: 16 }}>Каталог</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
              {[
                { label: "Все товары", to: "/catalog" },
                { label: "Новинки", to: "/new" },
                { label: "Бренды", to: "/brands" },
              ].map((l) => (
                <li key={l.to}>
                  <Link to={l.to} style={{ fontSize: 13, color: "var(--muted)", textDecoration: "none", transition: "color 0.15s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
                    onMouseLeave={e => (e.currentTarget.style.color = "var(--muted)")}
                  >{l.label}</Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Info */}
          <div>
            <h4 style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.12em", textTransform: "uppercase", color: "var(--text)", marginBottom: 16 }}>Информация</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
              {["О магазине", "Доставка и оплата", "Возврат товара", "Гарантия оригинала"].map((item) => (
                <li key={item}>
                  <a href="#" style={{ fontSize: 13, color: "var(--muted)", textDecoration: "none", transition: "color 0.15s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
                    onMouseLeave={e => (e.currentTarget.style.color = "var(--muted)")}
                  >{item}</a>
                </li>
              ))}
            </ul>
          </div>

          {/* Delivery */}
          <div>
            <h4 style={{ fontSize: 11, fontWeight: 600, letterSpacing: "0.12em", textTransform: "uppercase", color: "var(--text)", marginBottom: 16 }}>Доставка</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
              <li style={{ fontSize: 13, color: "var(--muted)" }}>Доставка через Ozon</li>
              <li style={{ fontSize: 13, color: "var(--muted)" }}>По всей России</li>
              <li style={{ fontSize: 13, color: "var(--muted)" }}>Срок 1–5 дней</li>
            </ul>
          </div>
        </div>

        <div style={{ paddingTop: 24, display: "flex", flexWrap: "wrap", justifyContent: "space-between", alignItems: "center", gap: 16 }}>
          <div style={{ fontSize: 11, color: "var(--subtle)", display: "flex", flexWrap: "wrap", gap: "8px 24px" }}>
            <span>© {new Date().getFullYear()} Magic Vibes. Все права защищены.</span>
            <span>100% оригинальная продукция</span>
          </div>
          <a
            href="https://t.me/magicvibes_ru"
            target="_blank"
            rel="noopener noreferrer"
            style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, color: "var(--subtle)", textDecoration: "none", transition: "color 0.15s" }}
            onMouseEnter={e => (e.currentTarget.style.color = "var(--text)")}
            onMouseLeave={e => (e.currentTarget.style.color = "var(--subtle)")}
          >
            <Send size={13} strokeWidth={1.7} />
            Telegram
          </a>
        </div>
      </div>
    </footer>
  );
}
