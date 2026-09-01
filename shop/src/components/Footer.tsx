import { Link } from "react-router-dom";
import { Send } from "lucide-react";

export default function Footer() {
  return (
    <footer style={{ background: "#0b0b0b", borderTop: "1px solid rgba(255,255,255,0.06)", marginTop: 80 }}>
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "clamp(48px,6vw,72px) clamp(18px,4vw,56px)" }}>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(160px,1fr))", gap: "clamp(32px,4vw,56px)", paddingBottom: "clamp(32px,4vw,48px)", borderBottom: "1px solid rgba(255,255,255,0.06)" }}>

          {/* Brand */}
          <div style={{ gridColumn: "1 / -1", maxWidth: 280 }}>
            <Link to="/" style={{ textDecoration: "none" }}>
              <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: 22, letterSpacing: "0.01em", color: "#f5f4f0" }}>
                Magic Vibes
              </span>
            </Link>
            <p style={{ fontSize: 13, lineHeight: 1.7, color: "#7d7a73", marginTop: 14, fontWeight: 300 }}>
              Оригинальная парфюмерия мировых брендов с доставкой по всей России
            </p>
            <a
              href="mailto:info@magicvibes.ru"
              style={{ fontSize: 12, color: "#5d5a54", marginTop: 12, display: "block", textDecoration: "none", letterSpacing: "0.04em", transition: "color 0.3s" }}
              onMouseEnter={e => (e.currentTarget.style.color = "#c9a25e")}
              onMouseLeave={e => (e.currentTarget.style.color = "#5d5a54")}
            >
              info@magicvibes.ru
            </a>
          </div>

          {/* Catalog */}
          <div>
            <h4 style={{ fontSize: 10, fontWeight: 400, letterSpacing: "0.24em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 18 }}>Каталог</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 11 }}>
              {[
                { label: "Все товары", to: "/catalog" },
                { label: "Новинки",   to: "/new" },
                { label: "Бренды",    to: "/brands" },
              ].map(l => (
                <li key={l.to}>
                  <Link to={l.to} style={{ fontSize: 13, color: "#7d7a73", textDecoration: "none", letterSpacing: "0.02em", transition: "color 0.3s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "#f5f4f0")}
                    onMouseLeave={e => (e.currentTarget.style.color = "#7d7a73")}
                  >{l.label}</Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Info */}
          <div>
            <h4 style={{ fontSize: 10, fontWeight: 400, letterSpacing: "0.24em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 18 }}>Информация</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 11 }}>
              {[
                { label: "Доставка и оплата", to: "/delivery" },
                { label: "Возврат товара",    to: "/delivery#returns" },
                { label: "Гарантия оригинала", to: "/warranty" },
              ].map(item => (
                <li key={item.to}>
                  <Link to={item.to} style={{ fontSize: 13, color: "#7d7a73", textDecoration: "none", letterSpacing: "0.02em", transition: "color 0.3s" }}
                    onMouseEnter={e => (e.currentTarget.style.color = "#f5f4f0")}
                    onMouseLeave={e => (e.currentTarget.style.color = "#7d7a73")}
                  >{item.label}</Link>
                </li>
              ))}
            </ul>
          </div>

          {/* Delivery */}
          <div>
            <h4 style={{ fontSize: 10, fontWeight: 400, letterSpacing: "0.24em", textTransform: "uppercase", color: "#6f6c66", marginBottom: 18 }}>Доставка</h4>
            <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 11 }}>
              {["Доставка через Ozon", "По всей России", "Срок 1–5 дней", "Бесплатный возврат 14 дней"].map(item => (
                <li key={item} style={{ fontSize: 13, color: "#7d7a73", letterSpacing: "0.02em" }}>{item}</li>
              ))}
            </ul>
          </div>
        </div>

        <div style={{ paddingTop: 24, display: "flex", flexWrap: "wrap", justifyContent: "space-between", alignItems: "center", gap: 16 }}>
          <div style={{ fontSize: 11, color: "#5d5a54", letterSpacing: "0.06em", display: "flex", flexWrap: "wrap", gap: "6px 24px" }}>
            <span>© {new Date().getFullYear()} Magic Vibes</span>
            <span>100% оригинальная продукция</span>
          </div>
          <a
            href="https://t.me/magicvibes_ru"
            target="_blank"
            rel="noopener noreferrer"
            style={{ display: "inline-flex", alignItems: "center", gap: 8, fontSize: 12, letterSpacing: "0.1em", textTransform: "uppercase", color: "#5d5a54", textDecoration: "none", transition: "color 0.3s" }}
            onMouseEnter={e => (e.currentTarget.style.color = "#c9a25e")}
            onMouseLeave={e => (e.currentTarget.style.color = "#5d5a54")}
          >
            <Send size={13} strokeWidth={1.5} />
            Telegram
          </a>
        </div>
      </div>
    </footer>
  );
}
