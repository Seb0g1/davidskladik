import { useState } from "react";
import { Link } from "react-router-dom";
import { Send, Mail, Clock } from "lucide-react";
import { useAuth } from "../AuthContext";
import AuthModal from "./AuthModal";

const FOOTER_STYLE = `
.footer-link {
  font-size: 13px;
  color: rgba(245,244,240,0.46);
  text-decoration: none;
  letter-spacing: 0.02em;
  transition: color 0.25s;
  line-height: 1.5;
  display: inline-block;
}
.footer-link:hover { color: #f5f4f0; }
.footer-col-title {
  font-size: 9.5px;
  font-weight: 400;
  letter-spacing: 0.28em;
  text-transform: uppercase;
  color: rgba(201,162,94,0.7);
  margin: 0 0 18px;
}
`;

export default function Footer() {
  const { customer } = useAuth();
  const [authModal, setAuthModal] = useState<{ open: boolean; tab: "login" | "register" }>({ open: false, tab: "login" });

  return (
    <>
      <style>{FOOTER_STYLE}</style>
      <footer style={{ background: "#080808", borderTop: "1px solid rgba(255,255,255,0.05)", marginTop: 80 }}>

        {/* Main grid */}
        <div style={{ maxWidth: 1240, margin: "0 auto", padding: "clamp(48px,6vw,72px) clamp(18px,4vw,56px) 0" }}>

          {/* Brand row */}
          <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", flexWrap: "wrap", gap: 24, paddingBottom: 40, borderBottom: "1px solid rgba(255,255,255,0.05)", marginBottom: 48 }}>
            <div style={{ maxWidth: 300 }}>
              <Link to="/" style={{ textDecoration: "none" }}>
                <span style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: 26, letterSpacing: "0.01em", color: "#f5f4f0" }}>
                  Magic Vibes
                </span>
              </Link>
              <p style={{ fontSize: 13, lineHeight: 1.7, color: "#6b6760", marginTop: 12, fontWeight: 300 }}>
                Оригинальная парфюмерия и косметика мировых брендов. Доставка по всей России.
              </p>
            </div>
            <div style={{ display: "flex", gap: 10, flexShrink: 0, alignItems: "center" }}>
              <a
                href="https://t.me/magicvibes_ru"
                target="_blank"
                rel="noopener noreferrer"
                title="Telegram"
                style={{ width: 36, height: 36, borderRadius: "50%", border: "1px solid rgba(255,255,255,0.1)", display: "flex", alignItems: "center", justifyContent: "center", color: "rgba(245,244,240,0.4)", textDecoration: "none", transition: "border-color 0.3s, color 0.3s" }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.5)"; (e.currentTarget as HTMLElement).style.color = "#c9a25e"; }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.1)"; (e.currentTarget as HTMLElement).style.color = "rgba(245,244,240,0.4)"; }}
              >
                <Send size={14} strokeWidth={1.5} />
              </a>
            </div>
          </div>

          {/* 4-column grid */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "clamp(28px,4vw,48px)", paddingBottom: "clamp(40px,5vw,60px)" }}>

            {/* Сервис и помощь */}
            <div>
              <p className="footer-col-title">Сервис и помощь</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {[
                  { label: "Как сделать заказ", to: "/faq" },
                  { label: "Вопросы и ответы", to: "/faq" },
                  { label: "Способы оплаты", to: "/delivery#payment" },
                  { label: "Способы доставки", to: "/delivery" },
                  { label: "Возврат товара", to: "/delivery#returns" },
                  { label: "Гарантия оригинала", to: "/warranty" },
                  { label: "Конфиденциальность", to: "/faq#privacy" },
                ].map(l => (
                  <li key={l.to + l.label}>
                    <Link to={l.to} className="footer-link">{l.label}</Link>
                  </li>
                ))}
              </ul>
            </div>

            {/* О компании */}
            <div>
              <p className="footer-col-title">О компании</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {[
                  { label: "Блог и статьи", to: "/blog" },
                  { label: "Новости", to: "/news" },
                  { label: "Гиды по ароматам", to: "/guide/women" },
                  { label: "Женские ароматы", to: "/guide/women" },
                  { label: "Мужские ароматы", to: "/guide/men" },
                  { label: "Парфюм в подарок", to: "/guide/gift" },
                  { label: "Ароматы для офиса", to: "/guide/office" },
                  { label: "Карта ароматов", to: "/world" },
                ].map(l => (
                  <li key={l.to + l.label}>
                    <Link to={l.to} className="footer-link">{l.label}</Link>
                  </li>
                ))}
              </ul>
            </div>

            {/* Каталог */}
            <div>
              <p className="footer-col-title">Каталог</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                {[
                  { label: "Все товары", to: "/catalog" },
                  { label: "Бренды", to: "/brands" },
                  { label: "Новинки", to: "/new" },
                  { label: "Женская парфюмерия", to: "/catalog?q=женская" },
                  { label: "Мужская парфюмерия", to: "/catalog?q=мужская" },
                  { label: "Пробники и отливанты", to: "/catalog?category=testers" },
                  { label: "Подарочные наборы", to: "/catalog?category=sets" },
                  { label: "✦ AI-подбор аромата", to: "/find" },
                ].map(l => (
                  <li key={l.to + l.label}>
                    <Link to={l.to} className="footer-link">{l.label}</Link>
                  </li>
                ))}
              </ul>
            </div>

            {/* Личный кабинет + контакты */}
            <div>
              <p className="footer-col-title">Личный кабинет</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10, marginBottom: 28 }}>
                {customer ? (
                  <>
                    <li><Link to="/orders" className="footer-link">Мои заказы</Link></li>
                    <li><Link to="/account" className="footer-link">Профиль</Link></li>
                  </>
                ) : (
                  <>
                    <li>
                      <button
                        onClick={() => setAuthModal({ open: true, tab: "login" })}
                        className="footer-link"
                        style={{ background: "none", border: "none", cursor: "pointer", padding: 0, fontFamily: "inherit" }}
                      >
                        Войти
                      </button>
                    </li>
                    <li>
                      <button
                        onClick={() => setAuthModal({ open: true, tab: "register" })}
                        className="footer-link"
                        style={{ background: "none", border: "none", cursor: "pointer", padding: 0, fontFamily: "inherit" }}
                      >
                        Регистрация
                      </button>
                    </li>
                  </>
                )}
              </ul>

              <p className="footer-col-title">Контакты</p>
              <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 10 }}>
                <li>
                  <a href="mailto:info@magicvibes.ru" className="footer-link" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <Mail size={12} strokeWidth={1.5} style={{ flexShrink: 0, color: "rgba(201,162,94,0.5)" }} />
                    info@magicvibes.ru
                  </a>
                </li>
                <li>
                  <a href="https://t.me/magicvibes_ru" target="_blank" rel="noopener noreferrer" className="footer-link" style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <Send size={12} strokeWidth={1.5} style={{ flexShrink: 0, color: "rgba(201,162,94,0.5)" }} />
                    Telegram
                  </a>
                </li>
                <li style={{ display: "flex", alignItems: "flex-start", gap: 8, color: "rgba(245,244,240,0.32)", fontSize: 12.5 }}>
                  <Clock size={12} strokeWidth={1.5} style={{ flexShrink: 0, marginTop: 3, color: "rgba(201,162,94,0.4)" }} />
                  <span>Работаем круглосуточно</span>
                </li>
              </ul>
            </div>

          </div>
        </div>

        {/* Bottom bar */}
        <div style={{ borderTop: "1px solid rgba(255,255,255,0.05)" }}>
          <div style={{ maxWidth: 1240, margin: "0 auto", padding: "18px clamp(18px,4vw,56px)", display: "flex", flexWrap: "wrap", alignItems: "center", justifyContent: "space-between", gap: "10px 24px" }}>
            <div style={{ fontSize: 11.5, color: "#3d3b39", letterSpacing: "0.04em" }}>
              © {new Date().getFullYear()} Magic Vibes — 100% оригинальная продукция
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {["VISA", "Mastercard", "МИР", "СБП"].map(p => (
                <span key={p} style={{
                  fontSize: 9.5, fontWeight: 600, letterSpacing: "0.08em",
                  color: "rgba(245,244,240,0.2)",
                  border: "1px solid rgba(255,255,255,0.07)",
                  borderRadius: 3,
                  padding: "3px 7px",
                }}>
                  {p}
                </span>
              ))}
            </div>
            <div style={{ fontSize: 11.5, color: "#3d3b39", letterSpacing: "0.04em" }}>
              Доставка по всей России
            </div>
          </div>
        </div>
      </footer>

      <AuthModal
        open={authModal.open}
        defaultTab={authModal.tab}
        onClose={() => setAuthModal(s => ({ ...s, open: false }))}
      />
    </>
  );
}
