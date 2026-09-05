import { useState, useEffect, useRef } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Search, X, LayoutGrid, Home, Newspaper, ShoppingBag, LogOut, Package, Settings, Bell, BellOff } from "lucide-react";
import clsx from "clsx";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import AuthModal from "./AuthModal";
import { usePush } from "../hooks/usePush";

const BOT_NAV = [
  { label: "Главная",  to: "/",       icon: Home },
  { label: "Каталог",  to: "/catalog", icon: LayoutGrid },
  { label: "Новости",  to: "/news",   icon: Newspaper },
  { label: "Корзина",  to: "/cart",    icon: ShoppingBag, cart: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout, yandexLoading, yandexError, clearYandexError } = useAuth();
  const push = usePush();
  const navigate = useNavigate();
  const location = useLocation();

  const [q, setQ] = useState("");
  const [scrolled, setScrolled] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [authModal, setAuthModal] = useState<{ open: boolean; tab: "login" | "register" }>({ open: false, tab: "login" });

  const searchRef = useRef<HTMLInputElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 20);
    window.addEventListener("scroll", fn, { passive: true });
    return () => window.removeEventListener("scroll", fn);
  }, []);

  useEffect(() => { setSearchOpen(false); }, [location.pathname]);
  useEffect(() => { if (searchOpen) setTimeout(() => searchRef.current?.focus(), 60); }, [searchOpen]);

  useEffect(() => {
    if (!userMenuOpen) return;
    const fn = (e: MouseEvent) => {
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) setUserMenuOpen(false);
    };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, [userMenuOpen]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const t = q.trim();
    if (t) { navigate(`/catalog?q=${encodeURIComponent(t)}`); setSearchOpen(false); setQ(""); }
  }

  const path = location.pathname;
  const isActive = (to: string) => to === "/" ? path === "/" : path.startsWith(to.split("?")[0]);

  const initial = customer ? (customer.firstName || customer.email || "?")[0]?.toUpperCase() : null;

  return (
    <>
      <header style={{
        position: "sticky",
        top: 0,
        zIndex: 40,
        display: "flex",
        flexWrap: "wrap",
        alignItems: "center",
        gap: "12px 24px",
        padding: "14px clamp(18px, 4vw, 56px)",
        background: scrolled ? "rgba(11,11,11,0.88)" : "rgba(11,11,11,0.6)",
        backdropFilter: "blur(14px)",
        WebkitBackdropFilter: "blur(14px)",
        borderBottom: "1px solid rgba(201,162,94,0.14)",
        transition: "background 0.4s ease",
      }}>
        {/* Logo */}
        <Link to="/" style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic",
          fontWeight: 500,
          fontSize: 23,
          letterSpacing: "0.01em",
          color: "#f5f4f0",
          whiteSpace: "nowrap",
          textDecoration: "none",
          flexShrink: 0,
        }}>
          Magic Vibes
        </Link>

        {/* Desktop nav */}
        <nav className="hidden md:flex" style={{ flex: "1 1 240px", minWidth: 0, flexWrap: "wrap", alignItems: "center", gap: "10px 22px" }}>
          {[
            { label: "Каталог",  to: "/catalog" },
            { label: "Бренды",   to: "/brands" },
            { label: "Новинки",  to: "/new" },
            { label: "Подарки",  to: "/gift" },
            { label: "Новости",  to: "/news" },
          ].map((link) => (
            <Link
              key={link.to}
              to={link.to}
              style={{
                fontSize: 13.5,
                letterSpacing: "0.05em",
                textDecoration: "none",
                color: isActive(link.to) ? "#f5f4f0" : "#b8b4ab",
                transition: "color 0.3s ease",
              }}
              onMouseEnter={e => { (e.currentTarget as HTMLElement).style.color = "#f5f4f0"; }}
              onMouseLeave={e => { if (!isActive(link.to)) (e.currentTarget as HTMLElement).style.color = "#b8b4ab"; }}
            >
              {link.label}
            </Link>
          ))}
        </nav>

        {/* Right actions */}
        <div className="hidden md:flex" style={{ marginLeft: "auto", alignItems: "center", gap: 8, flexShrink: 0 }}>
          {/* Search */}
          <button
            onClick={() => setSearchOpen(s => !s)}
            style={{
              height: 38, padding: "0 14px",
              border: "1px solid rgba(255,255,255,0.1)",
              borderRadius: 2,
              background: "transparent",
              color: "#cfcbc2",
              fontSize: 12, letterSpacing: "0.14em", textTransform: "uppercase",
              display: "flex", alignItems: "center", gap: 6,
              transition: "border-color 0.3s ease, color 0.3s ease",
            }}
            onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
            onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}
          >
            <Search size={14} strokeWidth={1.5} />
            Поиск
          </button>

          {/* Push notifications bell */}
          {push.isSupported && push.permission !== "denied" && (
            <button
              onClick={() => {
                if (push.isSubscribed) push.unsubscribe();
                else push.subscribe(null);
              }}
              disabled={push.isLoading}
              title={push.isSubscribed ? "Отключить уведомления" : "Включить уведомления"}
              style={{
                height: 38, width: 38,
                border: "1px solid rgba(255,255,255,0.1)",
                borderRadius: 2,
                background: "transparent",
                color: push.isSubscribed ? "#c9a25e" : "#6b6760",
                display: "flex", alignItems: "center", justifyContent: "center",
                transition: "border-color 0.3s ease, color 0.3s ease",
                cursor: "pointer",
              }}
              onMouseEnter={e => {
                (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.5)";
                (e.currentTarget as HTMLElement).style.color = push.isSubscribed ? "#e8d5a3" : "#c9a25e";
              }}
              onMouseLeave={e => {
                (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.1)";
                (e.currentTarget as HTMLElement).style.color = push.isSubscribed ? "#c9a25e" : "#6b6760";
              }}
            >
              {push.isSubscribed
                ? <Bell size={16} strokeWidth={1.5} />
                : <BellOff size={16} strokeWidth={1.5} />
              }
            </button>
          )}

          {/* Account */}
          <div ref={userMenuRef} style={{ position: "relative" }}>
            {customer ? (
              <button
                onClick={() => setUserMenuOpen(s => !s)}
                style={{
                  height: 38, padding: "0 14px",
                  border: "1px solid rgba(255,255,255,0.1)",
                  borderRadius: 2,
                  background: "transparent",
                  color: "#cfcbc2",
                  fontSize: 12, letterSpacing: "0.14em", textTransform: "uppercase",
                  display: "flex", alignItems: "center", gap: 8,
                  transition: "border-color 0.3s ease, color 0.3s ease",
                }}
                onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
                onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}
              >
                <span style={{
                  display: "inline-flex", alignItems: "center", justifyContent: "center",
                  width: 22, height: 22, borderRadius: "50%",
                  background: "linear-gradient(140deg, #e9d2a0, #a3874f)",
                  color: "#14120f", fontSize: 10, fontWeight: 500,
                }}>
                  {initial}
                </span>
                Кабинет
              </button>
            ) : (
              <button
                onClick={() => setAuthModal({ open: true, tab: "login" })}
                style={{
                  height: 38, padding: "0 14px",
                  border: "1px solid rgba(255,255,255,0.1)",
                  borderRadius: 2,
                  background: "transparent",
                  color: "#cfcbc2",
                  fontSize: 12, letterSpacing: "0.14em", textTransform: "uppercase",
                  display: "flex", alignItems: "center", gap: 8,
                  transition: "border-color 0.3s ease, color 0.3s ease",
                }}
                onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
                onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}
              >
                Войти
              </button>
            )}
            {userMenuOpen && customer && (
              <div className="modal-content" style={{
                position: "absolute", right: 0, top: "calc(100% + 10px)",
                width: 220,
                background: "#141414",
                borderRadius: 3,
                border: "1px solid rgba(255,255,255,0.1)",
                boxShadow: "0 20px 60px rgba(0,0,0,0.7)",
                padding: "6px 0", overflow: "hidden",
              }}>
                <div style={{ padding: "12px 16px 10px", borderBottom: "1px solid var(--border)", marginBottom: 4 }}>
                  <div style={{ fontSize: 13, fontWeight: 400, color: "var(--text)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", letterSpacing: "0.03em" }}>
                    {customer.firstName || customer.email}
                  </div>
                  <div style={{ fontSize: 11, color: "var(--subtle)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginTop: 3 }}>
                    {customer.email}
                  </div>
                </div>
                {[
                  { to: "/orders",  icon: <Package size={13} />, label: "Мои заказы" },
                  { to: "/account", icon: <Settings size={13} />, label: "Профиль" },
                ].map(({ to, icon, label }) => (
                  <Link key={to} to={to} onClick={() => setUserMenuOpen(false)}
                    style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, letterSpacing: "0.04em", color: "var(--muted)", textDecoration: "none", transition: "background 0.2s ease, color 0.2s ease" }}
                    onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.04)"; (e.currentTarget as HTMLElement).style.color = "var(--text)"; }}
                    onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "transparent"; (e.currentTarget as HTMLElement).style.color = "var(--muted)"; }}
                  >
                    {icon}{label}
                  </Link>
                ))}
                <button
                  onClick={() => { logout(); setUserMenuOpen(false); }}
                  style={{ width: "100%", display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, color: "#F87171", background: "transparent", border: "none", cursor: "pointer", transition: "background 0.2s ease", letterSpacing: "0.04em" }}
                  onMouseEnter={e => (e.currentTarget.style.background = "rgba(239,68,68,0.07)")}
                  onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                >
                  <LogOut size={13} /> Выйти
                </button>
              </div>
            )}
          </div>

          {/* Cart */}
          <Link to="/cart" style={{
            position: "relative",
            height: 38, padding: "0 16px",
            borderRadius: 2,
            background: "#f2efe6",
            color: "#14120f",
            fontSize: 12, fontWeight: 500, letterSpacing: "0.14em", textTransform: "uppercase",
            display: "flex", alignItems: "center", gap: 8,
            textDecoration: "none",
            transition: "background 0.3s ease",
          }}
            onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "#fffdf7"; }}
            onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "#f2efe6"; }}
          >
            Корзина{totalItems > 0 && ` · ${totalItems}`}
          </Link>
        </div>

        {/* Mobile right */}
        <div className="flex md:hidden" style={{ marginLeft: "auto", alignItems: "center", gap: 8 }}>
          <button
            onClick={() => setSearchOpen(s => !s)}
            style={{ padding: 8, background: "transparent", border: "none", color: "var(--muted)" }}
          >
            <Search size={20} strokeWidth={1.5} />
          </button>
          <Link to="/cart" style={{ position: "relative", padding: 8, color: "var(--muted)", textDecoration: "none" }}>
            <ShoppingBag size={20} strokeWidth={1.5} />
            {totalItems > 0 && (
              <span style={{ position: "absolute", top: 4, right: 4, width: 14, height: 14, background: "var(--accent)", color: "#0b0b0b", fontSize: 8, fontWeight: 600, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                {totalItems > 9 ? "9+" : totalItems}
              </span>
            )}
          </Link>
        </div>

        {/* Search bar */}
        {searchOpen && (
          <div className="anim-fade-in" style={{ width: "100%", borderTop: "1px solid var(--border)", paddingTop: 10, paddingBottom: 4 }}>
            <form onSubmit={handleSearch} style={{ position: "relative", maxWidth: 560 }}>
              <Search size={14} style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: "var(--subtle)", pointerEvents: "none" }} />
              <input
                ref={searchRef}
                type="text"
                value={q}
                onChange={e => setQ(e.target.value)}
                placeholder="Бренд, название, аромат..."
                className="input-base"
                style={{ paddingLeft: 36, paddingRight: 38 }}
              />
              <button type="button" onClick={() => setSearchOpen(false)} style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", color: "var(--subtle)", cursor: "pointer" }}>
                <X size={15} />
              </button>
            </form>
          </div>
        )}
      </header>

      {/* Bottom nav (mobile) */}
      <nav className="bottom-nav md:hidden">
        <div className="bottom-nav-inner">
          {BOT_NAV.map(({ label, to, icon: Icon, cart }) => (
            <Link key={to} to={to} className={clsx("bottom-nav-item", isActive(to) && "active")}>
              <div style={{ position: "relative" }}>
                <Icon size={22} strokeWidth={isActive(to) ? 2 : 1.5} />
                {cart && totalItems > 0 && (
                  <span style={{ position: "absolute", top: -6, right: -8, width: 15, height: 15, background: "var(--accent)", color: "#0b0b0b", fontSize: 8, fontWeight: 600, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                    {totalItems > 9 ? "9+" : totalItems}
                  </span>
                )}
              </div>
              <span>{label}</span>
            </Link>
          ))}
        </div>
      </nav>

      <AuthModal
        open={authModal.open}
        defaultTab={authModal.tab}
        onClose={() => setAuthModal(s => ({ ...s, open: false }))}
      />

      {/* Yandex OAuth: full-screen loading overlay */}
      {yandexLoading && (
        <div style={{ position: "fixed", inset: 0, zIndex: 200, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", background: "rgba(0,0,0,0.72)", backdropFilter: "blur(8px)" }}>
          <svg width="44" height="44" viewBox="0 0 24 24" fill="none" style={{ marginBottom: 16 }}>
            <circle cx="12" cy="12" r="12" fill="#FC3F1D" />
            <path d="M13.32 7.12h-.92c-1.44 0-2.2.68-2.2 1.8 0 1.26.56 1.9 1.72 2.7l.96.64-2.76 4.14H8.3l2.54-3.8c-1.46-1.04-2.28-2.06-2.28-3.6 0-2.04 1.42-3.4 3.8-3.4h2.88v10.8H13.3V7.12z" fill="#fff" />
          </svg>
          <p style={{ color: "#fff", fontSize: 15, fontWeight: 500 }}>Входим через Яндекс ID...</p>
        </div>
      )}

      {/* Yandex OAuth: error notification */}
      {yandexError && (
        <div style={{ position: "fixed", top: 20, left: "50%", transform: "translateX(-50%)", zIndex: 300, maxWidth: 400, width: "calc(100% - 32px)", background: "rgba(248,113,113,0.12)", border: "1px solid rgba(248,113,113,0.35)", borderRadius: 10, padding: "14px 18px", display: "flex", alignItems: "flex-start", gap: 12, boxShadow: "0 8px 32px rgba(0,0,0,0.4)" }}>
          <span style={{ color: "#F87171", fontSize: 14, flex: 1, lineHeight: 1.4 }}>Ошибка входа через Яндекс: {yandexError}</span>
          <button onClick={clearYandexError} style={{ color: "rgba(255,255,255,0.5)", background: "none", border: "none", cursor: "pointer", padding: 2, lineHeight: 1, fontSize: 16 }}>✕</button>
        </div>
      )}
    </>
  );
}
