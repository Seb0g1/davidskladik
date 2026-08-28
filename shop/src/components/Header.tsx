import { useState, useEffect, useRef } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Search, ShoppingBag, User, X, LayoutGrid, Home, LogOut, Package, Settings, Newspaper } from "lucide-react";
import clsx from "clsx";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import AuthModal from "./AuthModal";

const BOT_NAV = [
  { label: "Главная",  to: "/",       icon: Home },
  { label: "Каталог",  to: "/catalog", icon: LayoutGrid },
  { label: "Новости",  to: "/news",   icon: Newspaper },
  { label: "Корзина",  to: "/cart",    icon: ShoppingBag, cart: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout } = useAuth();
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
    const fn = () => setScrolled(window.scrollY > 40);
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

  return (
    <>
      <header style={{
        position: "sticky",
        top: 0,
        zIndex: 40,
        background: scrolled ? "rgba(14,13,11,0.94)" : "transparent",
        borderBottom: scrolled ? "1px solid var(--border)" : "1px solid transparent",
        backdropFilter: scrolled ? "blur(20px)" : "none",
        WebkitBackdropFilter: scrolled ? "blur(20px)" : "none",
        transition: "background 0.3s ease, border-color 0.3s ease",
      }}>
        <div style={{
          display: "flex", alignItems: "center", height: 62,
          padding: "0 clamp(16px,4vw,40px)", maxWidth: 1320, margin: "0 auto", gap: 24,
        }}>

          {/* Logo */}
          <Link to="/" style={{ display: "flex", alignItems: "center", gap: 0, flexShrink: 0, textDecoration: "none" }}>
            <span className="serif" style={{ fontSize: 18, fontWeight: 500, letterSpacing: "0.02em", color: "var(--text)", fontStyle: "italic" }}>
              Magic Vibes
            </span>
          </Link>

          {/* Desktop nav */}
          <nav className="hidden md:flex" style={{ flex: 1, justifyContent: "center", gap: 0, marginLeft: 16 }}>
            {[
              { label: "Каталог", to: "/catalog" },
              { label: "Бренды",  to: "/brands" },
              { label: "Новинки", to: "/new" },
              { label: "Новости", to: "/news" },
            ].map((link) => (
              <Link
                key={link.to}
                to={link.to}
                style={{
                  padding: "8px 16px",
                  fontSize: 13.5,
                  fontWeight: 500,
                  textDecoration: "none",
                  color: isActive(link.to) ? "var(--text)" : "var(--muted)",
                  borderBottom: isActive(link.to) ? "1px solid var(--accent)" : "1px solid transparent",
                  transition: "color 0.15s ease, border-color 0.15s ease",
                  letterSpacing: "0.01em",
                }}
                onMouseEnter={e => { if (!isActive(link.to)) (e.currentTarget as HTMLElement).style.color = "var(--text)"; }}
                onMouseLeave={e => { if (!isActive(link.to)) (e.currentTarget as HTMLElement).style.color = "var(--muted)"; }}
              >
                {link.label}
              </Link>
            ))}
          </nav>

          {/* Right */}
          <div style={{ display: "flex", alignItems: "center", gap: 2, marginLeft: "auto" }}>
            <button
              onClick={() => setSearchOpen((s) => !s)}
              style={{ padding: 10, borderRadius: 8, color: "var(--muted)", background: "transparent", border: "none", display: "flex", transition: "color 0.15s ease" }}
              onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text)"}
              onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--muted)"}
            >
              <Search size={19} strokeWidth={1.7} />
            </button>

            <div className="hidden md:block" ref={userMenuRef} style={{ position: "relative" }}>
              {customer ? (
                <button
                  onClick={() => setUserMenuOpen((s) => !s)}
                  style={{ padding: 10, borderRadius: 8, color: "var(--muted)", background: "transparent", border: "none", display: "flex", transition: "color 0.15s ease" }}
                  onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text)"}
                  onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--muted)"}
                >
                  <User size={19} strokeWidth={1.7} />
                </button>
              ) : (
                <button
                  onClick={() => setAuthModal({ open: true, tab: "login" })}
                  style={{
                    padding: "7px 18px", borderRadius: 8, fontSize: 13, fontWeight: 500,
                    color: "var(--muted)", background: "transparent",
                    border: "1px solid var(--border)", cursor: "pointer",
                    transition: "all 0.15s ease", fontFamily: "inherit",
                  }}
                  onMouseEnter={e => { (e.currentTarget as HTMLElement).style.color = "var(--text)"; (e.currentTarget as HTMLElement).style.borderColor = "var(--border-md)"; }}
                  onMouseLeave={e => { (e.currentTarget as HTMLElement).style.color = "var(--muted)"; (e.currentTarget as HTMLElement).style.borderColor = "var(--border)"; }}
                >
                  Войти
                </button>
              )}
              {userMenuOpen && customer && (
                <div className="modal-content" style={{
                  position: "absolute", right: 0, top: "calc(100% + 8px)",
                  width: 220, background: "var(--surface2)", borderRadius: 12,
                  border: "1px solid var(--border-md)",
                  boxShadow: "0 20px 60px rgba(0,0,0,0.6)",
                  padding: "6px 0", overflow: "hidden",
                }}>
                  <div style={{ padding: "12px 16px 10px", borderBottom: "1px solid var(--border)", marginBottom: 4 }}>
                    <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {customer.firstName || customer.email}
                    </div>
                    <div style={{ fontSize: 11, color: "var(--muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginTop: 2 }}>
                      {customer.email}
                    </div>
                  </div>
                  {[
                    { to: "/orders",  icon: <Package size={13} />, label: "Мои заказы" },
                    { to: "/account", icon: <Settings size={13} />, label: "Профиль" },
                  ].map(({ to, icon, label }) => (
                    <Link key={to} to={to} onClick={() => setUserMenuOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, color: "var(--muted)", textDecoration: "none", transition: "background 0.12s ease, color 0.12s ease" }}
                      onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "rgba(255,252,245,0.04)"; (e.currentTarget as HTMLElement).style.color = "var(--text)"; }}
                      onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "transparent"; (e.currentTarget as HTMLElement).style.color = "var(--muted)"; }}
                    >
                      {icon}{label}
                    </Link>
                  ))}
                  <button
                    onClick={() => { logout(); setUserMenuOpen(false); }}
                    style={{ width: "100%", display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, color: "#F87171", background: "transparent", border: "none", cursor: "pointer", transition: "background 0.12s ease" }}
                    onMouseEnter={e => (e.currentTarget.style.background = "rgba(239,68,68,0.07)")}
                    onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                  >
                    <LogOut size={13} /> Выйти
                  </button>
                </div>
              )}
            </div>

            <Link to="/cart" className="hidden md:flex" style={{ position: "relative", padding: 10, borderRadius: 8, color: "var(--muted)", textDecoration: "none", transition: "color 0.15s ease" }}
              onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text)"}
              onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--muted)"}
            >
              <ShoppingBag size={19} strokeWidth={1.7} />
              {totalItems > 0 && (
                <span style={{ position: "absolute", top: 6, right: 6, width: 16, height: 16, background: "var(--accent)", color: "#0E0D0B", fontSize: 9, fontWeight: 800, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>
                  {totalItems > 9 ? "9+" : totalItems}
                </span>
              )}
            </Link>
          </div>
        </div>

        {/* Search bar */}
        {searchOpen && (
          <div className="anim-fade-in" style={{ borderTop: "1px solid var(--border)", background: "rgba(14,13,11,0.97)" }}>
            <div style={{ maxWidth: 600, margin: "0 auto", padding: "10px 16px" }}>
              <form onSubmit={handleSearch} style={{ position: "relative" }}>
                <Search size={15} style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", color: "var(--subtle)", pointerEvents: "none" }} />
                <input ref={searchRef} type="text" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Бренд, название, аромат..." className="input-base" style={{ paddingLeft: 40, paddingRight: 40 }} />
                <button type="button" onClick={() => setSearchOpen(false)} style={{ position: "absolute", right: 12, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", color: "var(--subtle)", cursor: "pointer" }}>
                  <X size={16} />
                </button>
              </form>
            </div>
          </div>
        )}
      </header>

      {/* Bottom nav (mobile) */}
      <nav className="bottom-nav md:hidden">
        <div className="bottom-nav-inner">
          {BOT_NAV.map(({ label, to, icon: Icon, cart }) => (
            <Link key={to} to={to} className={clsx("bottom-nav-item", isActive(to) && "active")}>
              <div style={{ position: "relative" }}>
                <Icon size={22} strokeWidth={isActive(to) ? 2.2 : 1.6} />
                {cart && totalItems > 0 && (
                  <span style={{ position: "absolute", top: -6, right: -8, width: 16, height: 16, background: "var(--accent)", color: "#0E0D0B", fontSize: 8, fontWeight: 800, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>
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
        onClose={() => setAuthModal((s) => ({ ...s, open: false }))}
      />
    </>
  );
}
