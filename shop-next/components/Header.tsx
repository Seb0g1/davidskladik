"use client";
import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { useRouter, usePathname } from "next/navigation";
import { Search, X, ShoppingBag, Home, LayoutGrid, Sparkles, LogOut, Package, Settings, Bell, BellOff } from "lucide-react";
import { useCart } from "./CartContext";
import { useAuth } from "./AuthContext";

const NAV_LINKS = [
  { label: "Каталог", href: "/catalog" },
  { label: "Бренды", href: "/brands" },
  { label: "Новинки", href: "/new" },
  { label: "Блог", href: "/blog" },
  { label: "Доставка", href: "/delivery" },
];

const BOT_NAV = [
  { label: "Главная", href: "/", icon: Home },
  { label: "Каталог", href: "/catalog", icon: LayoutGrid },
  { label: "AI-подбор", href: "/find", icon: Sparkles },
  { label: "Корзина", href: "/cart", icon: ShoppingBag, cart: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const [q, setQ] = useState("");
  const [scrolled, setScrolled] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 20);
    window.addEventListener("scroll", fn, { passive: true });
    return () => window.removeEventListener("scroll", fn);
  }, []);

  useEffect(() => { setSearchOpen(false); }, [pathname]);
  useEffect(() => { if (searchOpen) setTimeout(() => searchRef.current?.focus(), 60); }, [searchOpen]);

  useEffect(() => {
    if (!userMenuOpen) return;
    const fn = (e: MouseEvent) => { if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) setUserMenuOpen(false); };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, [userMenuOpen]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const t = q.trim();
    if (t) { router.push(`/catalog?q=${encodeURIComponent(t)}`); setSearchOpen(false); setQ(""); }
  }

  const isActive = (href: string) => href === "/" ? pathname === "/" : pathname.startsWith(href);
  const initial = customer ? (customer.firstName || customer.email || "?")[0]?.toUpperCase() : null;

  return (
    <>
      <header style={{
        position: "sticky", top: 0, zIndex: 40,
        background: scrolled ? "rgba(11,11,11,0.95)" : "rgba(11,11,11,0.75)",
        backdropFilter: "blur(16px)", WebkitBackdropFilter: "blur(16px)",
        borderBottom: "1px solid rgba(201,162,94,0.1)",
        transition: "background 0.4s ease",
      }}>
        <div style={{ display: "flex", alignItems: "center", flexWrap: "wrap", gap: "12px 20px", padding: "12px clamp(18px,4vw,56px)" }}>

          {/* Logo */}
          <Link href="/" style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 500, fontSize: 23, letterSpacing: "0.01em", color: "#f5f4f0", textDecoration: "none", flexShrink: 0 }}>
            Magic Vibes
          </Link>

          {/* Desktop nav */}
          <nav className="hidden md:flex" style={{ alignItems: "center", gap: 4, flex: 1, justifyContent: "center" }}>
            {NAV_LINKS.map(({ label, href }) => (
              <Link key={href} href={href} style={{
                fontSize: 11, letterSpacing: "0.18em", textTransform: "uppercase",
                color: isActive(href) ? "#e9d2a0" : "rgba(245,244,240,0.5)",
                textDecoration: "none", padding: "6px 14px", borderRadius: 2,
                transition: "color 0.3s",
              }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.color = "#f5f4f0"; }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.color = isActive(href) ? "#e9d2a0" : "rgba(245,244,240,0.5)"; }}
              >{label}</Link>
            ))}
          </nav>

          {/* Desktop right actions */}
          <div className="hidden md:flex" style={{ alignItems: "center", gap: 8, flexShrink: 0 }}>
            <button onClick={() => setSearchOpen(s => !s)} style={{ height: 36, padding: "0 14px", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 2, background: "transparent", color: "#cfcbc2", fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", display: "flex", alignItems: "center", gap: 6, cursor: "pointer", transition: "border-color 0.3s,color 0.3s" }}
              onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
              onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}>
              <Search size={13} strokeWidth={1.5} /> Поиск
            </button>

            <div ref={userMenuRef} style={{ position: "relative" }}>
              {customer ? (
                <button onClick={() => setUserMenuOpen(s => !s)} style={{ height: 36, padding: "0 14px", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 2, background: "transparent", color: "#cfcbc2", fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}
                  onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
                  onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}>
                  <span style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", width: 20, height: 20, borderRadius: "50%", background: "linear-gradient(140deg,#e9d2a0,#a3874f)", color: "#14120f", fontSize: 9, fontWeight: 500 }}>{initial}</span>
                  Кабинет
                </button>
              ) : (
                <Link href="/account" style={{ height: 36, padding: "0 14px", border: "1px solid rgba(255,255,255,0.1)", borderRadius: 2, background: "transparent", color: "#cfcbc2", fontSize: 12, letterSpacing: "0.12em", textTransform: "uppercase", display: "flex", alignItems: "center", textDecoration: "none", transition: "border-color 0.3s,color 0.3s" }}
                  onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,162,94,0.6)"; el.style.color = "#f5f4f0"; }}
                  onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(255,255,255,0.1)"; el.style.color = "#cfcbc2"; }}>
                  Войти
                </Link>
              )}
              {userMenuOpen && customer && (
                <div className="modal-content" style={{ position: "absolute", right: 0, top: "calc(100% + 10px)", width: 220, background: "#141414", borderRadius: 3, border: "1px solid rgba(255,255,255,0.1)", boxShadow: "0 20px 60px rgba(0,0,0,0.7)", padding: "6px 0", zIndex: 60 }}>
                  <div style={{ padding: "12px 16px 10px", borderBottom: "1px solid rgba(255,255,255,0.06)", marginBottom: 4 }}>
                    <div style={{ fontSize: 13, color: "#f5f4f0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{customer.firstName || customer.email}</div>
                    <div style={{ fontSize: 11, color: "rgba(245,244,240,0.4)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginTop: 3 }}>{customer.email}</div>
                  </div>
                  {[{ href: "/orders", icon: <Package size={13} />, label: "Мои заказы" }, { href: "/account", icon: <Settings size={13} />, label: "Профиль" }].map(({ href, icon, label }) => (
                    <Link key={href} href={href} onClick={() => setUserMenuOpen(false)}
                      style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, color: "rgba(245,244,240,0.52)", textDecoration: "none", transition: "background 0.2s,color 0.2s" }}
                      onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.background = "rgba(255,255,255,0.04)"; el.style.color = "#f5f4f0"; }}
                      onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.background = "transparent"; el.style.color = "rgba(245,244,240,0.52)"; }}>
                      {icon}{label}
                    </Link>
                  ))}
                  <button onClick={() => { logout(); setUserMenuOpen(false); }} style={{ width: "100%", display: "flex", alignItems: "center", gap: 10, padding: "10px 16px", fontSize: 13, color: "#F87171", background: "transparent", border: "none", cursor: "pointer", transition: "background 0.2s" }}
                    onMouseEnter={e => (e.currentTarget.style.background = "rgba(239,68,68,0.07)")}
                    onMouseLeave={e => (e.currentTarget.style.background = "transparent")}>
                    <LogOut size={13} /> Выйти
                  </button>
                </div>
              )}
            </div>

            <Link href="/cart" style={{ position: "relative", height: 36, padding: "0 16px", borderRadius: 2, background: "#f2efe6", color: "#14120f", fontSize: 12, fontWeight: 500, letterSpacing: "0.12em", textTransform: "uppercase", display: "flex", alignItems: "center", gap: 8, textDecoration: "none", transition: "background 0.3s" }}
              onMouseEnter={e => { (e.currentTarget as HTMLElement).style.background = "#fffdf7"; }}
              onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "#f2efe6"; }}>
              Корзина{totalItems > 0 && ` · ${totalItems}`}
            </Link>
          </div>

          {/* Mobile right */}
          <div className="flex md:hidden" style={{ alignItems: "center", gap: 8, marginLeft: "auto" }}>
            <button onClick={() => setSearchOpen(s => !s)} style={{ padding: 8, background: "transparent", border: "none", color: "rgba(245,244,240,0.52)", cursor: "pointer" }}>
              <Search size={20} strokeWidth={1.5} />
            </button>
            <Link href="/cart" style={{ position: "relative", padding: 8, color: "rgba(245,244,240,0.52)", textDecoration: "none" }}>
              <ShoppingBag size={20} strokeWidth={1.5} />
              {totalItems > 0 && <span style={{ position: "absolute", top: 4, right: 4, width: 14, height: 14, background: "#c9a25e", color: "#0b0b0b", fontSize: 8, fontWeight: 600, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>{totalItems > 9 ? "9+" : totalItems}</span>}
            </Link>
          </div>

          {/* Search bar */}
          {searchOpen && (
            <div className="anim-fade-in" style={{ width: "100%", borderTop: "1px solid rgba(255,255,255,0.06)", paddingTop: 10, paddingBottom: 4 }}>
              <form onSubmit={handleSearch} style={{ position: "relative", maxWidth: 560 }}>
                <Search size={14} style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: "rgba(245,244,240,0.28)", pointerEvents: "none" }} />
                <input ref={searchRef} type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Бренд, название, аромат..." className="input-base" style={{ paddingLeft: 36, paddingRight: 38 }} />
                <button type="button" onClick={() => setSearchOpen(false)} style={{ position: "absolute", right: 10, top: "50%", transform: "translateY(-50%)", background: "none", border: "none", color: "rgba(245,244,240,0.28)", cursor: "pointer" }}>
                  <X size={15} />
                </button>
              </form>
            </div>
          )}
        </div>
      </header>

      {/* Mobile bottom nav */}
      <nav className="bottom-nav md:hidden">
        <div className="bottom-nav-inner">
          {BOT_NAV.map(({ label, href, icon: Icon, cart }) => (
            <Link key={href} href={href} className={`bottom-nav-item${isActive(href) ? " active" : ""}`}>
              <div style={{ position: "relative" }}>
                <Icon size={22} strokeWidth={isActive(href) ? 2 : 1.5} />
                {cart && totalItems > 0 && <span style={{ position: "absolute", top: -6, right: -8, width: 15, height: 15, background: "#c9a25e", color: "#0b0b0b", fontSize: 8, fontWeight: 600, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" }}>{totalItems > 9 ? "9+" : totalItems}</span>}
              </div>
              <span>{label}</span>
            </Link>
          ))}
        </div>
      </nav>
    </>
  );
}
