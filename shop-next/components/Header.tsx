"use client";
import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Search, ShoppingBag, Home, LayoutGrid, Sparkles, LogOut, Package, Settings, Bell, BellOff, Menu, User } from "lucide-react";
import { useCart } from "./CartContext";
import { useAuth } from "./AuthContext";
import Logo from "./Logo";
import MenuDrawer from "./MenuDrawer";
import SearchOverlay from "./SearchOverlay";

function usePushNotifications() {
  const [isSubscribed, setIsSubscribed] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isSupported, setIsSupported] = useState(false);
  const [permission, setPermission] = useState<NotificationPermission>("default");

  useEffect(() => {
    const supported = "Notification" in window && "serviceWorker" in navigator;
    setIsSupported(supported);
    if (!supported) return;
    setPermission(Notification.permission);
    navigator.serviceWorker.ready.then(reg => reg.pushManager.getSubscription()).then(sub => setIsSubscribed(!!sub)).catch(() => {});
  }, []);

  async function subscribe() {
    if (!isSupported) return;
    setIsLoading(true);
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: undefined as unknown as string });
      await fetch((process.env.NEXT_PUBLIC_API_BASE ?? "") + "/api/shop/push/subscribe", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(sub),
      });
      setIsSubscribed(true);
    } catch { /* best-effort */ }
    setIsLoading(false);
  }

  async function unsubscribe() {
    setIsLoading(true);
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.getSubscription();
      if (sub) { await sub.unsubscribe(); setIsSubscribed(false); }
    } catch { /* best-effort */ }
    setIsLoading(false);
  }

  return { isSupported, permission, isSubscribed, isLoading, subscribe, unsubscribe };
}

const BOT_NAV = [
  { label: "Главная", href: "/", icon: Home },
  { label: "Каталог", href: "/catalog", icon: LayoutGrid },
  { label: "AI-подбор", href: "/find", icon: Sparkles },
  { label: "Корзина", href: "/cart", icon: ShoppingBag, cart: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout } = useAuth();
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [hidden, setHidden] = useState(false);
  const userMenuRef = useRef<HTMLDivElement>(null);
  const push = usePushNotifications();
  const isHome = pathname === "/";

  /* hide on scroll down, show on scroll up */
  useEffect(() => {
    let last = window.scrollY;
    const fn = () => {
      const y = window.scrollY;
      setHidden((h) => (y > 160 && y > last + 4 ? true : y < last - 4 ? false : h));
      last = y;
    };
    window.addEventListener("scroll", fn, { passive: true });
    return () => window.removeEventListener("scroll", fn);
  }, []);

  useEffect(() => { setMenuOpen(false); setSearchOpen(false); setUserMenuOpen(false); }, [pathname]);
  useEffect(() => { document.documentElement.dataset.hdr = hidden && !menuOpen ? "hidden" : "shown"; }, [hidden, menuOpen]);

  useEffect(() => {
    if (!userMenuOpen) return;
    const fn = (e: MouseEvent) => { if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) setUserMenuOpen(false); };
    document.addEventListener("mousedown", fn);
    return () => document.removeEventListener("mousedown", fn);
  }, [userMenuOpen]);

  const isActive = (href: string) => href === "/" ? pathname === "/" : pathname.startsWith(href);
  const initial = customer ? (customer.firstName || customer.email || "?")[0]?.toUpperCase() : null;

  return (
    <>
      <header className={`mv-header${hidden && !menuOpen ? " hidden-up" : ""}`}>
        {/* left */}
        <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
          <button className="mv-hbtn" onClick={() => setMenuOpen(true)} aria-label="Открыть меню">
            <Menu size={20} /> <span className="mv-desk">Меню</span>
          </button>
          <button className="mv-hbtn mv-desk" onClick={() => setSearchOpen(true)} aria-label="Поиск">
            <Search size={18} /> Поиск
          </button>
        </div>

        {/* center */}
        <Link href="/" aria-label="Magic Vibes — на главную" style={{ display: "flex", justifyContent: "center", textDecoration: "none" }}>
          <span className="hidden sm:inline-flex"><Logo height={30} /></span>
          <span className="sm:hidden"><Logo height={26} /></span>
        </Link>

        {/* right */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 2 }}>
          <Link href="/find" className="mv-hbtn mv-wide" style={{ color: "var(--accent)" }}>
            <Sparkles size={17} /> AI-подбор
          </Link>
          <button className="mv-hbtn icon mv-mob" onClick={() => setSearchOpen(true)} aria-label="Поиск"><Search size={19} /></button>

          {push.isSupported && push.permission !== "denied" && (
            <button
              className="mv-hbtn icon mv-desk"
              onClick={() => push.isSubscribed ? push.unsubscribe() : push.subscribe()}
              disabled={push.isLoading}
              title={push.isSubscribed ? "Отключить уведомления" : "Включить уведомления"}
              style={{ color: push.isSubscribed ? "var(--accent)" : "var(--ink)" }}
            >
              {push.isSubscribed ? <Bell size={18} /> : <BellOff size={18} />}
            </button>
          )}

          <div ref={userMenuRef} style={{ position: "relative" }}>
            {customer ? (
              <button className="mv-hbtn" onClick={() => setUserMenuOpen(s => !s)}>
                <span style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", width: 26, height: 26, borderRadius: "50%", background: "var(--lime)", color: "var(--ink)", fontSize: 12, fontWeight: 700 }}>{initial}</span>
                <span className="mv-desk">Кабинет</span>
              </button>
            ) : (
              <Link href="/login" className="mv-hbtn" aria-label="Войти"><User size={18} /> <span className="mv-desk">Войти</span></Link>
            )}
            {userMenuOpen && customer && (
              <div className="modal-content" style={{ position: "absolute", right: 0, top: "calc(100% + 12px)", width: 240, background: "var(--surface)", borderRadius: "var(--r-md)", boxShadow: "var(--shadow-lg)", padding: 6, zIndex: 60 }}>
                <div style={{ padding: "12px 12px 10px", borderBottom: "1px solid var(--border)", marginBottom: 4 }}>
                  <div style={{ fontSize: 14, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{customer.firstName || customer.email}</div>
                  <div style={{ fontSize: 12, color: "var(--muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginTop: 2 }}>{customer.email}</div>
                </div>
                {[{ href: "/orders", icon: <Package size={15} />, label: "Мои заказы" }, { href: "/account", icon: <Settings size={15} />, label: "Профиль" }].map(({ href, icon, label }) => (
                  <Link key={href} href={href} onClick={() => setUserMenuOpen(false)} className="sort-opt" style={{ color: "var(--ink)" }}>{icon}{label}</Link>
                ))}
                <button onClick={() => { logout(); setUserMenuOpen(false); }} className="sort-opt" style={{ color: "var(--danger)" }}><LogOut size={15} /> Выйти</button>
              </div>
            )}
          </div>

          <Link href="/cart" className="mv-hbtn solid" aria-label={`Корзина, товаров: ${totalItems}`}>
            <ShoppingBag size={17} />
            <span className="mv-desk">Корзина</span>
            {totalItems > 0 && <span className="mv-badge">{totalItems > 99 ? "99+" : totalItems}</span>}
          </Link>
        </div>
      </header>
      {!isHome && <div className="mv-header-spacer" />}

      <MenuDrawer open={menuOpen} onClose={() => setMenuOpen(false)} />
      <SearchOverlay open={searchOpen} onClose={() => setSearchOpen(false)} />

      {/* Mobile bottom nav */}
      <nav className="bottom-nav md:hidden" aria-label="Быстрая навигация">
        <div className="bottom-nav-inner">
          {BOT_NAV.map(({ label, href, icon: Icon, cart }) => (
            <Link key={href} href={href} className={`bottom-nav-item${isActive(href) ? " active" : ""}`}>
              <div style={{ position: "relative" }}>
                <Icon size={22} strokeWidth={isActive(href) ? 2.2 : 1.7} />
                {cart && totalItems > 0 && <span className="mv-badge" style={{ position: "absolute", top: -8, right: -12, minWidth: 18, height: 18, fontSize: 10 }}>{totalItems > 9 ? "9+" : totalItems}</span>}
              </div>
              <span>{label}</span>
            </Link>
          ))}
        </div>
      </nav>
    </>
  );
}
