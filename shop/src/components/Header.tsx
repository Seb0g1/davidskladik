import { useState, useEffect, useRef } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Search, ShoppingBag, User, X, LayoutGrid, Sparkles, Home, LogOut, Package, Settings } from "lucide-react";
import clsx from "clsx";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import AuthModal from "./AuthModal";

const BOT_NAV = [
  { label: "Главная",  to: "/",        icon: Home },
  { label: "Каталог",  to: "/catalog",  icon: LayoutGrid },
  { label: "Бренды",   to: "/brands",   icon: Sparkles },
  { label: "Корзина",  to: "/cart",     icon: ShoppingBag, cart: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const [q, setQ] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [authModal, setAuthModal] = useState<{ open: boolean; tab: "login" | "register" }>({ open: false, tab: "login" });

  const searchRef = useRef<HTMLInputElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 4);
    window.addEventListener("scroll", fn, { passive: true });
    return () => window.removeEventListener("scroll", fn);
  }, []);

  useEffect(() => { setSearchOpen(false); }, [location.pathname]);

  useEffect(() => {
    if (searchOpen) setTimeout(() => searchRef.current?.focus(), 60);
  }, [searchOpen]);

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
    const trimmed = q.trim();
    if (trimmed) { navigate(`/catalog?q=${encodeURIComponent(trimmed)}`); setSearchOpen(false); setQ(""); }
  }

  const path = location.pathname;
  const isActive = (to: string) => to === "/" ? path === "/" : path.startsWith(to);

  return (
    <>
      {/* ── Top bar ── */}
      <header
        className={clsx(
          "sticky top-0 z-40 transition-all duration-200",
          scrolled ? "bg-white/95 backdrop-blur-xl shadow-[0_1px_0_#EBEBEB]" : "bg-white border-b border-[#EBEBEB]"
        )}
      >
        <div className="flex items-center h-14 px-4 max-w-7xl mx-auto gap-3">

          {/* Logo */}
          <Link to="/" className="flex items-center gap-2 flex-shrink-0 group">
            <div
              className="w-8 h-8 rounded-[10px] flex items-center justify-center text-white text-[11px] font-bold tracking-tight flex-shrink-0"
              style={{ background: "linear-gradient(135deg, #6D28D9, #7C3AED)" }}
            >
              MV
            </div>
            <span className="font-bold text-[15px] tracking-tight text-[#111] group-hover:text-violet-700 transition-colors hidden sm:block">
              Magic Vibes
            </span>
          </Link>

          {/* Desktop nav */}
          <nav className="hidden md:flex items-center gap-0.5 ml-6 flex-1">
            {[
              { label: "Каталог", to: "/catalog" },
              { label: "Бренды",  to: "/brands"  },
              { label: "Новинки", to: "/catalog?sort=price_desc" },
              { label: "Акции",   to: "/catalog?inStock=true&sort=price_asc", accent: true },
            ].map((link) => (
              <Link
                key={link.to}
                to={link.to}
                className={clsx(
                  "px-3.5 py-2 rounded-xl text-[13.5px] font-medium transition-colors",
                  link.accent
                    ? "text-violet-600 hover:bg-violet-50"
                    : "text-[#555] hover:text-[#111] hover:bg-[#F5F5F3]"
                )}
              >
                {link.label}
              </Link>
            ))}
          </nav>

          {/* Right actions */}
          <div className="flex items-center gap-1 ml-auto">
            {/* Search */}
            <button
              onClick={() => setSearchOpen((s) => !s)}
              className="p-2.5 rounded-xl text-[#666] hover:text-[#111] hover:bg-[#F5F5F3] transition-colors"
              aria-label="Поиск"
            >
              <Search size={19} strokeWidth={2} />
            </button>

            {/* Profile (desktop only) */}
            <div className="relative hidden md:block" ref={userMenuRef}>
              {customer ? (
                <button
                  onClick={() => setUserMenuOpen((s) => !s)}
                  className="p-2.5 rounded-xl text-[#666] hover:text-[#111] hover:bg-[#F5F5F3] transition-colors"
                >
                  <User size={19} strokeWidth={2} />
                </button>
              ) : (
                <button
                  onClick={() => setAuthModal({ open: true, tab: "login" })}
                  className="px-4 py-2 rounded-xl text-[13px] font-semibold text-[#555] hover:text-[#111] hover:bg-[#F5F5F3] transition-colors"
                >
                  Войти
                </button>
              )}
              {userMenuOpen && customer && (
                <div className="absolute right-0 top-full mt-2 w-52 bg-white rounded-2xl shadow-[0_8px_32px_rgba(0,0,0,0.12)] border border-[#F0F0F0] py-1.5 anim-scale-in">
                  <div className="px-4 py-2.5 border-b border-[#F5F5F5] mb-1">
                    <div className="text-[13px] font-semibold text-[#111] truncate">{customer.firstName || customer.email}</div>
                    <div className="text-[11px] text-[#888] truncate mt-0.5">{customer.email}</div>
                  </div>
                  <Link to="/orders" onClick={() => setUserMenuOpen(false)}
                    className="flex items-center gap-2.5 px-4 py-2.5 text-[13px] text-[#333] hover:bg-[#F7F7F7] transition-colors">
                    <Package size={15} className="text-[#888]" /> Мои заказы
                  </Link>
                  <Link to="/account" onClick={() => setUserMenuOpen(false)}
                    className="flex items-center gap-2.5 px-4 py-2.5 text-[13px] text-[#333] hover:bg-[#F7F7F7] transition-colors">
                    <Settings size={15} className="text-[#888]" /> Профиль
                  </Link>
                  <button onClick={() => { logout(); setUserMenuOpen(false); }}
                    className="w-full flex items-center gap-2.5 px-4 py-2.5 text-[13px] text-red-500 hover:bg-red-50 transition-colors">
                    <LogOut size={15} /> Выйти
                  </button>
                </div>
              )}
            </div>

            {/* Cart (desktop) */}
            <Link
              to="/cart"
              className="relative hidden md:flex p-2.5 rounded-xl text-[#666] hover:text-[#111] hover:bg-[#F5F5F3] transition-colors"
            >
              <ShoppingBag size={19} strokeWidth={2} />
              {totalItems > 0 && (
                <span className="absolute top-1.5 right-1.5 w-4 h-4 bg-violet-600 text-white text-[9px] font-bold rounded-full flex items-center justify-center leading-none">
                  {totalItems > 9 ? "9+" : totalItems}
                </span>
              )}
            </Link>
          </div>
        </div>

        {/* Search bar */}
        {searchOpen && (
          <div className="border-t border-[#F0F0F0] anim-fade-in">
            <div className="max-w-2xl mx-auto px-4 py-2.5">
              <form onSubmit={handleSearch} className="relative">
                <Search size={15} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-[#ABABAB] pointer-events-none" />
                <input
                  ref={searchRef}
                  type="text"
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Бренд, название, аромат..."
                  className="input-base pl-9 pr-10"
                />
                {q ? (
                  <button type="button" onClick={() => setQ("")} className="absolute right-3 top-1/2 -translate-y-1/2 text-[#ABABAB] hover:text-[#333]">
                    <X size={15} />
                  </button>
                ) : (
                  <button type="button" onClick={() => setSearchOpen(false)} className="absolute right-3 top-1/2 -translate-y-1/2 text-[#ABABAB] hover:text-[#333]">
                    <X size={15} />
                  </button>
                )}
              </form>
            </div>
          </div>
        )}
      </header>

      {/* ── Bottom nav (mobile only) ── */}
      <nav className="bottom-nav md:hidden">
        <div className="bottom-nav-inner">
          {BOT_NAV.map(({ label, to, icon: Icon, cart }) => (
            <Link
              key={to}
              to={to}
              className={clsx("bottom-nav-item", isActive(to) && "active")}
            >
              <div className="relative">
                <Icon size={22} strokeWidth={isActive(to) ? 2.5 : 1.8} />
                {cart && totalItems > 0 && (
                  <span className="absolute -top-1.5 -right-2 w-4 h-4 bg-violet-600 text-white text-[8px] font-bold rounded-full flex items-center justify-center leading-none">
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
