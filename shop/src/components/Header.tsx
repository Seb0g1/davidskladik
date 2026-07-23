import { useState, useEffect, useRef } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Search, ShoppingBag, User, X, Menu, LogOut, Package } from "lucide-react";
import clsx from "clsx";
import { useCart } from "../CartContext";
import { useAuth } from "../AuthContext";
import AuthModal from "./AuthModal";

const NAV = [
  { label: "Каталог", to: "/catalog" },
  { label: "Бренды", to: "/catalog?sort=name" },
  { label: "Новинки", to: "/catalog?sort=price_desc" },
  { label: "Акции", to: "/catalog/sale", accent: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const { customer, logout } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const [q, setQ] = useState("");
  const [scrolled, setScrolled] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const [authModal, setAuthModal] = useState<{ open: boolean; tab: "login" | "register" }>({ open: false, tab: "login" });

  const searchRef = useRef<HTMLInputElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const fn = () => setScrolled(window.scrollY > 8);
    window.addEventListener("scroll", fn, { passive: true });
    return () => window.removeEventListener("scroll", fn);
  }, []);

  useEffect(() => { setMenuOpen(false); setSearchOpen(false); }, [location.pathname]);

  useEffect(() => {
    if (searchOpen) setTimeout(() => searchRef.current?.focus(), 80);
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
    if (q.trim()) { navigate(`/catalog?q=${encodeURIComponent(q.trim())}`); setSearchOpen(false); setQ(""); }
  }

  return (
    <>
      <header className={clsx(
        "sticky top-0 z-50 transition-all duration-300",
        scrolled
          ? "bg-white/90 backdrop-blur-xl border-b border-gray-100 shadow-sm"
          : "bg-white border-b border-gray-100"
      )}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-14 flex items-center gap-4">
          {/* Logo */}
          <Link to="/" className="flex-shrink-0 flex items-center gap-2 group">
            <div className="w-8 h-8 rounded-xl bg-violet-600 flex items-center justify-center">
              <span className="text-white text-xs font-bold tracking-tight">MV</span>
            </div>
            <span className="font-bold text-[15px] tracking-tight text-apple-black group-hover:text-violet-600 transition-colors hidden sm:block">
              Magic Vibes
            </span>
          </Link>

          {/* Desktop nav */}
          <nav className="hidden lg:flex items-center gap-1 flex-1 justify-center">
            {NAV.map((link) => (
              <Link
                key={link.to}
                to={link.to}
                className={clsx(
                  "px-4 py-2 rounded-lg text-[13px] font-medium transition-colors",
                  link.accent
                    ? "text-rose-500 hover:bg-rose-50"
                    : "text-apple-black/70 hover:text-apple-black hover:bg-gray-100"
                )}
              >
                {link.label}
              </Link>
            ))}
          </nav>

          {/* Actions */}
          <div className="flex items-center gap-1 ml-auto">
            {/* Search toggle */}
            <button
              onClick={() => setSearchOpen(!searchOpen)}
              className="p-2.5 rounded-xl text-apple-black/60 hover:text-apple-black hover:bg-gray-100 transition-colors"
            >
              <Search size={18} />
            </button>

            {/* User */}
            <div className="relative" ref={userMenuRef}>
              {customer ? (
                <button
                  onClick={() => setUserMenuOpen(!userMenuOpen)}
                  className="p-2.5 rounded-xl text-apple-black/60 hover:text-apple-black hover:bg-gray-100 transition-colors"
                >
                  <User size={18} />
                </button>
              ) : (
                <button
                  onClick={() => setAuthModal({ open: true, tab: "login" })}
                  className="hidden sm:flex items-center gap-1.5 px-3.5 py-2 text-[13px] font-medium text-apple-black/70 hover:text-apple-black hover:bg-gray-100 rounded-xl transition-colors"
                >
                  <User size={16} /> Войти
                </button>
              )}

              {userMenuOpen && customer && (
                <div className="absolute right-0 top-full mt-2 w-52 bg-white rounded-2xl shadow-xl-soft border border-gray-100 py-2 animate-scale-in">
                  <div className="px-4 py-2 border-b border-gray-100 mb-1">
                    <div className="text-[13px] font-semibold text-apple-black truncate">{customer.firstName || customer.email}</div>
                    <div className="text-xs text-apple-gray truncate">{customer.email}</div>
                  </div>
                  <Link to="/orders" onClick={() => setUserMenuOpen(false)}
                    className="flex items-center gap-2.5 px-4 py-2.5 text-[13px] text-apple-black hover:bg-gray-50 transition-colors">
                    <Package size={15} className="text-apple-gray" /> Мои заказы
                  </Link>
                  <button onClick={() => { logout(); setUserMenuOpen(false); }}
                    className="w-full flex items-center gap-2.5 px-4 py-2.5 text-[13px] text-red-500 hover:bg-red-50 transition-colors">
                    <LogOut size={15} /> Выйти
                  </button>
                </div>
              )}
            </div>

            {/* Cart */}
            <Link
              to="/cart"
              className="relative p-2.5 rounded-xl text-apple-black/60 hover:text-apple-black hover:bg-gray-100 transition-colors"
            >
              <ShoppingBag size={18} />
              {totalItems > 0 && (
                <span className="absolute top-1 right-1 w-4 h-4 bg-violet-600 text-white text-[9px] font-bold rounded-full flex items-center justify-center leading-none">
                  {totalItems > 9 ? "9+" : totalItems}
                </span>
              )}
            </Link>

            {/* Mobile menu */}
            <button
              onClick={() => setMenuOpen(!menuOpen)}
              className="lg:hidden p-2.5 rounded-xl text-apple-black/60 hover:text-apple-black hover:bg-gray-100 transition-colors"
            >
              {menuOpen ? <X size={18} /> : <Menu size={18} />}
            </button>
          </div>
        </div>

        {/* Search bar */}
        {searchOpen && (
          <div className="border-t border-gray-100 bg-white/95 backdrop-blur-xl animate-fade-in">
            <div className="max-w-3xl mx-auto px-4 py-3">
              <form onSubmit={handleSearch} className="relative">
                <Search size={16} className="absolute left-4 top-1/2 -translate-y-1/2 text-apple-gray" />
                <input
                  ref={searchRef}
                  type="text"
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Название, бренд, артикул..."
                  className="w-full pl-10 pr-12 py-3 bg-apple-gray-bg rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-violet-400 transition-all"
                />
                {q && (
                  <button type="button" onClick={() => setQ("")} className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-apple-gray hover:text-apple-black">
                    <X size={14} />
                  </button>
                )}
              </form>
            </div>
          </div>
        )}

        {/* Mobile nav */}
        {menuOpen && (
          <nav className="lg:hidden border-t border-gray-100 bg-white animate-fade-in">
            <div className="max-w-7xl mx-auto px-4 py-3 flex flex-col gap-1">
              {NAV.map((link) => (
                <Link key={link.to} to={link.to}
                  className={clsx("px-4 py-3 rounded-xl text-sm font-medium transition-colors", link.accent ? "text-rose-500" : "text-apple-black hover:bg-gray-50")}
                >
                  {link.label}
                </Link>
              ))}
              {!customer && (
                <button onClick={() => { setMenuOpen(false); setAuthModal({ open: true, tab: "login" }); }}
                  className="flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium text-violet-600 hover:bg-violet-50 transition-colors">
                  <User size={16} /> Войти / Регистрация
                </button>
              )}
            </div>
          </nav>
        )}
      </header>

      <AuthModal
        open={authModal.open}
        defaultTab={authModal.tab}
        onClose={() => setAuthModal((s) => ({ ...s, open: false }))}
      />
    </>
  );
}
