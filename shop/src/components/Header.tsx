import { useState, useRef, useEffect } from "react";
import { Link, useNavigate, useLocation } from "react-router-dom";
import { Search, ShoppingBag, Menu, X, Sparkles } from "lucide-react";
import { useCart } from "../CartContext";
import clsx from "clsx";

const NAV_LINKS = [
  { label: "Парфюмерия", slug: "parfumery" },
  { label: "Уход", slug: "care" },
  { label: "Для волос", slug: "hair" },
  { label: "Для мужчин", slug: "men" },
  { label: "Подарки", slug: "gifts" },
  { label: "Бренды", slug: "brands" },
  { label: "Акции", slug: "sale", highlight: true },
];

export default function Header() {
  const { totalItems } = useCart();
  const navigate = useNavigate();
  const location = useLocation();
  const [q, setQ] = useState("");
  const [menuOpen, setMenuOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 10);
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  // close mobile menu on route change
  useEffect(() => { setMenuOpen(false); }, [location.pathname]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    if (q.trim()) navigate(`/catalog?q=${encodeURIComponent(q.trim())}`);
  }

  return (
    <header className={clsx(
      "sticky top-0 z-50 bg-white transition-shadow duration-200",
      scrolled ? "shadow-md" : "shadow-sm"
    )}>
      {/* Top promo bar */}
      <div className="bg-gradient-to-r from-violet-700 to-brand-secondary text-white text-center text-xs py-2 px-4 font-medium tracking-wide">
        Бесплатная доставка от 3 000 ₽ · Оригинальная продукция · Доставка Ozon
      </div>

      {/* Main header */}
      <div className="max-w-7xl mx-auto px-4 py-3 flex items-center gap-4">
        {/* Logo */}
        <Link to="/" className="flex items-center gap-2 flex-shrink-0">
          <Sparkles className="text-violet-600" size={24} />
          <div>
            <div className="font-display font-bold text-lg leading-none text-brand-dark">Magic Vibes</div>
            <div className="text-[10px] text-gray-400 uppercase tracking-widest leading-none">Парфюмерия</div>
          </div>
        </Link>

        {/* Search */}
        <form onSubmit={handleSearch} className="flex-1 max-w-2xl mx-auto">
          <div className="relative">
            <input
              ref={inputRef}
              type="text"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Поиск парфюмерии, бренда, артикула..."
              className="w-full pl-4 pr-12 py-2.5 rounded-xl border border-gray-200 bg-gray-50 focus:bg-white focus:border-violet-400 focus:outline-none focus:ring-2 focus:ring-violet-100 text-sm transition-all"
            />
            <button
              type="submit"
              className="absolute right-2 top-1/2 -translate-y-1/2 bg-violet-600 text-white rounded-lg px-3 py-1.5 hover:bg-violet-700 transition-colors"
            >
              <Search size={16} />
            </button>
          </div>
        </form>

        {/* Actions */}
        <div className="flex items-center gap-3">
          <Link
            to="/cart"
            className="relative flex items-center gap-1.5 bg-violet-50 hover:bg-violet-100 text-violet-700 px-4 py-2.5 rounded-xl transition-colors font-medium text-sm"
          >
            <ShoppingBag size={18} />
            <span className="hidden sm:inline">Корзина</span>
            {totalItems > 0 && (
              <span className="absolute -top-1.5 -right-1.5 bg-rose-500 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center font-bold">
                {totalItems > 9 ? "9+" : totalItems}
              </span>
            )}
          </Link>
          <button
            onClick={() => setMenuOpen((v) => !v)}
            className="lg:hidden p-2 rounded-lg hover:bg-gray-100 transition-colors"
          >
            {menuOpen ? <X size={22} /> : <Menu size={22} />}
          </button>
        </div>
      </div>

      {/* Category nav — desktop */}
      <nav className="hidden lg:block border-t border-gray-100 bg-white">
        <div className="max-w-7xl mx-auto px-4 flex gap-1">
          {NAV_LINKS.map((link) => (
            <Link
              key={link.slug}
              to={`/catalog/${link.slug}`}
              className={clsx(
                "px-4 py-3 text-sm font-medium transition-colors whitespace-nowrap border-b-2 border-transparent hover:border-violet-500 hover:text-violet-700",
                link.highlight
                  ? "text-rose-600 font-semibold hover:text-rose-700 hover:border-rose-500"
                  : "text-gray-700"
              )}
            >
              {link.label}
            </Link>
          ))}
        </div>
      </nav>

      {/* Mobile menu */}
      {menuOpen && (
        <nav className="lg:hidden border-t border-gray-100 bg-white shadow-lg">
          <div className="flex flex-col py-2">
            {NAV_LINKS.map((link) => (
              <Link
                key={link.slug}
                to={`/catalog/${link.slug}`}
                className={clsx(
                  "px-6 py-3 text-sm font-medium hover:bg-violet-50 transition-colors",
                  link.highlight ? "text-rose-600" : "text-gray-700"
                )}
              >
                {link.label}
              </Link>
            ))}
          </div>
        </nav>
      )}
    </header>
  );
}
