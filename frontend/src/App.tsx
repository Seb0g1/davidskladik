import { Activity, AlertCircle, AlertTriangle, BadgeDollarSign, BarChart3, ChevronDown, CirclePlay, ClipboardList, Home, LogOut, Menu, PackageCheck, RefreshCcw, Search, Settings, ShoppingCart, Sparkles, Truck, Star, HelpCircle, MessageCircle, UserCircle, Upload } from "lucide-react";
import { ReactNode, useEffect, useRef, useState } from "react";
import { WarehousePage } from "./routes/WarehousePage";
import { NotificationsBell } from "./components/NotificationsBell";
import { ThemeSwitcher } from "./components/ThemeSwitcher";
import { DashboardPage } from "./routes/DashboardPage";
import { ReviewsPage } from "./routes/ReviewsPage";
import { QuestionsPage } from "./routes/QuestionsPage";
import { ChatsPage } from "./routes/ChatsPage";
import { ImportPage } from "./routes/ImportPage";
import { OperationsPage } from "./routes/OperationsPage";
import { SettingsPage } from "./routes/SettingsPage";
import { AiDraftsPage } from "./routes/AiDraftsPage";
import { NoSupplierPage } from "./routes/NoSupplierPage";
import { SupplierCartPage } from "./routes/SupplierCartPage";
import { RecoveryQueuePage } from "./routes/RecoveryQueuePage";
import { PricesPage } from "./routes/PricesPage";
import { SystemPage } from "./routes/SystemPage";
import { PickingListPage } from "./routes/PickingListPage";
import { ProblemProductsPage } from "./routes/ProblemProductsPage";
import { FinancePage } from "./routes/FinancePage";
import { SuppliersPage } from "./routes/SuppliersPage";
import { StatisticsPage } from "./routes/StatisticsPage";

type AppRoute = "dashboard" | "import" | "chats" | "questions" | "reviews" | "warehouse" | "picking-list" | "suppliers" | "operations" | "supplier-cart" | "recovery-queue" | "prices" | "problem-products" | "finance" | "statistics" | "settings" | "system" | "ai-drafts" | "no-supplier";
type SessionState = { authenticated?: boolean; role?: string | null; username?: string | null };

const navItems: Array<{ route: AppRoute; href: string; label: string; icon: ReactNode }> = [
  { route: "dashboard", href: "/app/dashboard", label: "Дашборд", icon: <Home size={16} /> },
  { route: "warehouse", href: "/app/warehouse", label: "Склад", icon: <PackageCheck size={16} /> },
  { route: "suppliers", href: "/app/suppliers", label: "Поставщики", icon: <Truck size={16} /> },
  { route: "picking-list", href: "/app/picking-list", label: "Сборка", icon: <ClipboardList size={16} /> },
  { route: "reviews", href: "/app/reviews", label: "Отзывы", icon: <Star size={16} /> },
  { route: "chats", href: "/app/chats", label: "Чаты", icon: <MessageCircle size={16} /> },
  { route: "import", href: "/app/import", label: "Импорт на Яндекс", icon: <Upload size={16} /> },
  { route: "statistics", href: "/app/statistics", label: "Статистика", icon: <BarChart3 size={16} /> },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} /> },
  { route: "questions", href: "/app/questions", label: "Вопросы", icon: <HelpCircle size={16} /> },
  { route: "prices", href: "/app/prices", label: "Цены", icon: <BadgeDollarSign size={16} /> },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} /> },
  { route: "supplier-cart", href: "/app/supplier-cart", label: "Автокорзина", icon: <ShoppingCart size={16} /> },
  { route: "recovery-queue", href: "/app/recovery-queue", label: "Восстановление", icon: <RefreshCcw size={16} /> },
  { route: "problem-products", href: "/app/problem-products", label: "Проблемные товары", icon: <AlertTriangle size={16} /> },
  { route: "finance", href: "/app/finance", label: "Финансы", icon: <BadgeDollarSign size={16} /> },
  { route: "system", href: "/app/system", label: "Система", icon: <Activity size={16} /> },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} /> },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} /> },
];

function currentRoute(): AppRoute {
  const path = window.location.pathname;
  if (path.startsWith("/app/dashboard")) return "dashboard";
  if (path.startsWith("/app/picking-list")) return "picking-list";
  if (path.startsWith("/app/suppliers")) return "suppliers";
  if (path.startsWith("/app/operations")) return "operations";
  if (path.startsWith("/app/supplier-cart")) return "supplier-cart";
  if (path.startsWith("/app/recovery-queue")) return "recovery-queue";
  if (path.startsWith("/app/reviews")) return "reviews";
  if (path.startsWith("/app/questions")) return "questions";
  if (path.startsWith("/app/chats")) return "chats";
  if (path.startsWith("/app/import")) return "import";
  if (path.startsWith("/app/prices")) return "prices";
  if (path.startsWith("/app/problem-products")) return "problem-products";
  if (path.startsWith("/app/finance")) return "finance";
  if (path.startsWith("/app/statistics")) return "statistics";
  if (path.startsWith("/app/settings")) return "settings";
  if (path.startsWith("/app/system")) return "system";
  if (path.startsWith("/app/ai-drafts")) return "ai-drafts";
  if (path.startsWith("/app/no-supplier")) return "no-supplier";
  return "warehouse";
}

function AppShell() {
  const [route, setRoute] = useState<AppRoute>(() => currentRoute());
  const [session, setSession] = useState<SessionState | null>(null);
  const [globalSearch, setGlobalSearch] = useState("");
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarCompact, setSidebarCompact] = useState(false);
  const [isMobileNav, setIsMobileNav] = useState(() => window.matchMedia("(max-width: 980px)").matches);
  const activeNavRef = useRef<HTMLAnchorElement | null>(null);
  const globalSearchInputRef = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    const onPop = () => {
      setRoute(currentRoute());
      setSidebarOpen(false);
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSidebarOpen(false);
        if (document.activeElement === globalSearchInputRef.current) globalSearchInputRef.current?.blur();
      }
      if ((event.ctrlKey || event.metaKey) && event.key === "/") {
        event.preventDefault();
        setSidebarOpen(false);
        globalSearchInputRef.current?.focus();
        globalSearchInputRef.current?.select();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);
  useEffect(() => {
    const media = window.matchMedia("(max-width: 980px)");
    const sync = () => {
      setIsMobileNav(media.matches);
      if (!media.matches) setSidebarOpen(false);
    };
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);
  useEffect(() => {
    let active = true;
    fetch("/api/session", { credentials: "include" })
      .then((response) => response.ok ? response.json() : null)
      .then((payload) => {
        if (active) setSession(payload || { authenticated: false });
      })
      .catch(() => {
        if (active) setSession({ authenticated: false });
      });
    return () => {
      active = false;
    };
  }, []);
  const navigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    event.preventDefault();
    window.history.pushState(null, "", href);
    setRoute(currentRoute());
    setSidebarOpen(false);
  };
  const submitGlobalSearch = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const q = globalSearch.trim();
    if (!q) return;
    window.history.pushState(null, "", `/app/warehouse?q=${encodeURIComponent(q)}`);
    window.dispatchEvent(new PopStateEvent("popstate"));
    setRoute("warehouse");
    setSidebarOpen(false);
  };
  const toggleNavigation = () => {
    if (isMobileNav) {
      setSidebarOpen((value) => !value);
      return;
    }
    setSidebarCompact((value) => !value);
  };
  const sessionReady = session !== null;
  const isAdmin = session?.role === "admin";
  const canUseStaffRoutes = session?.role === "manager" || isAdmin;
  const canUseAdminRoutes = sessionReady ? isAdmin : false;
  const headerRoutes = new Set<AppRoute>(isAdmin ? ["dashboard", "warehouse", "suppliers", "picking-list", "reviews", "chats", "statistics", "settings", "questions", "prices", "operations", "supplier-cart", "recovery-queue", "problem-products", "finance", "system", "ai-drafts", "no-supplier", "import"] : ["warehouse", "picking-list"]);
  const visibleNavItems = navItems.filter((item) => headerRoutes.has(item.route));
  useEffect(() => {
    activeNavRef.current?.scrollIntoView({ block: "nearest" });
  }, [route, visibleNavItems.length]);
  const accessDenied = sessionReady && ((route !== "warehouse" && route !== "picking-list" && !canUseAdminRoutes) || (route === "picking-list" && !canUseStaffRoutes));
  const roleLabel = !sessionReady ? "Загрузка" : (isAdmin ? "Администратор" : (session?.role === "manager" ? "Менеджер" : "Сотрудник"));
  const navExpanded = isMobileNav ? sidebarOpen : !sidebarCompact;
  return (
    <main className={`app-shell with-sidebar${sidebarOpen ? " sidebar-open" : ""}${sidebarCompact ? " sidebar-compact" : ""}`}>
      <aside className="side-nav" aria-label="Основная навигация">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">D</span>
          <div>
            <h1>David<span>Sklad</span></h1>
          </div>
        </div>
        <div className="side-role-chip">
          <span className="role-dot" />
          {roleLabel}
        </div>
        <nav className="side-nav-links">
          {visibleNavItems.map((item) => (
            <a key={item.route} ref={route === item.route ? activeNavRef : undefined} className={route === item.route ? "is-active" : ""} href={item.href} onClick={(event) => navigate(event, item.href)}>
              {item.icon}{item.label}
            </a>
          ))}
        </nav>
        <div className="side-user-card">
          <span>Статистика пользователя</span>
          <strong>{session?.username || roleLabel}</strong>
          <small>Сегодня: каталог, привязки и сборка</small>
        </div>
        <a className="side-logout" href="/login.html"><LogOut size={16} /> Выйти</a>
      </aside>
      <button className="sidebar-backdrop" type="button" aria-label="Закрыть меню" onClick={() => setSidebarOpen(false)} />
      <div className="app-content">
      <header className="topbar content-topbar">
        <button className="topbar-menu" type="button" aria-label={navExpanded ? "Свернуть меню" : "Открыть меню"} aria-expanded={navExpanded} onClick={toggleNavigation}><Menu size={22} /></button>
        <form className="global-search" onSubmit={submitGlobalSearch}>
          <Search size={17} />
          <input ref={globalSearchInputRef} value={globalSearch} onChange={(event) => setGlobalSearch(event.target.value)} placeholder="Поиск по товарам, SKU, артикулу или штрихкоду" />
          <kbd>Ctrl + /</kbd>
        </form>
        <div className="topbar-actions">
          <ThemeSwitcher />
          <NotificationsBell />
          <div className="topbar-user">
            <UserCircle size={30} />
            <span>{roleLabel}</span>
            <ChevronDown size={15} />
          </div>
        </div>
      </header>
      {!sessionReady ? (
        <section className="app-loading-screen" aria-live="polite" aria-label="ДавидСклад загружается">
          <div className="premium-loader" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>
          <div>
            <span className="eyebrow">DavidSklad</span>
            <h2>Подготавливаю рабочее пространство</h2>
            <p>Проверяю сессию, собираю навигацию и поднимаю свежие данные склада.</p>
          </div>
          <div className="premium-skeleton-grid" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>
        </section>
      ) : null}
      {sessionReady && accessDenied ? (
        <section className="access-denied-panel">
          <AlertCircle size={24} />
          <strong>Нет доступа</strong>
          <span>Для роли manager доступны каталог, привязки PriceMaster и лист сборки.</span>
          <a href="/app/warehouse" onClick={(event) => navigate(event, "/app/warehouse")}>Вернуться в каталог</a>
        </section>
      ) : null}
      {sessionReady && !accessDenied && route === "dashboard" ? <DashboardPage /> : null}
      {sessionReady && !accessDenied && route === "operations" ? <OperationsPage /> : null}
      {sessionReady && !accessDenied && route === "picking-list" ? <PickingListPage /> : null}
      {sessionReady && !accessDenied && route === "suppliers" ? <SuppliersPage /> : null}
      {sessionReady && !accessDenied && route === "supplier-cart" ? <SupplierCartPage /> : null}
      {sessionReady && !accessDenied && route === "recovery-queue" ? <RecoveryQueuePage /> : null}
      {sessionReady && !accessDenied && route === "reviews" ? <ReviewsPage /> : null}
      {sessionReady && !accessDenied && route === "questions" ? <QuestionsPage /> : null}
      {sessionReady && !accessDenied && route === "chats" ? <ChatsPage /> : null}
      {sessionReady && !accessDenied && route === "import" ? <ImportPage /> : null}
      {sessionReady && !accessDenied && route === "prices" ? <PricesPage /> : null}
      {sessionReady && !accessDenied && route === "problem-products" ? <ProblemProductsPage /> : null}
      {sessionReady && !accessDenied && route === "finance" ? <FinancePage /> : null}
      {sessionReady && !accessDenied && route === "statistics" ? <StatisticsPage /> : null}
      {sessionReady && !accessDenied && route === "settings" ? <SettingsPage /> : null}
      {sessionReady && !accessDenied && route === "system" ? <SystemPage /> : null}
      {sessionReady && !accessDenied && route === "ai-drafts" ? <AiDraftsPage /> : null}
      {sessionReady && !accessDenied && route === "no-supplier" ? <NoSupplierPage /> : null}
      {sessionReady && route === "warehouse" ? <WarehousePage isAdmin={isAdmin} /> : null}
      </div>
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
