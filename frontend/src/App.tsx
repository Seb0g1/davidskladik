import { Activity, AlertCircle, AlertTriangle, BadgeDollarSign, BarChart3, ChevronDown, CirclePlay, ClipboardList, HandCoins, Home, Loader2, LogOut, Menu, PackageCheck, RefreshCcw, Settings, ShoppingCart, Sparkles, Truck, Star, HelpCircle, MessageCircle, MessageCircleHeart, Tag, UserCircle, Upload } from "lucide-react";
import { lazy, ReactNode, Suspense, useEffect, useRef, useState } from "react";
import { NotificationsBell } from "./components/NotificationsBell";
import { SystemHealthIndicator } from "./components/SystemHealthIndicator";
import { ThemeSwitcher } from "./components/ThemeSwitcher";

// Страницы грузятся лениво: один бандл на всё приложение весил 780 КБ и
// тормозил первый вход — теперь каждый раздел приезжает своим чанком.
const WarehousePage = lazy(() => import("./routes/WarehousePage").then((m) => ({ default: m.WarehousePage })));
const DashboardPage = lazy(() => import("./routes/DashboardPage").then((m) => ({ default: m.DashboardPage })));
const ReviewsPage = lazy(() => import("./routes/ReviewsPage").then((m) => ({ default: m.ReviewsPage })));
const QuestionsPage = lazy(() => import("./routes/QuestionsPage").then((m) => ({ default: m.QuestionsPage })));
const ChatsPage = lazy(() => import("./routes/ChatsPage").then((m) => ({ default: m.ChatsPage })));
const ImportPage = lazy(() => import("./routes/ImportPage").then((m) => ({ default: m.ImportPage })));
const AvitoPage = lazy(() => import("./routes/AvitoPage").then((m) => ({ default: m.AvitoPage })));
const OperationsPage = lazy(() => import("./routes/OperationsPage").then((m) => ({ default: m.OperationsPage })));
const SettingsPage = lazy(() => import("./routes/SettingsPage").then((m) => ({ default: m.SettingsPage })));
const AiDraftsPage = lazy(() => import("./routes/AiDraftsPage").then((m) => ({ default: m.AiDraftsPage })));
const NoSupplierPage = lazy(() => import("./routes/NoSupplierPage").then((m) => ({ default: m.NoSupplierPage })));
const SupplierCartPage = lazy(() => import("./routes/SupplierCartPage").then((m) => ({ default: m.SupplierCartPage })));
const RecoveryQueuePage = lazy(() => import("./routes/RecoveryQueuePage").then((m) => ({ default: m.RecoveryQueuePage })));
const PricesPage = lazy(() => import("./routes/PricesPage").then((m) => ({ default: m.PricesPage })));
const SystemPage = lazy(() => import("./routes/SystemPage").then((m) => ({ default: m.SystemPage })));
const PickingListPage = lazy(() => import("./routes/PickingListPage").then((m) => ({ default: m.PickingListPage })));
const ProblemProductsPage = lazy(() => import("./routes/ProblemProductsPage").then((m) => ({ default: m.ProblemProductsPage })));
const FinancePage = lazy(() => import("./routes/FinancePage").then((m) => ({ default: m.FinancePage })));
const SuppliersPage = lazy(() => import("./routes/SuppliersPage").then((m) => ({ default: m.SuppliersPage })));
const StatisticsPage = lazy(() => import("./routes/StatisticsPage").then((m) => ({ default: m.StatisticsPage })));
const ConsignmentPage = lazy(() => import("./routes/ConsignmentPage").then((m) => ({ default: m.ConsignmentPage })));
const NewProductsPage = lazy(() => import("./routes/NewProductsPage").then((m) => ({ default: m.NewProductsPage })));
const ShopAdminPage = lazy(() => import("./routes/ShopAdminPage").then((m) => ({ default: m.default })));
const TnvedPage = lazy(() => import("./routes/TnvedPage").then((m) => ({ default: m.TnvedPage })));
const SupportChatsPage = lazy(() => import("./routes/SupportChatsPage").then((m) => ({ default: m.SupportChatsPage })));
const BrandBansPage = lazy(() => import("./routes/BrandBansPage").then((m) => ({ default: m.BrandBansPage })));
const BrandsTnvedPage = lazy(() => import("./routes/BrandsTnvedPage").then((m) => ({ default: m.BrandsTnvedPage })));

type AppRoute = "dashboard" | "import" | "avito" | "shop" | "chats" | "questions" | "reviews" | "warehouse" | "picking-list" | "suppliers" | "operations" | "supplier-cart" | "recovery-queue" | "prices" | "problem-products" | "finance" | "consignment" | "statistics" | "settings" | "system" | "ai-drafts" | "no-supplier" | "new-products" | "tnved" | "support" | "brand-bans" | "brands-tnved";
type SessionState = { authenticated?: boolean; role?: string | null; username?: string | null; allowedPages?: string[] | null };

const navItems: Array<{ route: AppRoute; href: string; label: string; icon: ReactNode }> = [
  { route: "dashboard", href: "/app/dashboard", label: "Дашборд", icon: <Home size={16} /> },
  { route: "warehouse", href: "/app/warehouse", label: "Склад", icon: <PackageCheck size={16} /> },
  { route: "picking-list", href: "/app/picking-list", label: "Сборка", icon: <ClipboardList size={16} /> },
  { route: "avito", href: "/app/avito", label: "Автозагрузка Avito", icon: <Upload size={16} /> },
  { route: "consignment", href: "/app/consignment", label: "Реализация", icon: <HandCoins size={16} /> },
  { route: "reviews", href: "/app/reviews", label: "Отзывы", icon: <Star size={16} /> },
  { route: "chats", href: "/app/chats", label: "Чаты", icon: <MessageCircle size={16} /> },
  { route: "questions", href: "/app/questions", label: "Вопросы", icon: <HelpCircle size={16} /> },
  { route: "support", href: "/app/support", label: "Поддержка сайта", icon: <MessageCircleHeart size={16} /> },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} /> },
  { route: "system", href: "/app/system", label: "Система", icon: <Activity size={16} /> },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} /> },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} /> },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} /> },
  { route: "recovery-queue", href: "/app/recovery-queue", label: "Восстановление", icon: <RefreshCcw size={16} /> },
  { route: "suppliers", href: "/app/suppliers", label: "Поставщики", icon: <Truck size={16} /> },
  { route: "shop", href: "/app/shop", label: "Магазин MV", icon: <Sparkles size={16} /> },
  { route: "import", href: "/app/import", label: "Импорт на Яндекс", icon: <Upload size={16} /> },
  { route: "supplier-cart", href: "/app/supplier-cart", label: "Автокорзина", icon: <ShoppingCart size={16} /> },
  { route: "prices", href: "/app/prices", label: "Цены", icon: <BadgeDollarSign size={16} /> },
  { route: "statistics", href: "/app/statistics", label: "Статистика", icon: <BarChart3 size={16} /> },
  { route: "problem-products", href: "/app/problem-products", label: "Проблемные товары", icon: <AlertTriangle size={16} /> },
  { route: "finance", href: "/app/finance", label: "Финансы", icon: <BadgeDollarSign size={16} /> },
  { route: "new-products", href: "/app/new-products", label: "Новые товары", icon: <Sparkles size={16} /> },
  { route: "tnved", href: "/app/tnved", label: "Коды ТН ВЭД", icon: <Tag size={16} /> },
  { route: "brand-bans", href: "/app/brand-bans", label: "Запрет брендов", icon: <AlertTriangle size={16} /> },
  { route: "brands-tnved", href: "/app/brands-tnved", label: "Бренды / ТН ВЭД", icon: <Tag size={16} /> },
];

// Сайдбар: первые пять пунктов — без заголовка, дальше сворачиваемые группы.
const NAV_SECTIONS: Array<{ id: string; title?: string; routes: AppRoute[] }> = [
  { id: "main", routes: ["dashboard", "warehouse", "picking-list", "supplier-cart", "consignment"] },
  { id: "clients", title: "Работа с клиентами", routes: ["reviews", "chats", "questions", "support"] },
  { id: "admin", title: "Настройки", routes: ["settings", "system", "ai-drafts", "no-supplier", "operations", "recovery-queue"] },
  { id: "extra", title: "Дополнительное", routes: ["suppliers", "shop", "import", "avito", "prices", "statistics", "problem-products", "finance", "new-products", "tnved", "brand-bans", "brands-tnved"] },
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
  if (path.startsWith("/app/support")) return "support";
  if (path.startsWith("/app/import")) return "import";
  if (path.startsWith("/app/avito")) return "avito";
  if (path.startsWith("/app/shop")) return "shop";
  if (path.startsWith("/app/prices")) return "prices";
  if (path.startsWith("/app/problem-products")) return "problem-products";
  if (path.startsWith("/app/finance")) return "finance";
  if (path.startsWith("/app/consignment")) return "consignment";
  if (path.startsWith("/app/statistics")) return "statistics";
  if (path.startsWith("/app/settings")) return "settings";
  if (path.startsWith("/app/system")) return "system";
  if (path.startsWith("/app/ai-drafts")) return "ai-drafts";
  if (path.startsWith("/app/no-supplier")) return "no-supplier";
  if (path.startsWith("/app/new-products")) return "new-products";
  if (path.startsWith("/app/tnved")) return "tnved";
  if (path.startsWith("/app/brand-bans")) return "brand-bans";
  if (path.startsWith("/app/brands-tnved")) return "brands-tnved";
  return "warehouse";
}

// Embedded page preview (iframe inside the page-access modal): hide the app chrome.
const isPreviewEmbed = new URLSearchParams(window.location.search).get("embed") === "preview";

function AppShell() {
  const [route, setRoute] = useState<AppRoute>(() => currentRoute());
  const [session, setSession] = useState<SessionState | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarCompact, setSidebarCompact] = useState(false);
  const [collapsedSections, setCollapsedSections] = useState<Record<string, boolean>>(() => {
    try {
      const stored = JSON.parse(window.localStorage.getItem("nav-collapsed-sections") || "");
      if (stored && typeof stored === "object") return stored;
    } catch { /* дефолт ниже */ }
    return { extra: true };
  });
  const toggleNavSection = (id: string) => {
    setCollapsedSections((current) => {
      const next = { ...current, [id]: !current[id] };
      try { window.localStorage.setItem("nav-collapsed-sections", JSON.stringify(next)); } catch { /* приватный режим */ }
      return next;
    });
  };
  const [isMobileNav, setIsMobileNav] = useState(() => window.matchMedia("(max-width: 980px)").matches);
  const activeNavRef = useRef<HTMLAnchorElement | null>(null);
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
      if (event.key === "Escape") setSidebarOpen(false);
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

  useEffect(() => {
    let cancelled = false;
    const BASE_TITLE = "DavidSklad";
    async function poll() {
      try {
        const res = await fetch("/api/supplier-picking-list?status=open&limit=1", { credentials: "include" });
        if (!res.ok || cancelled) return;
        const data = await res.json().catch(() => null);
        const open = Number((data?.summary as Record<string,number> | undefined)?.open ?? 0);
        if (!cancelled) document.title = open > 0 ? `(${open}) ${BASE_TITLE}` : BASE_TITLE;
      } catch { /* ignore */ }
    }
    void poll();
    const id = window.setInterval(poll, 60_000);
    return () => { cancelled = true; clearInterval(id); document.title = BASE_TITLE; };
  }, []);
  const navigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    event.preventDefault();
    window.history.pushState(null, "", href);
    setRoute(currentRoute());
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
  // Per-user page visibility: admins see everything, others see the pages granted
  // in Настройки -> Сотрудники (empty grant falls back to the old manager defaults).
  const defaultEmployeeRoutes: AppRoute[] = ["warehouse", "picking-list"];
  const allRouteKeys = navItems.map((item) => item.route);
  const grantedPages = (session?.allowedPages || []).filter((page): page is AppRoute => (allRouteKeys as string[]).includes(page));
  const headerRoutes = new Set<AppRoute>(isAdmin ? allRouteKeys : (grantedPages.length ? grantedPages : defaultEmployeeRoutes));
  const visibleNavItems = navItems.filter((item) => headerRoutes.has(item.route));
  useEffect(() => {
    activeNavRef.current?.scrollIntoView({ block: "nearest" });
  }, [route, visibleNavItems.length]);
  const accessDenied = sessionReady && !headerRoutes.has(route);
  useEffect(() => {
    // Users without access to the current page land on their first granted page
    // (the sponsor logs in and goes straight to Реализация).
    if (!sessionReady || !accessDenied) return;
    const fallback = navItems.find((item) => headerRoutes.has(item.route));
    if (fallback && fallback.route !== route) {
      window.history.replaceState(null, "", fallback.href);
      setRoute(currentRoute());
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionReady, accessDenied]);
  const roleLabel = !sessionReady ? "Загрузка" : (isAdmin ? "Администратор" : (session?.role === "manager" ? "Менеджер" : "Сотрудник"));
  const navExpanded = isMobileNav ? sidebarOpen : !sidebarCompact;
  return (
    <main className={`app-shell with-sidebar${sidebarOpen ? " sidebar-open" : ""}${sidebarCompact ? " sidebar-compact" : ""}${isPreviewEmbed ? " preview-embed" : ""}`}>
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
          {NAV_SECTIONS.map((section) => {
            const items = section.routes
              .map((sectionRoute) => visibleNavItems.find((item) => item.route === sectionRoute))
              .filter((item): item is typeof navItems[number] => Boolean(item));
            if (!items.length) return null;
            const containsActive = items.some((item) => item.route === route);
            // Свёрнутая группа с активной страницей раскрывается, чтобы подсветка не
            // терялась; в компактном (иконочном) режиме заголовков нет — показываем всё.
            const collapsed = Boolean(section.title && collapsedSections[section.id] && !containsActive && navExpanded);
            return (
              <div className={`nav-section${collapsed ? " is-collapsed" : ""}`} key={section.id}>
                {section.title ? (
                  <button type="button" className="nav-section-title" aria-expanded={!collapsed} onClick={() => toggleNavSection(section.id)}>
                    <span>{section.title}</span>
                    <ChevronDown size={13} className="nav-section-chevron" />
                  </button>
                ) : null}
                {!collapsed ? items.map((item) => (
                  <a key={item.route} ref={route === item.route ? activeNavRef : undefined} className={route === item.route ? "is-active" : ""} href={item.href} onClick={(event) => navigate(event, item.href)}>
                    {item.icon}{item.label}
                  </a>
                )) : null}
              </div>
            );
          })}
        </nav>
        <div className="side-user-card">
          <span>Статистика пользователя</span>
          <strong>{session?.username || roleLabel}</strong>
          <small>Сегодня: каталог, привязки и сборка</small>
        </div>
        <button
          className="side-logout"
          type="button"
          onClick={() => {
            fetch("/api/logout", { method: "POST" }).finally(() => {
              window.location.href = "/login.html";
            });
          }}
        ><LogOut size={16} /> Выйти</button>
      </aside>
      <button className="sidebar-backdrop" type="button" aria-label="Закрыть меню" onClick={() => setSidebarOpen(false)} />
      <div className="app-content">
      <header className="topbar content-topbar">
        <button className="topbar-menu" type="button" aria-label={navExpanded ? "Свернуть меню" : "Открыть меню"} aria-expanded={navExpanded} onClick={toggleNavigation}><Menu size={22} /></button>
        <div className="topbar-spacer" />
        <div className="topbar-actions">
          <ThemeSwitcher />
          {isAdmin ? <SystemHealthIndicator /> : null}
          <NotificationsBell />
          <div className="topbar-user" title={roleLabel}>
            <UserCircle size={30} />
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
          <span>Эта страница не входит в ваш доступ. Обратитесь к администратору.</span>
          {visibleNavItems[0] ? <a href={visibleNavItems[0].href} onClick={(event) => navigate(event, visibleNavItems[0].href)}>Перейти: {visibleNavItems[0].label}</a> : null}
        </section>
      ) : null}
      <Suspense fallback={<div className="page-lazy-loading"><Loader2 className="spin" size={22} /> Загружаю раздел…</div>}>
      {sessionReady && !accessDenied && route === "dashboard" ? <DashboardPage /> : null}
      {sessionReady && !accessDenied && route === "operations" ? <OperationsPage /> : null}
      {sessionReady && !accessDenied && route === "picking-list" ? <PickingListPage /> : null}
      {sessionReady && !accessDenied && route === "suppliers" ? <SuppliersPage /> : null}
      {sessionReady && !accessDenied && route === "supplier-cart" ? <SupplierCartPage /> : null}
      {sessionReady && !accessDenied && route === "recovery-queue" ? <RecoveryQueuePage /> : null}
      {sessionReady && !accessDenied && route === "reviews" ? <ReviewsPage /> : null}
      {sessionReady && !accessDenied && route === "questions" ? <QuestionsPage /> : null}
      {sessionReady && !accessDenied && route === "chats" ? <ChatsPage /> : null}
      {sessionReady && !accessDenied && route === "support" ? <SupportChatsPage /> : null}
      {sessionReady && !accessDenied && route === "import" ? <ImportPage /> : null}
      {sessionReady && !accessDenied && route === "avito" ? <AvitoPage /> : null}
      {sessionReady && !accessDenied && route === "shop" ? <ShopAdminPage /> : null}
      {sessionReady && !accessDenied && route === "prices" ? <PricesPage /> : null}
      {sessionReady && !accessDenied && route === "problem-products" ? <ProblemProductsPage /> : null}
      {sessionReady && !accessDenied && route === "finance" ? <FinancePage /> : null}
      {sessionReady && !accessDenied && route === "consignment" ? <ConsignmentPage /> : null}
      {sessionReady && !accessDenied && route === "statistics" ? <StatisticsPage /> : null}
      {sessionReady && !accessDenied && route === "settings" ? <SettingsPage /> : null}
      {sessionReady && !accessDenied && route === "system" ? <SystemPage /> : null}
      {sessionReady && !accessDenied && route === "ai-drafts" ? <AiDraftsPage /> : null}
      {sessionReady && !accessDenied && route === "no-supplier" ? <NoSupplierPage /> : null}
      {sessionReady && !accessDenied && route === "new-products" ? <NewProductsPage /> : null}
      {sessionReady && !accessDenied && route === "tnved" ? <TnvedPage /> : null}
      {sessionReady && !accessDenied && route === "brand-bans" ? <BrandBansPage /> : null}
      {sessionReady && !accessDenied && route === "brands-tnved" ? <BrandsTnvedPage /> : null}
      {sessionReady && !accessDenied && route === "warehouse" ? <WarehousePage isAdmin={isAdmin} /> : null}
      </Suspense>
      </div>
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
