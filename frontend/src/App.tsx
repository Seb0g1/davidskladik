import { Activity, AlertCircle, AlertTriangle, BadgeDollarSign, CirclePlay, ClipboardList, PackageCheck, RefreshCcw, Settings, ShoppingCart, Sparkles, Truck , Star } from "lucide-react";
import { ReactNode, useEffect, useState } from "react";
import { WarehousePage } from "./routes/WarehousePage";
import { NotificationsBell } from "./components/NotificationsBell";
import { ReviewsPage } from "./routes/ReviewsPage";
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

type AppRoute = "reviews" | "warehouse" | "picking-list" | "suppliers" | "operations" | "supplier-cart" | "recovery-queue" | "prices" | "problem-products" | "finance" | "settings" | "system" | "ai-drafts" | "no-supplier";
type SessionState = { authenticated?: boolean; role?: string | null; username?: string | null };

const navItems: Array<{ route: AppRoute; href: string; label: string; icon: ReactNode }> = [
  { route: "warehouse", href: "/app/warehouse", label: "Каталог", icon: <PackageCheck size={16} /> },
  { route: "picking-list", href: "/app/picking-list", label: "Сборка", icon: <ClipboardList size={16} /> },
  { route: "suppliers", href: "/app/suppliers", label: "Поставщики", icon: <Truck size={16} /> },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} /> },
  { route: "supplier-cart", href: "/app/supplier-cart", label: "Автокорзина", icon: <ShoppingCart size={16} /> },
  { route: "recovery-queue", href: "/app/recovery-queue", label: "Восстановление", icon: <RefreshCcw size={16} /> },
  { route: "reviews", href: "/app/reviews", label: "Отзывы", icon: <Star size={16} /> },
  { route: "prices", href: "/app/prices", label: "Цены", icon: <BadgeDollarSign size={16} /> },
  { route: "problem-products", href: "/app/problem-products", label: "Проблемные товары", icon: <AlertTriangle size={16} /> },
  { route: "finance", href: "/app/finance", label: "Финансы", icon: <BadgeDollarSign size={16} /> },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} /> },
  { route: "system", href: "/app/system", label: "Система", icon: <Activity size={16} /> },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} /> },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} /> },
];

function currentRoute(): AppRoute {
  const path = window.location.pathname;
  if (path.startsWith("/app/picking-list")) return "picking-list";
  if (path.startsWith("/app/suppliers")) return "suppliers";
  if (path.startsWith("/app/operations")) return "operations";
  if (path.startsWith("/app/supplier-cart")) return "supplier-cart";
  if (path.startsWith("/app/recovery-queue")) return "recovery-queue";
  if (path.startsWith("/app/reviews")) return "reviews";
  if (path.startsWith("/app/prices")) return "prices";
  if (path.startsWith("/app/problem-products")) return "problem-products";
  if (path.startsWith("/app/finance")) return "finance";
  if (path.startsWith("/app/settings")) return "settings";
  if (path.startsWith("/app/system")) return "system";
  if (path.startsWith("/app/ai-drafts")) return "ai-drafts";
  if (path.startsWith("/app/no-supplier")) return "no-supplier";
  return "warehouse";
}

function AppShell() {
  const [route, setRoute] = useState<AppRoute>(() => currentRoute());
  const [session, setSession] = useState<SessionState | null>(null);
  useEffect(() => {
    const onPop = () => setRoute(currentRoute());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
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
  };
  const isAdmin = session?.role === "admin";
  const canUseStaffRoutes = session?.role === "manager" || isAdmin;
  const canUseAdminRoutes = session === null ? false : isAdmin;
  const headerRoutes = new Set<AppRoute>(isAdmin ? ["warehouse", "picking-list", "suppliers", "supplier-cart", "reviews", "settings"] : ["warehouse", "picking-list"]);
  const visibleNavItems = navItems.filter((item) => headerRoutes.has(item.route));
  const accessDenied = (route !== "warehouse" && route !== "picking-list" && !canUseAdminRoutes) || (route === "picking-list" && !canUseStaffRoutes);
  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand-lockup">
          <img className="brand-logo" src="/logo1.png" alt="Magic Vibe" />
          <div>
            <span className="eyebrow">Рабочий интерфейс</span>
            <h1>ДавидСклад</h1>
          </div>
        </div>
        <NotificationsBell />
        <nav>
          {visibleNavItems.map((item) => (
            <a key={item.route} className={route === item.route ? "is-active" : ""} href={item.href} onClick={(event) => navigate(event, item.href)}>
              {item.icon}{item.label}
            </a>
          ))}
        </nav>
      </header>
      {accessDenied ? (
        <section className="access-denied-panel">
          <AlertCircle size={24} />
          <strong>Нет доступа</strong>
          <span>Для роли manager доступны каталог, привязки PriceMaster и лист сборки.</span>
          <a href="/app/warehouse" onClick={(event) => navigate(event, "/app/warehouse")}>Вернуться в каталог</a>
        </section>
      ) : null}
      {!accessDenied && route === "operations" ? <OperationsPage /> : null}
      {!accessDenied && route === "picking-list" ? <PickingListPage /> : null}
      {!accessDenied && route === "suppliers" ? <SuppliersPage /> : null}
      {!accessDenied && route === "supplier-cart" ? <SupplierCartPage /> : null}
      {!accessDenied && route === "recovery-queue" ? <RecoveryQueuePage /> : null}
      {!accessDenied && route === "reviews" ? <ReviewsPage /> : null}
      {!accessDenied && route === "prices" ? <PricesPage /> : null}
      {!accessDenied && route === "problem-products" ? <ProblemProductsPage /> : null}
      {!accessDenied && route === "finance" ? <FinancePage /> : null}
      {!accessDenied && route === "settings" ? <SettingsPage /> : null}
      {!accessDenied && route === "system" ? <SystemPage /> : null}
      {!accessDenied && route === "ai-drafts" ? <AiDraftsPage /> : null}
      {!accessDenied && route === "no-supplier" ? <NoSupplierPage /> : null}
      {route === "warehouse" ? <WarehousePage isAdmin={isAdmin} /> : null}
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
