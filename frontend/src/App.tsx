import { AlertCircle, CirclePlay, PackageCheck, Settings, Sparkles } from "lucide-react";
import { ReactNode, useEffect, useState } from "react";
import { WarehousePage } from "./routes/WarehousePage";
import { OperationsPage } from "./routes/OperationsPage";
import { SettingsPage } from "./routes/SettingsPage";
import { AiDraftsPage } from "./routes/AiDraftsPage";
import { NoSupplierPage } from "./routes/NoSupplierPage";

type AppRoute = "warehouse" | "operations" | "settings" | "ai-drafts" | "no-supplier";
type SessionState = { authenticated?: boolean; role?: string | null; username?: string | null };

const navItems: Array<{ route: AppRoute; href: string; label: string; icon: ReactNode }> = [
  { route: "warehouse", href: "/app/warehouse", label: "Каталог", icon: <PackageCheck size={16} /> },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} /> },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} /> },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} /> },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} /> },
];

function currentRoute(): AppRoute {
  const path = window.location.pathname;
  if (path.startsWith("/app/operations")) return "operations";
  if (path.startsWith("/app/settings")) return "settings";
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
  const canUseAdminRoutes = session === null ? false : isAdmin;
  const visibleNavItems = isAdmin ? navItems : navItems.filter((item) => item.route === "warehouse");
  const accessDenied = route !== "warehouse" && !canUseAdminRoutes;
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
          <span>Для роли manager доступен только каталог и привязки PriceMaster.</span>
          <a href="/app/warehouse" onClick={(event) => navigate(event, "/app/warehouse")}>Вернуться в каталог</a>
        </section>
      ) : null}
      {!accessDenied && route === "operations" ? <OperationsPage /> : null}
      {!accessDenied && route === "settings" ? <SettingsPage /> : null}
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
