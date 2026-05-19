import { AlertCircle, CirclePlay, PackageCheck, Settings, Sparkles } from "lucide-react";
import { ReactNode, useEffect, useState } from "react";
import { WarehousePage } from "./routes/WarehousePage";
import { OperationsPage } from "./routes/OperationsPage";
import { SettingsPage } from "./routes/SettingsPage";
import { AiDraftsPage } from "./routes/AiDraftsPage";
import { NoSupplierPage } from "./routes/NoSupplierPage";

type AppRoute = "warehouse" | "operations" | "settings" | "ai-drafts" | "no-supplier";

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
  useEffect(() => {
    const onPop = () => setRoute(currentRoute());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const navigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    event.preventDefault();
    window.history.pushState(null, "", href);
    setRoute(currentRoute());
  };
  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <span className="eyebrow">Рабочий интерфейс</span>
          <h1>ДавидСклад</h1>
        </div>
        <nav>
          {navItems.map((item) => (
            <a key={item.route} className={route === item.route ? "is-active" : ""} href={item.href} onClick={(event) => navigate(event, item.href)}>
              {item.icon}{item.label}
            </a>
          ))}
        </nav>
      </header>
      {route === "operations" ? <OperationsPage /> : null}
      {route === "settings" ? <SettingsPage /> : null}
      {route === "ai-drafts" ? <AiDraftsPage /> : null}
      {route === "no-supplier" ? <NoSupplierPage /> : null}
      {route === "warehouse" ? <WarehousePage /> : null}
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
