import { useQueryClient } from "@tanstack/react-query";
import { AlertCircle, ChevronDown, Keyboard, LogOut, Menu, PanelLeft, RefreshCw, Search, Star, X } from "lucide-react";
import { Activity, Component, ErrorInfo, ReactNode, Suspense, useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react";
import { CommandPalette, PaletteAction } from "./components/CommandPalette";
import { NotificationsBell } from "./components/NotificationsBell";
import { PingIndicator } from "./components/PingIndicator";
import { SystemHealthIndicator } from "./components/SystemHealthIndicator";
import { ThemeSwitcher } from "./components/ThemeSwitcher";
import { AppRoute, NAV_SECTIONS, navItems, NavItem, pageComponent, prefetchRoute, prefetchRoutesWhenIdle, routeFromPath } from "./lib/nav";
import { Toaster } from "./lib/toast";

type SessionState = { authenticated?: boolean; role?: string | null; username?: string | null; allowedPages?: string[] | null; displayName?: string | null; avatarUrl?: string | null };

// Сколько разделов держать «живыми» в фоне: возврат на них мгновенный, с сохранёнными
// фильтрами, выделением и прокруткой. Скрытые разделы не опрашивают сервер (эффекты
// в <Activity mode="hidden"> остановлены), свежие данные подтягиваются при возврате.
const KEEP_ALIVE_LIMIT = 6;
const isMac = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform);

// Embedded page preview (iframe inside the page-access modal): hide the app chrome.
const isPreviewEmbed = new URLSearchParams(window.location.search).get("embed") === "preview";

function readStoredList(key: string): AppRoute[] {
  try {
    const value = JSON.parse(window.localStorage.getItem(key) || "[]");
    return Array.isArray(value) ? value.filter((item) => navItems.some((nav) => nav.route === item)) : [];
  } catch {
    return [];
  }
}
function writeStoredList(key: string, value: AppRoute[]) {
  try { window.localStorage.setItem(key, JSON.stringify(value)); } catch { /* приватный режим */ }
}

function isTypingTarget(target: EventTarget | null) {
  const element = target as HTMLElement | null;
  if (!element) return false;
  const tag = element.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || element.isContentEditable;
}

/** Первое видимое поле поиска на текущей странице (для горячей клавиши «/»). */
function findPageSearchInput(): HTMLInputElement | null {
  const content = document.querySelector(".app-content");
  if (!content) return null;
  const candidates = Array.from(content.querySelectorAll<HTMLInputElement>(
    '.search-box input, input[type="search"], input[placeholder*="Поиск"], input[placeholder*="поиск"], input[placeholder*="SKU"]',
  ));
  return candidates.find((input) => input.offsetParent !== null && !input.disabled) || null;
}

// Ошибка в одном разделе больше не роняет всё приложение: показываем её на месте раздела
// с кнопкой перезагрузки, остальные разделы и меню продолжают работать.
class RouteErrorBoundary extends Component<{ children: ReactNode; label: string }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  componentDidCatch(error: Error, info: ErrorInfo) { console.error("[route crash]", this.props.label, error, info.componentStack); }
  render() {
    if (!this.state.error) return this.props.children;
    return (
      <section className="route-error" role="alert">
        <strong>Раздел «{this.props.label}» не открылся</strong>
        <span className="muted">Остальные разделы работают. Попробуйте открыть его заново; если ошибка повторяется — пришлите текст ниже.</span>
        <code>{String(this.state.error?.message || this.state.error)}</code>
        <div style={{ display: "flex", gap: 8 }}>
          <button type="button" className="primary-action" onClick={() => this.setState({ error: null })}>Открыть заново</button>
          <button type="button" className="secondary-action" onClick={() => window.location.reload()}>Перезагрузить страницу</button>
        </div>
      </section>
    );
  }
}

function RoutePage({ route, isAdmin }: { route: AppRoute; isAdmin: boolean }) {
  const Page = pageComponent(route);
  if (route === "reviews") return <Page defaultTab="reviews" />;
  if (route === "questions") return <Page defaultTab="questions" />;
  if (route === "warehouse") return <Page isAdmin={isAdmin} />;
  return <Page />;
}

function ShortcutsHelp({ onClose }: { onClose: () => void }) {
  const mod = isMac ? "⌘" : "Ctrl";
  const rows: Array<[string[], string]> = [
    [[mod, "K"], "Быстрый переход: разделы, действия, поиск товара"],
    [["/"], "Фокус на поиск текущей страницы"],
    [["Alt", "1…9"], "Открыть раздел из избранного (или меню по порядку)"],
    [["Alt", "←"], "Назад к предыдущему разделу"],
    [["↑", "↓"], "Склад: переход по товарам, Enter — открыть карточку"],
    [["Esc"], "Закрыть окно, карточку или меню"],
    [["?"], "Эта подсказка"],
  ];
  return (
    <div className="cmdk-overlay" onMouseDown={onClose}>
      <div className="shortcuts-help" role="dialog" aria-modal="true" aria-label="Горячие клавиши" onMouseDown={(event) => event.stopPropagation()}>
        <div className="shortcuts-help-head">
          <Keyboard size={18} />
          <strong>Горячие клавиши</strong>
          <button type="button" className="icon-action" onClick={onClose} aria-label="Закрыть"><X size={15} /></button>
        </div>
        {rows.map(([keys, text]) => (
          <div className="shortcuts-help-row" key={text}>
            <span className="shortcuts-help-keys">{keys.map((key) => <kbd key={key}>{key}</kbd>)}</span>
            <span>{text}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function AppShell() {
  const queryClient = useQueryClient();
  const [route, setRoute] = useState<AppRoute>(() => routeFromPath());
  // Разделы, которые уже открывались (последний — самый свежий) + «версия» для перемонтирования.
  const [alive, setAlive] = useState<AppRoute[]>(() => [routeFromPath()]);
  const [remountKeys, setRemountKeys] = useState<Partial<Record<AppRoute, number>>>({});
  const [isPending, startTransition] = useTransition();
  const [session, setSession] = useState<SessionState | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarCompact, setSidebarCompact] = useState(() => {
    try { return window.localStorage.getItem("nav-compact") === "1"; } catch { return false; }
  });
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const [favorites, setFavorites] = useState<AppRoute[]>(() => readStoredList("nav-favorites"));
  const [recent, setRecent] = useState<AppRoute[]>(() => readStoredList("nav-recent"));
  const [openPicking, setOpenPicking] = useState(0);
  const [collapsedSections, setCollapsedSections] = useState<Record<string, boolean>>(() => {
    try {
      const stored = JSON.parse(window.localStorage.getItem("nav-collapsed-sections") || "");
      if (stored && typeof stored === "object") return stored;
    } catch { /* дефолт ниже */ }
    return { tools: true, admin: true };
  });
  const scrollPositions = useRef<Partial<Record<AppRoute, number>>>({});
  const previousRoute = useRef<AppRoute | null>(null);
  const toggleNavSection = (id: string) => {
    setCollapsedSections((current) => {
      const next = { ...current, [id]: !current[id] };
      try { window.localStorage.setItem("nav-collapsed-sections", JSON.stringify(next)); } catch { /* приватный режим */ }
      return next;
    });
  };
  const [isMobileNav, setIsMobileNav] = useState(() => window.matchMedia("(max-width: 980px)").matches);
  const activeNavRef = useRef<HTMLAnchorElement | null>(null);

  const showRoute = useCallback((next: AppRoute, forceRemount = false) => {
    setRoute((current) => {
      if (current !== next) {
        scrollPositions.current[current] = window.scrollY;
        previousRoute.current = current;
      }
      return next;
    });
    setAlive((current) => [...current.filter((item) => item !== next), next].slice(-KEEP_ALIVE_LIMIT));
    if (forceRemount) setRemountKeys((current) => ({ ...current, [next]: (current[next] || 0) + 1 }));
    setRecent((current) => {
      const updated = [next, ...current.filter((item) => item !== next)].slice(0, 6);
      writeStoredList("nav-recent", updated);
      return updated;
    });
  }, []);

  const navigateTo = useCallback((href: string) => {
    const url = new URL(href, window.location.origin);
    const next = routeFromPath(url.pathname);
    // Переход с параметрами (например, поиск товара из палитры) открывает раздел заново,
    // чтобы он прочитал параметры из адреса, а не показал старое состояние.
    const forceRemount = Boolean(url.search);
    window.history.pushState(null, "", href);
    setSidebarOpen(false);
    startTransition(() => showRoute(next, forceRemount));
  }, [showRoute]);

  const navigate = (event: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    if (event.ctrlKey || event.metaKey || event.shiftKey || event.button === 1) return; // новая вкладка
    event.preventDefault();
    navigateTo(href);
  };

  // Восстанавливаем прокрутку раздела после его показа.
  useEffect(() => {
    const top = scrollPositions.current[route] ?? 0;
    window.requestAnimationFrame(() => window.scrollTo({ top, behavior: "instant" as ScrollBehavior }));
  }, [route]);

  useEffect(() => {
    const onPop = () => {
      startTransition(() => showRoute(routeFromPath()));
      setSidebarOpen(false);
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, [showRoute]);
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
      if (document.hidden) return;
      try {
        const res = await fetch("/api/supplier-picking-list?status=open&limit=1", { credentials: "include" });
        if (!res.ok || cancelled) return;
        const data = await res.json().catch(() => null);
        const open = Number((data?.summary as Record<string, number> | undefined)?.open ?? 0);
        if (!cancelled) {
          setOpenPicking(Number.isFinite(open) ? open : 0);
          document.title = open > 0 ? `(${open}) ${BASE_TITLE}` : BASE_TITLE;
        }
      } catch { /* ignore */ }
    }
    void poll();
    const id = window.setInterval(poll, 60_000);
    const onVisible = () => { if (!document.hidden) void poll(); };
    document.addEventListener("visibilitychange", onVisible);
    return () => { cancelled = true; clearInterval(id); document.removeEventListener("visibilitychange", onVisible); document.title = BASE_TITLE; };
  }, []);

  const toggleNavigation = useCallback(() => {
    if (isMobileNav) {
      setSidebarOpen((value) => !value);
      return;
    }
    setSidebarCompact((value) => {
      try { window.localStorage.setItem("nav-compact", value ? "0" : "1"); } catch { /* ignore */ }
      return !value;
    });
  }, [isMobileNav]);

  const toggleFavorite = (target: AppRoute) => {
    setFavorites((current) => {
      const next = current.includes(target) ? current.filter((item) => item !== target) : [...current, target];
      writeStoredList("nav-favorites", next);
      return next;
    });
  };

  const sessionReady = session !== null;
  const isAdmin = session?.role === "admin";
  // Per-user page visibility: admins see everything, others see the pages granted
  // in Настройки -> Сотрудники (empty grant falls back to the old manager defaults).
  const visibleNavItems = useMemo(() => {
    const defaultEmployeeRoutes: AppRoute[] = ["warehouse", "picking-list"];
    const allRouteKeys = navItems.map((item) => item.route);
    const grantedPages = (session?.allowedPages || []).filter((page): page is AppRoute => (allRouteKeys as string[]).includes(page));
    const allowed = new Set<AppRoute>(isAdmin ? allRouteKeys : (grantedPages.length ? grantedPages : defaultEmployeeRoutes));
    return navItems.filter((item) => allowed.has(item.route));
  }, [isAdmin, session?.allowedPages]);
  const allowedRoutes = useMemo(() => new Set(visibleNavItems.map((item) => item.route)), [visibleNavItems]);
  const visibleFavorites = favorites.filter((item) => allowedRoutes.has(item));
  const quickRoutes = (visibleFavorites.length ? visibleFavorites : visibleNavItems.map((item) => item.route)).slice(0, 9);

  useEffect(() => {
    activeNavRef.current?.scrollIntoView({ block: "nearest" });
  }, [route, visibleNavItems.length]);

  // Чанки разрешённых разделов подтягиваются в простое — переходы без ожидания загрузки.
  useEffect(() => {
    if (!sessionReady || isPreviewEmbed) return;
    const ordered = [...visibleFavorites, ...recent, ...visibleNavItems.map((item) => item.route)].filter((item) => allowedRoutes.has(item));
    prefetchRoutesWhenIdle(Array.from(new Set(ordered)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionReady]);

  const accessDenied = sessionReady && !allowedRoutes.has(route);
  useEffect(() => {
    // Users without access to the current page land on their first granted page
    // (the sponsor logs in and goes straight to Реализация).
    if (!sessionReady || !accessDenied) return;
    const fallback = visibleNavItems[0];
    if (fallback && fallback.route !== route) {
      window.history.replaceState(null, "", fallback.href);
      showRoute(fallback.route);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionReady, accessDenied]);

  // Глобальные горячие клавиши.
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      const mod = event.ctrlKey || event.metaKey;
      if (mod && !event.altKey && (event.key.toLowerCase() === "k" || event.key.toLowerCase() === "л" || event.code === "KeyK")) {
        event.preventDefault();
        setHelpOpen(false);
        setPaletteOpen((value) => !value);
        return;
      }
      if (event.key === "Escape") {
        setSidebarOpen(false);
        return;
      }
      if (paletteOpen || helpOpen) return;
      if (event.altKey && !mod && /^Digit[1-9]$/.test(event.code)) {
        const target = quickRoutes[Number(event.code.slice(5)) - 1];
        const item = visibleNavItems.find((nav) => nav.route === target);
        if (item) {
          event.preventDefault();
          navigateTo(item.href);
        }
        return;
      }
      if (event.altKey && event.key === "ArrowLeft" && previousRoute.current) {
        const item = visibleNavItems.find((nav) => nav.route === previousRoute.current);
        if (item) {
          event.preventDefault();
          navigateTo(item.href);
        }
        return;
      }
      if (event.defaultPrevented || isTypingTarget(event.target) || mod || event.altKey) return;
      if (event.key === "/") {
        const input = findPageSearchInput();
        if (input) {
          event.preventDefault();
          input.focus();
          input.select();
        }
      } else if (event.key === "?") {
        event.preventDefault();
        setHelpOpen(true);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [paletteOpen, helpOpen, quickRoutes, visibleNavItems, navigateTo]);

  const logout = () => {
    fetch("/api/logout", { method: "POST" }).finally(() => {
      window.location.href = "/login.html";
    });
  };

  const paletteActions = useMemo<PaletteAction[]>(() => [
    { id: "refresh", label: "Обновить данные", hint: "перезапросить всё с сервера", icon: <RefreshCw size={15} />, keywords: "обновить refresh перезагрузить", run: () => { void queryClient.invalidateQueries(); } },
    { id: "sidebar", label: sidebarCompact ? "Развернуть меню" : "Свернуть меню", icon: <PanelLeft size={15} />, keywords: "меню сайдбар sidebar", run: toggleNavigation },
    ...(visibleNavItems.some((item) => item.route === route) ? [{
      id: "favorite",
      label: favorites.includes(route) ? "Убрать раздел из избранного" : "Добавить раздел в избранное",
      icon: <Star size={15} />,
      keywords: "избранное закрепить pin",
      run: () => toggleFavorite(route),
    }] : []),
    { id: "shortcuts", label: "Горячие клавиши", hint: "?", icon: <Keyboard size={15} />, keywords: "клавиши hotkeys подсказка", run: () => setHelpOpen(true) },
    { id: "logout", label: "Выйти", icon: <LogOut size={15} />, keywords: "выход logout", run: logout },
    // eslint-disable-next-line react-hooks/exhaustive-deps
  ], [queryClient, sidebarCompact, toggleNavigation, favorites, route, visibleNavItems]);

  const roleLabel = !sessionReady ? "Загрузка" : (isAdmin ? "Администратор" : (session?.role === "manager" ? "Менеджер" : "Сотрудник"));
  const navExpanded = isMobileNav ? sidebarOpen : !sidebarCompact;
  const currentItem = navItems.find((item) => item.route === route);
  const currentSection = NAV_SECTIONS.find((section) => section.routes.includes(route));

  const renderNavLink = (item: NavItem, keyPrefix = "") => {
    const isFavorite = favorites.includes(item.route);
    const quickIndex = quickRoutes.indexOf(item.route);
    const badge = item.route === "picking-list" && openPicking > 0 ? openPicking : 0;
    return (
      <a
        key={`${keyPrefix}${item.route}`}
        ref={route === item.route && !keyPrefix ? activeNavRef : undefined}
        title={quickIndex >= 0 ? `${item.label} · Alt+${quickIndex + 1}` : item.label}
        className={route === item.route ? "is-active" : ""}
        href={item.href}
        onClick={(event) => navigate(event, item.href)}
        onMouseEnter={() => prefetchRoute(item.route)}
        onFocus={() => prefetchRoute(item.route)}
      >
        {item.icon}
        <span className="nav-label">{item.label}</span>
        {badge ? <span className="nav-badge">{badge > 99 ? "99+" : badge}</span> : null}
        <span
          role="button"
          tabIndex={-1}
          className={`nav-fav${isFavorite ? " is-on" : ""}`}
          title={isFavorite ? "Убрать из избранного" : "В избранное"}
          onClick={(event) => { event.preventDefault(); event.stopPropagation(); toggleFavorite(item.route); }}
        >
          <Star size={12} />
        </span>
      </a>
    );
  };

  const favoriteItems = visibleFavorites.map((favorite) => visibleNavItems.find((item) => item.route === favorite)).filter((item): item is NavItem => Boolean(item));

  return (
    <main className={`app-shell with-sidebar${sidebarOpen ? " sidebar-open" : ""}${sidebarCompact ? " sidebar-compact" : ""}${isPreviewEmbed ? " preview-embed" : ""}`}>
      <aside className="side-nav" aria-label="Основная навигация">
        <div className="brand-lockup">
          <span className="brand-mark" aria-hidden="true">D</span>
          <div>
            <h1>David<span>Sklad</span></h1>
          </div>
        </div>
        <nav className="side-nav-links">
          {favoriteItems.length ? (
            <div className="nav-section nav-favorites">
              <div className="nav-section-title nav-section-static"><span>Избранное</span></div>
              {favoriteItems.map((item) => renderNavLink(item, "fav-"))}
            </div>
          ) : null}
          {NAV_SECTIONS.map((section) => {
            const items = section.routes
              .map((sectionRoute) => visibleNavItems.find((item) => item.route === sectionRoute))
              .filter((item): item is NavItem => Boolean(item));
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
                <div className="nav-section-items">
                  {!collapsed ? items.map((item) => renderNavLink(item)) : null}
                </div>
              </div>
            );
          })}
        </nav>
        <div className="side-user-card">
          {session?.avatarUrl ? (
            <img className="side-user-avatar" src={session.avatarUrl} alt="" width={32} height={32} />
          ) : (
            <span className="side-user-avatar-placeholder">
              {(session?.displayName || session?.username || "?").charAt(0).toUpperCase()}
            </span>
          )}
          <div className="side-user-info">
            <span>{roleLabel}</span>
            <strong>{session?.displayName || session?.username || "—"}</strong>
          </div>
          <button className="side-logout-icon" type="button" onClick={logout} title="Выйти" aria-label="Выйти"><LogOut size={16} /></button>
        </div>
      </aside>
      <button className="sidebar-backdrop" type="button" aria-label="Закрыть меню" onClick={() => setSidebarOpen(false)} />
      <div className="app-content">
        <header className="topbar content-topbar">
          <button className="topbar-menu" type="button" aria-label={navExpanded ? "Свернуть меню" : "Открыть меню"} aria-expanded={navExpanded} onClick={toggleNavigation}><Menu size={20} /></button>
          <div className="topbar-crumbs" aria-hidden={!currentItem}>
            {currentSection?.title ? <span className="topbar-crumb-section">{currentSection.title}</span> : null}
            {currentItem ? <strong>{currentItem.label}</strong> : null}
          </div>
          <button className="topbar-search" type="button" onClick={() => setPaletteOpen(true)} title="Быстрый переход">
            <Search size={16} />
            <span>Найти раздел, товар или действие…</span>
            <kbd>{isMac ? "⌘" : "Ctrl"} K</kbd>
          </button>
          <div className="topbar-spacer" />
          <div className="topbar-actions">
            <PingIndicator />
            <ThemeSwitcher />
            {isAdmin ? <SystemHealthIndicator /> : null}
            <NotificationsBell />
          </div>
          <div className={`route-progress${isPending ? " is-active" : ""}`} aria-hidden="true" />
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
        <Suspense fallback={<div className="page-lazy-loading"><span className="page-lazy-bar" /></div>}>
          {sessionReady ? alive.filter((item) => allowedRoutes.has(item)).map((item) => (
            <Activity key={`${item}-${remountKeys[item] || 0}`} mode={item === route && !accessDenied ? "visible" : "hidden"}>
              <div className="route-view" data-route={item}>
                <RouteErrorBoundary label={navItems.find((nav) => nav.route === item)?.label || item}>
                  <RoutePage route={item} isAdmin={isAdmin} />
                </RouteErrorBoundary>
              </div>
            </Activity>
          )) : null}
        </Suspense>
      </div>
      <CommandPalette
        open={paletteOpen}
        onClose={() => setPaletteOpen(false)}
        items={visibleNavItems}
        recent={recent.filter((item) => allowedRoutes.has(item) && item !== route)}
        favorites={visibleFavorites}
        actions={paletteActions}
        onNavigate={navigateTo}
      />
      {helpOpen ? <ShortcutsHelp onClose={() => setHelpOpen(false)} /> : null}
      <Toaster />
    </main>
  );
}

export function App() {
  return <AppShell />;
}

export default App;
