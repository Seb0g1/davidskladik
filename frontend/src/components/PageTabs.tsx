import { createContext, lazy, Suspense, useContext, useEffect, useMemo, useState } from "react";
import type { ComponentType, LazyExoticComponent, ReactNode } from "react";

// Один раздел из нескольких бывших страниц: шапка раздела (PageHeader) показывает общий заголовок и вкладки,
// содержимое вкладки — прежняя страница целиком. Вкладка хранится в адресе (?tab=…).

type TabsState = { title: string; tabs: Array<{ id: string; label: string; badge?: ReactNode }>; active: string; setActive: (id: string) => void };

const PageTabsContext = createContext<TabsState | null>(null);

export function usePageTabs() {
  return useContext(PageTabsContext);
}

export function PageTabsStrip() {
  const state = useContext(PageTabsContext);
  if (!state || state.tabs.length < 2) return null;
  return (
    <nav className="section-tabs" role="tablist" aria-label={state.title}>
      {state.tabs.map((tab) => (
        <button key={tab.id} type="button" role="tab" aria-selected={state.active === tab.id}
          className={state.active === tab.id ? "is-active" : ""} onClick={() => state.setActive(tab.id)}>
          {tab.label}{tab.badge ? <span className="section-tabs-badge">{tab.badge}</span> : null}
        </button>
      ))}
    </nav>
  );
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type PageModule = Record<string, any>;
export type TabDef = { id: string; label: string; load: () => Promise<PageModule>; exportName: string; props?: Record<string, unknown> };

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const cache = new Map<string, LazyExoticComponent<ComponentType<any>>>();
function tabComponent(tab: TabDef) {
  const key = `${tab.exportName}`;
  let component = cache.get(key);
  if (!component) {
    component = lazy(() => tab.load().then((module) => ({ default: module[tab.exportName] })));
    cache.set(key, component);
  }
  return component;
}

function readTab(ids: string[]) {
  try {
    const value = new URLSearchParams(window.location.search).get("tab");
    if (value && ids.includes(value)) return value;
  } catch {
    // ignore
  }
  return ids[0];
}

export function TabbedPage({ title, tabs }: { title: string; tabs: TabDef[] }) {
  const ids = tabs.map((tab) => tab.id);
  const [active, setActiveState] = useState(() => readTab(ids));
  // открытые вкладки остаются смонтированными: возврат на вкладку мгновенный, фильтры и прокрутка сохраняются
  const [visited, setVisited] = useState<string[]>(() => [readTab(ids)]);

  useEffect(() => {
    const onPop = () => {
      const next = readTab(ids);
      setActiveState(next);
      setVisited((list) => (list.includes(next) ? list : [...list, next]));
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ids.join(",")]);

  const state = useMemo<TabsState>(() => ({
    title,
    tabs: tabs.map(({ id, label }) => ({ id, label })),
    active,
    setActive: (id: string) => {
      setActiveState(id);
      setVisited((list) => (list.includes(id) ? list : [...list, id]));
      try {
        const url = new URL(window.location.href);
        if (id === ids[0]) url.searchParams.delete("tab");
        else url.searchParams.set("tab", id);
        window.history.replaceState(window.history.state, "", url.toString());
      } catch {
        // ignore
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [title, active, ids.join(",")]);

  return (
    <PageTabsContext.Provider value={state}>
      {tabs.filter((tab) => visited.includes(tab.id)).map((tab) => {
        const Page = tabComponent(tab);
        return (
          <div key={tab.id} hidden={tab.id !== active} className="page-tab-panel">
            <Suspense fallback={<div className="page-lazy-loading"><span className="page-lazy-bar" /></div>}>
              <Page {...(tab.props || {})} />
            </Suspense>
          </div>
        );
      })}
    </PageTabsContext.Provider>
  );
}
