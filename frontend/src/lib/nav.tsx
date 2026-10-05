import { ShieldAlert, Activity, FileCheck2, Flower2, AlertCircle, AlertTriangle, BadgeDollarSign, Ban, BarChart3, BookOpen, CirclePlay, ClipboardList, DollarSign, Download, HandCoins, HelpCircle, Home, Link2, MessageCircle, MessageCircleHeart, PackageCheck, PackagePlus, RefreshCcw, Settings, ShoppingCart, Sparkles, Star, Store, Tag, Truck, Upload, Wrench } from "lucide-react";
import { ComponentType, lazy, LazyExoticComponent, ReactNode } from "react";
import { TabbedPage } from "../components/PageTabs";

export type AppRoute = "dashboard" | "import" | "avito" | "shop" | "chats" | "questions" | "reviews" | "warehouse" | "picking-list" | "suppliers" | "operations" | "supplier-cart" | "prices" | "problem-products" | "finance" | "consignment" | "settings" | "system" | "tnved" | "support" | "brand-bans" | "fragrantica" | "ozon-docs" | "card-health" | "card-improve" | "supplier-match";

export type NavItem = { route: AppRoute; href: string; label: string; icon: ReactNode; keywords?: string };

export const navItems: NavItem[] = [
  { route: "dashboard", href: "/app/dashboard", label: "Дашборд", icon: <Home size={16} />, keywords: "статистика сотрудники главная сводка dashboard" },
  { route: "warehouse", href: "/app/warehouse", label: "Склад", icon: <PackageCheck size={16} />, keywords: "каталог товары привязки sklad" },
  { route: "picking-list", href: "/app/picking-list", label: "Сборка", icon: <ClipboardList size={16} />, keywords: "лист закупки собрать заказ sborka" },
  { route: "avito", href: "/app/avito", label: "Автозагрузка Avito", icon: <Upload size={16} />, keywords: "авито фид avito" },
  { route: "consignment", href: "/app/consignment", label: "Реализация", icon: <HandCoins size={16} />, keywords: "спонсор накладные realizaciya" },
  { route: "reviews", href: "/app/reviews", label: "Отзывы", icon: <Star size={16} />, keywords: "отзывы оценки otzyvy" },
  { route: "chats", href: "/app/chats", label: "Чаты", icon: <MessageCircle size={16} />, keywords: "сообщения покупатели chaty" },
  { route: "questions", href: "/app/questions", label: "Вопросы", icon: <HelpCircle size={16} />, keywords: "вопросы покупателей voprosy" },
  { route: "support", href: "/app/support", label: "Поддержка сайта", icon: <MessageCircleHeart size={16} />, keywords: "сайт чат поддержка" },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} />, keywords: "сотрудники доступы ключи nastroyki" },
  { route: "system", href: "/app/system", label: "Система", icon: <Activity size={16} />, keywords: "восстановление из архива разархив здоровье очереди логи" },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} />, keywords: "синхронизация запуск" },
  { route: "suppliers", href: "/app/suppliers", label: "Поставщики", icon: <Truck size={16} />, keywords: "балансы оплаты postavshiki" },
  { route: "supplier-match", href: "/app/supplier-match", label: "Подбор поставщиков", icon: <Link2 size={16} />, keywords: "ошибки наличия без поставщика подбор поставщиков привязки pricemaster нет поставщика запустить в продажу новые привязки" },
  { route: "shop", href: "/app/shop", label: "Магазин MV", icon: <Store size={16} />, keywords: "magicvibes сайт магазин" },
  { route: "import", href: "/app/import", label: "Импорт на Яндекс", icon: <Download size={16} />, keywords: "яндекс маркет перенос" },
  { route: "supplier-cart", href: "/app/supplier-cart", label: "Автокорзина", icon: <ShoppingCart size={16} />, keywords: "корзина pricemaster заказы avtokorzina" },
  { route: "prices", href: "/app/prices", label: "Цены", icon: <DollarSign size={16} />, keywords: "проверка цен подозрительные карантин наценка курс ceny" },
  { route: "problem-products", href: "/app/problem-products", label: "Проблемные товары", icon: <AlertTriangle size={16} />, keywords: "ошибки товары" },
  { route: "finance", href: "/app/finance", label: "Финансы", icon: <BadgeDollarSign size={16} />, keywords: "расходы закупки деньги" },
  { route: "tnved", href: "/app/tnved", label: "ТН ВЭД", icon: <Tag size={16} />, keywords: "бренды декларации тнвэд коды" },
  { route: "brand-bans", href: "/app/brand-bans", label: "Запрет брендов", icon: <Ban size={16} />, keywords: "бренды запрет бан" },
  { route: "card-health", href: "/app/card-health", label: "Ошибки карточек", icon: <Wrench size={16} />, keywords: "ozon карточки озон модерация на доработку ошибки карточек маркет починить исправить группа вариантов габариты тн вэд карантин" },
  { route: "card-improve", href: "/app/card-improve", label: "Улучшение карточек", icon: <Sparkles size={16} />, keywords: "улучшение карточек старые карточки фото описание рейтинг одобрить было стало" },
  { route: "ozon-docs", href: "/app/ozon-docs", label: "Документы Ozon", icon: <FileCheck2 size={16} />, keywords: "декларации сертификаты документы проверка отклонено одобрено озон" },
  { route: "fragrantica", href: "/app/fragrantica", label: "Фрагрантика", icon: <Flower2 size={16} />, keywords: "fragrantica ароматы каталог добавить на озон новые карточки" },
];

// Сайдбар по ходу работы CRM: сверху — то, чем пользуются каждый день (без заголовка), дальше группы по
// задачам: покупатели → карточки → деньги → каналы продаж → справочники → система.
export const NAV_SECTIONS: Array<{ id: string; title?: string; routes: AppRoute[] }> = [
  { id: "daily", routes: ["dashboard", "warehouse", "picking-list", "supplier-cart", "suppliers"] },
  { id: "customers", title: "Покупатели", routes: ["chats", "questions", "reviews", "support"] },
  { id: "cards", title: "Карточки товаров", routes: ["card-improve", "card-health", "fragrantica", "supplier-match", "ozon-docs", "problem-products"] },
  { id: "money", title: "Цены и деньги", routes: ["prices", "finance", "consignment"] },
  { id: "channels", title: "Каналы продаж", routes: ["shop", "avito", "import"] },
  { id: "reference", title: "Справочники", routes: ["tnved", "brand-bans"] },
  { id: "system", title: "Система", routes: ["settings", "operations", "system"] },
];

// Порядок важен: более длинные префиксы раньше коротких.
const ROUTE_PREFIXES: Array<[string, AppRoute]> = navItems
  .map((item) => [item.href, item.route] as [string, AppRoute])
  .sort((a, b) => b[0].length - a[0].length);

// Бывшие отдельные страницы → раздел и вкладка (старые ссылки и закладки продолжают работать).
const ROUTE_ALIASES: Record<string, [AppRoute, string]> = {
  "/app/statistics": ["dashboard", "stats"],
  "/app/recovery-queue": ["system", "recovery"],
  "/app/no-supplier": ["supplier-match", "no-supplier"],
  "/app/ozon-card-fix": ["card-health", "ozon"],
  "/app/brands-tnved": ["tnved", "brands"],
  "/app/price-guard": ["prices", "guard"],
  "/app/ai-drafts": ["card-improve", ""],
  "/app/new-products": ["fragrantica", ""],
};

export function routeFromPath(path = window.location.pathname): AppRoute {
  const alias = ROUTE_ALIASES[path.replace(/\/+$/, "")];
  if (alias) {
    const [route, tab] = alias;
    const href = navItems.find((item) => item.route === route)?.href || "/app/dashboard";
    try {
      if (window.location.pathname === path) window.history.replaceState(window.history.state, "", tab ? `${href}?tab=${tab}` : href);
    } catch {
      // ignore
    }
    return route;
  }
  for (const [prefix, route] of ROUTE_PREFIXES) {
    if (path === prefix || path.startsWith(`${prefix}/`) || path.startsWith(`${prefix}?`)) return route;
  }
  return "warehouse";
}

type PageModule = Record<string, unknown>;
type Loader = { load: () => Promise<PageModule>; exportName: string };

// Страницы грузятся лениво (свой чанк на раздел). Загрузчик вынесен отдельно, чтобы
// чанк можно было подтянуть заранее — при наведении на пункт меню или в простое.
const LOADERS: Record<AppRoute, Loader> = {
  dashboard: { load: async () => ({ Page: () => <TabbedPage title="Дашборд" tabs={[{ id: "overview", label: "Обзор", load: () => import("../routes/DashboardPage"), exportName: "DashboardPage" }, { id: "stats", label: "Статистика", load: () => import("../routes/StatisticsPage"), exportName: "StatisticsPage" }]} /> }), exportName: "Page" },
  warehouse: { load: () => import("../routes/WarehousePage"), exportName: "WarehousePage" },
  "picking-list": { load: () => import("../routes/PickingListPage"), exportName: "PickingListPage" },
  avito: { load: () => import("../routes/AvitoPage"), exportName: "AvitoPage" },
  consignment: { load: () => import("../routes/ConsignmentPage"), exportName: "ConsignmentPage" },
  reviews: { load: () => import("../routes/FeedbackPage"), exportName: "FeedbackPage" },
  questions: { load: () => import("../routes/FeedbackPage"), exportName: "FeedbackPage" },
  chats: { load: () => import("../routes/ChatsPage"), exportName: "ChatsPage" },
  support: { load: () => import("../routes/SupportChatsPage"), exportName: "SupportChatsPage" },
  settings: { load: () => import("../routes/SettingsPage"), exportName: "SettingsPage" },
  system: { load: async () => ({ Page: () => <TabbedPage title="Система" tabs={[{ id: "health", label: "Состояние", load: () => import("../routes/SystemPage"), exportName: "SystemPage" }, { id: "recovery", label: "Восстановление из архива", load: () => import("../routes/RecoveryQueuePage"), exportName: "RecoveryQueuePage" }]} /> }), exportName: "Page" },
  operations: { load: () => import("../routes/OperationsPage"), exportName: "OperationsPage" },
  suppliers: { load: () => import("../routes/SuppliersPage"), exportName: "SuppliersPage" },
  shop: { load: () => import("../routes/ShopAdminPage"), exportName: "default" },
  import: { load: () => import("../routes/ImportPage"), exportName: "ImportPage" },
  "supplier-cart": { load: () => import("../routes/SupplierCartPage"), exportName: "SupplierCartPage" },
  prices: { load: async () => ({ Page: () => <TabbedPage title="Цены" tabs={[{ id: "send", label: "Отправка цен", load: () => import("../routes/PricesPage"), exportName: "PricesPage" }, { id: "guard", label: "Подозрительные цены", load: () => import("../routes/PriceGuardPage"), exportName: "PriceGuardPage" }]} /> }), exportName: "Page" },
  "problem-products": { load: () => import("../routes/ProblemProductsPage"), exportName: "ProblemProductsPage" },
  finance: { load: () => import("../routes/FinancePage"), exportName: "FinancePage" },
  tnved: { load: async () => ({ Page: () => <TabbedPage title="ТН ВЭД" tabs={[{ id: "categories", label: "Категории", load: () => import("../routes/TnvedPage"), exportName: "TnvedPage" }, { id: "brands", label: "Бренды", load: () => import("../routes/BrandsTnvedPage"), exportName: "BrandsTnvedPage" }]} /> }), exportName: "Page" },
  "brand-bans": { load: () => import("../routes/BrandBansPage"), exportName: "BrandBansPage" },
  fragrantica: { load: () => import("../routes/FragranticaPage"), exportName: "FragranticaPage" },
  "ozon-docs": { load: () => import("../routes/OzonDocsPage"), exportName: "OzonDocsPage" },
  "card-health": { load: async () => ({ Page: () => <TabbedPage title="Ошибки карточек" tabs={[{ id: "market", label: "Яндекс Маркет", load: () => import("../routes/CardHealthPage"), exportName: "CardHealthPage" }, { id: "ozon", label: "Ozon", load: () => import("../routes/OzonCardFixPage"), exportName: "OzonCardFixPage" }]} /> }), exportName: "Page" },
  "card-improve": { load: () => import("../routes/CardImprovePage"), exportName: "CardImprovePage" },
  "supplier-match": { load: async () => ({ Page: () => <TabbedPage title="Подбор поставщиков" tabs={[{ id: "match", label: "Подбор", load: () => import("../routes/SupplierMatchPage"), exportName: "SupplierMatchPage" }, { id: "no-supplier", label: "Ошибки наличия", load: () => import("../routes/NoSupplierPage"), exportName: "NoSupplierPage" }]} /> }), exportName: "Page" },
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const lazyCache = new Map<AppRoute, LazyExoticComponent<ComponentType<any>>>();

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function pageComponent(route: AppRoute): LazyExoticComponent<ComponentType<any>> {
  let component = lazyCache.get(route);
  if (!component) {
    const loader = LOADERS[route];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    component = lazy(() => loader.load()
      .then((module) => ({ default: module[loader.exportName] as ComponentType<any> }))
      .catch((error) => {
        // После деплоя старые чанки пропадают: открытая вкладка перезагружается один раз
        // и получает свежую сборку вместо «белого экрана».
        const key = "chunk-reload-at";
        let last = 0;
        try { last = Number(window.sessionStorage.getItem(key) || 0); } catch { /* ignore */ }
        if (Date.now() - last > 30_000) {
          try { window.sessionStorage.setItem(key, String(Date.now())); } catch { /* ignore */ }
          window.location.reload();
          return new Promise<never>(() => { /* ждём перезагрузку */ });
        }
        throw error;
      }));
    lazyCache.set(route, component);
  }
  return component;
}

const prefetched = new Set<AppRoute>();
export function prefetchRoute(route: AppRoute) {
  if (prefetched.has(route)) return;
  prefetched.add(route);
  LOADERS[route].load().catch(() => prefetched.delete(route));
}

/** Тихо подгружает чанки разрешённых разделов по одному, когда браузер простаивает. */
export function prefetchRoutesWhenIdle(routes: AppRoute[]) {
  const queue = routes.filter((route) => !prefetched.has(route));
  const idle = (callback: () => void) => {
    const ric = (window as unknown as { requestIdleCallback?: (cb: () => void, opts?: { timeout: number }) => number }).requestIdleCallback;
    if (ric) ric(callback, { timeout: 3000 });
    else window.setTimeout(callback, 400);
  };
  const next = () => {
    const route = queue.shift();
    if (!route) return;
    prefetchRoute(route);
    idle(next);
  };
  window.setTimeout(() => idle(next), 1500);
}
