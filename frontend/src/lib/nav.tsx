import { ShieldAlert, Activity, FileCheck2, Flower2, AlertCircle, AlertTriangle, BadgeDollarSign, Ban, BarChart3, BookOpen, CirclePlay, ClipboardList, DollarSign, Download, HandCoins, HelpCircle, Home, MessageCircle, MessageCircleHeart, PackageCheck, PackagePlus, RefreshCcw, Settings, ShoppingCart, Sparkles, Star, Store, Tag, Truck, Upload, Wrench } from "lucide-react";
import { ComponentType, lazy, LazyExoticComponent, ReactNode } from "react";

export type AppRoute = "dashboard" | "import" | "avito" | "shop" | "chats" | "questions" | "reviews" | "warehouse" | "picking-list" | "suppliers" | "operations" | "supplier-cart" | "recovery-queue" | "prices" | "problem-products" | "finance" | "consignment" | "statistics" | "settings" | "system" | "ai-drafts" | "no-supplier" | "new-products" | "tnved" | "support" | "brand-bans" | "brands-tnved" | "ozon-card-fix" | "fragrantica" | "ozon-docs" | "price-guard" | "card-health";

export type NavItem = { route: AppRoute; href: string; label: string; icon: ReactNode; keywords?: string };

export const navItems: NavItem[] = [
  { route: "dashboard", href: "/app/dashboard", label: "Дашборд", icon: <Home size={16} />, keywords: "главная сводка dashboard" },
  { route: "warehouse", href: "/app/warehouse", label: "Склад", icon: <PackageCheck size={16} />, keywords: "каталог товары привязки sklad" },
  { route: "picking-list", href: "/app/picking-list", label: "Сборка", icon: <ClipboardList size={16} />, keywords: "лист закупки собрать заказ sborka" },
  { route: "avito", href: "/app/avito", label: "Автозагрузка Avito", icon: <Upload size={16} />, keywords: "авито фид avito" },
  { route: "consignment", href: "/app/consignment", label: "Реализация", icon: <HandCoins size={16} />, keywords: "спонсор накладные realizaciya" },
  { route: "reviews", href: "/app/reviews", label: "Отзывы", icon: <Star size={16} />, keywords: "отзывы оценки otzyvy" },
  { route: "chats", href: "/app/chats", label: "Чаты", icon: <MessageCircle size={16} />, keywords: "сообщения покупатели chaty" },
  { route: "questions", href: "/app/questions", label: "Вопросы", icon: <HelpCircle size={16} />, keywords: "вопросы покупателей voprosy" },
  { route: "support", href: "/app/support", label: "Поддержка сайта", icon: <MessageCircleHeart size={16} />, keywords: "сайт чат поддержка" },
  { route: "settings", href: "/app/settings", label: "Настройки", icon: <Settings size={16} />, keywords: "сотрудники доступы ключи nastroyki" },
  { route: "system", href: "/app/system", label: "Система", icon: <Activity size={16} />, keywords: "здоровье очереди логи" },
  { route: "ai-drafts", href: "/app/ai-drafts", label: "AI drafts", icon: <Sparkles size={16} />, keywords: "ии черновики ai" },
  { route: "no-supplier", href: "/app/no-supplier", label: "Ошибки наличия", icon: <AlertCircle size={16} />, keywords: "нет поставщика" },
  { route: "operations", href: "/app/operations", label: "Операции", icon: <CirclePlay size={16} />, keywords: "синхронизация запуск" },
  { route: "recovery-queue", href: "/app/recovery-queue", label: "Восстановление", icon: <RefreshCcw size={16} />, keywords: "очередь восстановления" },
  { route: "suppliers", href: "/app/suppliers", label: "Поставщики", icon: <Truck size={16} />, keywords: "балансы оплаты postavshiki" },
  { route: "shop", href: "/app/shop", label: "Магазин MV", icon: <Store size={16} />, keywords: "magicvibes сайт магазин" },
  { route: "import", href: "/app/import", label: "Импорт на Яндекс", icon: <Download size={16} />, keywords: "яндекс маркет перенос" },
  { route: "supplier-cart", href: "/app/supplier-cart", label: "Автокорзина", icon: <ShoppingCart size={16} />, keywords: "корзина pricemaster заказы avtokorzina" },
  { route: "prices", href: "/app/prices", label: "Цены", icon: <DollarSign size={16} />, keywords: "наценка курс ceny" },
  { route: "statistics", href: "/app/statistics", label: "Статистика", icon: <BarChart3 size={16} />, keywords: "продажи графики" },
  { route: "problem-products", href: "/app/problem-products", label: "Проблемные товары", icon: <AlertTriangle size={16} />, keywords: "ошибки товары" },
  { route: "finance", href: "/app/finance", label: "Финансы", icon: <BadgeDollarSign size={16} />, keywords: "расходы закупки деньги" },
  { route: "new-products", href: "/app/new-products", label: "Новые товары", icon: <PackagePlus size={16} />, keywords: "новинки" },
  { route: "tnved", href: "/app/tnved", label: "Коды ТН ВЭД", icon: <Tag size={16} />, keywords: "тнвэд коды" },
  { route: "brand-bans", href: "/app/brand-bans", label: "Запрет брендов", icon: <Ban size={16} />, keywords: "бренды запрет бан" },
  { route: "brands-tnved", href: "/app/brands-tnved", label: "Бренды / ТН ВЭД", icon: <BookOpen size={16} />, keywords: "бренды декларации" },
  { route: "ozon-card-fix", href: "/app/ozon-card-fix", label: "Карточки Ozon", icon: <Wrench size={16} />, keywords: "озон карточки исправить" },
  { route: "card-health", href: "/app/card-health", label: "Ошибки карточек", icon: <Wrench size={16} />, keywords: "ошибки карточек маркет починить исправить группа вариантов габариты тн вэд карантин" },
  { route: "price-guard", href: "/app/price-guard", label: "Проверка цен", icon: <ShieldAlert size={16} />, keywords: "цены карантин подозрительные заглушка рубли проверка отправить" },
  { route: "ozon-docs", href: "/app/ozon-docs", label: "Документы Ozon", icon: <FileCheck2 size={16} />, keywords: "декларации сертификаты документы проверка отклонено одобрено озон" },
  { route: "fragrantica", href: "/app/fragrantica", label: "Фрагрантика", icon: <Flower2 size={16} />, keywords: "fragrantica ароматы каталог добавить на озон новые карточки" },
];

// Сайдбар: первые пункты — без заголовка, дальше сворачиваемые группы.
export const NAV_SECTIONS: Array<{ id: string; title?: string; routes: AppRoute[] }> = [
  { id: "main", routes: ["dashboard", "warehouse", "picking-list", "supplier-cart", "consignment"] },
  { id: "clients", title: "Работа с клиентами", routes: ["reviews", "chats", "questions", "support"] },
  { id: "marketplace", title: "Маркетплейсы", routes: ["suppliers", "avito", "import", "fragrantica", "ozon-docs", "prices", "price-guard", "card-health", "finance", "statistics"] },
  { id: "admin", title: "Настройки", routes: ["settings", "system", "ai-drafts", "operations"] },
  { id: "tools", title: "Инструменты", routes: ["tnved", "brand-bans", "brands-tnved", "ozon-card-fix", "no-supplier", "recovery-queue", "new-products", "problem-products", "shop"] },
];

// Порядок важен: более длинные префиксы раньше коротких.
const ROUTE_PREFIXES: Array<[string, AppRoute]> = navItems
  .map((item) => [item.href, item.route] as [string, AppRoute])
  .sort((a, b) => b[0].length - a[0].length);

export function routeFromPath(path = window.location.pathname): AppRoute {
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
  dashboard: { load: () => import("../routes/DashboardPage"), exportName: "DashboardPage" },
  warehouse: { load: () => import("../routes/WarehousePage"), exportName: "WarehousePage" },
  "picking-list": { load: () => import("../routes/PickingListPage"), exportName: "PickingListPage" },
  avito: { load: () => import("../routes/AvitoPage"), exportName: "AvitoPage" },
  consignment: { load: () => import("../routes/ConsignmentPage"), exportName: "ConsignmentPage" },
  reviews: { load: () => import("../routes/FeedbackPage"), exportName: "FeedbackPage" },
  questions: { load: () => import("../routes/FeedbackPage"), exportName: "FeedbackPage" },
  chats: { load: () => import("../routes/ChatsPage"), exportName: "ChatsPage" },
  support: { load: () => import("../routes/SupportChatsPage"), exportName: "SupportChatsPage" },
  settings: { load: () => import("../routes/SettingsPage"), exportName: "SettingsPage" },
  system: { load: () => import("../routes/SystemPage"), exportName: "SystemPage" },
  "ai-drafts": { load: () => import("../routes/AiDraftsPage"), exportName: "AiDraftsPage" },
  "no-supplier": { load: () => import("../routes/NoSupplierPage"), exportName: "NoSupplierPage" },
  operations: { load: () => import("../routes/OperationsPage"), exportName: "OperationsPage" },
  "recovery-queue": { load: () => import("../routes/RecoveryQueuePage"), exportName: "RecoveryQueuePage" },
  suppliers: { load: () => import("../routes/SuppliersPage"), exportName: "SuppliersPage" },
  shop: { load: () => import("../routes/ShopAdminPage"), exportName: "default" },
  import: { load: () => import("../routes/ImportPage"), exportName: "ImportPage" },
  "supplier-cart": { load: () => import("../routes/SupplierCartPage"), exportName: "SupplierCartPage" },
  prices: { load: () => import("../routes/PricesPage"), exportName: "PricesPage" },
  statistics: { load: () => import("../routes/StatisticsPage"), exportName: "StatisticsPage" },
  "problem-products": { load: () => import("../routes/ProblemProductsPage"), exportName: "ProblemProductsPage" },
  finance: { load: () => import("../routes/FinancePage"), exportName: "FinancePage" },
  "new-products": { load: () => import("../routes/NewProductsPage"), exportName: "NewProductsPage" },
  tnved: { load: () => import("../routes/TnvedPage"), exportName: "TnvedPage" },
  "brand-bans": { load: () => import("../routes/BrandBansPage"), exportName: "BrandBansPage" },
  "brands-tnved": { load: () => import("../routes/BrandsTnvedPage"), exportName: "BrandsTnvedPage" },
  "ozon-card-fix": { load: () => import("../routes/OzonCardFixPage"), exportName: "OzonCardFixPage" },
  fragrantica: { load: () => import("../routes/FragranticaPage"), exportName: "FragranticaPage" },
  "ozon-docs": { load: () => import("../routes/OzonDocsPage"), exportName: "OzonDocsPage" },
  "price-guard": { load: () => import("../routes/PriceGuardPage"), exportName: "PriceGuardPage" },
  "card-health": { load: () => import("../routes/CardHealthPage"), exportName: "CardHealthPage" },
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
