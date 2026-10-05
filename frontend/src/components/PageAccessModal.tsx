import { useEffect, useMemo, useState } from "react";
import { Activity, AlertCircle, Ban, BookOpen, FileCheck2, Flower2, Link2, MessageCircleHeart, PackagePlus, ShieldAlert, Store, Tag, Wrench, AlertTriangle, BadgeDollarSign, BarChart3, Check, CirclePlay, ClipboardList, Eye, HandCoins, HelpCircle, Home, Loader2, MessageCircle, PackageCheck, RefreshCcw, Settings, ShoppingCart, Sparkles, Star, Truck, Upload, X } from "lucide-react";
import type { ReactNode } from "react";

export type PageCatalogItem = {
  key: string;
  label: string;
  description: string;
  href: string;
  icon: ReactNode;
};

// Keys mirror AppRoute in App.tsx and APP_PAGE_KEYS on the server.
export const PAGE_CATALOG: PageCatalogItem[] = [
  { key: "dashboard", label: "Дашборд", description: "Сводка по складу и продажам", href: "/app/dashboard", icon: <Home size={15} /> },
  { key: "warehouse", label: "Склад", description: "Каталог товаров и привязки PriceMaster", href: "/app/warehouse", icon: <PackageCheck size={15} /> },
  { key: "suppliers", label: "Поставщики", description: "Справочник поставщиков и балансы", href: "/app/suppliers", icon: <Truck size={15} /> },
  { key: "picking-list", label: "Сборка", description: "Лист сборки заказов", href: "/app/picking-list", icon: <ClipboardList size={15} /> },
  { key: "reviews", label: "Отзывы", description: "Отзывы с маркетплейсов", href: "/app/reviews", icon: <Star size={15} /> },
  { key: "chats", label: "Чаты", description: "Чаты с покупателями", href: "/app/chats", icon: <MessageCircle size={15} /> },
  { key: "fragrantica", label: "Фрагрантика", description: "Каталог ароматов и создание карточек Ozon", href: "/app/fragrantica", icon: <Flower2 size={15} /> },
  { key: "import", label: "Импорт на Яндекс", description: "Перенос карточек Ozon на Яндекс", href: "/app/import", icon: <Upload size={15} /> },
  { key: "avito", label: "Автозагрузка Avito", description: "Фид Автозагрузки и правила импорта с Ozon", href: "/app/avito", icon: <Upload size={15} /> },
  { key: "statistics", label: "Статистика", description: "Статистика сотрудников и продаж", href: "/app/statistics", icon: <BarChart3 size={15} /> },
  { key: "settings", label: "Настройки", description: "Настройки сайта, цены, сотрудники", href: "/app/settings", icon: <Settings size={15} /> },
  { key: "questions", label: "Вопросы", description: "Вопросы покупателей", href: "/app/questions", icon: <HelpCircle size={15} /> },
  { key: "prices", label: "Цены", description: "Управление ценами и отправка на МП", href: "/app/prices", icon: <BadgeDollarSign size={15} /> },
  { key: "operations", label: "Операции", description: "Массовые операции по каталогу", href: "/app/operations", icon: <CirclePlay size={15} /> },
  { key: "supplier-cart", label: "Автокорзина", description: "Автосборка корзины поставщиков", href: "/app/supplier-cart", icon: <ShoppingCart size={15} /> },
  { key: "recovery-queue", label: "Восстановление из архива", description: "Очередь восстановления карточек", href: "/app/recovery-queue", icon: <RefreshCcw size={15} /> },
  { key: "problem-products", label: "Проблемные товары", description: "Карточки с проблемами", href: "/app/problem-products", icon: <AlertTriangle size={15} /> },
  { key: "finance", label: "Финансы", description: "Заказы, расходы, прибыль", href: "/app/finance", icon: <BadgeDollarSign size={15} /> },
  { key: "consignment", label: "Реализация", description: "Товар спонсора: продажи и профит 50/50", href: "/app/consignment", icon: <HandCoins size={15} /> },
  { key: "system", label: "Система", description: "Техническое состояние сервиса", href: "/app/system", icon: <Activity size={15} /> },
  { key: "support", label: "Поддержка сайта", description: "Чат поддержки magicvibes.ru", href: "/app/support", icon: <MessageCircleHeart size={15} /> },
  { key: "supplier-match", label: "Подбор поставщиков", description: "Поставщики для товаров без поставщика, прайсы из Excel", href: "/app/supplier-match", icon: <Link2 size={15} /> },
  { key: "shop", label: "Магазин MV", description: "Сайт magicvibes.ru: товары и заказы", href: "/app/shop", icon: <Store size={15} /> },
  { key: "new-products", label: "Новые товары", description: "Новинки для заведения карточек", href: "/app/new-products", icon: <PackagePlus size={15} /> },
  { key: "card-improve", label: "Улучшение карточек", description: "Новые фото, название и описание старых карточек", href: "/app/card-improve", icon: <Sparkles size={15} /> },
  { key: "card-health", label: "Ошибки карточек", description: "Ошибки карточек Маркета и их исправление", href: "/app/card-health", icon: <Wrench size={15} /> },
  { key: "ozon-card-fix", label: "Карточки Ozon", description: "Исправление карточек Ozon", href: "/app/ozon-card-fix", icon: <Wrench size={15} /> },
  { key: "ozon-docs", label: "Документы Ozon", description: "Декларации и сертификаты на Ozon", href: "/app/ozon-docs", icon: <FileCheck2 size={15} /> },
  { key: "price-guard", label: "Проверка цен", description: "Подозрительные цены, которые ждут проверки", href: "/app/price-guard", icon: <ShieldAlert size={15} /> },
  { key: "tnved", label: "Коды ТН ВЭД", description: "Справочник кодов ТН ВЭД", href: "/app/tnved", icon: <Tag size={15} /> },
  { key: "brands-tnved", label: "Бренды / ТН ВЭД", description: "Бренды и декларации", href: "/app/brands-tnved", icon: <BookOpen size={15} /> },
  { key: "brand-bans", label: "Запрет брендов", description: "Бренды, которые не продаём", href: "/app/brand-bans", icon: <Ban size={15} /> },
  { key: "ai-drafts", label: "ИИ-черновики", description: "Черновики AI-описаний и фото", href: "/app/ai-drafts", icon: <Sparkles size={15} /> },
  { key: "no-supplier", label: "Ошибки наличия", description: "Товары без поставщика", href: "/app/no-supplier", icon: <AlertCircle size={15} /> },
];

const PRESETS: Array<{ label: string; pages: string[] }> = [
  { label: "Только Реализация", pages: ["consignment"] },
  { label: "Склад + Сборка", pages: ["warehouse", "picking-list"] },
  { label: "Саппорт", pages: ["warehouse", "picking-list", "reviews", "questions", "chats"] },
  { label: "Все страницы", pages: PAGE_CATALOG.map((page) => page.key) },
];

export function PageAccessModal({
  username,
  role,
  allowedPages,
  saving,
  error,
  onClose,
  onSave,
}: {
  username: string;
  role: string;
  allowedPages: string[] | null;
  saving: boolean;
  error?: string;
  onClose: () => void;
  onSave: (pages: string[] | null) => void;
}) {
  const isAdminUser = role === "admin";
  const [selected, setSelected] = useState<Set<string>>(() => new Set(allowedPages || []));
  const [previewKey, setPreviewKey] = useState<string>(allowedPages?.[0] || "consignment");
  const [previewLoading, setPreviewLoading] = useState(true);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  useEffect(() => {
    setPreviewLoading(true);
  }, [previewKey]);

  const previewPage = useMemo(
    () => PAGE_CATALOG.find((page) => page.key === previewKey) || PAGE_CATALOG[0],
    [previewKey],
  );

  const toggle = (key: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <div className="page-access-overlay" role="dialog" aria-modal="true" aria-label={`Доступ к страницам: ${username}`}>
      <div className="page-access-modal">
        <header className="page-access-head">
          <div>
            <span className="eyebrow">Доступ к страницам</span>
            <h3>{username} <small>({role})</small></h3>
          </div>
          <button className="icon-action" type="button" aria-label="Закрыть" onClick={onClose}><X size={18} /></button>
        </header>

        {isAdminUser ? (
          <div className="table-note">Администратор всегда видит все страницы — ограничения к нему не применяются.</div>
        ) : (
          <div className="row-actions page-access-presets">
            {PRESETS.map((preset) => (
              <button key={preset.label} className="secondary-action" type="button" onClick={() => {
                setSelected(new Set(preset.pages));
                setPreviewKey(preset.pages[0]);
              }}>
                {preset.label}
              </button>
            ))}
            <button className="secondary-action" type="button" onClick={() => setSelected(new Set())}>Снять всё</button>
          </div>
        )}

        <div className="page-access-body">
          <div className="page-access-list" role="listbox" aria-label="Страницы">
            {PAGE_CATALOG.map((page) => {
              const checked = isAdminUser || selected.has(page.key);
              return (
                <div
                  key={page.key}
                  className={`page-access-row${previewKey === page.key ? " is-active" : ""}${checked ? " is-checked" : ""}`}
                  onClick={() => setPreviewKey(page.key)}
                >
                  <label onClick={(event) => event.stopPropagation()}>
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={isAdminUser}
                      onChange={() => toggle(page.key)}
                    />
                  </label>
                  <span className="page-access-icon">{page.icon}</span>
                  <div className="page-access-text">
                    <strong>{page.label}</strong>
                    <small>{page.description}</small>
                  </div>
                  <Eye size={14} className="page-access-eye" />
                </div>
              );
            })}
          </div>
          <div className="page-access-preview">
            <div className="page-access-preview-head">
              <strong>{previewPage.label}</strong>
              <small>предпросмотр страницы</small>
            </div>
            <div className="page-access-preview-frame">
              {previewLoading ? <div className="page-access-preview-loading"><Loader2 className="spin" size={20} /> Загружаю предпросмотр…</div> : null}
              <iframe
                key={previewPage.key}
                src={`${previewPage.href}?embed=preview`}
                title={`Предпросмотр: ${previewPage.label}`}
                loading="lazy"
                onLoad={() => setPreviewLoading(false)}
              />
            </div>
          </div>
        </div>

        <footer className="page-access-foot">
          <small>
            {isAdminUser
              ? "Роль admin даёт полный доступ."
              : (selected.size
                ? `Выбрано страниц: ${selected.size}`
                : "Ничего не выбрано — будет доступ по умолчанию: Склад + Сборка")}
          </small>
          <div className="row-actions">
            <button className="secondary-action" type="button" onClick={onClose}>Отмена</button>
            {!isAdminUser ? (
              <button className="primary-action" type="button" disabled={saving} onClick={() => onSave(selected.size ? [...selected] : null)}>
                {saving ? <Loader2 className="spin" size={16} /> : <Check size={16} />} Сохранить доступ
              </button>
            ) : null}
          </div>
        </footer>
        {error ? <div className="inline-error">{error}</div> : null}
      </div>
    </div>
  );
}
