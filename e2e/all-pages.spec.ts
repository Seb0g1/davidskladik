import { test, expect } from "@playwright/test";

// Смоук всех страниц приложения: каждая страница открывается через сайдбар
// (SPA pushState), API замокан пустыми ответами (все схемы имеют дефолты).
// Ловит: упавший рендер (пустой main / error boundary), необработанные
// исключения (pageerror) и ошибки загрузки lazy-чанков — после перехода на
// code-splitting каждый раздел приезжает отдельным файлом.
const ROUTES: Array<{ href: string; label: string }> = [
  { href: "/app/dashboard", label: "Дашборд" },
  { href: "/app/warehouse", label: "Склад" },
  { href: "/app/picking-list", label: "Сборка" },
  { href: "/app/avito", label: "Автозагрузка Avito" },
  { href: "/app/consignment", label: "Реализация" },
  { href: "/app/reviews", label: "Отзывы" },
  { href: "/app/chats", label: "Чаты" },
  { href: "/app/questions", label: "Вопросы" },
  { href: "/app/settings", label: "Настройки" },
  { href: "/app/system", label: "Система" },
  { href: "/app/ai-drafts", label: "AI drafts" },
  { href: "/app/no-supplier", label: "Ошибки наличия" },
  { href: "/app/operations", label: "Операции" },
  { href: "/app/recovery-queue", label: "Восстановление" },
  { href: "/app/suppliers", label: "Поставщики" },
  { href: "/app/import", label: "Импорт на Яндекс" },
  { href: "/app/supplier-cart", label: "Автокорзина" },
  { href: "/app/prices", label: "Цены" },
  { href: "/app/statistics", label: "Статистика" },
  { href: "/app/problem-products", label: "Проблемные товары" },
  { href: "/app/finance", label: "Финансы" },
];

test("все страницы открываются без крашей и pageerror", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(String(error?.message || error)));

  // Один обработчик: Playwright использует LIFO (последний зарегистрированный
  // выигрывает), поэтому несколько page.route() дают непредсказуемый порядок.
  await page.route("**/api/**", (route) => {
    const url = route.request().url();
    if (url.includes("/api/notifications/stream")) return route.abort();
    if (url.includes("/api/session")) return route.fulfill({ json: { authenticated: true, role: "admin", username: "admin" } });
    return route.fulfill({ json: {} });
  });

  await page.goto("/app-modern/");
  // Ждём пока сессия загрузится и появится dashboard-ссылка (признак admin-роли).
  // .side-nav-links рендерится сразу, но admin-маршруты видны только после /api/session.
  await page.waitForSelector('.side-nav-links a[href="/app/dashboard"]', { timeout: 10_000 });

  for (const { href, label } of ROUTES) {
    // Сворачиваемые секции сайдбара: раскрываем все перед кликом.
    const collapsed = page.locator(".nav-section.is-collapsed .nav-section-title");
    while (await collapsed.count()) await collapsed.first().click();

    const link = page.locator(`.side-nav-links a[href="${href}"]`);
    await expect(link, `в сайдбаре нет ссылки на ${label} (${href})`).toBeVisible();
    await link.click();

    // Страница отрендерила свою секцию (или явную заглушку) — не пустой экран.
    // Фильтруем :visible, чтобы React Suspense не скрывал предыдущую страницу
    // (display:none во время загрузки lazy-чанка) и не мешал .first().
    // .toolbar — WarehousePage; .settings-tabs — SettingsPage (не используют .page-section)
    const section = page.locator([
      "main .page-section:visible",
      "main .access-denied-panel:visible",
      "main .toolbar:visible",
      "main .settings-tabs:visible",
    ].join(", "));
    await expect(section.first(), `${label} (${href}): страница не отрендерилась`).toBeVisible({ timeout: 10_000 });

    expect(pageErrors, `${label} (${href}): необработанные исключения: ${pageErrors.join("; ")}`).toEqual([]);
  }
});
