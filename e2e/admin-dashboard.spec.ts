import { test, expect } from "@playwright/test";

// Admin dashboard snapshots (PLAN-HARDENING.md 5.4: "расширить визуальные снапшоты на
// админ-страницы через мок API"). Admin pages are gated behind /api/session returning
// role:"admin", and the dashboard itself fans out to ~7 endpoints. Every /api/** request is
// mocked via page.route so the page renders deterministically without a backend: every field
// in DashboardSummarySchema (and the other dashboard query schemas) has a default, so an empty
// `{}` payload renders an all-zeros dashboard whose markup is identical whether the queries are
// still loading or have resolved. The notifications SSE stream is aborted (no visible UI here).
//
// Navigation to /app/dashboard happens via the in-app sidebar link (SPA pushState) rather than
// page.goto: the vite dev server only serves the SPA shell under its /app-modern/ base, while
// the app's own route matching expects bare /app/... pathnames (true in production, where the
// express server serves /app/* directly).
test.beforeEach(async ({ page }) => {
  // Один обработчик: Playwright использует LIFO (последний зарегистрированный
  // выигрывает), поэтому несколько page.route() дают непредсказуемый порядок.
  await page.route("**/api/**", (route) => {
    const url = route.request().url();
    if (url.includes("/api/notifications/stream")) return route.abort();
    if (url.includes("/api/session")) return route.fulfill({ json: { authenticated: true, role: "admin", username: "admin" } });
    return route.fulfill({ json: {} });
  });
  await page.goto("/app-modern/");
  await page.waitForSelector('.side-nav-links a[href="/app/dashboard"]', { timeout: 10_000 });
  await page.locator('.side-nav-links a[href="/app/dashboard"]').click();
  await page.waitForSelector(".dashboard-metrics");
});

test("admin dashboard metrics (mocked API, empty state)", async ({ page }) => {
  const metrics = page.locator(".dashboard-metrics");
  await expect(metrics).toBeVisible();
  await expect(metrics).toHaveScreenshot("dashboard-metrics.png");
});

test("admin dashboard sidebar summary cards (mocked API, empty state)", async ({ page }) => {
  const side = page.locator(".dashboard-side");
  await expect(side).toBeVisible();
  await expect(side).toHaveScreenshot("dashboard-side-summary.png");
});
