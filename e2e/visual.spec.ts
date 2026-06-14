import { test, expect } from "@playwright/test";

// Element-scoped snapshots of deterministic UI. We snapshot the catalog toolbar (search +
// custom filters) and an open filter dropdown — neither carries dates/counts, so baselines
// are stable. The product list itself is intentionally excluded (demo data can vary).
test.beforeEach(async ({ page }) => {
  await page.goto("/app-modern/");
  await page.waitForSelector(".toolbar .select-field-trigger");
});

test("catalog toolbar filters", async ({ page }) => {
  const toolbar = page.locator(".toolbar").first();
  await expect(toolbar).toBeVisible();
  await expect(toolbar).toHaveScreenshot("catalog-toolbar.png");
});

test("custom filter dropdown (open)", async ({ page }) => {
  await page.locator(".toolbar .select-field-trigger").first().click();
  const pop = page.locator(".select-field-pop");
  await expect(pop).toBeVisible();
  await expect(pop).toHaveScreenshot("filter-dropdown-open.png");
});
