import { defineConfig, devices } from "@playwright/test";

// Visual snapshot tests (PLAN-HARDENING.md 5.4). Self-contained: the warehouse catalog is
// the default route and falls back to demo data when no API/session is present, so these
// run against `npm run frontend:dev` alone (no backend, no auth). Baselines live in
// e2e/__screenshots__ and are regenerated with `npm run test:visual:update`.
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: [["list"]],
  // Include {platform}: pixel rendering differs across OSes (font antialiasing), so each OS
  // keeps its own baseline instead of failing on a foreign one. Regenerate per-OS with
  // `npm run test:visual:update`.
  snapshotPathTemplate: "{testDir}/__screenshots__/{arg}-{platform}{ext}",
  use: {
    baseURL: "http://localhost:5173",
    viewport: { width: 1280, height: 900 },
    reducedMotion: "reduce",
    colorScheme: "dark",
    deviceScaleFactor: 1,
  },
  expect: {
    // Allow a tiny ratio of diff pixels for font-antialiasing jitter across runs.
    toHaveScreenshot: { animations: "disabled", caret: "hide", maxDiffPixelRatio: 0.01 },
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"], channel: "chrome" } }],
  webServer: {
    command: "npm run frontend:dev",
    url: "http://localhost:5173/app-modern/",
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
