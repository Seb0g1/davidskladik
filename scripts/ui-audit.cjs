// UI audit script — navigates all main pages on davidsklad.ru and saves screenshots
const { chromium } = require("playwright");
const path = require("path");
const fs = require("fs");

const BASE = "https://davidsklad.ru";
const USER = "david";
const PASS = "CGJ-Ge-90";
const OUT = path.join(__dirname, "../ui-audit-screenshots");

if (!fs.existsSync(OUT)) fs.mkdirSync(OUT, { recursive: true });

const PAGES = [
  { name: "01-warehouse", url: "/warehouse" },
  { name: "02-warehouse-unlinked", url: "/warehouse?linked=unlinked" },
  { name: "03-prices", url: "/prices" },
  { name: "04-suppliers", url: "/suppliers" },
  { name: "05-supplier-cart", url: "/supplier-cart" },
  { name: "06-picking-list", url: "/picking-list" },
  { name: "07-consignment", url: "/consignment" },
  { name: "08-operations", url: "/operations" },
  { name: "09-finance", url: "/finance" },
  { name: "10-statistics", url: "/statistics" },
  { name: "11-reviews", url: "/reviews" },
  { name: "12-questions", url: "/questions" },
  { name: "13-chats", url: "/chats" },
  { name: "14-settings", url: "/settings" },
  { name: "15-import", url: "/import" },
  { name: "16-avito", url: "/avito" },
  { name: "17-system", url: "/system" },
];

(async () => {
  const browser = await chromium.launch({
    headless: true,
    executablePath: "C:\\Users\\Seb0g1\\AppData\\Local\\ms-playwright\\chromium-1228\\chrome-win64\\chrome.exe",
  });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();

  // Login
  await page.goto(`${BASE}/login.html`, { waitUntil: "domcontentloaded" });
  await page.fill('input[name="username"], input[type="text"]', USER);
  await page.fill('input[name="password"], input[type="password"]', PASS);
  await page.click('button[type="submit"], input[type="submit"]');
  await page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});
  console.log("Logged in, current URL:", page.url());

  // Screenshot each page
  for (const { name, url } of PAGES) {
    try {
      await page.goto(`${BASE}/app${url}`, { waitUntil: "domcontentloaded", timeout: 20000 });
      await page.waitForTimeout(3000);
      const file = path.join(OUT, `${name}.png`);
      await page.screenshot({ path: file, fullPage: false });
      console.log(`✓ ${name}`);
    } catch (e) {
      console.error(`✗ ${name}: ${e.message}`);
    }
  }

  // Also mobile viewport
  const mobile = await browser.newContext({ viewport: { width: 390, height: 844 } });
  const mpage = await mobile.newPage();
  await mpage.goto(`${BASE}/login.html`, { waitUntil: "domcontentloaded" });
  await mpage.fill('input[name="username"], input[type="text"]', USER);
  await mpage.fill('input[name="password"], input[type="password"]', PASS);
  await mpage.click('button[type="submit"], input[type="submit"]');
  await mpage.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 15000 }).catch(() => {});

  for (const { name, url } of PAGES.slice(0, 6)) {
    try {
      await mpage.goto(`${BASE}/app${url}`, { waitUntil: "domcontentloaded", timeout: 20000 });
      await mpage.waitForTimeout(1500);
      const file = path.join(OUT, `mobile-${name}.png`);
      await mpage.screenshot({ path: file, fullPage: false });
      console.log(`✓ mobile-${name}`);
    } catch (e) {
      console.error(`✗ mobile-${name}: ${e.message}`);
    }
  }

  await browser.close();
  console.log("Done. Screenshots in:", OUT);
})();
