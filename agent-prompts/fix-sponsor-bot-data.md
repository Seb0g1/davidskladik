# Agent: Fix Sponsor Bot "Данные недоступны"

## Problem
The Telegram sponsor bot (`server/parts/02f-sponsor-bot.js`) shows "⚠️ Данные временно недоступны. Попробуйте позже." when a user selects a role (partner/David). The `sbGetStats()` function returns `null` even though the database has data.

## Repository
`C:\Users\Seb0g1\Documents\New project`

## How the Bot Works
- Token: env var `SPONSOR_BOT_TOKEN`
- Password: env var `SPONSOR_BOT_PASSWORD`
- Only starts on `SERVER_ROLE=worker` (PM2 worker process)
- Assembles all server files via `server/assemble.js` using `Module._compile` — all `server/parts/*.js` files are concatenated into one module scope
- `getPrisma()` is defined in `lib/postgres.js`, imported in `01-bootstrap-app-init.js`, and accessible as a variable in the assembled module
- `getUsdRate()` is defined in `server/parts/02a-supplier-pricing-audit-rate.js`

## The sbGetStats Function (current code)
```js
async function sbGetStats({ days = 1 } = {}) {
  const prisma = getPrisma();
  if (!prisma) return null;  // ← returns null if Prisma not ready
  const since = new Date();
  since.setDate(since.getDate() - (days - 1));
  since.setHours(0, 0, 0, 0);
  let periodOps, allOps, rate;
  try {
    [periodOps, allOps, rate] = await Promise.all([
      prisma.consignmentOperation.findMany({...}),
      prisma.consignmentOperation.findMany({...}),
      sbGetRate(),
    ]);
  } catch (err) {
    logger.warn("sponsor_bot_stats_error", { detail: String(err?.message || err) });
    return null;
  }
  // ... compute and return result
}
```

## What You Need to Do

### 1. Find WHY getPrisma() returns null
Check `lib/postgres.js`:
```js
function getPrisma() {
  if (!hasDatabaseUrl()) return null;  // DATABASE_URL must be set
  if (!prisma) { prisma = new PrismaClient(); }
  return prisma;
}
```
The bot runs in assembled module. Check if `DATABASE_URL` could be undefined at the time the bot runs. The 20-second startup delay should give time for dotenv to load, but maybe it doesn't.

### 2. Check if the issue is timing
The bot starts polling after a 20-second delay. But `getPrisma()` is called only when a user sends a message. That's well after startup. If `DATABASE_URL` is in `.env` and dotenv is loaded in `01-bootstrap-app-init.js` (which runs before the bot), it SHOULD be available.

Try: **What if dotenv is not loading the .env file?** The `Module._compile` context sets `assembled.filename = path.join(projectRoot, "server.js")`. The `require("dotenv").config()` call looks for `.env` relative to `process.cwd()`, not `__dirname`. Check if `process.cwd()` matches the project root on production.

### 3. Check the actual error in logs
The catch block logs: `logger.warn("sponsor_bot_stats_error", { detail: String(err?.message || err) })`

Add **better diagnostics** — send the actual error to the chat temporarily:

Modify sbGetStats to show the actual error reason:
```js
} catch (err) {
  const detail = String(err?.message || err);
  logger.warn("sponsor_bot_stats_error", { detail });
  return { _error: detail };  // return error object instead of null
}
```

And in msgWelcomePartner / msgToday etc:
```js
if (!d) return "⚠️ Данные недоступны.";
if (d._error) return `⚠️ Ошибка: <code>${d._error}</code>`;
```

This way the user sees the REAL error, not just "недоступны".

### 4. Check if there's a Prisma migration issue
The schema has a new model `ConsignmentInvoice` (added in recent commits). If the migration `20260828130000_add_consignment_invoices` wasn't applied on prod, Prisma client might fail to connect entirely.

Check: does `consignment_invoices` table need to exist for Prisma to function? (It shouldn't block other table queries, but check if there's any validation on connect)

### 5. Fix the root cause
After identifying the actual error:
- If `DATABASE_URL` not found: investigate dotenv loading and fix the path resolution
- If migration not run: add a migration check or make the bot handle partial schema
- If Prisma connect fails: add `prisma.$connect()` call after the 20-second delay
- If it's a query error: fix the query

### 6. Add explicit Prisma connect
Add this after the startup delay:
```js
await new Promise((r) => setTimeout(r, 20000));
if (!sponsorBotRunning) return;
// Explicitly ensure Prisma is connected
try {
  const p = getPrisma();
  if (p) await p.$connect();
  logger.info("sponsor_bot_prisma_connected");
} catch (err) {
  logger.warn("sponsor_bot_prisma_connect_failed", { detail: String(err?.message || err) });
}
await sponsorBotDrainOldUpdates();
```

## Key Files
- `server/parts/02f-sponsor-bot.js` — the bot (main file to fix)
- `lib/postgres.js` — getPrisma definition
- `server/parts/01-bootstrap-app-init.js` — dotenv loading, getPrisma import
- `server/assemble.js` — how the module is compiled
- `prisma/schema.prisma` — check ConsignmentOperation model exists

## After Fixing
- Run `npm test` — must pass 294/294
- Commit and the person will deploy
