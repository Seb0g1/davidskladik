// Ozon action guard: Ozon keeps adding our products to its special offers (акции) on its own.
// Every OZON_ACTION_GUARD_INTERVAL_MINUTES (default 15) the worker walks every special offer of
// every Ozon account and removes the products Ozon added (add_mode other than MANUAL).
// Products a person added by hand in the cabinet (MANUAL) stay, unless
// OZON_ACTION_GUARD_REMOVE_MANUAL=true. Offers in their freeze period reject removal; that is
// logged and retried on the next tick.

const ozonActionGuardEnabled = process.env.OZON_ACTION_GUARD_ENABLED !== "false";
const ozonActionGuardIntervalMs = Math.max(
  5 * 60_000,
  Number(process.env.OZON_ACTION_GUARD_INTERVAL_MINUTES || 15) * 60_000 || 15 * 60_000,
);
const ozonActionGuardRemoveManual = process.env.OZON_ACTION_GUARD_REMOVE_MANUAL === "true";
const OZON_ACTION_PAGE_SIZE = 500;
const OZON_ACTION_DEACTIVATE_CHUNK = 500;
let ozonActionGuardTimer = null;
let ozonActionGuardRunning = false;
const ozonActionGuardStatus = { lastRunAt: null, lastResult: null, lastError: null };

// GET /v1/actions: ozonRequest only does POST.
async function ozonGetRequest(pathname, account) {
  if (!account?.clientId || !account?.apiKey) throw new Error("Ozon account has no Client-Id / Api-Key");
  return enqueueOzonRequest(async () => {
    const response = await fetch(`${ozonBaseUrl}${pathname}`, {
      method: "GET",
      headers: { "Client-Id": account.clientId, "Api-Key": account.apiKey },
      signal: AbortSignal.timeout(60_000),
    });
    const data = parseApiResponse(await response.text());
    if (!response.ok) {
      const error = new Error(data.message || data.error || `Ozon API error ${response.status}`);
      error.statusCode = response.status;
      error.ozon = data;
      throw error;
    }
    return data;
  });
}

function shouldRemoveFromOzonAction(product) {
  if (ozonActionGuardRemoveManual) return true;
  return String(product?.add_mode || "").toUpperCase() !== "MANUAL";
}

async function listOzonActionProducts(account, actionId) {
  const products = [];
  let lastId = "";
  for (let page = 0; page < 200; page += 1) {
    const data = await ozonRequest("/v1/actions/products", { action_id: actionId, limit: OZON_ACTION_PAGE_SIZE, ...(lastId ? { last_id: lastId } : {}) }, account);
    const batch = data?.result?.products || [];
    products.push(...batch);
    const next = data?.result?.last_id;
    if (!batch.length || !next || String(next) === String(lastId)) break;
    lastId = next;
  }
  return products;
}

async function runOzonActionGuard({ dryRun = false } = {}) {
  if (ozonActionGuardRunning) return { status: "busy" };
  ozonActionGuardRunning = true;
  const result = { dryRun, accounts: [], removed: 0, rejected: 0, found: 0 };
  try {
    for (const account of getOzonAccounts({ includeSyncDisabled: true })) {
      if (!account?.clientId || !account?.apiKey) continue;
      const accountResult = { account: account.name || account.id, actions: [] };
      result.accounts.push(accountResult);
      let actions = [];
      try {
        actions = (await ozonGetRequest("/v1/actions", account))?.result || [];
      } catch (error) {
        accountResult.error = error?.message || String(error);
        logger.warn("ozon_action_guard_list_failed", { account: accountResult.account, detail: accountResult.error });
        continue;
      }
      for (const action of actions) {
        if (!Number(action.participating_products_count)) continue;
        const entry = { id: action.id, title: action.title, participating: action.participating_products_count, found: 0, removed: 0, rejected: [] };
        try {
          const products = await listOzonActionProducts(account, action.id);
          const ids = products.filter(shouldRemoveFromOzonAction).map((product) => Number(product.id)).filter(Boolean);
          entry.found = ids.length;
          entry.modes = products.reduce((acc, product) => { const mode = product.add_mode || "?"; acc[mode] = (acc[mode] || 0) + 1; return acc; }, {});
          result.found += ids.length;
          if (!dryRun) {
            for (let i = 0; i < ids.length; i += OZON_ACTION_DEACTIVATE_CHUNK) {
              const chunk = ids.slice(i, i + OZON_ACTION_DEACTIVATE_CHUNK);
              const data = await ozonRequest("/v1/actions/products/deactivate", { action_id: action.id, product_ids: chunk }, account);
              entry.removed += (data?.result?.product_ids || []).length;
              entry.rejected.push(...(data?.result?.rejected || []));
            }
          }
        } catch (error) {
          entry.error = error?.message || String(error);
        }
        result.removed += entry.removed;
        result.rejected += entry.rejected.length;
        if (entry.found || entry.error) {
          accountResult.actions.push({ ...entry, rejected: entry.rejected.slice(0, 20), rejectedTotal: entry.rejected.length });
          if (!dryRun) {
            logger.info("ozon_action_guard_action", {
              account: accountResult.account, actionId: entry.id, title: entry.title, found: entry.found,
              removed: entry.removed, rejected: entry.rejected.length, reason: entry.rejected[0]?.reason || null, error: entry.error || null,
            });
          }
        }
      }
    }
    Object.assign(ozonActionGuardStatus, { lastRunAt: new Date().toISOString(), lastResult: result, lastError: null });
    logger.info("ozon_action_guard_done", { dryRun, found: result.found, removed: result.removed, rejected: result.rejected });
    return result;
  } catch (error) {
    ozonActionGuardStatus.lastError = error?.message || String(error);
    logger.warn("ozon_action_guard_failed", { detail: ozonActionGuardStatus.lastError });
    throw error;
  } finally {
    ozonActionGuardRunning = false;
  }
}

function scheduleOzonActionGuard(delayMs = ozonActionGuardIntervalMs) {
  if (!ozonActionGuardEnabled) return;
  if (ozonActionGuardTimer) clearTimeout(ozonActionGuardTimer);
  ozonActionGuardTimer = setTimeout(async () => {
    try {
      await runOzonActionGuard();
    } catch {
      // logged in runOzonActionGuard
    } finally {
      scheduleOzonActionGuard(ozonActionGuardIntervalMs);
    }
  }, Math.max(5_000, Number(delayMs) || ozonActionGuardIntervalMs));
  ozonActionGuardTimer.unref?.();
}

app.get("/api/ozon-action-guard", requireAdmin, (_request, response) => {
  response.json({ enabled: ozonActionGuardEnabled, intervalMinutes: ozonActionGuardIntervalMs / 60_000, removeManual: ozonActionGuardRemoveManual, ...ozonActionGuardStatus });
});

// POST {"dryRun": true} shows what would be removed without touching Ozon.
app.post("/api/ozon-action-guard/run", requireAdmin, async (request, response, next) => {
  try {
    response.json(await runOzonActionGuard({ dryRun: request.body?.dryRun === true }));
  } catch (error) {
    next(error);
  }
});
