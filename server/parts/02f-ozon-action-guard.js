// Ozon action guard: Ozon keeps adding our products to its special offers (акции) on its own.
// Every OZON_ACTION_GUARD_INTERVAL_MINUTES (default 5) the worker walks every special offer of
// every Ozon account and removes the products Ozon added (add_mode other than MANUAL).
// Products a person added by hand in the cabinet (MANUAL) stay, unless
// OZON_ACTION_GUARD_REMOVE_MANUAL=true. Offers in their freeze period reject removal; that is
// logged and retried on the next tick.

const ozonActionGuardEnabled = process.env.OZON_ACTION_GUARD_ENABLED !== "false";
// An idle pass costs one GET /v1/actions per account (offers with no products are skipped).
const ozonActionGuardIntervalMs = Math.max(
  2 * 60_000,
  Number(process.env.OZON_ACTION_GUARD_INTERVAL_MINUTES || 5) * 60_000 || 5 * 60_000,
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

// Scheduled auto-add: Ozon also books products into an offer from a future date («Участвуют с 14.10» in the
// cabinet). They are not participants yet — /v1/actions/products shows 0 — so they live in a separate list per
// auto_add_date and are removed from it with /v1/actions/auto-add/products/delete.
const OZON_AUTO_ADD_PAGE = 100;
async function listOzonActionAutoAddProducts(account, actionId, autoAddDate) {
  const products = [];
  for (let offset = 0; offset < 20000; offset += OZON_AUTO_ADD_PAGE) {
    const data = await ozonRequest("/v1/actions/auto-add/products/list", { action_id: actionId, auto_add_date: autoAddDate, limit: OZON_AUTO_ADD_PAGE, offset }, account);
    const batch = data?.products || data?.result?.products || [];
    products.push(...batch);
    if (batch.length < OZON_AUTO_ADD_PAGE) break;
  }
  return products;
}

async function guardOzonActionAutoAdd(account, action, { dryRun }) {
  const out = [];
  for (const autoAddDate of Array.isArray(action.auto_add_dates) ? action.auto_add_dates : []) {
    const entry = { date: autoAddDate, found: 0, removed: 0, modes: {} };
    try {
      const products = await listOzonActionAutoAddProducts(account, action.id, autoAddDate);
      // «produсt_id» with a Cyrillic «с» is how the docs spell it in one example — accept every spelling
      const ids = products.filter(shouldRemoveFromOzonAction)
        .map((product) => String(product.product_id ?? product["produсt_id"] ?? product.id ?? "").trim()).filter(Boolean);
      entry.modes = products.reduce((acc, product) => { const mode = String(product.add_mode || "?"); acc[mode] = (acc[mode] || 0) + 1; return acc; }, {});
      entry.found = ids.length;
      if (!dryRun) {
        for (let i = 0; i < ids.length; i += 1000) {
          const data = await ozonRequest("/v1/actions/auto-add/products/delete", { action_id: action.id, auto_add_date: autoAddDate, product_ids: ids.slice(i, i + 1000) }, account);
          entry.removed += (data?.product_ids || data?.result?.product_ids || []).length;
        }
      }
    } catch (error) {
      entry.error = error?.message || String(error);
    }
    if (entry.found || entry.error) out.push(entry);
  }
  return out;
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
        // products booked from a future date first: they are not counted as participants yet
        const scheduled = await guardOzonActionAutoAdd(account, action, { dryRun }).catch((error) => [{ error: error?.message || String(error) }]);
        for (const item of scheduled) {
          result.found += item.found || 0;
          result.removed += item.removed || 0;
          accountResult.actions.push({ id: action.id, title: action.title, autoAddDate: item.date, found: item.found || 0, removed: item.removed || 0, modes: item.modes, error: item.error || undefined });
          if (!dryRun) {
            logger.info("ozon_action_guard_auto_add", { account: accountResult.account, actionId: action.id, title: action.title, autoAddDate: item.date, found: item.found || 0, removed: item.removed || 0, error: item.error || null });
          }
        }
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
        // every offer with our products is reported (with add modes): «found 0» alone hid why nothing left an offer
        if (entry.participating || entry.found || entry.error) {
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
