// Snooze sweep: find links with expired snoozes and reactivate the product
// when its supplier is back in the price list. Runs every SNOOZE_SWEEP_INTERVAL_SECONDS.

const snoozeSweepEnabled = process.env.SNOOZE_SWEEP_ENABLED !== "false";
const snoozeSweepIntervalMs = Math.max(5 * 60_000, Number(process.env.SNOOZE_SWEEP_INTERVAL_SECONDS || 1800) * 1000 || 30 * 60_000);
const linkedDefaultSnoozedTargetStock = Math.max(1, Number(process.env.LINKED_DEFAULT_TARGET_STOCK || 5) || 5);
let snoozeSweepTimer = null;
let snoozeSweepRunning = false;

async function runSnoozeSweep({ source = "schedule" } = {}) {
  if (snoozeSweepRunning) return { status: "already_running" };
  const prisma = getPrisma();
  if (!prisma || !shouldUsePostgresStorage()) return { status: "postgres_disabled" };
  snoozeSweepRunning = true;
  try {
    // Find ProductLink rows with expired snoozes
    const rows = await prisma.$queryRawUnsafe(`
      SELECT DISTINCT product_id AS "productId"
      FROM product_links
      WHERE raw->'snooze' IS NOT NULL
        AND (raw->'snooze'->>'snoozedUntil')::timestamptz <= now()
      LIMIT 200
    `);
    if (!rows.length) return { status: "ok", expired: 0, reactivated: 0 };

    const expiredProductIds = rows.map((row) => String(row.productId));
    const products = await readWarehouseProductsFromPostgresByIds(expiredProductIds);
    if (!products.length) return { status: "ok", expired: expiredProductIds.length, reactivated: 0 };

    // Clear expired snoozes from links
    const now = new Date().toISOString();
    const nowMs = Date.now();
    const updatedProducts = products.map((product) => ({
      ...product,
      updatedAt: now,
      links: (product.links || []).map((link) => {
        const snoozedUntil = link.snooze?.snoozedUntil;
        if (!snoozedUntil || new Date(snoozedUntil).getTime() > nowMs) return link;
        const { snooze: _removed, ...rest } = link;
        return rest;
      }),
    }));

    await writeWarehouseProductPatch(updatedProducts, { reason: "snooze_expired", writeLinks: true });

    // Rebuild to see which products now have a supplier
    const warehouse = await readWarehouse();
    const builtProducts = await buildFreshWarehouseProductsFromKnownProducts(warehouse, updatedProducts).catch((error) => {
      logger.warn("snooze sweep build failed", { detail: error?.message || String(error) });
      return updatedProducts;
    });

    const toReactivate = builtProducts.filter((product) => product.selectedSupplier && product.hasLinks);
    if (toReactivate.length) {
      const productIds = toReactivate.map((product) => product.id).filter(Boolean);
      queueLinkedProductActivation(productIds, "snooze_sweep")
        .catch((error) => logger.warn("snooze sweep recovery queue failed", { count: productIds.length, detail: error?.message || String(error) }));
    }

    logger.info("snooze_sweep_complete", {
      source,
      expiredProducts: expiredProductIds.length,
      reactivated: toReactivate.length,
      noSupplier: builtProducts.length - toReactivate.length,
    });
    return { status: "ok", expired: expiredProductIds.length, reactivated: toReactivate.length };
  } catch (error) {
    logger.warn("snooze sweep failed", { detail: error?.message || String(error) });
    return { status: "error", error: error?.message || String(error) };
  } finally {
    snoozeSweepRunning = false;
  }
}

function scheduleSnoozeSweep(delayMs = snoozeSweepIntervalMs) {
  if (!snoozeSweepEnabled) return;
  if (snoozeSweepTimer) clearTimeout(snoozeSweepTimer);
  const normalizedDelay = Math.max(60_000, Number(delayMs) || snoozeSweepIntervalMs);
  snoozeSweepTimer = setTimeout(async () => {
    let result = null;
    try {
      result = await runSnoozeSweep({ source: "schedule" });
    } catch (error) {
      logger.warn("snooze sweep tick failed", { detail: error?.message || String(error) });
      result = { status: "error", error: error?.message || String(error) };
    } finally {
      await recordSweepHeartbeat("snooze_sweep", { status: result?.status || "unknown", intervalMs: snoozeSweepIntervalMs, detail: result || {} }).catch(() => {});
      scheduleSnoozeSweep(snoozeSweepIntervalMs);
    }
  }, normalizedDelay);
  snoozeSweepTimer.unref?.();
}
