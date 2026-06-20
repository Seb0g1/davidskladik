// Snooze sweep: check products with expired snooze and reactivate those whose
// supplier is back in the price list. Runs every SNOOZE_SWEEP_INTERVAL_SECONDS (default 30 min).

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
    // Find products whose snooze has expired
    const rows = await prisma.$queryRawUnsafe(`
      SELECT id
      FROM warehouse_products
      WHERE raw->'snooze' IS NOT NULL
        AND (raw->'snooze'->>'snoozedUntil')::timestamptz <= now()
      LIMIT 200
    `);
    if (!rows.length) return { status: "ok", expired: 0, reactivated: 0 };

    const expiredIds = rows.map((row) => String(row.id));
    const builtProducts = await buildFreshWarehouseProducts(expiredIds, { livePriceMaster: false }).catch((error) => {
      logger.warn("snooze sweep build failed", { detail: error?.message || String(error) });
      return [];
    });

    const now = new Date().toISOString();
    const toReactivate = [];
    const toClear = [];

    for (const product of builtProducts) {
      // Clear snooze regardless of supplier availability
      product.snooze = null;
      product.updatedAt = now;

      if (product.selectedSupplier && product.hasLinks) {
        // Supplier is back — restore stock
        product.targetStock = Math.max(linkedDefaultSnoozedTargetStock, Number(product.targetStock || 0) || 0);
        toReactivate.push(product);
      } else {
        toClear.push(product);
      }
    }

    const allChanged = [...toReactivate, ...toClear];
    if (allChanged.length) {
      await writeWarehouseProductPatch(allChanged, { reason: "snooze_expired", writeLinks: false });
    }

    if (toReactivate.length) {
      const productIds = toReactivate.map((product) => product.id).filter(Boolean);
      queueMarketplaceJob("linked-supplier-recovery", { productIds, source: "snooze_sweep" }, { priority: QUEUE_PRIORITY.RECOVERY })
        .catch((error) => logger.warn("snooze sweep recovery queue failed", { count: productIds.length, detail: error?.message || String(error) }));
    }

    logger.info("snooze_sweep_complete", {
      source,
      expired: expiredIds.length,
      reactivated: toReactivate.length,
      cleared: toClear.length,
    });
    return { status: "ok", expired: expiredIds.length, reactivated: toReactivate.length, cleared: toClear.length };
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
