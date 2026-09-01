// Periodic sync of Ozon product ratings into WarehouseProduct.marketplaceState.
// Uses POST /v1/review/statistic/list (Ozon Premium API).
// Result stored as { ...existingState, ozonRating: float, ozonReviewCount: int }.
// Runs every 6 hours on the worker process only.
// No-op if Ozon account not configured or premium review API is unavailable.

const RATINGS_SYNC_INTERVAL_MS = 6 * 60 * 60 * 1000;
const RATINGS_BATCH_SIZE = 100;

async function syncOzonProductRatings() {
  if (process.env.BACKGROUND_JOBS_ENABLED !== "true") return;
  const prisma = getPrisma();
  if (!prisma) return;

  const accounts = getOzonAccounts().filter((a) => a.clientId && a.apiKey);
  if (!accounts.length) return;

  let totalUpdated = 0;

  for (const account of accounts) {
    try {
      // Fetch all Ozon products with a numeric productId
      const products = await prisma.warehouseProduct.findMany({
        where: {
          marketplace: "ozon",
          target: account.id || undefined,
          archived: false,
          NOT: { status: "deleted" },
          productId: { not: null },
        },
        select: { id: true, offerId: true, productId: true, marketplaceState: true },
      });

      if (!products.length) continue;

      // Batch by RATINGS_BATCH_SIZE
      for (let i = 0; i < products.length; i += RATINGS_BATCH_SIZE) {
        const batch = products.slice(i, i + RATINGS_BATCH_SIZE);
        const productIds = batch
          .map((p) => Number(p.productId))
          .filter((n) => Number.isFinite(n) && n > 0);

        if (!productIds.length) continue;

        let stats;
        try {
          const data = await ozonRequest("/v1/review/statistic/list", { product_ids: productIds }, account);
          stats = Array.isArray(data?.result) ? data.result
            : Array.isArray(data?.stats) ? data.stats : [];
        } catch (err) {
          // Premium API might not be available — log and stop for this account
          logger.warn("ozon rating sync: API unavailable", { account: account.id, detail: err?.message });
          break;
        }

        // Build a map productId → { avg_rating, total_count }
        const statMap = new Map();
        for (const s of stats) {
          const pid = Number(s.product_id || s.productId);
          if (pid) statMap.set(pid, { avg: Number(s.avg_rating || s.avgRating || 0), count: Number(s.total_count || s.totalCount || 0) });
        }

        // Update marketplaceState for each product in the batch
        for (const p of batch) {
          const pid = Number(p.productId);
          const stat = statMap.get(pid);
          if (!stat || !stat.avg) continue;

          const existing = p.marketplaceState && typeof p.marketplaceState === "object" ? p.marketplaceState : {};
          await prisma.warehouseProduct.update({
            where: { id: p.id },
            data: {
              marketplaceState: {
                ...existing,
                ozonRating: Math.round(stat.avg * 10) / 10,
                ozonReviewCount: stat.count,
                ozonRatingSyncedAt: new Date().toISOString(),
              },
            },
          });
          totalUpdated++;
        }

        // Pace requests
        await new Promise((r) => setTimeout(r, 500));
      }
    } catch (err) {
      logger.warn("ozon rating sync: error for account", { account: account.id, detail: err?.message });
    }
  }

  if (totalUpdated > 0) {
    logger.info("ozon rating sync: updated", { count: totalUpdated });
  }
}

// Run once at startup (delayed by 5 min to not spike load), then every 6h
if (process.env.BACKGROUND_JOBS_ENABLED === "true") {
  setTimeout(() => {
    syncOzonProductRatings().catch((err) => logger.warn("ozon rating sync startup error", { detail: err?.message }));
    setInterval(() => {
      syncOzonProductRatings().catch((err) => logger.warn("ozon rating sync interval error", { detail: err?.message }));
    }, RATINGS_SYNC_INTERVAL_MS);
  }, 5 * 60 * 1000);
}
