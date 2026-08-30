// Yield to the event loop every N iterations to avoid blocking the health-check
// endpoint during the 267k-row PM snapshot compare (single timeout restart).
const SNAPSHOT_COMPARE_YIELD_EVERY = 20_000;

async function compareSnapshots(previousItems, currentOffers) {
  const currentItems = {};
  const changes = [];

  let i = 0;
  for (const offer of currentOffers) {
    if (!currentItems[offer.key]) {
      currentItems[offer.key] = offer;
    }
    if (++i % SNAPSHOT_COMPARE_YIELD_EVERY === 0) await new Promise((resolve) => setImmediate(resolve));
  }

  i = 0;
  for (const offer of Object.values(currentItems)) {
    const previous = previousItems[offer.key];

    if (!previous) {
      changes.push({ type: "new", current: offer });
    } else {
      if (Number(previous.price) !== Number(offer.price)) {
        changes.push({
          type: "price_changed",
          previous,
          current: offer,
          delta: Number(offer.price) - Number(previous.price),
        });
      }

      if (Boolean(previous.active) !== Boolean(offer.active)) {
        changes.push({
          type: offer.active ? "returned" : "inactive",
          previous,
          current: offer,
        });
      }
    }
    if (++i % SNAPSHOT_COMPARE_YIELD_EVERY === 0) await new Promise((resolve) => setImmediate(resolve));
  }

  i = 0;
  for (const [key, previous] of Object.entries(previousItems || {})) {
    if (!currentItems[key]) {
      changes.push({ type: "missing", previous });
    }
    if (++i % SNAPSHOT_COMPARE_YIELD_EVERY === 0) await new Promise((resolve) => setImmediate(resolve));
  }

  changes.sort((a, b) => {
    const rank = { missing: 0, price_changed: 1, new: 2, inactive: 3, returned: 4 };
    return (rank[a.type] ?? 9) - (rank[b.type] ?? 9);
  });

  return { currentItems, changes };
}


