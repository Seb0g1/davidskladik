"use strict";

// Fields that buildFreshWarehouseProductsForWarehouse() (server/parts/02a-price-master-fresh-products.js)
// computes at read time for a warehouse product. They are NEVER written to
// warehouse_products.raw, so a SQL predicate like `raw -> 'selectedSupplier'`
// silently matches zero rows instead of erroring — that's how the dead
// stock-sweep regression happened. Resolve these via a fresh build
// (buildFreshWarehouseProducts) instead of filtering for them in SQL.
const COMPUTED_WAREHOUSE_PRODUCT_FIELDS = [
  "selectedSupplier",
  "selectedSupplierReason",
  "suppliers",
  "fallbackSuppliers",
  "supplierAlternatives",
  "supplierCount",
  "availableSupplierCount",
  "stockOnlyAvailableSupplierCount",
  "stockOnlyFallbackActive",
  "stockOnlyManualPriceMissing",
  "availabilityRule",
  "priceFormula",
  "priceSource",
  "priceSelectionReason",
  "markupCoefficient",
  "usdRate",
  "ready",
  "changed",
  "nextPrice",
  "ozonMinPrice",
  "hasLinks",
  "autoArchiveCandidate",
];

module.exports = { COMPUTED_WAREHOUSE_PRODUCT_FIELDS };
