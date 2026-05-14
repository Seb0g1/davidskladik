#!/usr/bin/env node

const {
  readWarehouse,
  writeWarehouse,
  materializeYandexExportedProductsForWarehouse,
} = require("../server.js");

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const warehouse = await readWarehouse();
  const before = Array.isArray(warehouse.products) ? warehouse.products.length : 0;
  const result = materializeYandexExportedProductsForWarehouse(warehouse);
  const after = Array.isArray(result.warehouse.products) ? result.warehouse.products.length : before;

  if (!dryRun && result.added > 0) {
    await writeWarehouse(result.warehouse);
  }

  console.log(JSON.stringify({
    ok: true,
    dryRun,
    before,
    after,
    added: result.added,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
