#!/usr/bin/env node
"use strict";

// One-time fix: clear the manualMarkup override for HEDP50MEG Yandex product.
// The restore-yandex-markups operation stored markup=1.3851 + manualMarkup:true
// based on Инна's 850 RUB price being wrongly treated as $850 USD, yielding ~111k ₽
// as the "historical" price and back-computing an absurd coefficient.
// With the code fix deployed (Инна always RUB), normal markup rules now compute the
// correct price — but the stored manualMarkup overrides them. Setting markup=0 makes
// marketplaceProductMarkupOverride return 0 (before checking manualMarkup) so rules apply.

process.env.DISABLE_BACKGROUND_JOBS = "true";

require("dotenv").config();
const { getPrisma } = require("../lib/postgres.js");

const PRODUCT_ID = "yandex-cc222851ea48baa4abbafc8c";

async function main() {
  const prisma = getPrisma();
  if (!prisma) throw new Error("Postgres not configured");

  const before = await prisma.warehouseProduct.findUnique({ where: { id: PRODUCT_ID } });
  if (!before) throw new Error(`Product ${PRODUCT_ID} not found`);

  const raw = typeof before.raw === "object" && before.raw ? { ...before.raw } : {};
  const beforeMarkup = raw.markup;
  const beforeManualMarkup = raw.yandex?.extra?.manualMarkup;

  console.log("Before:", JSON.stringify({ markup: beforeMarkup, manualMarkup: beforeManualMarkup }));

  // Clear markup (set to 0) — marketplaceProductMarkupOverride returns 0 for markup<=0
  raw.markup = 0;
  // Also explicitly clear manualMarkup flag so future restore operations don't re-apply
  if (raw.yandex?.extra) {
    raw.yandex = { ...raw.yandex, extra: { ...raw.yandex.extra, manualMarkup: false } };
  }

  await prisma.warehouseProduct.update({
    where: { id: PRODUCT_ID },
    data: { raw },
  });

  const after = await prisma.warehouseProduct.findUnique({ where: { id: PRODUCT_ID } });
  const afterRaw = typeof after.raw === "object" && after.raw ? after.raw : {};
  console.log("After:", JSON.stringify({ markup: afterRaw.markup, manualMarkup: afterRaw.yandex?.extra?.manualMarkup }));
  console.log("Done — HEDP50MEG Yandex markup cleared. Reprice Yandex to apply.");
}

main().catch((e) => { console.error(e.message); process.exitCode = 1; });
