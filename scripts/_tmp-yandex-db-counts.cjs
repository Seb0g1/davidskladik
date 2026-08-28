#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { getPrisma } = require("../lib/postgres.js");
(async () => {
  const p = getPrisma();
  const [statuses, reasons, total, linked] = await Promise.all([
    p.salesAutomationSkuState.groupBy({ by: ["priceStatus"], where: { marketplace: "yandex" }, _count: { _all: true } }),
    p.salesAutomationSkuState.groupBy({ by: ["reason"], where: { marketplace: "yandex" }, _count: { _all: true } }),
    p.warehouseProduct.count({ where: { marketplace: "yandex" } }),
    p.warehouseProduct.count({ where: { marketplace: "yandex", links: { some: {} } } }),
  ]);
  console.log(JSON.stringify({ statuses, reasons, total, linked }));
  await p.$disconnect();
})().catch((e) => { console.error(e); process.exit(1); });
