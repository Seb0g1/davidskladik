#!/usr/bin/env node
"use strict";
require("dotenv").config();
const path = require("path");
process.chdir(path.resolve(__dirname, ".."));
const { getPrisma } = require("../lib/postgres.js");
const p = getPrisma();
p.$queryRawUnsafe(
  "SELECT id, offer_id, marketplace, current_price, target_price, raw->>'lastYandexPriceSend' AS last_send FROM warehouse_products WHERE offer_id ILIKE '20185' AND marketplace='yandex'"
).then((rows) => {
  rows.forEach((r) => console.log(JSON.stringify(r)));
  return p.$disconnect();
}).catch((e) => { console.error(e.message); process.exit(1); });
