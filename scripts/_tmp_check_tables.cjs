#!/usr/bin/env node
"use strict";
const { PrismaClient } = require("@prisma/client");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const p = new PrismaClient();
p.$queryRawUnsafe("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
  .then((r) => { console.log(r.map((x) => x.tablename).join("\n")); return p.$disconnect(); })
  .catch((e) => { console.error(e.message); process.exit(1); });
