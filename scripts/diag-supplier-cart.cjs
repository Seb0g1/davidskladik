#!/usr/bin/env node
"use strict";
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();

async function main() {
  const settingsRow = await prisma.appSetting.findFirst({ orderBy: { updatedAt: "desc" } });
  const settings = settingsRow?.value || settingsRow?.settings || {};

  // --- Supplier cart statuses ---
  const cart = settings.supplierCart || {};
  console.log("=== SUPPLIER CART SETTINGS ===");
  console.log("enabled:", cart.enabled);
  console.log("mode:", cart.mode);
  console.log("lookbackHours:", cart.lookbackHours);
  console.log("includeOzonStatuses:", JSON.stringify(cart.includeOzonStatuses));
  console.log("includeYandexStatuses:", JSON.stringify(cart.includeYandexStatuses));
  console.log("includeYandexSubstatuses:", JSON.stringify(cart.includeYandexSubstatuses));
  console.log("marketplaces:", JSON.stringify(cart.marketplaces));

  // --- Managed suppliers (find Инна) ---
  const suppliers = Array.isArray(settings.managedSuppliers) ? settings.managedSuppliers : [];
  console.log("\n=== MANAGED SUPPLIERS ===");
  console.log("Total:", suppliers.length);

  const inna = suppliers.filter(s => /инна|inna/i.test(String(s.name || s.supplierName || "")));
  if (inna.length) {
    console.log("\nИнна entries:");
    for (const s of inna) console.log(" ", JSON.stringify(s));
  } else {
    console.log("Инна — NOT FOUND in managedSuppliers by name. All suppliers:");
    for (const s of suppliers) {
      console.log(" ", s.name || s.supplierName || s.partnerId, "| stopped:", s.stopped, "| stockOnly:", s.stockOnly, "| cutoff:", s.orderCutoffTime, "| trustFactor:", s.trustFactor);
    }
  }

  // --- Supplier blocks in cart state ---
  const cartStateRow = await prisma.appState.findFirst({ where: { key: "supplier_cart" } });
  const cartState = cartStateRow?.value || {};
  const blocks = cartState.supplierBlocks || {};
  const blockList = Object.values(blocks);
  console.log("\n=== SUPPLIER BLOCKS (" + blockList.length + ") ===");
  const innaBlocks = blockList.filter(b => /инна|inna/i.test(String(b.supplierName || "")));
  if (innaBlocks.length) {
    console.log("Инна blocks:");
    for (const b of innaBlocks) console.log(" ", JSON.stringify(b));
  } else {
    console.log("Инна — not in blocks");
    if (blockList.length > 0) {
      console.log("Active blocks (first 10):");
      for (const b of blockList.slice(0, 10)) {
        console.log(" ", b.supplierName, b.offerId, "expires:", b.expiresAt, "reason:", b.reason);
      }
    }
  }

  // --- Processed (already committed) entries ---
  const processed = cartState.processed || {};
  const processedCount = Object.keys(processed).length;
  console.log("\n=== PROCESSED (alreadyCommitted) entries:", processedCount, "===");
  if (processedCount > 0) {
    const sample = Object.entries(processed).slice(0, 5);
    for (const [k, v] of sample) {
      console.log(" ", k, "->", JSON.stringify(v).slice(0, 100));
    }
  }
}

main().then(() => prisma.$disconnect()).catch(e => { console.error(e.message); process.exit(1); });
