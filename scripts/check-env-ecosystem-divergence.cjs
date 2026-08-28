#!/usr/bin/env node
"use strict";

// PLAN-HARDENING.md 2.2: ecosystem.config.cjs sets env vars on the PM2 process before
// dotenv loads .env, and dotenv does not override an already-set process.env value —
// so a value changed in .env for one of these keys is silently ignored on that
// process. Warns (non-fatal at the shell level via --warn-only) about keys defined in
// both .env and ecosystem.config.cjs with different values.
//
// Run before deploying: node scripts/check-env-ecosystem-divergence.cjs [--warn-only]

const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");
const { findEnvEcosystemDivergences, runtimeConfigKeys, ecosystemEnvByApp } = require("../lib/runtime-config-keys");

const envPath = path.join(__dirname, "..", ".env");

let envFileVars = {};
if (fs.existsSync(envPath)) {
  envFileVars = dotenv.parse(fs.readFileSync(envPath, "utf8"));
} else {
  console.log("No .env file found — nothing to compare against ecosystem.config.cjs.");
}

const divergences = findEnvEcosystemDivergences(envFileVars, runtimeConfigKeys(), ecosystemEnvByApp());

if (divergences.length) {
  console.warn("WARN: .env values differ from ecosystem.config.cjs (PM2 wins silently for these keys):\n");
  for (const d of divergences) {
    console.warn(`  ${d.key}: .env="${d.envValue}" vs ${d.app}.env="${d.ecosystemValue}"`);
  }
  console.warn(
    "\nPLAN-HARDENING.md 2.2: ecosystem.config.cjs sets these on the PM2 process before\n" +
    "dotenv runs; dotenv does not override an already-set value, so the .env value above\n" +
    "is ignored for that process. Update ecosystem.config.cjs (not .env) if you intended\n" +
    "to change runtime behavior, or remove the stale key from .env.",
  );
  if (!process.argv.includes("--warn-only")) process.exit(1);
  process.exit(0);
}

console.log("OK: no .env / ecosystem.config.cjs divergence for tracked runtime config keys.");
