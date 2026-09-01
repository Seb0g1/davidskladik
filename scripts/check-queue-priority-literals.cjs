#!/usr/bin/env node
"use strict";

// Guards against the "recovery starves price" bug class (PLAN-HARDENING.md 1.3): a
// queueMarketplaceJob / queueAuthoritativePriceReprice / enqueueMarketplaceJobAccepted
// call site hardcoding a raw numeric `priority`, or assigning a known job name to the
// wrong priority tier (price pushes must be 1-2, recovery/unarchive jobs must be 3-4 so
// they can never run ahead of a price push).
//
// See lib/queue-priorities.js for the canonical QUEUE_PRIORITY enum and job->tier map.

const fs = require("fs");
const path = require("path");
const { QUEUE_JOB_PRIORITY_TIER } = require("../lib/queue-priorities");

const partsDir = path.join(__dirname, "..", "server", "parts");

const CONST_TIER = {
  PRICE_IMMEDIATE: "price",
  PRICE_BACKGROUND: "price",
  RECOVERY: "recovery",
  UNARCHIVE: "recovery",
  USER_ACTION: "user_action",
  DEFAULT: null,
};

const CALL_NAMES = ["queueMarketplaceJob", "enqueueMarketplaceJobAccepted", "queueAuthoritativePriceReprice"];

const violations = [];

for (const file of fs.readdirSync(partsDir)) {
  if (!file.endsWith(".js")) continue;
  const fullPath = path.join(partsDir, file);
  const content = fs.readFileSync(fullPath, "utf8");

  for (const callName of CALL_NAMES) {
    let searchFrom = 0;
    for (;;) {
      const idx = content.indexOf(`${callName}(`, searchFrom);
      if (idx === -1) break;
      searchFrom = idx + callName.length;

      // Skip the function declarations themselves (default-parameter definitions).
      const lineStart = content.lastIndexOf("\n", idx) + 1;
      const linePrefix = content.slice(lineStart, idx);
      if (/\bfunction\s*$/.test(linePrefix)) continue;

      // Extract the balanced-paren call body.
      const openParen = idx + callName.length;
      let depth = 0;
      let end = -1;
      for (let i = openParen; i < content.length; i++) {
        if (content[i] === "(") depth++;
        else if (content[i] === ")") {
          depth--;
          if (depth === 0) { end = i; break; }
        }
      }
      if (end === -1) continue;
      const body = content.slice(openParen, end);
      const line = content.slice(0, idx).split("\n").length;

      const priorityMatch = body.match(/priority\s*:\s*([^,\n}]+)/);
      if (!priorityMatch) continue;
      const value = priorityMatch[1].trim();

      if (/^\d+$/.test(value)) {
        violations.push({ file, line, callName, detail: `hardcoded numeric priority: ${value} (use a QUEUE_PRIORITY constant from lib/queue-priorities.js)` });
        continue;
      }

      const constMatch = value.match(/^QUEUE_PRIORITY\.(\w+)$/);
      if (!constMatch || !(constMatch[1] in CONST_TIER)) {
        violations.push({ file, line, callName, detail: `priority value "${value}" is not a recognized QUEUE_PRIORITY.* constant` });
        continue;
      }

      const nameMatch = body.match(/^\s*["']([\w-]+)["']/);
      const jobName = nameMatch ? nameMatch[1] : (callName === "queueAuthoritativePriceReprice" ? "auto-price-push" : null);
      const expectedTier = jobName ? QUEUE_JOB_PRIORITY_TIER[jobName] : null;
      const actualTier = CONST_TIER[constMatch[1]];
      if (expectedTier && actualTier !== expectedTier) {
        violations.push({
          file,
          line,
          callName,
          detail: `job "${jobName}" must use a ${expectedTier}-tier priority (got QUEUE_PRIORITY.${constMatch[1]})`,
        });
      }
    }
  }
}

if (violations.length) {
  console.error("Found marketplace queue calls with invalid priority:\n");
  for (const v of violations) {
    console.error(`  ${v.file}:${v.line}  ${v.callName}(...)  ${v.detail}`);
  }
  console.error(
    "\nPLAN-HARDENING.md 1.3 contract: price pushes (\"auto-price-push\") must use\n" +
    "QUEUE_PRIORITY.PRICE_IMMEDIATE / PRICE_BACKGROUND (1-2); recovery/unarchive jobs must\n" +
    "use QUEUE_PRIORITY.RECOVERY / UNARCHIVE (3-4) so they can never run ahead of a price\n" +
    "push. See lib/queue-priorities.js.",
  );
  process.exit(1);
}

console.log("OK: marketplace queue priority literals use QUEUE_PRIORITY constants with correct tiers.");
