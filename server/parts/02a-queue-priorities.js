// Single source of truth for marketplace queue priorities — see
// lib/queue-priorities.js (also consumed by scripts/check-queue-priority-literals.cjs).
// Contract: price pushes ("auto-price-push") use PRICE_IMMEDIATE/PRICE_BACKGROUND (1-2);
// recovery/unarchive jobs use RECOVERY/UNARCHIVE (3-4) so they cannot starve price jobs.
const { QUEUE_PRIORITY } = require("./lib/queue-priorities");
