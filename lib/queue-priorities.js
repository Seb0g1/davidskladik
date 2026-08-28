"use strict";

// Single source of truth for marketplace-tasks (BullMQ) job priorities.
// BullMQ: LOWER numeric priority = processed FIRST.
//
// Contract (see PLAN-HARDENING.md 1.3 "recovery starves price"): price pushes
// (job "auto-price-push") must always use the PRICE tier (1-2) so that bulk
// recovery/unarchive jobs (RECOVERY/UNARCHIVE, 3-4) can never starve them —
// a price job enqueued after a backlog of recovery/unarchive jobs must still
// jump ahead of that backlog.
const QUEUE_PRIORITY = {
  PRICE_IMMEDIATE: 1,
  PRICE_BACKGROUND: 2,
  RECOVERY: 3,
  UNARCHIVE: 4,
  DEFAULT: 5,
};

// Expected tier per BullMQ job name (marketplace-tasks queue). Used by
// scripts/check-queue-priority-literals.cjs to catch jobs queued at the
// wrong tier (e.g. a recovery job hardcoded at priority 1).
const QUEUE_JOB_PRIORITY_TIER = {
  "auto-price-push": "price",
  "no-supplier-automation": "recovery",
  "supplier-recovery-automation": "recovery",
  "ozon-unarchive-queue-process": "recovery",
  "yandex-unarchive-queue-process": "recovery",
};

const QUEUE_PRIORITY_TIERS = {
  price: new Set([QUEUE_PRIORITY.PRICE_IMMEDIATE, QUEUE_PRIORITY.PRICE_BACKGROUND]),
  recovery: new Set([QUEUE_PRIORITY.RECOVERY, QUEUE_PRIORITY.UNARCHIVE]),
};

module.exports = { QUEUE_PRIORITY, QUEUE_JOB_PRIORITY_TIER, QUEUE_PRIORITY_TIERS };
