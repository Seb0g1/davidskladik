// Single source of truth for "effective runtime config" — ecosystem.config.cjs's
// per-app env entries are the key tunables (concurrency, sweep intervals, limits) that
// silently win over .env on PM2 (dotenv does not override an already-set process.env
// value). See lib/runtime-config-keys.js (also consumed by
// scripts/check-env-ecosystem-divergence.cjs, PLAN-HARDENING.md 2.2).
const { effectiveRuntimeConfigSnapshot } = require("./lib/runtime-config-keys");

// PLAN-HARDENING.md 2.2: log the env values that actually take effect for this
// process at startup, so a divergence between .env and ecosystem.config.cjs is visible
// immediately instead of surfacing later as unexplained behavior.
function logEffectiveRuntimeConfig() {
  logger.info("effective runtime config", { serverRole, ...effectiveRuntimeConfigSnapshot() });
}
