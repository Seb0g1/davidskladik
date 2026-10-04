"use strict";

let prisma = null;

function hasDatabaseUrl() {
  return Boolean(String(process.env.DATABASE_URL || "").trim());
}

function postgresModeEnabled() {
  const mode = String(process.env.DB_MODE || "").trim().toLowerCase();
  return hasDatabaseUrl() && (mode === "postgres" || mode === "postgresql");
}

function jsonFallbackEnabled() {
  return String(process.env.JSON_FALLBACK_ENABLED || "true").trim().toLowerCase() !== "false";
}

// Prisma's default pool is cpus × 2 + 1 (17 here). The worker runs syncs, price sends and the card conveyor at
// once and ran out of connections («Timed out fetching a new connection»), so the pool is sized per role:
// worker 40, api 25 (Postgres max_connections is 100). PRISMA_CONNECTION_LIMIT / PRISMA_POOL_TIMEOUT override.
function pooledDatabaseUrl() {
  const raw = String(process.env.DATABASE_URL || "").trim();
  if (!raw || /[?&]connection_limit=/.test(raw)) return raw;
  const role = String(process.env.SERVER_ROLE || "").trim().toLowerCase();
  const limit = Number(process.env.PRISMA_CONNECTION_LIMIT) || (role === "worker" ? 40 : role === "api" ? 25 : 0);
  if (!limit) return raw;
  const timeout = Number(process.env.PRISMA_POOL_TIMEOUT) || 20;
  return `${raw}${raw.includes("?") ? "&" : "?"}connection_limit=${limit}&pool_timeout=${timeout}`;
}

function getPrisma() {
  if (!hasDatabaseUrl()) return null;
  if (!prisma) {
    // Loaded lazily so local JSON-only development and tests do not require DATABASE_URL.
    const { PrismaClient } = require("@prisma/client");
    const url = pooledDatabaseUrl();
    prisma = url === String(process.env.DATABASE_URL || "").trim() ? new PrismaClient() : new PrismaClient({ datasources: { db: { url } } });
  }
  return prisma;
}

async function closePrisma() {
  if (!prisma) return;
  await prisma.$disconnect();
  prisma = null;
}

module.exports = {
  hasDatabaseUrl,
  postgresModeEnabled,
  jsonFallbackEnabled,
  getPrisma,
  closePrisma,
};
