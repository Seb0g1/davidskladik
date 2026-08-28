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

function getPrisma() {
  if (!hasDatabaseUrl()) return null;
  if (!prisma) {
    // Loaded lazily so local JSON-only development and tests do not require DATABASE_URL.
    const { PrismaClient } = require("@prisma/client");
    prisma = new PrismaClient();
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
