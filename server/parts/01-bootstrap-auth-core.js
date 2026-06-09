function base64Url(input) {
  return Buffer.from(input).toString("base64url");
}

function sign(value) {
  return crypto
    .createHmac("sha256", process.env.APP_SESSION_SECRET || "dev-secret")
    .update(value)
    .digest("base64url");
}

function timingSafeEqual(a, b) {
  const first = Buffer.from(String(a));
  const second = Buffer.from(String(b));
  if (first.length !== second.length) return false;
  return crypto.timingSafeEqual(first, second);
}

function parseCookies(header = "") {
  return Object.fromEntries(
    header
      .split(";")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((part) => {
        const index = part.indexOf("=");
        return [decodeURIComponent(part.slice(0, index)), decodeURIComponent(part.slice(index + 1))];
      }),
  );
}

function promiseTimeout(ms, message = "operation_timeout") {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), Math.max(1, Number(ms) || 1));
  });
}

function pageTrace(label, startedAt) {
  if (process.env.WAREHOUSE_PAGE_TRACE !== "true") return;
  logger.info("warehouse page trace", { label, elapsedMs: Date.now() - startedAt });
}

function shouldUsePostgresStorage() {
  return postgresModeEnabled();
}

async function runWithPostgresFallback(label, postgresAction, fallbackAction) {
  if (!shouldUsePostgresStorage()) return fallbackAction();
  try {
    const prisma = getPrisma();
    if (!prisma) return fallbackAction();
    return await postgresAction(prisma);
  } catch (error) {
    if (!jsonFallbackEnabled()) throw error;
    logger.warn(`${label} postgres failed, using JSON fallback`, { detail: error?.message || String(error) });
    return fallbackAction();
  }
}

function toDateOrNull(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function configuredUsers() {
  const deletedUsernames = readDeletedAppUsernamesSync();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...readStoredAppUsersSync());
  return dedupeAppUsers(users).filter((user) => !user.disabled && !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

async function configuredUsersAsync() {
  const deletedUsernames = await readDeletedAppUsernames();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...await readStoredAppUsers());
  return dedupeAppUsers(users).filter((user) => !user.disabled && !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

async function configuredUsersForAdminAsync() {
  const deletedUsernames = await readDeletedAppUsernames();
  const users = [];
  const primary = normalizeAppUser({
    username: process.env.APP_USER || "admin",
    password: process.env.APP_PASSWORD || "",
    role: process.env.APP_ROLE || "admin",
  }, { source: "env", protectedUser: true, defaultRole: "admin" });
  if (primary.username && primary.password) users.push(primary);

  users.push(...readEnvJsonUsers());
  users.push(...await readStoredAppUsers({ includeDisabled: true }));
  return dedupeAppUsers(users).filter((user) => !deletedUsernames.has(cleanText(user.username).toLowerCase()));
}

function normalizeAppRole(value, fallback = "manager") {
  return cleanText(value).toLowerCase() === "admin" ? "admin" : fallback;
}

const passwordHashPrefix = "scrypt";

function isPasswordHash(value) {
  return cleanText(value).startsWith(`${passwordHashPrefix}$`);
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString("base64url");
  const hash = crypto.scryptSync(String(password || ""), salt, 64).toString("base64url");
  return `${passwordHashPrefix}$${salt}$${hash}`;
}

function verifyStoredPassword(password, storedPassword) {
  const stored = cleanText(storedPassword);
  if (!isPasswordHash(stored)) return timingSafeEqual(password, stored);
  const [, salt, expectedHash] = stored.split("$");
  if (!salt || !expectedHash) return false;
  const actual = crypto.scryptSync(String(password || ""), salt, 64);
  const expected = Buffer.from(expectedHash, "base64url");
  if (actual.length !== expected.length) return false;
  return crypto.timingSafeEqual(actual, expected);
}

function passwordForStorage(password) {
  const value = cleanText(password);
  if (!value || isPasswordHash(value)) return value;
  return hashPassword(value);
}

function normalizeAppUser(input = {}, { source = "local", protectedUser = false, defaultRole = "manager" } = {}) {
  const username = cleanText(input.username || input.user || input.login);
  const role = normalizeAppRole(input.role, defaultRole);
  return {
    username,
    password: cleanText(input.password),
    role,
    source: input.source || source,
    protected: Boolean(input.protected ?? protectedUser),
    disabled: Boolean(input.disabled),
    createdAt: input.createdAt || new Date().toISOString(),
    updatedAt: input.updatedAt || new Date().toISOString(),
  };
}

function readEnvJsonUsers() {
  const rawUsers = cleanText(process.env.APP_USERS_JSON || "");
  if (!rawUsers) return [];
  try {
    const parsed = JSON.parse(rawUsers);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => normalizeAppUser(item, { source: "env-json", protectedUser: true, defaultRole: "manager" }))
      .filter((item) => item.username && item.password);
  } catch (error) {
    logger.warn("APP_USERS_JSON parse failed", { detail: error?.message || String(error) });
    return [];
  }
}
