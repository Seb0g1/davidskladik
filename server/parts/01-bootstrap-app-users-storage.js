function readStoredAppUsersSync() {
  try {
    const parsed = JSON.parse(fsSync.readFileSync(appUsersPath, "utf8"));
    const users = Array.isArray(parsed.users) ? parsed.users : [];
    return users
      .map((item) => normalizeAppUser(item, { source: "local", defaultRole: "manager" }))
      .filter((item) => item.username && item.password);
  } catch (_error) {
    return [];
  }
}

function normalizeDeletedAppUser(input = {}) {
  const username = cleanText(input.username || input.user || input.login);
  if (!username) return null;
  return {
    username,
    deletedAt: input.deletedAt || new Date().toISOString(),
    deletedBy: cleanText(input.deletedBy || ""),
    reason: cleanText(input.reason || "hard_delete"),
  };
}

function readDeletedAppUsersSync() {
  try {
    const parsed = JSON.parse(fsSync.readFileSync(appDeletedUsersPath, "utf8"));
    const users = Array.isArray(parsed.users) ? parsed.users : [];
    return users.map(normalizeDeletedAppUser).filter(Boolean);
  } catch (_error) {
    return [];
  }
}

function readDeletedAppUsernamesSync() {
  return new Set(readDeletedAppUsersSync().map((item) => cleanText(item.username).toLowerCase()).filter(Boolean));
}

async function readDeletedAppUsers() {
  return readDeletedAppUsersSync();
}

async function readDeletedAppUsernames() {
  return new Set((await readDeletedAppUsers()).map((item) => cleanText(item.username).toLowerCase()).filter(Boolean));
}

async function writeDeletedAppUsers(users = []) {
  const byUser = new Map();
  for (const user of users.map(normalizeDeletedAppUser).filter(Boolean)) {
    byUser.set(cleanText(user.username).toLowerCase(), user);
  }
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    users: Array.from(byUser.values()).sort((a, b) => a.username.localeCompare(b.username)),
  };
  const temporaryPath = `${appDeletedUsersPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(temporaryPath, appDeletedUsersPath);
  return payload.users;
}

function appUserFromPostgres(row = {}) {
  return normalizeAppUser({
    username: row.username,
    password: row.passwordHash,
    role: row.role,
    source: row.source || "postgres",
    protected: row.protected,
    disabled: row.active === false,
    allowedPages: row.allowedPages,
    createdAt: row.createdAt ? row.createdAt.toISOString() : null,
    updatedAt: row.updatedAt ? row.updatedAt.toISOString() : null,
  }, { source: row.source || "postgres", defaultRole: "manager" });
}

async function readStoredAppUsersFromPostgres(prisma, { includeDisabled = false } = {}) {
  const rows = await prisma.appUser.findMany({
    where: {
      protected: false,
      ...(includeDisabled ? {} : { active: true }),
    },
    orderBy: [
      { role: "asc" },
      { username: "asc" },
    ],
  });
  return rows.map(appUserFromPostgres).filter((item) => item.username && item.password);
}

async function readStoredAppUsers({ includeDisabled = false } = {}) {
  return runWithPostgresFallback(
    "read app users",
    (prisma) => readStoredAppUsersFromPostgres(prisma, { includeDisabled }),
    async () => readStoredAppUsersSync(),
  );
}

function dedupeAppUsers(users = []) {
  const result = new Map();
  for (const user of users) {
    if (!user?.username) continue;
    const key = user.username.toLowerCase();
    if (result.has(key) && result.get(key).protected) continue;
    result.set(key, user);
  }
  return Array.from(result.values());
}

function publicAppUser(user = {}) {
  return {
    username: user.username,
    role: user.role || "manager",
    source: user.source || "local",
    protected: Boolean(user.protected),
    disabled: Boolean(user.disabled),
    allowedPages: normalizeAllowedPages(user.allowedPages),
    effectivePages: effectiveAllowedPages(user),
    createdAt: user.createdAt || null,
    updatedAt: user.updatedAt || null,
  };
}

async function writeStoredAppUsers(users = []) {
  invalidateAllowedPagesCache();
  const normalized = dedupeAppUsers(users.map((item) => normalizeAppUser(item, { source: "local", defaultRole: "manager" })))
    .filter((item) => item.username && item.password)
    .map((item) => ({ ...item, source: "local", protected: false }));
  if (shouldUsePostgresStorage()) {
    try {
      const prisma = getPrisma();
      const desiredUsernames = normalized.map((item) => item.username);
      await prisma.$transaction(async (tx) => {
        await tx.appUser.updateMany({
          where: {
            protected: false,
            ...(desiredUsernames.length ? { username: { notIn: desiredUsernames } } : {}),
          },
          data: { active: false },
        });
        for (const user of normalized) {
          await tx.appUser.upsert({
            where: { username: user.username },
            create: {
              username: user.username,
              passwordHash: passwordForStorage(user.password),
              role: user.role === "admin" ? "admin" : "manager",
              active: !user.disabled,
              source: "postgres",
              protected: false,
              allowedPages: normalizeAllowedPages(user.allowedPages),
              createdAt: toDateOrNull(user.createdAt) || new Date(),
              updatedAt: toDateOrNull(user.updatedAt) || new Date(),
            },
            update: {
              passwordHash: passwordForStorage(user.password),
              role: user.role === "admin" ? "admin" : "manager",
              active: !user.disabled,
              source: "postgres",
              protected: false,
              allowedPages: normalizeAllowedPages(user.allowedPages),
            },
          });
        }
      });
      if (!jsonFallbackEnabled()) return normalized;
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("write app users postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  await fs.mkdir(dataDir, { recursive: true });
  const payload = {
    updatedAt: new Date().toISOString(),
    users: normalized.map((user) => ({ ...user, password: passwordForStorage(user.password) })),
  };
  const temporaryPath = `${appUsersPath}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(temporaryPath, JSON.stringify(payload, null, 2), "utf8");
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await fs.rename(temporaryPath, appUsersPath);
      break;
    } catch (error) {
      if (attempt === 4 || !["EPERM", "EBUSY", "EACCES"].includes(error.code)) throw error;
      await new Promise((resolve) => setTimeout(resolve, 80 * (attempt + 1)));
    }
  }
  return normalized;
}


