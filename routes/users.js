function registerUsersRoutes(app, deps) {
  const {
    requireAdmin,
    cleanText,
    requestUsername,
    configuredUsersForAdminAsync,
    publicAppUser,
    normalizeAppUser,
    normalizeAppRole,
    readStoredAppUsers,
    writeStoredAppUsers,
    appendAudit,
    getPrisma,
    shouldUsePostgresStorage,
    jsonFallbackEnabled,
    readAuditFiltered,
    readWarehouse,
    logger,
  } = deps;

const linkAuditActions = new Set([
  "warehouse.link.save",
  "warehouse.links.bulk_save",
  "warehouse.link.delete",
  "warehouse.links.bulk_delete",
]);

function usersStatsPeriod(input) {
  const value = cleanText(input || "30d").toLowerCase();
  const now = new Date();
  if (value === "all") return { key: "all", periodDays: null, from: null, to: now };
  const days = value === "7d" ? 7 : (value === "90d" ? 90 : 30);
  return {
    key: `${days}d`,
    periodDays: days,
    from: new Date(now.getTime() - days * 24 * 60 * 60 * 1000),
    to: now,
  };
}

function auditEntryDate(entry = {}) {
  const raw = entry.createdAt || entry.at || entry.time;
  const date = raw ? new Date(raw) : null;
  return date && !Number.isNaN(date.getTime()) ? date : null;
}

function auditEntryDetails(entry = {}) {
  return entry.details && typeof entry.details === "object" && !Array.isArray(entry.details)
    ? entry.details
    : {};
}

function auditEntryProductIdsForStats(entry = {}) {
  const details = auditEntryDetails(entry);
  const raw = details.productIds || details.productId || entry.productId || entry.entityId || [];
  return (Array.isArray(raw) ? raw : [raw])
    .flatMap((value) => cleanText(value).split(","))
    .map((value) => cleanText(value))
    .filter(Boolean);
}

function auditEntryOfferIdsForStats(entry = {}) {
  const details = auditEntryDetails(entry);
  const raw = details.offerIds || details.offerId || [];
  return (Array.isArray(raw) ? raw : [raw])
    .flatMap((value) => cleanText(value).split(","))
    .map((value) => cleanText(value))
    .filter(Boolean);
}

function linkIdentityForStats(link = {}) {
  return [
    cleanText(link.id),
    cleanText(link.article || link.supplierArticle).toLowerCase(),
    cleanText(link.sourceRowId || link.rowId),
    cleanText(link.exactName).toLowerCase(),
    cleanText(link.partnerId).toLowerCase(),
    cleanText(link.supplierName).toLowerCase(),
    cleanText(link.keyword).toLowerCase(),
    cleanText(link.priceCurrency || "USD").toUpperCase(),
  ].join("|");
}

function linkDeltaForStats(entry = {}, action = "") {
  const details = auditEntryDetails(entry);
  const productCount = Math.max(1, auditEntryProductIdsForStats(entry).length);
  const detailLinks = Array.isArray(details.links) ? details.links.length : 0;
  const oldLinks = Array.isArray(details.oldValue?.links)
    ? details.oldValue.links
    : (Array.isArray(entry.oldValue?.links) ? entry.oldValue.links : []);
  const newLinks = Array.isArray(details.newValue?.links)
    ? details.newValue.links
    : (Array.isArray(entry.newValue?.links) ? entry.newValue.links : []);

  if (action === "warehouse.link.delete") return { added: 0, updated: 0, deleted: 1 };
  if (action === "warehouse.links.bulk_delete") {
    const oldCount = Array.isArray(details.oldValue)
      ? details.oldValue.reduce((sum, item) => sum + (Array.isArray(item?.links) ? item.links.length : 0), 0)
      : oldLinks.length;
    const newCount = Array.isArray(details.newValue)
      ? details.newValue.reduce((sum, item) => sum + (Array.isArray(item?.links) ? item.links.length : 0), 0)
      : newLinks.length;
    return { added: 0, updated: 0, deleted: Math.max(1, oldCount - newCount) };
  }

  const oldKeys = new Set(oldLinks.map(linkIdentityForStats));
  const newKeys = new Set(newLinks.map(linkIdentityForStats));
  let added = 0;
  let updated = 0;
  for (const key of newKeys) {
    if (oldKeys.has(key)) updated += 1;
    else added += 1;
  }
  if (!added && !updated) added = Math.max(1, detailLinks) * productCount;
  return { added, updated, deleted: 0 };
}

function makeUserStats(username, user = {}) {
  return {
    username,
    role: user.role || "",
    active: user.active !== false && user.disabled !== true,
    source: user.source || "",
    currentLinksCreated: 0,
    currentLinksUpdated: 0,
    actionsTotal: 0,
    linksAdded: 0,
    linksUpdated: 0,
    linksDeleted: 0,
    affectedProductsSet: new Set(),
    affectedOfferIdsSet: new Set(),
    lastActionAt: null,
  };
}

function publicUserStats(stats) {
  return {
    username: stats.username,
    role: stats.role,
    active: stats.active,
    source: stats.source,
    currentLinksCreated: stats.currentLinksCreated,
    currentLinksUpdated: stats.currentLinksUpdated,
    actionsTotal: stats.actionsTotal,
    linksAdded: stats.linksAdded,
    linksUpdated: stats.linksUpdated,
    linksDeleted: stats.linksDeleted,
    affectedProducts: stats.affectedProductsSet.size,
    affectedOfferIds: stats.affectedOfferIdsSet.size,
    lastActionAt: stats.lastActionAt,
  };
}

function getStatsBucket(statsByUser, usernameInput, user = {}) {
  const username = cleanText(usernameInput) || "system";
  const key = username.toLowerCase();
  if (!statsByUser.has(key)) statsByUser.set(key, makeUserStats(username, user));
  const stats = statsByUser.get(key);
  if (user.role && !stats.role) stats.role = user.role;
  if (user.source && !stats.source) stats.source = user.source;
  if (user.active !== undefined || user.disabled !== undefined) stats.active = user.active !== false && user.disabled !== true;
  return stats;
}

async function readCurrentLinkStatsFromPostgres(statsByUser) {
  const rows = await getPrisma().productLink.findMany({
    select: {
      raw: true,
      createdAt: true,
      updatedAt: true,
      createdBy: { select: { username: true, role: true, active: true, source: true } },
      updatedBy: { select: { username: true, role: true, active: true, source: true } },
    },
  });
  for (const row of rows) {
    const raw = row.raw && typeof row.raw === "object" && !Array.isArray(row.raw) ? row.raw : {};
    const createdBy = row.createdBy?.username || raw.createdBy;
    const updatedBy = row.updatedBy?.username || raw.updatedBy || createdBy;
    if (createdBy) getStatsBucket(statsByUser, createdBy, row.createdBy || {}).currentLinksCreated += 1;
    if (updatedBy) getStatsBucket(statsByUser, updatedBy, row.updatedBy || {}).currentLinksUpdated += 1;
  }
}

async function readCurrentLinkStatsFromWarehouse(statsByUser) {
  const warehouse = await readWarehouse();
  for (const product of Array.isArray(warehouse.products) ? warehouse.products : []) {
    for (const link of Array.isArray(product.links) ? product.links : []) {
      if (link.createdBy) getStatsBucket(statsByUser, link.createdBy).currentLinksCreated += 1;
      if (link.updatedBy || link.createdBy) getStatsBucket(statsByUser, link.updatedBy || link.createdBy).currentLinksUpdated += 1;
    }
  }
}

function applyAuditStats(statsByUser, entries = []) {
  for (const entry of entries) {
    const action = cleanText(entry.action);
    if (!linkAuditActions.has(action)) continue;
    const stats = getStatsBucket(statsByUser, entry.username || entry.user || "system");
    const at = auditEntryDate(entry);
    const delta = linkDeltaForStats(entry, action);
    stats.actionsTotal += 1;
    stats.linksAdded += delta.added;
    stats.linksUpdated += delta.updated;
    stats.linksDeleted += delta.deleted;
    for (const id of auditEntryProductIdsForStats(entry)) stats.affectedProductsSet.add(id);
    for (const offerId of auditEntryOfferIdsForStats(entry)) stats.affectedOfferIdsSet.add(offerId);
    if (at && (!stats.lastActionAt || at.toISOString() > stats.lastActionAt)) stats.lastActionAt = at.toISOString();
  }
}

async function readLinkAuditEntriesForStats(period) {
  if (shouldUsePostgresStorage()) {
    try {
      const rows = await getPrisma().auditLog.findMany({
        where: {
          action: { in: Array.from(linkAuditActions) },
          ...(period.from ? { createdAt: { gte: period.from, lte: period.to } } : {}),
        },
        orderBy: { createdAt: "desc" },
        take: 10000,
      });
      return rows.map((row) => ({
        createdAt: row.createdAt ? row.createdAt.toISOString() : null,
        username: row.username,
        action: row.action,
        entityId: row.entityId,
        oldValue: row.oldValue,
        newValue: row.newValue,
        details: row.details || {},
      }));
    } catch (error) {
      if (!jsonFallbackEnabled()) throw error;
      logger.warn("user stats audit postgres failed, using JSON fallback", { detail: error?.message || String(error) });
    }
  }
  const entries = await readAuditFiltered({
    dateFrom: period.from ? period.from.toISOString() : "",
    dateTo: period.to ? period.to.toISOString() : "",
  }, 10000);
  return entries.filter((entry) => linkAuditActions.has(cleanText(entry.action)));
}

app.get("/api/users", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/users/stats", requireAdmin, async (request, response, next) => {
  try {
    const period = usersStatsPeriod(request.query.period);
    const statsByUser = new Map();
    const users = (await configuredUsersForAdminAsync()).map(publicAppUser);
    for (const user of users) getStatsBucket(statsByUser, user.username, user);

    if (shouldUsePostgresStorage()) {
      try {
        await readCurrentLinkStatsFromPostgres(statsByUser);
      } catch (error) {
        if (!jsonFallbackEnabled()) throw error;
        logger.warn("user stats links postgres failed, using JSON fallback", { detail: error?.message || String(error) });
        await readCurrentLinkStatsFromWarehouse(statsByUser);
      }
    } else {
      await readCurrentLinkStatsFromWarehouse(statsByUser);
    }

    applyAuditStats(statsByUser, await readLinkAuditEntriesForStats(period));
    const stats = Array.from(statsByUser.values())
      .map(publicUserStats)
      .sort((a, b) => {
        const totalB = b.currentLinksCreated + b.currentLinksUpdated + b.actionsTotal;
        const totalA = a.currentLinksCreated + a.currentLinksUpdated + a.actionsTotal;
        if (totalB !== totalA) return totalB - totalA;
        return a.username.localeCompare(b.username);
      });

    response.json({
      ok: true,
      period: period.key,
      periodDays: period.periodDays,
      from: period.from ? period.from.toISOString() : null,
      to: period.to.toISOString(),
      users: stats,
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/users", requireAdmin, async (request, response, next) => {
  try {
    const user = normalizeAppUser(request.body || {}, { source: "local", defaultRole: "manager" });
    if (!user.username) return response.status(400).json({ error: "Укажите логин сотрудника." });
    if (!user.password || user.password.length < 6) return response.status(400).json({ error: "Укажите пароль сотрудника минимум 6 символов." });
    const exists = (await configuredUsersForAdminAsync()).some((item) => item.username.toLowerCase() === user.username.toLowerCase());
    if (exists) return response.status(409).json({ error: "Пользователь с таким логином уже существует." });
    const users = await readStoredAppUsers();
    users.push({ ...user, source: "local", protected: false, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() });
    await writeStoredAppUsers(users);
    appendAudit(request, "users.create", {
      username: user.username,
      role: user.role,
      oldValue: null,
      newValue: publicAppUser(user),
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.put("/api/users/:username", requireAdmin, async (request, response, next) => {
  try {
    const username = cleanText(request.params.username);
    const currentUsername = requestUsername(request);
    const users = await readStoredAppUsers({ includeDisabled: true });
    const index = users.findIndex((item) => item.username.toLowerCase() === username.toLowerCase());
    if (index < 0) return response.status(404).json({ error: "Локальный сотрудник не найден. Пользователей из .env можно менять только в .env." });
    const before = publicAppUser(users[index]);
    const nextUser = {
      ...users[index],
      role: normalizeAppRole(request.body.role, users[index].role || "manager"),
      updatedAt: new Date().toISOString(),
    };
    if (request.body.active !== undefined || request.body.disabled !== undefined) {
      const active = request.body.active !== undefined
        ? Boolean(request.body.active)
        : !Boolean(request.body.disabled);
      if (!active && username.toLowerCase() === currentUsername.toLowerCase()) {
        return response.status(400).json({ error: "Нельзя выключить текущего пользователя. Сначала войдите под другим администратором." });
      }
      nextUser.disabled = !active;
    }
    if (nextUser.role !== "admin" && username.toLowerCase() === currentUsername.toLowerCase()) {
      return response.status(400).json({ error: "Нельзя снять роль администратора с текущего пользователя. Сначала войдите под другим администратором." });
    }
    if (request.body.password) {
      const password = cleanText(request.body.password);
      if (password.length < 6) return response.status(400).json({ error: "Пароль должен быть минимум 6 символов." });
      nextUser.password = password;
    }
    users[index] = nextUser;
    await writeStoredAppUsers(users);
    appendAudit(request, "users.update", {
      username,
      role: nextUser.role,
      oldValue: before,
      newValue: publicAppUser(nextUser),
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/users/:username", requireAdmin, async (request, response, next) => {
  try {
    const username = cleanText(request.params.username);
    const currentUsername = requestUsername(request);
    if (username.toLowerCase() === currentUsername.toLowerCase()) {
      return response.status(400).json({ error: "Нельзя удалить текущего пользователя. Сначала войдите под другим администратором." });
    }
    const users = await readStoredAppUsers({ includeDisabled: true });
    const target = users.find((item) => item.username.toLowerCase() === username.toLowerCase());
    if (!target) return response.status(404).json({ error: "Локальный сотрудник не найден. Пользователей из .env удалить нельзя." });
    const remaining = users.filter((item) => item.username.toLowerCase() !== username.toLowerCase());
    await writeStoredAppUsers(remaining);
    appendAudit(request, "users.delete", {
      username,
      oldValue: publicAppUser(target),
      newValue: null,
    }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
    response.json({ ok: true, users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
  } catch (error) {
    next(error);
  }
});
}

module.exports = {
  registerUsersRoutes,
};
