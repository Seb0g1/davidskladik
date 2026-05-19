const fs = require("fs");
const path = require("path");
const PDFDocument = require("pdfkit");

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
    readDeletedAppUsers,
    writeDeletedAppUsers,
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

function booleanQuery(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = cleanText(value).toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function selectedUsernamesFromInput(input) {
  const raw = Array.isArray(input) ? input : cleanText(input).split(",");
  return raw
    .map((value) => cleanText(value))
    .filter(Boolean);
}

function statsSummary(users = []) {
  const activeUsers = users.filter((user) => user.active !== false && !user.deleted).length;
  return {
    totalUsers: users.length,
    activeUsers,
    deletedUsers: users.filter((user) => user.deleted).length,
    currentLinksCreated: users.reduce((sum, user) => sum + Number(user.currentLinksCreated || 0), 0),
    currentLinksUpdated: users.reduce((sum, user) => sum + Number(user.currentLinksUpdated || 0), 0),
    actionsTotal: users.reduce((sum, user) => sum + Number(user.actionsTotal || 0), 0),
    linksAdded: users.reduce((sum, user) => sum + Number(user.linksAdded || 0), 0),
    linksUpdated: users.reduce((sum, user) => sum + Number(user.linksUpdated || 0), 0),
    linksDeleted: users.reduce((sum, user) => sum + Number(user.linksDeleted || 0), 0),
    affectedProducts: users.reduce((sum, user) => sum + Number(user.affectedProducts || 0), 0),
    affectedOfferIds: users.reduce((sum, user) => sum + Number(user.affectedOfferIds || 0), 0),
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
    deleted: Boolean(user.deleted || user.hardDeleted),
    hardDeleted: Boolean(user.hardDeleted || user.deleted),
    deletedAt: user.deletedAt || null,
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
    deleted: Boolean(stats.deleted),
    hardDeleted: Boolean(stats.hardDeleted),
    deletedAt: stats.deletedAt || null,
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
  if (user.deleted || user.hardDeleted) {
    stats.deleted = true;
    stats.hardDeleted = true;
    stats.active = false;
    stats.deletedAt = user.deletedAt || stats.deletedAt || null;
    if (!stats.source) stats.source = "deleted";
  }
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

async function buildUsersStatsResponse(options = {}) {
  const period = usersStatsPeriod(options.period);
  const selectedUsers = new Set(selectedUsernamesFromInput(options.users).map((username) => username.toLowerCase()));
  const includeInactive = Boolean(options.includeInactive);
  const includeDeleted = Boolean(options.includeDeleted);
  const statsByUser = new Map();
  const users = (await configuredUsersForAdminAsync()).map(publicAppUser);
  for (const user of users) getStatsBucket(statsByUser, user.username, user);
  const deletedUsers = typeof readDeletedAppUsers === "function" ? await readDeletedAppUsers() : [];
  for (const user of deletedUsers) {
    getStatsBucket(statsByUser, user.username, {
      role: "",
      source: "deleted",
      active: false,
      deleted: true,
      hardDeleted: true,
      deletedAt: user.deletedAt,
    });
  }

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
  const allStats = Array.from(statsByUser.values())
    .map(publicUserStats)
    .filter((user) => includeInactive || user.active !== false || user.deleted)
    .filter((user) => includeDeleted || !user.deleted)
    .filter((user) => !selectedUsers.size || selectedUsers.has(cleanText(user.username).toLowerCase()))
    .sort((a, b) => {
      const totalB = b.currentLinksCreated + b.currentLinksUpdated + b.actionsTotal;
      const totalA = a.currentLinksCreated + a.currentLinksUpdated + a.actionsTotal;
      if (totalB !== totalA) return totalB - totalA;
      return a.username.localeCompare(b.username);
    });

  return {
    ok: true,
    period: period.key,
    periodDays: period.periodDays,
    from: period.from ? period.from.toISOString() : null,
    to: period.to.toISOString(),
    includeInactive,
    includeDeleted,
    selectedUsers: Array.from(selectedUsers),
    summary: statsSummary(allStats),
    users: allStats,
  };
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
    response.json(await buildUsersStatsResponse({
      period: request.query.period,
      users: request.query.users,
      includeInactive: booleanQuery(request.query.includeInactive, true),
      includeDeleted: booleanQuery(request.query.includeDeleted, true),
    }));
  } catch (error) {
    next(error);
  }
});

function periodLabel(report = {}) {
  if (report.period === "all") return "все время";
  return `${report.periodDays || ""} дней`;
}

function formatDateRu(value) {
  if (!value) return "-";
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("ru-RU", { timeZone: "Europe/Moscow" });
}

function pdfText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function makeUsersStatsPdf(report = {}) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 42, info: { Title: "Magic Vibe - статистика сотрудников" } });
    const chunks = [];
    doc.on("data", (chunk) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    const regularFont = path.join(__dirname, "..", "assets", "fonts", "NotoSans-Regular.ttf");
    const boldFont = path.join(__dirname, "..", "assets", "fonts", "NotoSans-Bold.ttf");
    const hasFonts = fs.existsSync(regularFont) && fs.existsSync(boldFont);
    if (hasFonts) {
      doc.registerFont("regular", regularFont);
      doc.registerFont("bold", boldFont);
      doc.font("regular");
    }

    const font = (name) => {
      if (hasFonts) doc.font(name === "bold" ? "bold" : "regular");
      else doc.font(name === "bold" ? "Helvetica-Bold" : "Helvetica");
    };
    const width = doc.page.width - doc.page.margins.left - doc.page.margins.right;
    const left = doc.page.margins.left;
    const top = doc.page.margins.top;
    const summary = report.summary || {};

    doc.rect(0, 0, doc.page.width, 118).fill("#07111f");
    doc.fillColor("#ffffff");
    font("bold");
    doc.fontSize(22).text("Magic Vibe", left, top - 8);
    doc.fontSize(15).text("Статистика сотрудников ДавидСклад", left, top + 22);
    font("regular");
    doc.fillColor("#b9c7dc").fontSize(9).text(`Период: ${periodLabel(report)} · сформировано: ${formatDateRu(new Date())}`, left, top + 48);
    doc.fillColor("#ffffff").fontSize(9).text("Подпись: Magic Vibe / ДавидСклад", left, top + 66);

    doc.y = 142;
    const cardGap = 8;
    const cardWidth = (width - cardGap * 3) / 4;
    const cards = [
      ["Активных связей", summary.currentLinksCreated || 0],
      ["Действий", summary.actionsTotal || 0],
      ["Добавлено", summary.linksAdded || 0],
      ["Товаров затронуто", summary.affectedProducts || 0],
    ];
    cards.forEach((card, index) => {
      const x = left + index * (cardWidth + cardGap);
      doc.roundedRect(x, doc.y, cardWidth, 58, 6).fillAndStroke("#f4f7fb", "#d7dfec");
      doc.fillColor("#5b677a");
      font("regular");
      doc.fontSize(8).text(card[0], x + 10, doc.y + 10, { width: cardWidth - 20 });
      doc.fillColor("#0f172a");
      font("bold");
      doc.fontSize(18).text(String(card[1]), x + 10, doc.y + 27, { width: cardWidth - 20 });
    });
    doc.y += 84;

    font("bold");
    doc.fillColor("#0f172a").fontSize(13).text("Сотрудники", left, doc.y);
    doc.y += 22;
    const columns = [
      { key: "username", label: "Сотрудник", width: 118 },
      { key: "currentLinksCreated", label: "Активн.", width: 52 },
      { key: "linksAdded", label: "Добав.", width: 52 },
      { key: "linksUpdated", label: "Обнов.", width: 54 },
      { key: "linksDeleted", label: "Удал.", width: 48 },
      { key: "affectedProducts", label: "Товаров", width: 56 },
      { key: "lastActionAt", label: "Последнее действие", width: 130 },
    ];
    const rowHeight = 34;
    const drawHeader = () => {
      let x = left;
      doc.rect(left, doc.y, width, 24).fill("#eaf1fb");
      font("bold");
      doc.fillColor("#1f2a3d").fontSize(8);
      for (const column of columns) {
        doc.text(column.label, x + 4, doc.y + 8, { width: column.width - 8, ellipsis: true });
        x += column.width;
      }
      doc.y += 24;
    };
    drawHeader();
    font("regular");
    for (const user of report.users || []) {
      if (doc.y + rowHeight > doc.page.height - 54) {
        doc.addPage();
        doc.y = top;
        drawHeader();
      }
      let x = left;
      doc.rect(left, doc.y, width, rowHeight).fillAndStroke("#ffffff", "#edf2f7");
      const status = user.deleted ? "удален" : (user.active === false ? "выключен" : "активен");
      const values = {
        username: `${user.username || "system"} · ${status}`,
        currentLinksCreated: user.currentLinksCreated || 0,
        linksAdded: user.linksAdded || 0,
        linksUpdated: user.linksUpdated || 0,
        linksDeleted: user.linksDeleted || 0,
        affectedProducts: user.affectedProducts || 0,
        lastActionAt: formatDateRu(user.lastActionAt),
      };
      doc.fillColor("#111827").fontSize(8);
      for (const column of columns) {
        doc.text(pdfText(values[column.key]), x + 4, doc.y + 8, { width: column.width - 8, height: 20, ellipsis: true });
        x += column.width;
      }
      doc.y += rowHeight;
    }
    if (!(report.users || []).length) {
      doc.fillColor("#5b677a").fontSize(10).text("По выбранным фильтрам действий нет.", left, doc.y + 8);
    }

    const bottom = doc.page.height - 36;
    doc.fillColor("#64748b");
    font("regular");
    doc.fontSize(8).text("Magic Vibe · документ сформирован автоматически, история действий и привязок сохранена в ДавидСклад.", left, bottom, { width, align: "center" });
    doc.end();
  });
}

app.post("/api/users/stats/export", requireAdmin, async (request, response, next) => {
  try {
    const report = await buildUsersStatsResponse({
      period: request.body?.period,
      users: request.body?.usernames || request.body?.users,
      includeInactive: request.body?.includeInactive !== false,
      includeDeleted: request.body?.includeDeleted !== false,
    });
    const pdf = await makeUsersStatsPdf(report);
    response.setHeader("Content-Type", "application/pdf");
    response.setHeader("Content-Disposition", `attachment; filename="magic-vibe-user-stats-${report.period || "report"}.pdf"`);
    response.send(pdf);
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
    const hardDelete = booleanQuery(request.query.hard, false) || booleanQuery(request.body?.hard, false);
    if (username.toLowerCase() === currentUsername.toLowerCase()) {
      return response.status(400).json({ error: "Нельзя удалить текущего пользователя. Сначала войдите под другим администратором." });
    }
    if (hardDelete) {
      const allUsers = await configuredUsersForAdminAsync();
      const targetUser = allUsers.find((item) => item.username.toLowerCase() === username.toLowerCase());
      if (!targetUser) return response.status(404).json({ error: "User not found." });
      const activeAdminsAfterDelete = allUsers.filter((item) => (
        item.username.toLowerCase() !== username.toLowerCase()
        && item.role === "admin"
        && item.disabled !== true
      ));
      if (targetUser.role === "admin" && !activeAdminsAfterDelete.length) {
        return response.status(400).json({ error: "Cannot delete the last active admin." });
      }

      const storedUsers = await readStoredAppUsers({ includeDisabled: true });
      const storedTarget = storedUsers.find((item) => item.username.toLowerCase() === username.toLowerCase());
      const before = publicAppUser(storedTarget || targetUser);
      if (storedTarget) {
        const remaining = storedUsers.filter((item) => item.username.toLowerCase() !== username.toLowerCase());
        if (shouldUsePostgresStorage()) {
          try {
            await getPrisma().appUser.deleteMany({ where: { username, protected: false } });
          } catch (error) {
            if (!jsonFallbackEnabled()) throw error;
            logger.warn("hard delete app user postgres failed, using JSON fallback", { detail: error?.message || String(error) });
          }
        }
        await writeStoredAppUsers(remaining);
      } else {
        if (typeof readDeletedAppUsers !== "function" || typeof writeDeletedAppUsers !== "function") {
          return response.status(409).json({ error: "Env user hard delete requires tombstone storage." });
        }
        const deletedUsers = await readDeletedAppUsers();
        deletedUsers.push({
          username,
          deletedAt: new Date().toISOString(),
          deletedBy: currentUsername,
          reason: "hard_delete_env_user",
        });
        await writeDeletedAppUsers(deletedUsers);
      }
      appendAudit(request, "users.hard_delete", {
        username,
        oldValue: before,
        newValue: null,
      }).catch((auditError) => logger.warn("user audit append failed", { detail: auditError?.message || String(auditError) }));
      return response.json({ ok: true, users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
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
