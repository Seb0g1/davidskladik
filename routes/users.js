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
    logger,
  } = deps;

app.get("/api/users", requireAdmin, async (_request, response, next) => {
  try {
    response.json({ users: (await configuredUsersForAdminAsync()).map(publicAppUser) });
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
