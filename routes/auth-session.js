function registerAuthSessionRoutes(app, deps) {
  const {
    loginLimiter,
    configuredUsersAsync,
    timingSafeEqual,
    verifyStoredPassword,
    createSessionToken,
    sessionCookieName,
    sessionTtlMs,
    readSession,
    isAdminSession,
    isSecureSessionCookie,
    resolveAllowedPagesForUsername,
  } = deps;

app.post("/api/login", loginLimiter, async (request, response, next) => {
  const username = String(request.body.username || "");
  const password = String(request.body.password || "");
  let users;
  try {
    users = await configuredUsersAsync();
  } catch (error) {
    return next(error);
  }

  if (!users.length || users.every((item) => !item.password)) {
    return response.status(500).json({ error: "APP_PASSWORD или APP_USERS_JSON не задан в .env" });
  }

  const user = users.find((item) => timingSafeEqual(username, item.username) && verifyStoredPassword(password, item.password));

  if (!user) {
    return response.status(401).json({ error: "Неверный логин или пароль" });
  }

  const token = createSessionToken(user);
  const secure = isSecureSessionCookie();
  response.cookie(sessionCookieName, token, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    maxAge: sessionTtlMs,
    path: "/",
  });
  response.json({ ok: true, username: user.username, role: user.role });
});

app.post("/api/logout", (_request, response) => {
  response.clearCookie(sessionCookieName, { path: "/" });
  response.json({ ok: true });
});

app.get("/api/session", async (request, response) => {
  const session = readSession(request);
  let allowedPages = [];
  if (session?.username && typeof resolveAllowedPagesForUsername === "function") {
    allowedPages = await resolveAllowedPagesForUsername(session.username).catch(() => []);
  }
  response.json({
    authenticated: Boolean(session),
    username: session?.username || null,
    role: session?.role || null,
    displayName: session?.displayName || null,
    avatarUrl: session?.avatarUrl || null,
    allowedPages,
    permissions: {
      admin: isAdminSession(session),
      settings: isAdminSession(session),
      priceMasterAudit: isAdminSession(session),
    },
  });
});
}

module.exports = {
  registerAuthSessionRoutes,
};
