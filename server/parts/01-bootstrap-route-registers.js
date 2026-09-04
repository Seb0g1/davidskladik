registerAuthSessionRoutes(app, {
  loginLimiter,
  configuredUsersAsync,
  timingSafeEqual,
  verifyStoredPassword,
  createSessionToken,
  sessionCookieName,
  sessionTtlMs,
  readSession,
  isAdminSession,
  isSecureSessionCookie: () => String(process.env.PUBLIC_BASE_URL || "").startsWith("https://"),
  resolveAllowedPagesForUsername,
});

registerYandexAuthRoutes(app, {
  loginLimiter,
  configuredUsersAsync,
  createSessionToken,
  sessionCookieName,
  sessionTtlMs,
  isSecureSessionCookie: () => String(process.env.PUBLIC_BASE_URL || "").startsWith("https://"),
  log: logger,
  getOrCreateYandexUser: async (username, { configuredUsersAsync: getUsers }) => {
    const users = await getUsers();
    const existing = users.find((u) => u.username === username);
    if (existing) return existing;
    const randomPassword = crypto.randomBytes(24).toString("base64url");
    if (shouldUsePostgresStorage()) {
      try {
        const prisma = getPrisma();
        if (prisma) {
          const row = await prisma.appUser.upsert({
            where: { username },
            create: {
              username,
              passwordHash: hashPassword(randomPassword),
              role: "manager",
              source: "yandex",
              active: true,
              protected: false,
            },
            update: {},
          });
          return appUserFromPostgres(row);
        }
      } catch (error) {
        logger.warn("yandex user create postgres failed", { detail: error?.message });
      }
    }
    const newUser = normalizeAppUser(
      { username, password: randomPassword, role: "manager", source: "yandex" },
      { source: "yandex", defaultRole: "manager" },
    );
    const storedUsers = readStoredAppUsersSync();
    await writeStoredAppUsers([...storedUsers, newUser]);
    return newUser;
  },
});

app.use(requireAuth);
registerStaticAppRoutes(app, {
  express,
  path,
  publicDir,
  cacheControlForMutableAsset,
  serveIndexHtml,
  servePublicHtml,
  serveModernAppHtml,
});
