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
