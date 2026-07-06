function requireAuth(request, response, next) {
  const publicPaths = [
    "/login",
    "/login.html",
    "/styles.css",
    "/login.js",
    "/app.js",
    "/product.js",
    "/product-builder-ui.js",
    "/ozon-product.js",
    "/yandex-product.js",
    "/ozon-yandex-import.html",
    "/ozon-yandex-import.js",
    "/operations.html",
    "/operations.js",
    "/ai-drafts.html",
    "/ai-drafts.js",
    "/health",
  ];
  if (publicPaths.includes(request.path)) return next();
  if (request.path.startsWith("/uploads/images/")) return next();
  if (request.path.startsWith("/uploads/ai-images/")) return next();
  if (request.path.startsWith("/uploads/branding/")) return next();
  // Фид Avito Автозагрузки: Авито скачивает XML без сессии, доступ по секретному
  // токену в URL (проверяется в самом роуте).
  if (request.path.startsWith("/public/avito-feed/")) return next();
  if (request.path === "/api/login" || request.path === "/api/session") return next();

  const session = readSession(request);
  if (session) {
    request.session = session;
    if (isAdminPagePath(request.path) && !isAdminSession(session)) {
      return response.redirect("/");
    }
    return next();
  }

  if (request.path.startsWith("/api/")) {
    return response.status(401).json({ error: "Требуется вход" });
  }

  return response.redirect("/login.html");
}

function requireAdmin(request, response, next) {
  if (isAdminSession(request.session)) return next();
  // Non-admins may still use APIs of pages explicitly granted to them
  // (Настройки -> Сотрудники -> Доступ к страницам). Unmapped API paths stay admin-only.
  const pageKey = apiPathPageKey(request.path);
  if (pageKey && request.session?.username) {
    resolveAllowedPagesForUsername(request.session.username)
      .then((pages) => {
        if (pages.includes(pageKey)) return next();
        return response.status(403).json({ error: "Доступ только для администратора.", code: "admin_required" });
      })
      .catch(() => response.status(403).json({ error: "Доступ только для администратора.", code: "admin_required" }));
    return undefined;
  }
  return response.status(403).json({ error: "Доступ только для администратора.", code: "admin_required" });
}

function requireStaff(request, response, next) {
  const role = cleanText(request.session?.role).toLowerCase();
  if (role === "admin" || role === "manager") return next();
  return response.status(403).json({ error: "Нужен доступ сотрудника.", code: "staff_required" });
}
