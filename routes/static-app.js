function registerStaticAppRoutes(app, deps) {
  const {
    express,
    path,
    publicDir,
    cacheControlForMutableAsset,
    serveIndexHtml,
    servePublicHtml,
    serveModernAppHtml,
  } = deps;

  app.get(/^\/app(?:\/.*)?$/u, serveModernAppHtml);
  // Legacy-интерфейс выведен из эксплуатации: все прежние /legacy/* адреса
  // ведут в современное приложение. Кабинеты переехали в Настройки → Маркетплейсы.
  app.get(/^\/legacy(?:\/.*)?$/u, (_request, response) => response.redirect("/app/warehouse"));
  app.get(["/", "/index.html"], serveModernAppHtml);
  // Vite puts a content hash into every file name under app-modern/assets, so a changed file
  // always gets a new URL: these can be cached for a year instead of being re-downloaded
  // (~650 KB of JS/CSS) on every page open. index.html itself stays uncached.
  const hashedAssetsDir = `${path.sep}app-modern${path.sep}assets${path.sep}`;
  app.use(express.static(publicDir, {
    setHeaders(response, filePath) {
      const ext = path.extname(filePath).toLowerCase();
      if (filePath.includes(hashedAssetsDir)) {
        response.setHeader("Cache-Control", "public, max-age=31536000, immutable");
        return;
      }
      if (ext === ".js" || ext === ".css" || ext === ".html") {
        cacheControlForMutableAsset(response);
      }
    },
  }));
}

module.exports = {
  registerStaticAppRoutes,
};
