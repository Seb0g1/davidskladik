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
  app.use(express.static(publicDir, {
    setHeaders(response, filePath) {
      const ext = path.extname(filePath).toLowerCase();
      if (ext === ".js" || ext === ".css" || ext === ".html") {
        cacheControlForMutableAsset(response);
      }
    },
  }));
}

module.exports = {
  registerStaticAppRoutes,
};
