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
  app.get(["/legacy", "/legacy/", "/legacy/index.html"], serveIndexHtml);
  app.get(["/legacy/settings", "/legacy/settings.html"], (request, response, next) => servePublicHtml("settings.html", request, response, next));
  app.get(["/legacy/ai-drafts", "/legacy/ai-drafts.html"], (request, response, next) => servePublicHtml("ai-drafts.html", request, response, next));
  app.get(["/legacy/operations", "/legacy/operations.html"], (request, response, next) => servePublicHtml("operations.html", request, response, next));
  app.get(["/legacy/no-supplier", "/legacy/no-supplier.html"], (request, response, next) => servePublicHtml("no-supplier.html", request, response, next));
  app.get(["/legacy/pricemaster", "/legacy/pricemaster.html"], (request, response, next) => servePublicHtml("pricemaster.html", request, response, next));
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
