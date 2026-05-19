function cacheControlForMutableAsset(response) {
  response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  response.setHeader("Pragma", "no-cache");
  response.setHeader("Expires", "0");
}

function addBuildVersionToIndexHtml(html, buildVersion) {
  const version = encodeURIComponent(buildVersion);
  return String(html || "")
    .replace(/href="\/styles\.css(?:\?v=[^"]*)?"/g, `href="/styles.css?v=${version}"`)
    .replace(/src="\/app\.js(?:\?v=[^"]*)?"/g, `src="/app.js?v=${version}"`);
}

function createStaticAppHandlers({ fs, path, publicDir, modernAppDir, buildVersion }) {
  async function serveIndexHtml(_request, response, next) {
    try {
      const html = await fs.readFile(path.join(publicDir, "index.html"), "utf8");
      cacheControlForMutableAsset(response);
      response.type("html").send(addBuildVersionToIndexHtml(html, buildVersion));
    } catch (error) {
      next(error);
    }
  }

  async function servePublicHtml(fileName, _request, response, next) {
    try {
      const html = await fs.readFile(path.join(publicDir, fileName), "utf8");
      cacheControlForMutableAsset(response);
      response.type("html").send(addBuildVersionToIndexHtml(html, buildVersion));
    } catch (error) {
      next(error);
    }
  }

  async function serveModernAppHtml(_request, response, next) {
    try {
      const html = await fs.readFile(path.join(modernAppDir, "index.html"), "utf8");
      cacheControlForMutableAsset(response);
      response.type("html").send(html);
    } catch (error) {
      if (error?.code === "ENOENT") {
        return response.status(503).type("html").send("Modern warehouse app is not built. Run npm run frontend:build.");
      }
      next(error);
    }
  }

  return {
    cacheControlForMutableAsset,
    serveIndexHtml,
    servePublicHtml,
    serveModernAppHtml,
  };
}

module.exports = {
  addBuildVersionToIndexHtml,
  cacheControlForMutableAsset,
  createStaticAppHandlers,
};
