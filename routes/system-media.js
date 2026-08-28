function registerSystemMediaRoutes(app, deps) {
  const {
    getUsdRate,
    uploadImages,
    fs,
    path,
    crypto,
    sharp,
    uploadImageDir,
    imageExtension,
    uploadBaseUrl,
    consumeUploadQuota,
    pruneUploadDirectory,
    logger,
  } = deps;

app.get("/api/exchange-rate", async (request, response, next) => {
  try {
    response.json(await getUsdRate({ force: request.query.force === "true" }));
  } catch (error) {
    next(error);
  }
});

app.post("/api/uploads/images", uploadImages.array("images", 10), async (request, response, next) => {
  try {
    const files = Array.isArray(request.files) ? request.files : [];
    if (!files.length) return response.status(400).json({ error: "Выберите хотя бы одно изображение." });

    consumeUploadQuota(request, files.length);

    await fs.mkdir(uploadImageDir, { recursive: true });
    const saved = [];
    for (const file of files) {
      // Auto-rotate by EXIF orientation only — no resize, no recompression.
      let processedBuffer = file.buffer;
      let finalExtension = imageExtension(file);
      try {
        processedBuffer = await sharp(file.buffer).rotate().toBuffer();
      } catch (sharpErr) {
        logger.warn("upload sharp rotate failed, saving original", { name: file.originalname, detail: sharpErr?.message });
        processedBuffer = file.buffer;
      }
      const fileName = `${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID()}${finalExtension}`;
      const filePath = path.join(uploadImageDir, fileName);
      await fs.writeFile(filePath, processedBuffer);
      const relativeUrl = `/uploads/images/${fileName}`;
      saved.push({
        originalName: file.originalname,
        size: processedBuffer.length,
        mimeType: file.mimetype,
        path: relativeUrl,
        url: `${uploadBaseUrl(request)}${relativeUrl}`,
      });
    }

    response.json({ ok: true, files: saved });
    setImmediate(() => {
      pruneUploadDirectory().catch((err) => logger.warn("upload prune failed", { detail: err?.message || String(err) }));
    });
  } catch (error) {
    next(error);
  }
});
}

module.exports = {
  registerSystemMediaRoutes,
};
