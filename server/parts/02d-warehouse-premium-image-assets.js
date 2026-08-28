function premiumImageBackgrounds() {
  return [
    { id: "white-luxury", label: "White luxury", from: "#fbfcff", to: "#edf3fb", accent: "#d8e2ef", text: "#d7e0ec" },
    { id: "warm-champagne", label: "Warm champagne", from: "#fff8ed", to: "#ead8bd", accent: "#d7b77d", text: "#e5cfaa" },
    { id: "marble-light", label: "Light marble", from: "#f7f8f7", to: "#dce2e2", accent: "#b8c3c8", text: "#d5dbdd" },
    { id: "dark-premium", label: "Dark premium", from: "#111827", to: "#25324a", accent: "#c7a86d", text: "#2f3b53" },
    { id: "soft-gradient", label: "Soft gradient", from: "#f8fbff", to: "#e9ddf2", accent: "#b7c9e9", text: "#e2d9ef" },
  ];
}

function premiumImageBackgroundSvg(theme, size) {
  const dark = theme.id === "dark-premium";
  return Buffer.from(`
<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="${theme.from}"/>
      <stop offset="1" stop-color="${theme.to}"/>
    </linearGradient>
    <radialGradient id="glow" cx="46%" cy="34%" r="58%">
      <stop offset="0" stop-color="${dark ? "#ffffff" : "#ffffff"}" stop-opacity="${dark ? "0.14" : "0.72"}"/>
      <stop offset="1" stop-color="${theme.to}" stop-opacity="0"/>
    </radialGradient>
    <filter id="blur"><feGaussianBlur stdDeviation="18"/></filter>
  </defs>
  <rect width="${size}" height="${size}" fill="url(#bg)"/>
  <rect width="${size}" height="${size}" fill="url(#glow)"/>
  <path d="M${size * 0.12} ${size * 0.77} C${size * 0.32} ${size * 0.68}, ${size * 0.62} ${size * 0.88}, ${size * 0.9} ${size * 0.72}" fill="none" stroke="${theme.accent}" stroke-opacity="${dark ? "0.22" : "0.18"}" stroke-width="3"/>
  <circle cx="${size * 0.78}" cy="${size * 0.20}" r="${size * 0.20}" fill="${theme.accent}" opacity="${dark ? "0.08" : "0.10"}" filter="url(#blur)"/>
  <rect x="${size * 0.10}" y="${size * 0.11}" width="${size * 0.80}" height="${size * 0.78}" rx="34" fill="none" stroke="${dark ? "#ffffff" : theme.accent}" stroke-opacity="${dark ? "0.08" : "0.13"}"/>
</svg>`);
}

function premiumImageShadowSvg(size) {
  return Buffer.from(`
<svg width="${size}" height="${Math.round(size * 0.18)}" viewBox="0 0 ${size} ${Math.round(size * 0.18)}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="shadow" cx="50%" cy="50%" r="50%">
      <stop offset="0" stop-color="#000000" stop-opacity="0.28"/>
      <stop offset="1" stop-color="#000000" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <ellipse cx="${size / 2}" cy="${Math.round(size * 0.09)}" rx="${Math.round(size * 0.36)}" ry="${Math.round(size * 0.07)}" fill="url(#shadow)"/>
</svg>`);
}

function premiumImageLogoBadgeSvg(size) {
  return Buffer.from(`
<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg">
  <rect x="0.5" y="0.5" width="${size - 1}" height="${size - 1}" rx="18" fill="#ffffff" fill-opacity="0.72" stroke="#ffffff" stroke-opacity="0.86"/>
</svg>`);
}

function productImageUrlsForPremium(product = {}) {
  return Array.from(new Set([
    ...splitList(product.imageUrl),
    ...splitList(product.ozon?.primaryImage),
    ...splitList(product.ozon?.images),
    ...splitList(product.yandex?.pictures),
    ...splitList(product.yandex?.images),
  ].map(cleanText).filter(Boolean)));
}

async function readProductSourceImageBuffer(sourceUrl, request) {
  const localPath = localPublicFilePathFromUrl(sourceUrl, request);
  let buffer = null;
  let mimeType = "";
  if (localPath) {
    buffer = await fs.readFile(localPath);
    mimeType = imageMimeFromPath(localPath);
  } else if (/^https?:\/\//i.test(sourceUrl)) {
    const response = await fetch(sourceUrl, { headers: { Accept: "image/*,*/*;q=0.8" } });
    if (!response.ok) {
      const error = new Error(`Source image fetch failed: HTTP ${response.status}.`);
      error.statusCode = 400;
      error.code = "source_image_fetch_failed";
      throw error;
    }
    mimeType = cleanText(response.headers.get("content-type")).split(";")[0];
    buffer = Buffer.from(await response.arrayBuffer());
  }
  if (!buffer || buffer.length < 1024 || !/^image\/(png|jpe?g|webp)$/i.test(mimeType || imageMimeFromPath(sourceUrl))) {
    const error = new Error("Source image is not ready for premium card generation.");
    error.statusCode = 400;
    error.code = "source_image_not_ready";
    throw error;
  }
  try {
    await sharp(buffer).metadata();
  } catch (error) {
    const sourceError = new Error("Source image is damaged or unsupported.");
    sourceError.statusCode = 400;
    sourceError.code = "source_image_not_ready";
    throw sourceError;
  }
  return buffer;
}

function brandingForMarketplace(settings = {}, marketplace = "ozon") {
  const normalized = normalizeAppSettings(settings);
  const marketplaces = normalized.branding?.marketplaces || {};
  const key = marketplace === "yandex" ? "yandex" : "ozon";
  return marketplaces[key] || {};
}

async function readBrandingLogoBuffer(settings = {}, marketplace = "ozon", request = null) {
  const branding = brandingForMarketplace(settings, marketplace);
  const logoUrl = cleanText(branding.logoUrl);
  if (!logoUrl) return null;
  const localPath = localPublicFilePathFromUrl(logoUrl, request);
  if (!localPath) return null;
  try {
    const buffer = await fs.readFile(localPath);
    const meta = await sharp(buffer).metadata();
    if (meta.format !== "png" || Number(meta.width) !== 258 || Number(meta.height) !== 258) return null;
    return buffer;
  } catch (_error) {
    return null;
  }
}

async function marketplaceExtraCardUrls(marketplace = "ozon", request = null) {
  const settings = await readAppSettings();
  const branding = brandingForMarketplace(settings, marketplace);
  const cards = Array.isArray(branding.extraCards) ? branding.extraCards : [];
  const urls = [];
  for (const card of cards.slice(0, 2)) {
    const rawUrl = cleanText(card?.url);
    if (!rawUrl) continue;
    urls.push(await normalizeMarketplaceImageUrlForSend(rawUrl, request));
  }
  return Array.from(new Set(urls));
}

function appendUniqueImages(primary = [], extra = []) {
  return Array.from(new Set([...(Array.isArray(primary) ? primary : []), ...(Array.isArray(extra) ? extra : [])].map(cleanText).filter(Boolean)));
}

async function buildPremiumPerfumeImageBuffer(sourceBuffer, { theme, logoBuffer = null, size = 1000 } = {}) {
  const targetSize = Math.max(512, Math.min(2048, Number(size || 1000) || 1000));
  const productMaxHeight = Math.round(targetSize * 0.78);
  const productMaxWidth = Math.round(targetSize * 0.68);
  let productPipeline = sharp(sourceBuffer).rotate();
  try {
    productPipeline = productPipeline.trim({ threshold: 10 });
  } catch (_error) {
    productPipeline = sharp(sourceBuffer).rotate();
  }
  const productBuffer = await productPipeline
    .resize(productMaxWidth, productMaxHeight, {
      fit: "inside",
      withoutEnlargement: false,
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    })
    .png()
    .toBuffer();
  const productMeta = await sharp(productBuffer).metadata();
  if (!productMeta.width || !productMeta.height || productMeta.width < 80 || productMeta.height < 80) {
    const error = new Error("Source image is too small after processing.");
    error.statusCode = 400;
    error.code = "source_image_not_ready";
    throw error;
  }
  const productLeft = Math.max(0, Math.round((targetSize - productMeta.width) / 2));
  const productTop = Math.max(0, Math.round(targetSize - productMeta.height - targetSize * 0.12));
  const shadowSize = Math.round(targetSize * 0.78);
  const composites = [
    {
      input: premiumImageShadowSvg(shadowSize),
      left: Math.round((targetSize - shadowSize) / 2),
      top: Math.round(targetSize * 0.79),
    },
    { input: productBuffer, left: productLeft, top: productTop },
  ];
  if (logoBuffer) {
    const badgeSize = Math.round(targetSize * 0.094);
    const badgeMargin = Math.round(targetSize * 0.055);
    const logoInset = Math.round(badgeSize * 0.15);
    const badgeLeft = targetSize - badgeMargin - badgeSize;
    const badgeTop = badgeMargin;
    const logo = await sharp(logoBuffer)
      .resize(badgeSize - logoInset * 2, badgeSize - logoInset * 2, { fit: "contain" })
      .png()
      .toBuffer();
    composites.push(
      { input: premiumImageLogoBadgeSvg(badgeSize), left: badgeLeft, top: badgeTop },
      { input: logo, left: badgeLeft + logoInset, top: badgeTop + logoInset },
    );
  }
  return sharp(premiumImageBackgroundSvg(theme, targetSize))
    .composite(composites)
    .png({ compressionLevel: 9 })
    .toBuffer();
}

