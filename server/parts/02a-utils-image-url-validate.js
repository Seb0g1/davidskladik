function cleanLimit(value, fallback = 100, max = 500) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(parsed, max);
}

function uploadBaseUrl(request) {
  return String(process.env.PUBLIC_BASE_URL || `${request.protocol}://${request.get("host")}`).replace(/\/$/, "");
}

function isLocalhostName(hostname = "") {
  const host = cleanText(hostname).toLowerCase();
  return host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "0.0.0.0";
}

function marketplaceImageError(code, message, details = {}) {
  const error = new Error(message);
  error.statusCode = 400;
  error.code = code;
  Object.assign(error, details);
  return error;
}

function marketplaceImageLooksLikePlaceholder(value = "") {
  const text = cleanText(value).toLowerCase();
  if (!text) return true;
  const filename = text.split("?")[0].split("#")[0].split("/").pop() || text;
  return /placeholder|no[-_ ]?image|missing[-_ ]?image|empty[-_ ]?image|broken[-_ ]?image|default[-_ ]?image|sync|reload|refresh/.test(filename);
}

async function validateLocalUploadImagePath(pathname = "") {
  const normalizedPath = cleanText(pathname);
  const roots = [
    { prefix: "/uploads/ai-images/", dir: aiImageDir },
    { prefix: "/uploads/images/", dir: uploadImageDir },
    { prefix: "/uploads/branding/", dir: brandingImageDir },
  ];
  const match = roots.find((item) => normalizedPath.startsWith(item.prefix));
  if (!match) return;
  const relativeName = decodeURIComponent(normalizedPath.slice(match.prefix.length));
  if (!relativeName || relativeName !== path.basename(relativeName)) {
    throw marketplaceImageError("marketplace_image_url_invalid", "Некорректный путь AI-фото для отправки в маркетплейс.", { imagePath: normalizedPath });
  }
  const filePath = path.join(match.dir, relativeName);
  let stat = null;
  try {
    stat = await fs.stat(filePath);
  } catch (_error) {
    throw marketplaceImageError("marketplace_image_file_missing", "Файл AI-фото не найден на сервере. Сгенерируйте фото заново.", { imagePath: normalizedPath });
  }
  if (!stat.isFile() || stat.size < 1024) {
    throw marketplaceImageError("marketplace_image_file_empty", "AI-фото пустое или повреждено. Сгенерируйте фото заново.", { imagePath: normalizedPath, size: stat.size || 0 });
  }
  try {
    const meta = await sharp(filePath).metadata();
    if (!meta.width || !meta.height || meta.width < 80 || meta.height < 80) throw new Error("invalid_dimensions");
  } catch (error) {
    throw marketplaceImageError("marketplace_image_decode_failed", "AI-С„РѕС‚Рѕ РЅРµ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РєР°Рє РІР°Р»РёРґРЅРѕРµ РёР·РѕР±СЂР°Р¶РµРЅРёРµ.", { imagePath: normalizedPath, detail: error?.message || String(error) });
  }
}

async function validatePublicMarketplaceImageUrl(imageUrl = "") {
  const url = cleanText(imageUrl);
  if (!url) return;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 7000);
  let response = null;
  try {
    response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        Accept: "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
      },
      signal: controller.signal,
    });
  } catch (error) {
    throw marketplaceImageError(
      "marketplace_image_public_fetch_failed",
      "Публичный URL AI-фото не открывается с сервера. Яндекс тоже не сможет забрать картинку.",
      { imageUrl: url, detail: error?.message || String(error) },
    );
  } finally {
    clearTimeout(timeout);
  }
  const contentType = cleanText(response.headers.get("content-type")).toLowerCase();
  if (!response.ok && response.status !== 206) {
    throw marketplaceImageError(
      "marketplace_image_public_http_error",
      `Публичный URL AI-фото вернул HTTP ${response.status}. Отправка в маркетплейс остановлена.`,
      { imageUrl: url, status: response.status, contentType },
    );
  }
  if (!contentType.startsWith("image/")) {
    throw marketplaceImageError(
      "marketplace_image_public_not_image",
      "Публичный URL AI-фото открывается не как изображение. Проверьте PUBLIC_BASE_URL и доступность /uploads/ai-images.",
      { imageUrl: url, status: response.status, contentType },
    );
  }
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length < 256) {
    throw marketplaceImageError(
      "marketplace_image_public_empty",
      "Публичный URL AI-фото возвращает слишком маленький файл. Сгенерируйте фото заново.",
      { imageUrl: url, status: response.status, contentType, size: bytes.length },
    );
  }
  try {
    const meta = await sharp(bytes).metadata();
    if (!meta.width || !meta.height || meta.width < 80 || meta.height < 80) throw new Error("invalid_dimensions");
  } catch (error) {
    throw marketplaceImageError("marketplace_image_decode_failed", "РџСѓР±Р»РёС‡РЅС‹Р№ URL РІРµСЂРЅСѓР» С„Р°Р№Р», РєРѕС‚РѕСЂС‹Р№ РЅРµ РѕС‚РєСЂС‹РІР°РµС‚СЃСЏ РєР°Рє РёР·РѕР±СЂР°Р¶РµРЅРёРµ.", { imageUrl: url, status: response.status, contentType, detail: error?.message || String(error) });
  }
}

async function normalizeMarketplaceImageUrlForSend(rawUrl = "", request = null) {
  const sourceUrl = cleanText(rawUrl);
  if (marketplaceImageLooksLikePlaceholder(sourceUrl)) {
    throw marketplaceImageError("marketplace_image_placeholder", "Р¤РѕС‚Рѕ РїРѕС…РѕР¶Рµ РЅР° placeholder/Р·Р°РіР»СѓС€РєСѓ. РћС‚РїСЂР°РІРєР° РѕСЃС‚Р°РЅРѕРІР»РµРЅР°.", { imageUrl: sourceUrl });
  }
  if (!sourceUrl) {
    throw marketplaceImageError("marketplace_image_url_missing", "В AI-черновике нет URL фото для отправки.");
  }
  const publicBase = cleanText(process.env.PUBLIC_BASE_URL);
  const fallbackBase = request ? uploadBaseUrl(request) : publicBase;
  let parsed = null;
  try {
    parsed = sourceUrl.startsWith("/") ? new URL(sourceUrl, publicBase || fallbackBase) : new URL(sourceUrl);
  } catch (_error) {
    throw marketplaceImageError("marketplace_image_url_invalid", "AI-фото имеет некорректный URL для отправки.", { imageUrl: sourceUrl });
  }
  if (!/^https?:$/i.test(parsed.protocol)) {
    throw marketplaceImageError("marketplace_image_url_invalid", "Маркетплейс принимает только http/https URL фото.", { imageUrl: sourceUrl });
  }
  if (isLocalhostName(parsed.hostname)) {
    if (!publicBase) {
      throw marketplaceImageError(
        "marketplace_image_public_base_missing",
        "AI-фото сейчас ссылается на localhost. Укажите PUBLIC_BASE_URL=https://davidsklad.ru и повторите отправку.",
        { imageUrl: sourceUrl },
      );
    }
    let publicParsed = null;
    try {
      publicParsed = new URL(publicBase);
    } catch (_error) {
      throw marketplaceImageError("marketplace_image_public_base_invalid", "PUBLIC_BASE_URL некорректный. Укажите публичный адрес сайта, например https://davidsklad.ru.", { publicBase });
    }
    parsed.protocol = publicParsed.protocol;
    parsed.host = publicParsed.host;
  }
  if (isLocalhostName(parsed.hostname)) {
    throw marketplaceImageError("marketplace_image_url_not_public", "AI-фото должно иметь публичный URL, не localhost.", { imageUrl: parsed.toString() });
  }
  await validateLocalUploadImagePath(parsed.pathname);
  await validatePublicMarketplaceImageUrl(parsed.toString());
  return parsed.toString();
}
