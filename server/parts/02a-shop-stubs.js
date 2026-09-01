function defaultShopStubs() {
  return {
    ozon: { enabled: false, stubUrls: [], position: "end" },
    yandex: { enabled: false, stubUrls: [], position: "end" },
    "ozon-aura": { enabled: false, stubUrls: [], position: "end" },
  };
}

function normalizeShopStubEntry(input = {}) {
  const raw = input && typeof input === "object" ? input : {};
  return {
    enabled: Boolean(raw.enabled),
    stubUrls: Array.isArray(raw.stubUrls)
      ? raw.stubUrls.map((url) => cleanText(url)).filter(Boolean).slice(0, 5)
      : [],
    position: "end",
  };
}

function normalizeShopStubs(input = {}) {
  const raw = input && typeof input === "object" ? input : {};
  return {
    ozon: normalizeShopStubEntry(raw.ozon),
    yandex: normalizeShopStubEntry(raw.yandex),
    "ozon-aura": normalizeShopStubEntry(raw["ozon-aura"]),
  };
}

function appendShopStubsToImages(existingImages, marketplace, accountId, appSettings) {
  const images = Array.isArray(existingImages)
    ? existingImages.map((url) => cleanText(url)).filter(Boolean)
    : [];
  const stubs = appSettings?.shopStubs;
  if (!stubs) return images;
  let entry;
  if (marketplace === "yandex") {
    entry = stubs.yandex;
  } else if (marketplace === "ozon" && accountId === "ozon-3d10ec43") {
    entry = stubs["ozon-aura"];
  } else if (marketplace === "ozon") {
    entry = stubs.ozon;
  }
  if (!entry?.enabled || !Array.isArray(entry.stubUrls) || !entry.stubUrls.length) return images;
  const existing = new Set(images);
  const toAppend = entry.stubUrls.filter((url) => url && !existing.has(url));
  if (!toAppend.length) return images;
  if (typeof logger !== "undefined") {
    logger.info("appending shop stubs to images", { marketplace, accountId, count: toAppend.length });
  }
  return [...images, ...toAppend];
}
