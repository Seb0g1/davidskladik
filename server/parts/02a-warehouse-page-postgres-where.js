function collectYandexWarehouseTargetAliases() {
  const aliases = new Set(["yandex"]);
  for (const shop of getYandexShops({ includeSyncDisabled: true })) {
    for (const value of [shop.id, shop.name, shop.businessId, shop.campaignId]) {
      const alias = cleanText(value);
      if (alias) aliases.add(alias);
    }
  }
  return Array.from(aliases);
}

function enabledWarehouseTargetWhere() {
  const or = [];
  const ozonAccounts = getOzonAccounts();
  if (ozonAccounts.length) {
    for (const account of ozonAccounts) {
      or.push({
        marketplace: "ozon",
        OR: [
          { target: account.id },
          { target: "ozon" },
        ],
      });
    }
  } else {
    or.push({ marketplace: "ozon" });
  }
  const yandexShops = getYandexShops({ includeSyncDisabled: true });
  if (yandexShops.length === 1) {
    or.push({ marketplace: "yandex" });
  } else if (yandexShops.length) {
    const yandexTargetFilters = [
      { target: "yandex" },
      { target: { startsWith: "yandex" } },
      ...collectYandexWarehouseTargetAliases().map((alias) => ({ target: alias })),
    ];
    or.push({
      marketplace: "yandex",
      OR: yandexTargetFilters,
    });
  } else {
    or.push({ marketplace: "yandex" });
  }
  return or.length ? { OR: or } : {};
}

function warehousePagePostgresWhere(filters = {}) {
  const and = [enabledWarehouseTargetWhere()];
  const marketplace = cleanText(filters.marketplace || "all");
  if (marketplace !== "all" && ["ozon", "yandex"].includes(marketplace)) and.push({ marketplace });
  const linked = cleanText(filters.linked || "all");
  if (linked === "linked") and.push({ links: { some: {} } });
  if (linked === "ready") and.push({ links: { some: {} } });
  if (linked === "unlinked") and.push({ links: { none: {} } });
  if (linked === "changed") and.push({ links: { some: {} } });
  if (linked === "linked_archived") and.push({ links: { some: {} } }, { OR: [{ archived: true }, { status: "archived" }] });
  const stateCode = cleanText(filters.state || "all");
  if (stateCode !== "all") {
    if (stateCode === "archived") {
      and.push({ OR: [{ archived: true }, { status: "archived" }] });
    } else {
      and.push({ status: stateCode });
    }
  }
  const brandFilter = cleanText(filters.brand || "");
  if (brandFilter) {
    and.push({
      OR: [
        { brand: { contains: brandFilter, mode: "insensitive" } },
        { name: { contains: brandFilter, mode: "insensitive" } },
      ],
    });
  }
  const q = cleanText(filters.q || "");
  if (q) {
    if (isWarehouseArticleLikeQuery(q)) {
      and.push({
        OR: [
          { id: { equals: q, mode: "insensitive" } },
          { offerId: { equals: q, mode: "insensitive" } },
          { productId: { equals: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { equals: q, mode: "insensitive" } } } },
        ],
      });
    } else {
      and.push({
        OR: [
          { id: { contains: q, mode: "insensitive" } },
          { offerId: { contains: q, mode: "insensitive" } },
          { productId: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { brand: { contains: q, mode: "insensitive" } },
          { links: { some: { supplierArticle: { contains: q, mode: "insensitive" } } } },
          { links: { some: { supplierName: { contains: q, mode: "insensitive" } } } },
          { links: { some: { partnerId: { contains: q, mode: "insensitive" } } } },
          { links: { some: { keyword: { contains: q, mode: "insensitive" } } } },
        ],
      });
    }
  }
  return { AND: and.filter((item) => Object.keys(item || {}).length) };
}
