function warehousePagePostgresPrimaryIdentityWhere(filters = {}) {
  const q = cleanText(filters.q || "");
  if (!q || !isWarehouseArticleLikeQuery(q)) return warehousePagePostgresWhere(filters);
  const base = warehousePagePostgresWhere({ ...filters, q: "" });
  return {
    AND: [
      ...(Array.isArray(base.AND) ? base.AND : []),
      {
        OR: [
          { id: { equals: q, mode: "insensitive" } },
          { offerId: { equals: q, mode: "insensitive" } },
          { productId: { equals: q, mode: "insensitive" } },
        ],
      },
    ].filter((item) => Object.keys(item || {}).length),
  };
}

function warehouseStateCounter(products = [], marketplace = "") {
  const rows = Array.isArray(products) ? products : [];
  const market = cleanText(marketplace).toLowerCase();
  const filtered = market ? rows.filter((product) => cleanText(product.marketplace).toLowerCase() === market) : rows;
  return {
    archived: filtered.filter((product) => productLooksArchived(product)).length,
    inactive: filtered.filter((product) => /inactive/i.test(cleanText(product.marketplaceState?.code))).length,
    outOfStock: filtered.filter((product) => cleanText(product.marketplaceState?.code).toLowerCase() === "out_of_stock").length,
  };
}

async function warehouseStateCounterFromPostgres(prisma, marketplace) {
  const base = { AND: [enabledWarehouseTargetWhere(), { marketplace }] };
  const [archived, inactive, outOfStock] = await Promise.all([
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { OR: [{ archived: true }, { status: "archived" }] },
        ],
      },
    }),
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { status: { contains: "inactive", mode: "insensitive" } },
        ],
      },
    }),
    prisma.warehouseProduct.count({
      where: {
        AND: [
          base,
          { status: "out_of_stock" },
        ],
      },
    }),
  ]);
  return { archived, inactive, outOfStock };
}

function warehousePagePostgresOrderBy() {
  return [
    { archived: "asc" },
    { status: "asc" },
    { offerId: "asc" },
    { marketplace: "asc" },
    { target: "asc" },
    { name: "asc" },
    { id: "asc" },
  ];
}

function warehousePageShouldUseLightBuild() {
  return process.env.WAREHOUSE_PAGE_LIGHT_BUILD !== "false"
    || Boolean(marketplaceMaintenanceRunning || manualWarehouseSyncPromise || dailySyncPromise);
}
