"use strict";
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();

async function main() {
  const offerId = process.argv[2] || "234123";

  console.log("ENV: OZON_MARKUP=" + (process.env.DEFAULT_OZON_MARKUP||"?") + " YANDEX_MARKUP=" + (process.env.DEFAULT_YANDEX_MARKUP||"?") + " USD_RATE=" + (process.env.DEFAULT_USD_RATE||"?"));

  const products = await p.warehouseProduct.findMany({
    where: { OR: [{ offerId }, { productId: offerId }] },
    select: { id: true, offerId: true, name: true, raw: true },
  });
  if (!products.length) { console.log("not found"); return; }

  for (const product of products) {
    const raw = product.raw || {};
    const tag = product.id.startsWith("ozon-") ? "OZON" : product.id.startsWith("yandex-") ? "YANDEX" : "?";
    console.log("\n=== [" + tag + "] markup=" + raw.markup + " ozonM=" + raw.ozonMarkup + " yaM=" + raw.yandexMarkup);

    // Check pm_snapshot WITHOUT active filter
    const links = await p.productLink.findMany({ where: { productId: product.id }, select: { supplierArticle: true, raw: true } });
    for (const link of links) {
      const lr = link.raw || {};
      const pmAll = await p.priceMasterSnapshotItem.findMany({
        where: { article: link.supplierArticle },
        select: { nativeName: true, price: true, currency: true, active: true, docDate: true, partnerName: true },
        orderBy: [{ docDate: "desc" }],
        take: 5,
      });
      console.log("  PM article=" + link.supplierArticle + " (all, no active filter): " + pmAll.length + " rows");
      pmAll.forEach(r => console.log("    " + r.nativeName + " | " + r.partnerName + " | price=" + r.price + " " + (r.currency||"USD") + " active=" + r.active + " " + (r.docDate ? r.docDate.toISOString().slice(0,10) : "")));

      const rpm = lr.resolvedPriceMasterRow;
      if (rpm) console.log("  resolvedPM (cached link): price=" + rpm.price + " " + (rpm.priceCurrency||rpm.currency||"USD") + " docDate=" + rpm.docDate);
    }

    // Latest price history with errors
    const hist = await p.priceHistory.findMany({
      where: { productId: product.id },
      orderBy: { createdAt: "desc" },
      take: 4,
      select: { createdAt: true, newPrice: true, oldPrice: true, marketplace: true, status: true, error: true, response: true },
    });
    hist.forEach(h => {
      const dt = h.createdAt ? h.createdAt.toISOString().slice(0,16) : "?";
      const err = h.error ? h.error.slice(0,120) : "";
      const resp = h.response ? JSON.stringify(h.response).slice(0,120) : "";
      console.log("  [" + dt + "] " + h.marketplace + " old=" + h.oldPrice + " new=" + h.newPrice + " " + h.status);
      if (err) console.log("    error: " + err);
      if (resp && h.status !== "success") console.log("    response: " + resp);
    });
  }
  await p.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
