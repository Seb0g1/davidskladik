"use strict";
const { PrismaClient } = require("../node_modules/@prisma/client");
const prisma = new PrismaClient();

async function run() {
  const rows = await prisma.appSetting.findMany({ where: { key: { startsWith: "picker_balance:" } } });
  let fixed = 0;
  for (const r of rows) {
    const before = r.value?.credits || [];
    const after = before.filter((x) => !String(x.id || "").startsWith("picking:"));
    if (after.length < before.length) {
      await prisma.appSetting.update({ where: { key: r.key }, data: { value: { credits: after } } });
      console.log(`Cleaned ${r.key}: removed ${before.length - after.length} picking credits`);
      fixed++;
    }
  }
  console.log(`Done. Fixed ${fixed} balance(s).`);
  await prisma.$disconnect();
}

run().catch((e) => { console.error(e); process.exit(1); });
