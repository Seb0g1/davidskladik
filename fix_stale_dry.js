require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const prisma = new PrismaClient({ log: [] });
async function main() {
  // Check pm_snapshot_items column types and a sample row
  const sample = await prisma.$queryRawUnsafe(
    "SELECT row_id, article, partner_id, native_name, price, active, doc_date FROM pm_snapshot_items LIMIT 3"
  );
  process.stdout.write("PM sample: " + JSON.stringify(sample) + "\n");

  // Check column types
  const types = await prisma.$queryRawUnsafe(
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='pm_snapshot_items' ORDER BY ordinal_position"
  );
  process.stdout.write("PM types: " + types.map(c => c.column_name + "=" + c.data_type).join(", ") + "\n");
}
main().catch(e => process.stdout.write("ERR: " + e.message + "\n")).finally(() => prisma.$disconnect().catch(() => {}));
