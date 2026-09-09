require("dotenv").config({ path: "/var/www/davidsklad/davidskladik/.env" });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const prisma = new PrismaClient({ log: [] });
async function main() {
  const tables = await prisma.$queryRawUnsafe("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name");
  process.stdout.write("Tables: " + tables.map(t => t.table_name).join(", ") + "\n");
  const cols = await prisma.$queryRawUnsafe("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='product_links' ORDER BY ordinal_position");
  process.stdout.write("product_links cols: " + cols.map(c => c.column_name + "(" + c.data_type + ")").join(", ") + "\n");
  const pmCols = await prisma.$queryRawUnsafe("SELECT column_name FROM information_schema.columns WHERE table_name='pm_snapshot_items' ORDER BY ordinal_position").catch(() => []);
  process.stdout.write("pm_snapshot_items cols: " + (pmCols.length ? pmCols.map(c => c.column_name).join(", ") : "TABLE NOT FOUND") + "\n");
}
main().catch(e => process.stdout.write("ERR: " + e.message.slice(0, 300) + "\n")).finally(() => prisma.$disconnect().catch(() => {}));
