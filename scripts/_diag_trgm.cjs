"use strict";
const { PrismaClient } = require("@prisma/client");
const p = new PrismaClient();
async function main() {
  try {
    const r1 = await p.$queryRawUnsafe("SELECT word_similarity('chanell', 'chanel no 5 edp 50ml') AS ws, similarity('chanell', 'chanel') AS s");
    console.log("word_similarity available:", JSON.stringify(r1));
  } catch (e) {
    console.error("word_similarity error:", e.message);
    try {
      const r2 = await p.$queryRawUnsafe("SELECT similarity('chanell', 'chanel') AS s");
      console.log("similarity() works:", JSON.stringify(r2));
    } catch (e2) {
      console.error("similarity() also failed:", e2.message);
    }
  }
  try {
    const r3 = await p.$queryRawUnsafe("SELECT id, native_name FROM pm_snapshot_items WHERE word_similarity('chanell', native_name) > 0.4 ORDER BY word_similarity('chanell', native_name) DESC LIMIT 5");
    console.log("fuzzy query result:", r3.length, "rows");
    r3.forEach(r => console.log(" -", r.native_name));
  } catch (e) {
    console.error("fuzzy query error:", e.message);
  }
  await p.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
