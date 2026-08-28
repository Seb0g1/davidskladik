"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 120000) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    let output = "";
    const timer = setTimeout(() => { client.end(); reject(new Error("SSH timeout")); }, timeout);
    client.on("ready", () => {
      client.exec(cmd, (err, stream) => {
        if (err) { clearTimeout(timer); client.end(); reject(err); return; }
        stream.on("data", (d) => { output += d; });
        stream.stderr.on("data", (d) => { output += d; });
        stream.on("close", () => { clearTimeout(timer); client.end(); resolve(output); });
      });
    }).on("error", (e) => { clearTimeout(timer); reject(e); })
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

function sshPut(content, remotePath) {
  return new Promise((resolve, reject) => {
    const client = new Client();
    client.on("ready", () => {
      client.sftp((err, sftp) => {
        if (err) { client.end(); reject(err); return; }
        const stream = sftp.createWriteStream(remotePath);
        stream.on("close", () => { client.end(); resolve(); });
        stream.on("error", (e) => { client.end(); reject(e); });
        stream.write(content);
        stream.end();
      });
    }).on("error", reject)
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });
}

const remoteScript = `
"use strict";
const { PrismaClient } = require("@prisma/client");
const prisma = new PrismaClient();
async function main() {
  // Get second account products (target = ozon-3d10ec43)
  const rows = await prisma.$queryRaw\`
    SELECT
      id,
      offer_id AS "offerId",
      name,
      target,
      status,
      archived,
      raw->'marketplaceState' AS marketplace_state,
      (SELECT COUNT(*) FROM product_links WHERE product_id = wp.id)::int AS link_count
    FROM warehouse_products wp
    WHERE target = 'ozon-3d10ec43'
      AND archived = false
    ORDER BY (SELECT COUNT(*) FROM product_links WHERE product_id = wp.id) DESC
    LIMIT 10
  \`;
  console.log("=== Second Ozon account products (top 10 by link count) ===");
  for (const r of rows) {
    const ms = r.marketplace_state;
    const warehouses = ms?.warehouses || [];
    console.log(JSON.stringify({
      id: r.id,
      offerId: r.offerId,
      name: r.name?.slice(0, 40),
      status: r.status,
      links: r.link_count,
      msCode: ms?.code,
      msStock: ms?.stock,
      warehouseCount: warehouses.length,
      warehouseIds: warehouses.map(w => w.warehouseId || w.warehouse_id).filter(Boolean)
    }));
  }

  // Stats
  const stats = await prisma.$queryRaw\`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE (SELECT COUNT(*) FROM product_links WHERE product_id = wp.id) > 0)::int AS with_links,
      COUNT(*) FILTER (WHERE raw->'marketplaceState'->'warehouses' IS NULL OR raw->'marketplaceState'->'warehouses' = '[]'::jsonb)::int AS empty_warehouses,
      COUNT(*) FILTER (WHERE raw->'marketplaceState'->'warehouses' IS NOT NULL AND raw->'marketplaceState'->'warehouses' != '[]'::jsonb)::int AS has_warehouses
    FROM warehouse_products wp
    WHERE target = 'ozon-3d10ec43'
      AND archived = false
  \`;
  console.log("\\n=== Stats ===");
  console.log(JSON.stringify(stats[0]));

  await prisma.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  console.log("Uploading diagnostic script...");
  await sshPut(remoteScript, "/tmp/ozon2-diag.cjs");
  await sshExec("cp /tmp/ozon2-diag.cjs /var/www/davidsklad/davidskladik/ozon2-diag-tmp.cjs");
  console.log("Running...");
  const result = await sshExec("cd /var/www/davidsklad/davidskladik && node ozon2-diag-tmp.cjs; rm -f ozon2-diag-tmp.cjs /tmp/ozon2-diag.cjs", 60000);
  console.log(result);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
