"use strict";
require("dotenv").config();
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

function sshExec(cmd, timeout = 60000) {
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
  const rows = await prisma.$queryRaw\`
    SELECT
      COUNT(*)::int FILTER (WHERE marketplace = 'ozon' AND archived = false) AS total_ozon,
      COUNT(*)::int FILTER (WHERE marketplace = 'ozon' AND archived = false AND images IS NULL) AS null_images,
      COUNT(*)::int FILTER (WHERE marketplace = 'ozon' AND archived = false AND images = '{}'::jsonb) AS empty_obj,
      COUNT(*)::int FILTER (WHERE marketplace = 'ozon' AND archived = false AND jsonb_typeof(images) = 'object' AND images != '{}'::jsonb AND images IS NOT NULL) AS has_images
    FROM warehouse_products
  \`;
  console.log(JSON.stringify(rows[0]));
  await prisma.$disconnect();
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  await sshPut(remoteScript, "/tmp/davidsklad-dbcheck.cjs");
  // Copy node_modules reference
  await sshExec("ln -sf /var/www/davidsklad/node_modules /tmp/node_modules 2>/dev/null; true");
  // Move script into project dir so it can resolve modules
  await sshExec("cp /tmp/davidsklad-dbcheck.cjs /var/www/davidsklad/davidsklad-dbcheck-tmp.cjs");
  const result = await sshExec("cd /var/www/davidsklad && node davidsklad-dbcheck-tmp.cjs; rm -f davidsklad-dbcheck-tmp.cjs /tmp/davidsklad-dbcheck.cjs");
  console.log("DB state:", result.trim());
}

main().catch((e) => { console.error(e.message); process.exit(1); });
