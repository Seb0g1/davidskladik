"use strict";
require("dotenv").config();
const http = require("http");
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const DRY_RUN = !process.argv.includes("--apply");
const TNVED_CODE = "3303001000";

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

const APP_USER = process.env.APP_USER || "david";
const APP_PASSWORD = process.env.APP_PASSWORD || "CGJ-Ge-90";

const remoteScript = `
"use strict";
const http = require("http");

const APP_USER = ${JSON.stringify(APP_USER)};
const APP_PASSWORD = ${JSON.stringify(APP_PASSWORD)};
const DRY_RUN = ${JSON.stringify(DRY_RUN)};
const TNVED_CODE = ${JSON.stringify(TNVED_CODE)};

function apiPost(path, body, cookie) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request({
      hostname: "127.0.0.1",
      port: 3000,
      path,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data),
        ...(cookie ? { Cookie: cookie } : {}),
      },
    }, (res) => {
      let out = "";
      res.on("data", (d) => { out += d; });
      res.on("end", () => {
        try { resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(out) }); }
        catch { resolve({ status: res.statusCode, headers: res.headers, body: out }); }
      });
    });
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

async function main() {
  // 1. Login
  const login = await apiPost("/api/login", { username: APP_USER, password: APP_PASSWORD });
  if (login.status !== 200) {
    console.error("Login failed:", login.status, JSON.stringify(login.body));
    process.exit(1);
  }
  const setCookie = login.headers["set-cookie"];
  const cookie = Array.isArray(setCookie) ? setCookie.map(c => c.split(";")[0]).join("; ") : (setCookie || "").split(";")[0];
  console.log("Logged in as", APP_USER);

  // 2. Trigger backfill-tnved — sets TNVED 3303001000 + clears marking requirement
  console.log("\\n=== Запускаем backfill-tnved: код " + TNVED_CODE + " (духи), dryRun=" + DRY_RUN + " ===");
  const result = await apiPost("/api/ozon/attributes/backfill-tnved", {
    tnvedCode: TNVED_CODE,
    dryRun: DRY_RUN,
    accountId: "ozon",
  }, cookie);
  console.log("Status:", result.status);
  console.log(JSON.stringify(result.body, null, 2));

  if (!DRY_RUN && result.body?.ok) {
    console.log("\\n=== Итог ===");
    console.log("Обновлено:", result.body.updated, "/", result.body.total);
    console.log("Пропущено (нет в словаре):", result.body.skippedNoDictEntry);
    console.log("Ошибок:", result.body.errors || 0);
  }
}
main().catch(e => { console.error(e.message); process.exit(1); });
`;

async function main() {
  if (DRY_RUN) {
    console.log("DRY RUN — показывает кандидатов без реального обновления.");
    console.log("Для реального применения добавьте флаг --apply");
  } else {
    console.log("ПРИМЕНЯЕМ: ТН ВЭД 3303001000 (духи) + снятие маркировки на всех товарах Ozon");
  }
  console.log();

  await sshPut(remoteScript, "/tmp/ozon-tnved-perfume.cjs");
  await sshExec("cp /tmp/ozon-tnved-perfume.cjs /var/www/davidsklad/davidskladik/ozon-tnved-perfume-tmp.cjs");
  const result = await sshExec(
    "cd /var/www/davidsklad/davidskladik && node ozon-tnved-perfume-tmp.cjs; rm -f ozon-tnved-perfume-tmp.cjs /tmp/ozon-tnved-perfume.cjs",
    300000,
  );
  console.log(result);
}

main().catch((e) => { console.error(e.message); process.exit(1); });
