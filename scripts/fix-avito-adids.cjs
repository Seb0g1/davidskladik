// Fix Avito expired adIds by appending -r1 suffix.
// Avito treats adIds with -r1 as new listings, bypassing the 30-day expiry.
// Usage: DEPLOY_PASSWORD=... node scripts/fix-avito-adids.cjs
require("dotenv").config();
const { Client } = require("ssh2");

const REMOTE_FILE = "/var/www/davidsklad/davidskladik/data/avito-listings.json";

async function run() {
  return new Promise((resolve, reject) => {
    const conn = new Client();
    conn.on("ready", () => {
      conn.sftp((err, sftp) => {
        if (err) { conn.end(); reject(err); return; }

        console.log("Reading remote file...");
        const chunks = [];
        const readStream = sftp.createReadStream(REMOTE_FILE);
        readStream.on("data", (chunk) => chunks.push(chunk));
        readStream.on("error", (e) => { conn.end(); reject(e); });
        readStream.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf-8");
          let data;
          try { data = JSON.parse(raw); } catch (e) { conn.end(); reject(new Error("JSON parse failed: " + e.message)); return; }

          const items = Array.isArray(data.items) ? data.items : [];
          console.log(`Total items: ${items.length}`);

          let changed = 0;
          let alreadyFixed = 0;
          const fixed = items.map((item) => {
            const adId = String(item.adId || "");
            if (!adId) return item;
            if (adId.endsWith("-r1")) { alreadyFixed++; return item; }
            changed++;
            return { ...item, adId: adId + "-r1" };
          });

          console.log(`Changed: ${changed}, already had -r1: ${alreadyFixed}`);
          if (!changed) { console.log("Nothing to change."); conn.end(); resolve(); return; }

          const newData = { ...data, items: fixed, updatedAt: new Date().toISOString() };
          const outJson = JSON.stringify(newData);
          console.log(`Writing ${(outJson.length / 1024 / 1024).toFixed(1)} MB...`);

          const writeStream = sftp.createWriteStream(REMOTE_FILE);
          writeStream.on("close", () => {
            console.log("Done. File written to server.");
            conn.end();
            resolve();
          });
          writeStream.on("error", (e) => { conn.end(); reject(e); });
          writeStream.end(outJson, "utf-8");
        });
      });
    });
    conn.on("error", reject);
    conn.connect({ host: "81.17.154.153", username: "root", password: process.env.DEPLOY_PASSWORD });
  });
}

run().then(() => process.exit(0)).catch((err) => { console.error(err); process.exit(1); });
