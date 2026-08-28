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

const remoteScript = `
"use strict";
const fs = require("fs");
const path = "/var/www/davidsklad/davidskladik/data/avito-listings.json";
const data = JSON.parse(fs.readFileSync(path, "utf8"));
const items = data.items || [];

function isWeak(title) {
  if (!title) return true;
  const t = title.trim();
  if (t.startsWith("#")) return true;
  if (/^[A-ZА-Я0-9#._\-]{1,40}$/i.test(t) && !/\s/.test(t)) return true;
  return false;
}

const weakTitle = items.filter(i => isWeak(i.title));
const noImages = items.filter(i => !i.imageUrls || !i.imageUrls.length);

console.log(JSON.stringify({
  total: items.length,
  weakTitleCount: weakTitle.length,
  noImagesCount: noImages.length,
  weakTitleSamples: weakTitle.slice(0, 15).map(i => ({ adId: i.adId, title: i.title, images: i.imageUrls?.length || 0, sourceProductId: i.sourceProductId })),
  noImagesSamples: noImages.slice(0, 15).map(i => ({ adId: i.adId, title: (i.title||"").slice(0,50), sourceProductId: i.sourceProductId })),
}));
`;

async function main() {
  const putResult = await new Promise((resolve, reject) => {
    const client = new Client();
    client.on("ready", () => {
      client.sftp((err, sftp) => {
        if (err) { client.end(); reject(err); return; }
        const stream = sftp.createWriteStream("/tmp/avito-title-check.cjs");
        stream.on("close", () => { client.end(); resolve(); });
        stream.on("error", e => { client.end(); reject(e); });
        stream.write(remoteScript);
        stream.end();
      });
    }).on("error", reject)
      .connect({ host: "81.17.154.153", port: 22, username: "root", password });
  });

  const result = await sshExec(`node /tmp/avito-title-check.cjs`);
  try {
    const data = JSON.parse(result);
    console.log(`Total listings: ${data.total}`);
    console.log(`Weak titles (article-like): ${data.weakTitleCount}`);
    console.log(`No images: ${data.noImagesCount}`);
    if (data.weakTitleSamples.length) {
      console.log("\nWeak title samples:");
      data.weakTitleSamples.forEach(s => console.log(`  ${s.adId}: "${s.title}" (${s.images} images) -> ${s.sourceProductId}`));
    }
    if (data.noImagesSamples.length) {
      console.log("\nNo images samples:");
      data.noImagesSamples.forEach(s => console.log(`  ${s.adId}: "${s.title}" -> ${s.sourceProductId}`));
    }
  } catch (e) { console.log("Raw:", result.slice(0, 500)); }
}

main().catch(e => { console.error(e.message); process.exit(1); });
