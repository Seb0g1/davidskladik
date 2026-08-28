#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");
const fs = require("fs");
const path = require("path");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const shopDist = path.join(__dirname, "../shop/dist");
const remoteBase = "/var/www/magicvibes";
const domain = "magicvibes.ru";

// Nginx slушает 6443 — stream-модуль маршрутизирует magicvibes.ru → порт 6443
const nginxConf = `server {
    listen 6443 ssl http2;
    server_name ${domain} www.${domain};

    ssl_certificate /etc/letsencrypt/live/${domain}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${domain}/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    root ${remoteBase};
    index index.html;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml image/svg+xml;
    gzip_min_length 1000;
    gzip_vary on;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /assets/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
`;

function exec(conn, cmd) {
  return new Promise((resolve, reject) => {
    let out = "";
    conn.exec(cmd, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => { out += d; process.stdout.write(d); });
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => (code ? reject(new Error("exit " + code)) : resolve(out)));
    });
  });
}

function openSftp(conn) {
  return new Promise((resolve, reject) => conn.sftp((e, s) => (e ? reject(e) : resolve(s))));
}

function sftpPut(sftp, local, remote) {
  return new Promise((resolve, reject) => {
    const r = fs.createReadStream(local);
    const w = sftp.createWriteStream(remote);
    w.on("close", resolve);
    w.on("error", reject);
    r.on("error", reject);
    r.pipe(w);
  });
}

function sftpWriteString(sftp, remote, content) {
  return new Promise((resolve, reject) => {
    const w = sftp.createWriteStream(remote);
    w.on("close", resolve);
    w.on("error", reject);
    w.end(content);
  });
}

function walkDir(dir) {
  const result = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) result.push(...walkDir(full));
    else result.push(full);
  }
  return result;
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password,
      readyTimeout: 60000, keepaliveInterval: 10000,
    })
  );

  try {
    const files = walkDir(shopDist);
    const dirs = new Set([remoteBase]);
    for (const f of files) {
      const rel = path.relative(shopDist, path.dirname(f)).replace(/\\/g, "/");
      if (rel && rel !== ".") dirs.add(remoteBase + "/" + rel);
    }
    await exec(conn, "mkdir -p " + Array.from(dirs).join(" "));
    console.log("✓ Directories ready");

    const sftp = await openSftp(conn);
    let count = 0;
    for (const local of files) {
      const rel = path.relative(shopDist, local).replace(/\\/g, "/");
      await sftpPut(sftp, local, remoteBase + "/" + rel);
      process.stdout.write(".");
      count++;
    }
    console.log("\n✓ Uploaded " + count + " files");

    await sftpWriteString(sftp, `/etc/nginx/sites-available/${domain}`, nginxConf);
    console.log("✓ nginx config written (listen 6443 ssl)");

    await exec(conn, [
      `ln -sf /etc/nginx/sites-available/${domain} /etc/nginx/sites-enabled/${domain}`,
      "nginx -t",
      "systemctl reload nginx",
    ].join(" && "));
    console.log("✓ nginx reloaded");

    // Проверка через прямой порт
    const out = await exec(conn, `curl -sk -o /dev/null -w '%{http_code}' https://localhost:6443/ -H 'Host: ${domain}'`);
    console.log("\nHTTP status (port 6443):", out.trim());
    console.log("✓ DONE — https://" + domain);
  } finally {
    conn.end();
  }
}

main().catch(e => { console.error("FAIL:", e.message); process.exit(1); });
