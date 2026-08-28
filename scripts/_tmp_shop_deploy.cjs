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

const nginxConf = `server {
    listen 80;
    server_name ${domain} www.${domain};
    return 301 https://$host$request_uri;
}

server {
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

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /assets/ {
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    # CORS для API: запросы /api/ идут на davidsklad.ru — не нужен обратный прокси,
    # фронтенд обращается напрямую к davidsklad.ru
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
    // 1. Создать директорию веб-корня
    const files = walkDir(shopDist);
    const dirs = new Set([remoteBase]);
    for (const f of files) {
      const rel = path.relative(shopDist, path.dirname(f)).replace(/\\/g, "/");
      if (rel && rel !== ".") dirs.add(remoteBase + "/" + rel);
    }
    await exec(conn, "mkdir -p " + Array.from(dirs).join(" "));
    console.log("✓ Directories created");

    // 2. Загрузить dist/
    const sftp = await openSftp(conn);
    let count = 0;
    for (const local of files) {
      const rel = path.relative(shopDist, local).replace(/\\/g, "/");
      await sftpPut(sftp, local, remoteBase + "/" + rel);
      process.stdout.write(".");
      count++;
    }
    console.log("\n✓ Uploaded " + count + " files");

    // 3. Записать nginx-конфиг (временно без SSL — certbot добавит)
    const nginxNoSsl = `server {
    listen 80;
    server_name ${domain} www.${domain};

    root ${remoteBase};
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
`;
    await sftpWriteString(sftp, `/etc/nginx/sites-available/${domain}`, nginxNoSsl);
    console.log("✓ nginx config written (HTTP only for certbot)");

    await exec(conn, [
      `ln -sf /etc/nginx/sites-available/${domain} /etc/nginx/sites-enabled/${domain}`,
      "nginx -t",
      "systemctl reload nginx",
    ].join(" && "));
    console.log("✓ nginx reloaded");

    // 4. Получить SSL-сертификат через certbot
    console.log("Getting SSL certificate (may take ~30s)...");
    await exec(conn, `certbot --nginx -d ${domain} -d www.${domain} --non-interactive --agree-tos -m seboggame@gmail.com --redirect`);
    console.log("✓ SSL certificate obtained");

    // 5. Заменить nginx-конфиг на финальный с кастомными настройками (кэш ассетов)
    await sftpWriteString(sftp, `/etc/nginx/sites-available/${domain}`, nginxConf);
    await exec(conn, "nginx -t && systemctl reload nginx");
    console.log("✓ Final nginx config applied");

    // 6. Прописать SHOP_ORIGINS в .env прода
    await exec(conn, [
      `cd /var/www/davidsklad/davidskladik`,
      `grep -q SHOP_ORIGINS .env && sed -i 's|^SHOP_ORIGINS=.*|SHOP_ORIGINS=https://${domain}|' .env || echo 'SHOP_ORIGINS=https://${domain}' >> .env`,
      `grep -q DEFAULT_SHOP_MARKUP .env && echo 'DEFAULT_SHOP_MARKUP already set' || echo 'DEFAULT_SHOP_MARKUP=2.2' >> .env`,
      `pm2 reload davidsklad-api --update-env`,
    ].join(" && "));
    console.log("✓ SHOP_ORIGINS set + api reloaded");

    // 7. Проверка
    await exec(conn, `sleep 3 && curl -s -o /dev/null -w 'magicvibes.ru HTTP: %{http_code}' https://${domain}/ || curl -s -o /dev/null -w 'HTTP: %{http_code}' http://${domain}/`);
    console.log("\n\n✓ DONE — магазин доступен на https://" + domain);

  } finally {
    conn.end();
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
