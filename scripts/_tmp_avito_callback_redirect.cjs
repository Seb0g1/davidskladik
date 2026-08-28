#!/usr/bin/env node
"use strict";
const { Client } = require("ssh2");

const password = process.env.DEPLOY_PASSWORD;
if (!password) { console.error("DEPLOY_PASSWORD required"); process.exit(1); }

const domain = "magicvibes.ru";
const nginxConf = `/etc/nginx/sites-available/${domain}`;

// Финальный nginx-конфиг с редиректом /avito/callback → davidsklad.ru
// Оригинальный конфиг слушает 6443 (за балансировщиком/Cloudflare), сохраняем это
const updatedConf = `server {
    listen 6443 ssl http2;
    server_name ${domain} www.${domain};

    ssl_certificate /etc/letsencrypt/live/${domain}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${domain}/privkey.pem;
    include /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;

    root /var/www/magicvibes;
    index index.html;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/xml application/xml image/svg+xml;
    gzip_min_length 1000;
    gzip_vary on;

    # Авито OAuth callback: редирект на davidsklad.ru (Авито не принял davidsklad.ru напрямую)
    location = /avito/callback {
        return 301 https://davidsklad.ru/avito/callback$is_args$args;
    }

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

function sftpWriteString(sftp, remote, content) {
  return new Promise((resolve, reject) => {
    const w = sftp.createWriteStream(remote);
    w.on("close", resolve);
    w.on("error", reject);
    w.end(content);
  });
}

async function main() {
  const conn = new Client();
  await new Promise((resolve, reject) =>
    conn.on("ready", resolve).on("error", reject).connect({
      host: "81.17.154.153", username: "root", password,
      readyTimeout: 30000, keepaliveInterval: 10000,
    })
  );

  try {
    // Показать текущий конфиг
    console.log("=== Текущий nginx конфиг ===");
    await exec(conn, `cat ${nginxConf}`);
    console.log("\n=== Записываю обновлённый конфиг ===");

    const sftp = await openSftp(conn);
    await sftpWriteString(sftp, nginxConf, updatedConf);
    console.log("✓ Конфиг записан");

    await exec(conn, "nginx -t");
    console.log("✓ nginx -t OK");

    await exec(conn, "systemctl reload nginx");
    console.log("✓ nginx перезагружен");

    // Проверка
    const result = await exec(conn, `curl -s -o /dev/null -w '%{http_code} %{redirect_url}' 'https://${domain}/avito/callback?code=test&state=abc'`);
    console.log("\nПроверка редиректа:", result);

    console.log("\n✓ ГОТОВО — https://magicvibes.ru/avito/callback теперь редиректит на davidsklad.ru");
    console.log("  Используй в Авито: https://magicvibes.ru/avito/callback");
  } finally {
    conn.end();
  }
}

main().catch(e => { console.error(e.message); process.exit(1); });
