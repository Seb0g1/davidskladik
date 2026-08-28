// Добавляет DGIS_API_KEY в .env на сервере
// Запуск: DGIS_KEY=ваш_ключ node scripts/_tmp_set_dgis_key.cjs

const { Client } = require("ssh2");

const DGIS_KEY = process.env.DGIS_KEY;
if (!DGIS_KEY) {
  console.error("Укажите ключ: DGIS_KEY=ваш_ключ node scripts/_tmp_set_dgis_key.cjs");
  process.exit(1);
}

const conn = new Client();
conn.on("ready", () => {
  const ENV_FILE = "/var/www/davidsklad/davidskladik/.env";
  const cmd = [
    // Удалить старый ключ если есть
    `grep -v '^DGIS_API_KEY=' ${ENV_FILE} > /tmp/.env.tmp && mv /tmp/.env.tmp ${ENV_FILE}`,
    // Добавить новый
    `echo 'DGIS_API_KEY=${DGIS_KEY}' >> ${ENV_FILE}`,
    // Перезапустить
    `pm2 restart davidsklad-api --update-env`,
    // Проверить
    `sleep 2 && curl -sk 'http://localhost:3000/api/shop/delivery/pvz?city=%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0'`,
  ].join(" && ");

  conn.exec(cmd, (err, stream) => {
    let out = "";
    stream.on("data", (d) => (out += d));
    stream.stderr.on("data", (d) => (out += d));
    stream.on("close", () => {
      console.log(out);
      conn.end();
    });
  });
}).connect({
  host: "81.17.154.153",
  username: "root",
  password: process.env.DEPLOY_PASSWORD || "pm^e7-jVL_gAyM",
  readyTimeout: 20000,
});
