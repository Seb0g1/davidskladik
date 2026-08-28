// Проверяет последние заказы магазина в БД
const { Client } = require("ssh2");
const conn = new Client();

const SCRIPT = `
const { PrismaClient } = require('@prisma/client');
const p = new PrismaClient();
p.shopOrder.findMany({
  orderBy: { createdAt: 'desc' },
  take: 10,
  include: { customer: { select: { email: true } } }
}).then(orders => {
  console.log('Total in DB:', orders.length);
  orders.forEach(o => {
    const d = o.delivery || {};
    console.log(JSON.stringify({
      id: o.id, status: o.status, totalRub: o.totalRub,
      created: o.createdAt, email: o.customer?.email || d.email,
      city: d.city, type: d.type, pvz: d.address
    }));
  });
  p.$disconnect();
}).catch(e => { console.error(e.message); p.$disconnect(); });
`;

conn.on("ready", () => {
  const tmpFile = "/tmp/_check_orders.js";
  conn.exec(`echo '${SCRIPT.replace(/'/g, "'\\''")}' > ${tmpFile} && cd /var/www/davidsklad/davidskladik && node ${tmpFile}`, (err, stream) => {
    let out = "";
    stream.on("data", d => (out += d));
    stream.stderr.on("data", d => (out += d));
    stream.on("close", () => { console.log(out); conn.end(); });
  });
}).connect({
  host: "81.17.154.153", username: "root",
  password: process.env.DEPLOY_PASSWORD || "pm^e7-jVL_gAyM",
  readyTimeout: 30000,
});
