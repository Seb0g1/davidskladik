#!/usr/bin/env node
"use strict";
require("dotenv").config();
const { Client } = require("ssh2");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

const password = process.env.DEPLOY_PASSWORD;
const defaultKeyPath = path.join(os.homedir(), ".ssh", "davidsklad_deploy");
const sshKeyPath = fs.existsSync(defaultKeyPath) ? defaultKeyPath : null;
const privateKey = sshKeyPath ? fs.readFileSync(sshKeyPath) : null;

const DB_URL = process.env.DATABASE_URL || "postgresql://davidsklad:17danj17@localhost:5432/davidsklad";
// Parse DATABASE_URL
const url = new URL(DB_URL.replace("postgresql://", "http://"));
const dbUser = url.username;
const dbPass = url.password;
const dbName = url.pathname.slice(1);
const dbHost = url.hostname === "81.17.154.153" ? "localhost" : url.hostname;

const SQL = `
DELETE FROM supplier_ledger_entries;
DELETE FROM supplier_picking_rows;
DELETE FROM supplier_cart_draft_rows;
DELETE FROM supplier_cart_drafts;
DELETE FROM finance_orders;
DELETE FROM finance_expenses;
SELECT 'supplier_ledger_entries' AS tbl, COUNT(*) AS remaining FROM supplier_ledger_entries
UNION ALL SELECT 'supplier_picking_rows', COUNT(*) FROM supplier_picking_rows
UNION ALL SELECT 'supplier_cart_drafts', COUNT(*) FROM supplier_cart_drafts
UNION ALL SELECT 'finance_orders', COUNT(*) FROM finance_orders
UNION ALL SELECT 'finance_expenses', COUNT(*) FROM finance_expenses;
`;

const command = `PGPASSWORD='${dbPass}' psql -h ${dbHost} -U ${dbUser} -d ${dbName} -c "${SQL.replace(/\n/g, " ").replace(/"/g, '\\"')}"`;

const conn = new Client();

async function main() {
  await new Promise((resolve, reject) => {
    const cfg = { host: "81.17.154.153", username: "root", readyTimeout: 30000 };
    if (privateKey) {
      cfg.privateKey = privateKey;
    } else if (password) {
      cfg.password = password;
    } else {
      throw new Error("Нужен DEPLOY_PASSWORD или SSH ключ");
    }
    conn.on("ready", resolve).on("error", reject).connect(cfg);
  });

  console.log("✓ SSH подключен");

  await new Promise((resolve, reject) => {
    conn.exec(command, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", (d) => process.stdout.write(d));
      stream.stderr.on("data", (d) => process.stderr.write(d));
      stream.on("close", (code) => {
        conn.end();
        code ? reject(new Error(`psql exit ${code}`)) : resolve();
      });
    });
  });

  console.log("\n✅ Все данные сброшены.");
}

main().catch((err) => {
  console.error("Ошибка:", err.message);
  process.exit(1);
});
