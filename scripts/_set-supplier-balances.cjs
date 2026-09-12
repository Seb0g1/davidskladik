#!/usr/bin/env node
"use strict";
// Устанавливает балансы поставщиков через API supplier-ledger/adjust.
// Запускается на сервере: node scripts/_set-supplier-balances.cjs [--run] [--skip-zero]
// Без --run — dry-run (только показывает текущий/целевой баланс, ничего не пишет).

const http = require("node:http");
const path = require("node:path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const port = Number(process.env.PORT || 3000) || 3000;
const host = "127.0.0.1";
const username = process.env.APP_USER || "admin";
const appPassword = process.env.APP_PASSWORD || "";
const dryRun = !process.argv.includes("--run");
const skipZero = process.argv.includes("--skip-zero");

if (!appPassword) { console.error("APP_PASSWORD missing in .env"); process.exit(1); }

// Целевые балансы поставщиков. Единица: USD.
// Источник: записи поставщиков 09-11-2026 (изображения).
// Отрицательный баланс = мы должны поставщику. Положительный = поставщик должен нам.
const SUPPLIER_TARGETS = [
  { name: "Селектив",         balance:     27    },
  { name: "Геворг",           balance:     19    },
  { name: "Ниш",              balance:     37.5  },
  { name: "Аллсент",          balance:     67.4  },
  { name: "Веста",            balance:     -8    },
  { name: "Бьюти парфюм",    balance:      0    },
  { name: "Клео",             balance:      0    },
  { name: "Гор",              balance:      0    },
  { name: "Слава",            balance:      0    },
  { name: "Вазген",           balance:     -2.45 },
  { name: "Хачаиурян",        balance:      1.44 },
  { name: "Тимур",            balance:      3.5  },
  { name: "Сергей 2216",      balance:      4    },
  { name: "Настя и марина",   balance:      0.1  },
  { name: "Зураб",            balance:      6    },
  { name: "Илья",             balance:      0    },
  { name: "Сёстры",           balance:     -0.81 },
  { name: "Депарфюм",         balance:      1.5  },
  { name: "Антонина",         balance:      8.8  },
  { name: "Авангард",         balance:     -3.95 },
  { name: "Маргарита",        balance:      0    },
  { name: "Ярик",             balance:      2    },
  { name: "Карен",            balance:    -13    },
  { name: "Ризо",             balance:     -6    },
  { name: "Руслан али",       balance:      2.8  },
  { name: "Санта",            balance:      0    },
  { name: "Евгений",          balance:      0    },
  { name: "Олег",             balance:     11.8  },
  { name: "Юля и вова",       balance:      3    },
  { name: "Тимофей",          balance:  -3907.7  },
  { name: "Юля лужа",         balance:      0    },
  { name: "Далик",            balance:    -21.5  },
  { name: "Армен",            balance:      0    },
  { name: "Станислав",        balance:      0    },
  { name: "Тигран",           balance:     28.3  },
  { name: "Вероника",         balance:      0    },
  { name: "Ваганов",          balance:      0    },
  { name: "Алекс",            balance:   -106    },
];

function httpReq(method, urlPath, { cookie = "", body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : "";
    const req = http.request({
      hostname: host, port, path: urlPath, method,
      headers: {
        ...(payload ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) } : {}),
        ...(cookie ? { Cookie: cookie } : {}),
        "Origin": `http://${host}:${port}`,
      },
    }, (res) => {
      let data = "";
      res.on("data", (c) => { data += c; });
      res.on("end", () => {
        let parsed = data;
        try { parsed = JSON.parse(data); } catch { /* raw */ }
        resolve({ status: res.statusCode, headers: res.headers, body: parsed });
      });
    });
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function sessionCookie(headers = {}) {
  const list = Array.isArray(headers["set-cookie"]) ? headers["set-cookie"] : [headers["set-cookie"]].filter(Boolean);
  const s = list.find((item) => String(item).includes("connect.sid=") || String(item).includes("pm_session="));
  return s ? String(s).split(";")[0] : "";
}

function sign(n) { return n > 0 ? `+${n}` : String(n); }

async function main() {
  console.log(`\n=== Балансы поставщиков | ${dryRun ? "DRY-RUN" : "ПРИМЕНЯЕМ"} ===`);
  console.log(`Подключение к ${host}:${port}...\n`);

  const lr = await httpReq("POST", "/api/login", { body: { username, password: appPassword } });
  const cookie = sessionCookie(lr.headers);
  if (!cookie || lr.status !== 200) {
    console.error("Ошибка входа:", lr.status, lr.body);
    process.exit(1);
  }
  console.log("✓ Вошли\n");

  const targets = skipZero
    ? SUPPLIER_TARGETS.filter((s) => s.balance !== 0)
    : SUPPLIER_TARGETS;

  let created = 0, skipped = 0, errors = 0;

  for (const s of targets) {
    try {
      if (dryRun) {
        const r = await httpReq("GET", `/api/supplier-ledger/summary?supplierName=${encodeURIComponent(s.name)}&status=active&period=all`, { cookie });
        const cur = r.body?.summary?.balance ?? "?";
        const delta = typeof cur === "number" ? sign(Math.round((s.balance - cur) * 100) / 100) : "?";
        console.log(`  ${s.name.padEnd(22)} cur=${String(cur).padStart(9)}  →  target=${String(s.balance).padStart(9)}  (delta ${delta})`);
        skipped++;
        continue;
      }

      const r = await httpReq("POST", "/api/supplier-ledger/adjust", {
        cookie,
        body: {
          supplierName: s.name,
          targetBalance: s.balance,
          currency: "USD",
          note: "Начальная корректировка баланса 2026-09-13",
        },
      });

      if (r.body?.skipped) {
        console.log(`  ✓ ${s.name}: уже ${s.balance} — пропуск`);
        skipped++;
      } else if (r.body?.ok) {
        console.log(`  ✅ ${s.name}: ${r.body.currentBalance} → ${r.body.targetBalance}  (delta ${sign(r.body.delta)} USD)`);
        created++;
      } else {
        console.error(`  ❌ ${s.name}: HTTP ${r.status} — ${JSON.stringify(r.body)}`);
        errors++;
      }
    } catch (err) {
      console.error(`  ❌ ${s.name}: ${err.message}`);
      errors++;
    }
  }

  console.log(`\nИтог: ${created} создано, ${skipped} пропущено, ${errors} ошибок`);
  if (dryRun) console.log("→ Запустите с --run чтобы применить.");
}

main().catch((e) => { console.error(e); process.exit(1); });
