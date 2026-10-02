// Докачка страниц ароматов Фрагрантики с домашнего ПК: curl → локальный парсер (тот же, что на сервере)
// → пачки готовых данных на прод через ssh (страницы на сервер не грузим — только JSON).
// Медленно и аккуратно: ~1 страница за 4 с, на 403/проверку Cloudflare — пауза 30 мин и больше.
const { execFileSync, spawnSync } = require("child_process");
const fs = require("fs");
const vm = require("vm");

const REPO = "C:/Users/Seb0g1/Documents/davidsklad-fragrantica";
const SSH = ["-i", `${process.env.HOME || process.env.USERPROFILE}/.ssh/davidsklad_deploy`, "-o", "ConnectTimeout=30", "-o", "ServerAliveInterval=15", "root@81.17.154.153"];
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36";
const LIMIT = Number(process.argv[2]) || Infinity; // сколько страниц за запуск
const BATCH = 40;
const WATCH = process.argv.includes("--watch"); // не выходить, а раз в 30 мин проверять новые товары PriceMaster
const STATUS = process.env.CLAUDE_JOB_DIR ? `${process.env.CLAUDE_JOB_DIR}/tmp/home/status.json` : "status.json";

const ctx = vm.createContext({ console, URL });
vm.runInContext(fs.readFileSync(`${REPO}/server/parts/02a-fragrantica-catalog-parse.js`, "utf8"), ctx);
const parse = vm.runInContext("parseFragranticaPerfumePage", ctx);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(5, 19).replace("T", " "), ...a);

function ssh(cmd, input) {
  for (let attempt = 1; attempt <= 5; attempt++) {
    const r = spawnSync("ssh", [...SSH, cmd], { input, encoding: "utf8", maxBuffer: 64 << 20, timeout: 180_000 });
    if (r.status === 0) return r.stdout;
    log("ssh failed", attempt, (r.stderr || r.error?.message || "").slice(0, 200));
    spawnSync("sleep", [String(30 * attempt)]);
  }
  throw new Error("ssh failed 5 times");
}

function curl(url) {
  const r = spawnSync("curl", ["-s", "--compressed", "-A", UA, "-H", "Accept-Language: ru-RU,ru;q=0.9", "-L", "--max-time", "40", "-w", "\n__STATUS__%{http_code}", url], { encoding: "utf8", maxBuffer: 32 << 20 });
  const out = r.stdout || "";
  const i = out.lastIndexOf("\n__STATUS__");
  return { status: i >= 0 ? Number(out.slice(i + 11)) : 0, html: i >= 0 ? out.slice(0, i) : out };
}

(async () => {
  let done = 0, saved = 0, failed = 0, banPause = 30 * 60_000;
  let details = [], errors = [];
  const flush = () => {
    if (!details.length && !errors.length) return;
    const res = ssh("node /root/frag-home-import.cjs save", JSON.stringify({ details, errors }));
    saved += details.length;
    log("saved batch", res.trim());
    details = []; errors = [];
  };
  while (done < LIMIT) {
    const { stats, items } = JSON.parse(ssh(`node /root/frag-home-import.cjs queue ${Math.min(400, LIMIT - done, BATCH * 10)}`));
    log("queue", JSON.stringify(stats), "got", items.length);
    if (!items.length) {
      // Всё скачано — ждём новых совпадений с PriceMaster (сверка на сервере раз в 2 ч).
      if (!WATCH) break;
      fs.writeFileSync(STATUS, JSON.stringify({ at: new Date().toISOString(), done, saved, failed, idle: true }));
      await sleep(30 * 60_000);
      continue;
    }
    for (const item of items) {
      const { status, html } = curl(item.url);
      const challenged = status === 403 || status === 429 || /cf-chl|Just a moment|challenge-platform/i.test(html.slice(0, 20000));
      if (challenged) {
        flush();
        log(`Cloudflare ${status}: pause ${Math.round(banPause / 60000)} min`);
        fs.writeFileSync(STATUS, JSON.stringify({ at: new Date().toISOString(), done, saved, failed, pausedUntil: new Date(Date.now() + banPause).toISOString() }));
        await sleep(banPause);
        banPause = Math.min(6 * 3_600_000, banPause * 2);
        break; // заново взять очередь
      }
      banPause = 30 * 60_000;
      done += 1;
      if (status === 404 || status === 410) {
        errors.push({ id: item.id, message: `Фрагрантика ответила ${status}` });
        failed += 1;
      } else if (status !== 200) {
        log("skip", item.id, status); // 5xx и т. п. — останется в очереди
      } else {
        try {
          const d = parse(html, { url: item.url });
          if (!d.id || !d.name) throw new Error("Не удалось разобрать страницу аромата.");
          if (Number(d.id) !== item.id) throw new Error(`Страница отдала другой аромат (${d.id})`);
          details.push(JSON.parse(JSON.stringify(d)));
        } catch (e) {
          errors.push({ id: item.id, message: e.message });
          failed += 1;
        }
      }
      if (details.length + errors.length >= BATCH) flush();
      if (done % 20 === 0) fs.writeFileSync(STATUS, JSON.stringify({ at: new Date().toISOString(), done, saved, failed }));
      await sleep(2500 + Math.random() * 3000);
      if (done >= LIMIT) break;
    }
  }
  flush();
  log("finished", JSON.stringify({ done, saved, failed }));
})().catch((e) => { log("FATAL", e.stack || e.message); process.exit(1); });
