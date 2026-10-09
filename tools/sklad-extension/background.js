// «→ Склад»: страница аромата из вкладки Фрагрантики уходит в открытую вкладку davidsklad.ru,
// где её принимает приложение (сообщение ds-fragrantica-ext) и загружает своим запросом с вашей сессией.
// Из конвейера склада можно загрузить все недостающие страницы: по одной, в фоновой вкладке, с паузами.
// Проверку Cloudflare расширение не проходит за вас: видит её — останавливается и показывает вкладку.

const SKLAD = "https://davidsklad.ru";
const PERFUME_PAGE = /^https:\/\/(www\.)?fragrantica\.[a-z.]+\/(perfume|parfum)\/.+-\d+\.html/i;
const PAUSE_MS = [8000, 14000];
const MAX_PER_RUN = 60;

function badge(text, color, tabId) {
  chrome.action.setBadgeBackgroundColor({ color, ...(tabId ? { tabId } : {}) });
  chrome.action.setBadgeText({ text, ...(tabId ? { tabId } : {}) });
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// runs in the Fragrantica tab: the page as the server would download it, without scripts and styles;
// a Cloudflare check or a page without a perfume is reported instead
async function readPerfumePage() {
  const title = document.title || "";
  if (/just a moment|attention required|один момент|проверка/i.test(title) || document.querySelector("#challenge-form, #cf-challenge-running, .cf-turnstile, iframe[src*='challenges.cloudflare.com']")) {
    return { challenge: true };
  }
  let html = "";
  try {
    const response = await fetch(location.href, { credentials: "include", cache: "force-cache" });
    html = response.ok ? await response.text() : "";
  } catch (e) { /* the page in the tab is used */ }
  if (!html) html = document.documentElement.outerHTML;
  if (!/itemprop="name"|pyramid|accord/i.test(html)) return { notPerfume: true, title };
  html = html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, "<svg></svg>")
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/\s{2,}/g, " ");
  return { url: location.href, html };
}

// runs in the sklad tab (page world): waits for the app's receiver, hands the page over, waits for its answer
function deliverToSklad(page, quiet) {
  return new Promise((resolve) => {
    const nonce = Math.random().toString(36).slice(2);
    const started = Date.now();
    const onMessage = (event) => {
      if (event.source !== window || event.data?.type !== "ds-fragrantica-ext-done" || event.data.nonce !== nonce) return;
      window.removeEventListener("message", onMessage);
      resolve({ ok: Boolean(event.data.ok), text: String(event.data.text || "") });
    };
    window.addEventListener("message", onMessage);
    const send = () => {
      if (window.__dsFragranticaReceiver) {
        window.postMessage({ type: "ds-fragrantica-ext", nonce, quiet: Boolean(quiet), url: page.url, html: page.html }, location.origin);
        setTimeout(() => { window.removeEventListener("message", onMessage); resolve({ ok: false, text: "Склад не ответил за 2 минуты" }); }, 120000);
      } else if (Date.now() - started > 30000) {
        window.removeEventListener("message", onMessage);
        resolve({ ok: false, text: "Склад не загрузился: войдите на davidsklad.ru и нажмите ещё раз" });
      } else {
        setTimeout(send, 300);
      }
    };
    send();
  });
}

function waitLoaded(tabId, timeoutMs = 45000) {
  return new Promise((resolve) => {
    let finished = false;
    const finish = (ok) => { if (finished) return; finished = true; chrome.tabs.onUpdated.removeListener(done); resolve(ok); };
    const done = (id, info) => { if (id === tabId && info.status === "complete") finish(true); };
    chrome.tabs.onUpdated.addListener(done);
    chrome.tabs.get(tabId, (tab) => { if (!chrome.runtime.lastError && tab?.status === "complete" && tab.url && tab.url !== "about:blank") finish(true); });
    setTimeout(() => finish(false), timeoutMs);
  });
}

async function skladTab() {
  const tabs = await chrome.tabs.query({ url: `${SKLAD}/app*` });
  if (tabs.length) return tabs[0];
  const tab = await chrome.tabs.create({ url: `${SKLAD}/app/fragrantica`, active: false });
  await waitLoaded(tab.id);
  return tab;
}

async function readTab(tabId) {
  const [{ result }] = await chrome.scripting.executeScript({ target: { tabId }, func: readPerfumePage });
  return result;
}

// ─── One page by the shortcut ───────────────────────────────────────────────

async function sendToSklad(tab) {
  if (!tab?.id || !PERFUME_PAGE.test(tab.url || "")) {
    badge("?", "#b45309", tab?.id);
    chrome.action.setTitle({ tabId: tab?.id, title: "Откройте страницу аромата на Фрагрантике и нажмите Ctrl+Shift+B" });
    return;
  }
  badge("…", "#2563eb", tab.id);
  try {
    const page = await readTab(tab.id);
    if (page.challenge || page.notPerfume) throw new Error(page.challenge ? "Фрагрантика показывает проверку — пройдите её" : "на странице нет аромата");
    const sklad = await skladTab();
    const [{ result }] = await chrome.scripting.executeScript({ target: { tabId: sklad.id }, world: "MAIN", func: deliverToSklad, args: [page, false] });
    badge(result.ok ? "✓" : "!", result.ok ? "#15803d" : "#b91c1c", tab.id);
    chrome.action.setTitle({ tabId: tab.id, title: result.text || "→ Склад" });
  } catch (error) {
    badge("!", "#b91c1c", tab.id);
    chrome.action.setTitle({ tabId: tab.id, title: `Не отправилось: ${error?.message || error}` });
  }
  setTimeout(() => chrome.action.setBadgeText({ text: "", tabId: tab.id }), 6000);
}

chrome.commands.onCommand.addListener(async (command, tab) => {
  if (command !== "send-to-sklad") return;
  sendToSklad(tab || (await chrome.tabs.query({ active: true, currentWindow: true }))[0]);
});
chrome.action.onClicked.addListener((tab) => sendToSklad(tab));

// ─── The conveyor's missing pages, one after another ────────────────────────

let run = null; // { items, done, ok, failed, current, skladTabId, workTabId, stopped, message }

function publicState() {
  if (!run) return { running: false, done: 0, total: 0, ok: 0, failed: 0 };
  return {
    running: !run.finished, done: run.done, total: run.items.length, ok: run.ok, failed: run.failed,
    current: run.current || "", message: run.message || "", waitingCheck: Boolean(run.waitingCheck),
  };
}
function report() {
  if (!run) return;
  chrome.tabs.sendMessage(run.skladTabId, { type: "progress", state: publicState() }).catch(() => {});
  badge(run.finished ? "" : `${run.done}/${run.items.length}`, "#2563eb");
}

async function runQueue() {
  const state = run;
  try {
    const work = await chrome.tabs.create({ url: "about:blank", active: false });
    state.workTabId = work.id;
    for (const item of state.items) {
      if (state.stopped) break;
      state.current = item.name || item.url;
      state.message = "";
      report();
      await chrome.tabs.update(state.workTabId, { url: item.url });
      await waitLoaded(state.workTabId);
      await sleep(1500);
      let page = await readTab(state.workTabId);
      if (page.challenge) {
        // a plain «Just a moment» passes by itself in a real browser; an interactive check is the user's
        for (let i = 0; i < 6 && page.challenge && !state.stopped; i += 1) { await sleep(3000); page = await readTab(state.workTabId); }
        if (page.challenge) {
          state.waitingCheck = true;
          state.message = "Фрагрантика просит пройти проверку. Пройдите её во вкладке — загрузка продолжится сама.";
          report();
          await chrome.tabs.update(state.workTabId, { active: true });
          for (let i = 0; i < 100 && page.challenge && !state.stopped; i += 1) { await sleep(3000); page = await readTab(state.workTabId).catch(() => ({ challenge: true })); }
          state.waitingCheck = false;
          if (page.challenge) { state.message = "Проверка не пройдена за 5 минут — остановлено."; break; }
        }
      }
      if (page.notPerfume) {
        state.failed += 1;
        state.message = `Нет аромата на странице: ${item.name || item.url}`;
      } else {
        const [{ result }] = await chrome.scripting.executeScript({ target: { tabId: state.skladTabId }, world: "MAIN", func: deliverToSklad, args: [page, true] });
        if (result.ok) state.ok += 1; else { state.failed += 1; state.message = result.text; }
      }
      state.done += 1;
      report();
      if (state.done < state.items.length && !state.stopped) await sleep(PAUSE_MS[0] + Math.random() * (PAUSE_MS[1] - PAUSE_MS[0]));
    }
  } catch (error) {
    state.message = `Остановлено: ${error?.message || error}`;
  } finally {
    if (state.workTabId) chrome.tabs.remove(state.workTabId).catch(() => {});
    state.finished = true;
    state.current = "";
    if (state.stopped && !state.message) state.message = "Остановлено.";
    report();
  }
}

chrome.runtime.onMessage.addListener((message, sender) => {
  if (!sender.tab?.id || !String(sender.tab.url || "").startsWith(SKLAD)) return;
  if (message?.type === "run") {
    if (run && !run.finished) { run.skladTabId = sender.tab.id; report(); return; }
    const seen = new Set();
    const items = (Array.isArray(message.items) ? message.items : [])
      .filter((x) => x && PERFUME_PAGE.test(String(x.url || "")) && !seen.has(x.url) && seen.add(x.url))
      .slice(0, MAX_PER_RUN)
      .map((x) => ({ url: String(x.url), name: String(x.name || "") }));
    if (!items.length) return;
    run = { items, done: 0, ok: 0, failed: 0, skladTabId: sender.tab.id };
    runQueue();
  } else if (message?.type === "stop") {
    if (run && !run.finished) { run.stopped = true; report(); }
  } else if (message?.type === "status") {
    if (run) { run.skladTabId = sender.tab.id; report(); }
  }
});
