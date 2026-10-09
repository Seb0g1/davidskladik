// «→ Склад»: страница аромата из вкладки Фрагрантики уходит в открытую вкладку davidsklad.ru,
// где её принимает приложение (сообщение ds-fragrantica-ext) и загружает своим запросом с вашей сессией.

const SKLAD = "https://davidsklad.ru";
const PERFUME_PAGE = /^https:\/\/(www\.)?fragrantica\.[a-z.]+\/(perfume|parfum)\/.+-\d+\.html/i;

function badge(text, color, tabId) {
  chrome.action.setBadgeBackgroundColor({ color, tabId });
  chrome.action.setBadgeText({ text, tabId });
  setTimeout(() => chrome.action.setBadgeText({ text: "", tabId }), 6000);
}

// runs in the Fragrantica tab: the page as the server would download it, without scripts and styles
async function readPerfumePage() {
  let html = "";
  try {
    const response = await fetch(location.href, { credentials: "include" });
    html = response.ok ? await response.text() : "";
  } catch (e) { /* the page in the tab is used */ }
  if (!html) html = document.documentElement.outerHTML;
  html = html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, "")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, "")
    .replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi, "<svg></svg>")
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/\s{2,}/g, " ");
  return { url: location.href, html };
}

// runs in the sklad tab (page world): waits for the app's receiver, hands the page over, waits for its answer
function deliverToSklad(page) {
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
        window.postMessage({ type: "ds-fragrantica-ext", nonce, url: page.url, html: page.html }, location.origin);
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

function waitLoaded(tabId) {
  return new Promise((resolve) => {
    const done = (id, info) => {
      if (id === tabId && info.status === "complete") { chrome.tabs.onUpdated.removeListener(done); resolve(); }
    };
    chrome.tabs.onUpdated.addListener(done);
    chrome.tabs.get(tabId, (tab) => { if (tab?.status === "complete") { chrome.tabs.onUpdated.removeListener(done); resolve(); } });
  });
}

async function skladTab() {
  const tabs = await chrome.tabs.query({ url: `${SKLAD}/app*` });
  if (tabs.length) return tabs[0];
  const tab = await chrome.tabs.create({ url: `${SKLAD}/app/fragrantica`, active: false });
  await waitLoaded(tab.id);
  return tab;
}

async function sendToSklad(tab) {
  if (!tab?.id || !PERFUME_PAGE.test(tab.url || "")) {
    badge("?", "#b45309", tab?.id);
    chrome.action.setTitle({ tabId: tab?.id, title: "Откройте страницу аромата на Фрагрантике и нажмите Ctrl+Shift+B" });
    return;
  }
  badge("…", "#2563eb", tab.id);
  try {
    const [{ result: page }] = await chrome.scripting.executeScript({ target: { tabId: tab.id }, func: readPerfumePage });
    const sklad = await skladTab();
    const [{ result }] = await chrome.scripting.executeScript({ target: { tabId: sklad.id }, world: "MAIN", func: deliverToSklad, args: [page] });
    badge(result.ok ? "✓" : "!", result.ok ? "#15803d" : "#b91c1c", tab.id);
    chrome.action.setTitle({ tabId: tab.id, title: result.text || "→ Склад" });
  } catch (error) {
    badge("!", "#b91c1c", tab.id);
    chrome.action.setTitle({ tabId: tab.id, title: `Не отправилось: ${error?.message || error}` });
  }
}

chrome.commands.onCommand.addListener(async (command, tab) => {
  if (command !== "send-to-sklad") return;
  sendToSklad(tab || (await chrome.tabs.query({ active: true, currentWindow: true }))[0]);
});
chrome.action.onClicked.addListener((tab) => sendToSklad(tab));
