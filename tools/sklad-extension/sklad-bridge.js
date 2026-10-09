// Мост между страницей склада и расширением: склад узнаёт, что расширение стоит,
// просит загрузить список страниц (ds-fragrantica-ext-run / -stop) и получает ход работы (-progress).
// injected again into an open tab after an update: one bridge per page
if (!window.__dsSkladBridge) {
window.__dsSkladBridge = true;
document.documentElement.dataset.dsFragranticaExt = chrome.runtime.getManifest().version;
window.postMessage({ type: "ds-fragrantica-ext-hello" }, location.origin);

window.addEventListener("message", (event) => {
  if (event.source !== window || event.origin !== location.origin) return;
  const type = event.data?.type;
  if (type === "ds-fragrantica-ext-run") chrome.runtime.sendMessage({ type: "run", items: event.data.items || [] });
  else if (type === "ds-fragrantica-ext-stop") chrome.runtime.sendMessage({ type: "stop" });
  else if (type === "ds-fragrantica-ext-status") chrome.runtime.sendMessage({ type: "status" });
});

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type === "progress") window.postMessage({ type: "ds-fragrantica-ext-progress", ...message.state }, location.origin);
});
}
