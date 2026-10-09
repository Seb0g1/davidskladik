// Мост между страницей склада и расширением: склад узнаёт, что расширение стоит,
// просит загрузить список страниц (ds-fragrantica-ext-run / -stop) и получает ход работы (-progress).
document.documentElement.dataset.dsFragranticaExt = chrome.runtime.getManifest().version;

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
