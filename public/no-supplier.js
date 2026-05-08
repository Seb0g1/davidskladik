function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value) {
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 }).format(Number(value || 0));
}

function marketLabel(item) {
  if (item.marketplace === "ozon") return "Ozon";
  if (item.marketplace === "yandex") return "Yandex Market";
  return item.marketplace || "-";
}

async function api(path) {
  const response = await fetch(path, { credentials: "same-origin" });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return response.json();
}

const elements = {
  refreshButton: document.querySelector("#refreshNoSupplierButton"),
  status: document.querySelector("#noSupplierStatus"),
  meta: document.querySelector("#noSupplierMeta"),
  list: document.querySelector("#noSupplierList"),
};

function renderAlerts(payload) {
  const alerts = Array.isArray(payload.alerts) ? payload.alerts : [];
  elements.meta.textContent = `Карточек в ошибках: ${formatNumber(alerts.length)}. Показываются только товары с привязками поставщиков.`;
  if (!alerts.length) {
    elements.list.innerHTML = `<div class="empty"><strong>Ошибок наличия нет</strong><span>Если по карточке нет активных поставщиков, она появится здесь автоматически.</span></div>`;
    elements.status.textContent = "Все хорошо: ошибок наличия не найдено.";
    elements.status.classList.remove("is-warn");
    elements.status.classList.add("is-ok");
    return;
  }

  elements.list.innerHTML = alerts
    .map((item) => `
      <div class="history-row">
        <div>
          <strong>${escapeHtml(item.offerId || item.name || item.id)}</strong>
          <span>${escapeHtml(item.name || "Без названия")} · ${escapeHtml(marketLabel(item))} · привязок ${formatNumber(item.supplierCount || 0)} · активных ${formatNumber(item.availableSupplierCount || 0)}</span>
        </div>
        <small>${escapeHtml(item.action || "Проверить наличие")}</small>
      </div>
    `)
    .join("");
  elements.status.textContent = "Найдены товары без активного поставщика.";
  elements.status.classList.remove("is-ok");
  elements.status.classList.add("is-warn");
}

async function loadNoSupplierAlerts() {
  elements.status.textContent = "Обновляю список ошибок наличия...";
  try {
    const payload = await api("/api/warehouse/no-supplier");
    renderAlerts(payload);
  } catch (error) {
    elements.status.textContent = `Ошибка загрузки: ${error.message}`;
    elements.status.classList.add("is-warn");
  }
}

elements.refreshButton?.addEventListener("click", loadNoSupplierAlerts);
loadNoSupplierAlerts();
