const searchInput = document.querySelector("#pmSearchInput");
const changeTypeFilter = document.querySelector("#pmChangeTypeFilter");
const supplierFilter = document.querySelector("#pmSupplierFilter");
const reloadButton = document.querySelector("#pmReloadButton");
const exportCsvButton = document.querySelector("#pmExportCsvButton");
const statusBox = document.querySelector("#pmStatus");
const offersTable = document.querySelector("#pmOffersTable");
const historyList = document.querySelector("#pmHistoryList");
const offersCount = document.querySelector("#pmOffersCount");
const historyCount = document.querySelector("#pmHistoryCount");
const logoutButton = document.querySelector("#logoutButton");
let cachedOffers = [];
let cachedHistory = [];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formatDate(value) {
  return value ? new Intl.DateTimeFormat("ru-RU", { dateStyle: "short", timeStyle: "short" }).format(new Date(value)) : "-";
}

function formatMoney(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? `$${n.toFixed(2)}` : "-";
}

async function api(url) {
  const response = await fetch(url);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || "Ошибка запроса");
  return payload;
}

function renderOffers(rows = []) {
  offersCount.textContent = String(rows.length);
  if (!rows.length) {
    offersTable.innerHTML = `<div class="empty-mini">Нет товаров по текущему фильтру.</div>`;
    return;
  }
  offersTable.innerHTML = rows
    .slice(0, 300)
    .map(
      (row) => `
        <div class="history-row">
          <div>
            <strong>${escapeHtml(row.article || "-")} · ${escapeHtml(row.name || "-")}</strong>
            <span>${escapeHtml(row.partnerName || "Поставщик не указан")} · ${escapeHtml(row.barcode || "-")} · ${formatDate(row.docDate)}</span>
          </div>
          <small>${formatMoney(row.price)}</small>
        </div>
      `,
    )
    .join("");
}

function renderHistory(rows = []) {
  historyCount.textContent = String(rows.length);
  if (!rows.length) {
    historyList.innerHTML = `<div class="empty-mini">История пока пуста.</div>`;
    return;
  }
  historyList.innerHTML = rows
    .slice(0, 300)
    .map(
      (row) => `
        <div class="history-row">
          <div>
            <strong>${escapeHtml(row.type || "change")} · ${escapeHtml(row.article || "-")}</strong>
            <span>${escapeHtml(row.partnerName || "Поставщик")} · ${formatDate(row.createdAt)}</span>
          </div>
          <small>${formatMoney(row.oldPrice)} → ${formatMoney(row.newPrice)}</small>
        </div>
      `,
    )
    .join("");
}

function filteredHistory() {
  const type = String(changeTypeFilter?.value || "all");
  const supplierQuery = String(supplierFilter?.value || "").trim().toLowerCase();
  return cachedHistory.filter((row) => {
    const typeOk = type === "all" || String(row.type || "") === type;
    const supplierOk = !supplierQuery || String(row.partnerName || "").toLowerCase().includes(supplierQuery);
    return typeOk && supplierOk;
  });
}

function applyFilters() {
  renderOffers(cachedOffers);
  renderHistory(filteredHistory());
}

function toCsv(rows = []) {
  const header = ["createdAt", "type", "article", "partnerName", "oldPrice", "newPrice", "oldActive", "newActive"];
  const lines = [header.join(",")];
  for (const row of rows) {
    const values = [
      row.createdAt || "",
      row.type || "",
      row.article || "",
      row.partnerName || "",
      row.oldPrice ?? "",
      row.newPrice ?? "",
      row.oldActive ?? "",
      row.newActive ?? "",
    ].map((value) => `"${String(value).replaceAll('"', '""')}"`);
    lines.push(values.join(","));
  }
  return lines.join("\n");
}

function downloadCsv(rows) {
  const blob = new Blob([toCsv(rows)], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `pricemaster-history-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function loadData() {
  const q = String(searchInput.value || "").trim();
  statusBox.textContent = "Загружаю данные PriceMaster и аудит...";
  const [offersData, historyData] = await Promise.all([
    api(`/api/offers?search=${encodeURIComponent(q)}&limit=500`),
    api("/api/history?limit=500"),
  ]);
  cachedOffers = Array.isArray(offersData) ? offersData : [];
  cachedHistory = Array.isArray(historyData.history) ? historyData.history : [];
  applyFilters();
  statusBox.textContent = "Данные обновлены.";
}

reloadButton?.addEventListener("click", () => {
  loadData().catch((error) => {
    statusBox.textContent = error.message;
  });
});

searchInput?.addEventListener("input", () => {
  window.clearTimeout(searchInput._timer);
  searchInput._timer = window.setTimeout(() => {
    loadData().catch((error) => {
      statusBox.textContent = error.message;
    });
  }, 320);
});
changeTypeFilter?.addEventListener("change", applyFilters);
supplierFilter?.addEventListener("input", () => {
  window.clearTimeout(supplierFilter._timer);
  supplierFilter._timer = window.setTimeout(applyFilters, 220);
});
exportCsvButton?.addEventListener("click", () => {
  const rows = filteredHistory();
  if (!rows.length) {
    statusBox.textContent = "Нет данных для экспорта.";
    return;
  }
  downloadCsv(rows);
  statusBox.textContent = `CSV выгружен: ${rows.length} строк.`;
});

logoutButton?.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

loadData().catch((error) => {
  statusBox.textContent = error.message;
});
