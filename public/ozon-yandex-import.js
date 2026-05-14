const state = {
  rows: [],
  summary: { total: 0, eligible: 0, blocked: 0, existingInYandex: 0, missingRequired: 0 },
};

const els = {
  limit: document.getElementById("importLimitInput"),
  loadWarehouse: document.getElementById("loadWarehouseImportButton"),
  loadOzon: document.getElementById("loadOzonImportButton"),
  sendYandex: document.getElementById("sendYandexImportButton"),
  status: document.getElementById("importStatus"),
  total: document.getElementById("importTotalCount"),
  eligible: document.getElementById("importEligibleCount"),
  blocked: document.getElementById("importBlockedCount"),
  existing: document.getElementById("importExistingCount"),
  missing: document.getElementById("importMissingCount"),
  filter: document.getElementById("importFilterSelect"),
  warnings: document.getElementById("importWarnings"),
  rows: document.getElementById("importRows"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatMoney(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "-";
  return new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 0 }).format(number) + " ₽";
}

function setBusy(isBusy, message) {
  els.loadWarehouse.disabled = isBusy;
  els.loadOzon.disabled = isBusy;
  els.status.textContent = message || (isBusy ? "Загрузка..." : "Готово.");
}

async function api(path) {
  const response = await fetch(path, { credentials: "same-origin" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload;
}

function rowStatus(row) {
  if (row.eligible) return { className: "good", label: "Можно" };
  if (row.existingInYandex) return { className: "warn", label: "Есть в ЯМ" };
  if (row.blockReasons?.length) return { className: "danger", label: "Блок" };
  return { className: "warn", label: "Поля" };
}

function visibleRows() {
  const filter = els.filter.value;
  if (filter === "eligible") return state.rows.filter((row) => row.eligible);
  if (filter === "existing") return state.rows.filter((row) => row.existingInYandex);
  if (filter === "blocked") return state.rows.filter((row) => row.blockReasons?.length);
  if (filter === "missing") return state.rows.filter((row) => !row.yandexReady);
  return state.rows;
}

function renderSummary() {
  els.total.textContent = state.summary.total || 0;
  els.eligible.textContent = state.summary.eligible || 0;
  els.blocked.textContent = state.summary.blocked || 0;
  els.existing.textContent = state.summary.existingInYandex || 0;
  els.missing.textContent = state.summary.missingRequired || 0;
  els.sendYandex.disabled = true;
  els.sendYandex.title = "Выгрузку в Яндекс подключим вторым безопасным этапом после проверки предпросмотра.";
}

function renderWarnings(warnings) {
  const list = Array.isArray(warnings) ? warnings.filter(Boolean) : [];
  els.warnings.hidden = list.length === 0;
  els.warnings.innerHTML = list.map((item) => `<div>${escapeHtml(item)}</div>`).join("");
}

function renderRows() {
  const rows = visibleRows();
  if (!rows.length) {
    els.rows.innerHTML = `<div class="empty-state">Карточек по этому фильтру нет.</div>`;
    return;
  }
  els.rows.innerHTML = rows.map((row) => {
    const status = rowStatus(row);
    const reasons = [
      ...(row.existingInYandex ? ["Артикул уже есть в Яндекс Маркете"] : []),
      ...(row.blockReasons || []),
      ...((row.missing || []).map((field) => `Не хватает ${field}`)),
    ];
    const reasonHtml = reasons.length
      ? `<div class="import-row-reasons">${reasons.map((reason) => `<span>${escapeHtml(reason)}</span>`).join("")}</div>`
      : `<div class="import-row-reasons"><span>Проверки пройдены</span></div>`;
    const imageHtml = row.imageUrl
      ? `<img src="${escapeHtml(row.imageUrl)}" alt="" loading="lazy" />`
      : `<div class="import-row-placeholder">Ozon</div>`;
    return `
      <article class="import-row">
        <div class="import-row-image">${imageHtml}</div>
        <div class="import-row-main">
          <div class="import-row-title">
            <strong>${escapeHtml(row.name || "Без названия")}</strong>
            <span class="import-status ${status.className}">${escapeHtml(status.label)}</span>
          </div>
          <div class="import-row-meta">
            <span>${escapeHtml(row.offerId || "-")}</span>
            <span>ID ${escapeHtml(row.productId || "-")}</span>
            <span>${escapeHtml(row.vendor || "Без бренда")}</span>
            <span>${formatMoney(row.price)}</span>
          </div>
          ${reasonHtml}
        </div>
      </article>
    `;
  }).join("");
}

function render(payload) {
  state.rows = Array.isArray(payload.rows) ? payload.rows : [];
  state.summary = payload.summary || {};
  renderSummary();
  renderWarnings(payload.warnings);
  renderRows();
}

async function loadPreview(refresh) {
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  setBusy(true, refresh ? "Загружаю карточки из Ozon..." : "Читаю карточки из склада...");
  try {
    const payload = await api(`/api/ozon-yandex-import/preview?limit=${encodeURIComponent(limit)}&refresh=${refresh ? "true" : "false"}`);
    render(payload);
    const source = payload.source === "ozon_api" ? "Ozon API" : "склада";
    setBusy(false, `Загружено из ${source}: ${payload.summary?.total || 0}. Можно выгружать: ${payload.summary?.eligible || 0}.`);
  } catch (error) {
    setBusy(false, `Ошибка: ${error.message || error}`);
  }
}

els.loadWarehouse.addEventListener("click", () => loadPreview(false));
els.loadOzon.addEventListener("click", () => loadPreview(true));
els.filter.addEventListener("change", renderRows);

loadPreview(false).catch(() => {});
