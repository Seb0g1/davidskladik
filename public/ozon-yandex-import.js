const state = {
  rows: [],
  summary: { total: 0, eligible: 0, blocked: 0, existingInYandex: 0, missingRequired: 0 },
  importFailedByOffer: new Map(),
  cleanupRows: [],
  cleanupSummary: { total: 0, protected: 0, toDelete: 0, toArchive: 0, alreadyArchived: 0 },
  cleanupFailedByOffer: new Map(),
};

const els = {
  limit: document.getElementById("importLimitInput"),
  loadWarehouse: document.getElementById("loadWarehouseImportButton"),
  loadOzon: document.getElementById("loadOzonImportButton"),
  syncStocks: document.getElementById("syncYandexStocksButton"),
  archiveBlocked: document.getElementById("archiveBlockedButton"),
  sendYandex: document.getElementById("sendYandexImportButton"),
  status: document.getElementById("importStatus"),
  total: document.getElementById("importTotalCount"),
  eligible: document.getElementById("importEligibleCount"),
  blocked: document.getElementById("importBlockedCount"),
  existing: document.getElementById("importExistingCount"),
  missing: document.getElementById("importMissingCount"),
  filter: document.getElementById("importFilterSelect"),
  warnings: document.getElementById("importWarnings"),
  importFailures: document.getElementById("importFailures"),
  rows: document.getElementById("importRows"),
  cleanupBrands: document.getElementById("yandexProtectedBrandsInput"),
  cleanupPreview: document.getElementById("previewYandexCleanupButton"),
  cleanupCsv: document.getElementById("downloadYandexCleanupCsvButton"),
  cleanupArchive: document.getElementById("archiveYandexCleanupButton"),
  cleanupStatus: document.getElementById("yandexCleanupStatus"),
  cleanupTotal: document.getElementById("yandexCleanupTotalCount"),
  cleanupProtected: document.getElementById("yandexCleanupProtectedCount"),
  cleanupToArchive: document.getElementById("yandexCleanupArchiveCount"),
  cleanupAlreadyArchived: document.getElementById("yandexCleanupAlreadyArchivedCount"),
  cleanupSearch: document.getElementById("yandexCleanupSearchInput"),
  cleanupFilter: document.getElementById("yandexCleanupFilterSelect"),
  cleanupWarnings: document.getElementById("yandexCleanupWarnings"),
  cleanupFailures: document.getElementById("yandexCleanupFailures"),
  cleanupRows: document.getElementById("yandexCleanupRows"),
  cleanupRowsMeta: document.getElementById("yandexCleanupRowsMeta"),
  cleanupHistory: document.getElementById("yandexCleanupHistory"),
  cleanupHistoryStatus: document.getElementById("yandexCleanupHistoryStatus"),
  cleanupHistoryRefresh: document.getElementById("refreshYandexCleanupHistoryButton"),
  cleanupConfirmModal: document.getElementById("yandexCleanupConfirmModal"),
  cleanupConfirmSummary: document.getElementById("yandexCleanupConfirmSummary"),
  cleanupConfirmInput: document.getElementById("yandexCleanupConfirmInput"),
  cleanupConfirmSubmit: document.getElementById("yandexCleanupConfirmSubmit"),
  cleanupConfirmCancel: document.getElementById("yandexCleanupConfirmCancel"),
  cleanupConfirmClose: document.getElementById("yandexCleanupConfirmClose"),
  importHistory: document.getElementById("yandexImportHistory"),
  importHistoryStatus: document.getElementById("yandexImportHistoryStatus"),
  importHistoryRefresh: document.getElementById("refreshYandexImportHistoryButton"),
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
  els.syncStocks.disabled = isBusy;
  els.archiveBlocked.disabled = isBusy;
  els.status.textContent = message || (isBusy ? "Загрузка..." : "Готово.");
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: options.body ? { "content-type": "application/json" } : undefined,
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || payload.error || `HTTP ${response.status}`);
  return payload;
}

function rowStatus(row) {
  if (state.importFailedByOffer.has(String(row.offerId || ""))) return { className: "danger", label: "Ошибка Яндекс" };
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
  els.sendYandex.disabled = !(Number(state.summary.eligible || 0) > 0);
  els.sendYandex.title = els.sendYandex.disabled
    ? "Нет карточек, которые можно безопасно выгрузить в Яндекс."
    : "Отправить в Яндекс только карточки со статусом «Можно», без дублей по SKU.";
}

function renderWarnings(warnings) {
  const list = Array.isArray(warnings) ? warnings.filter(Boolean) : [];
  els.warnings.hidden = list.length === 0;
  els.warnings.innerHTML = list.map((item) => `<div>${escapeHtml(item)}</div>`).join("");
}

function renderImportFailures(results = []) {
  const failed = (Array.isArray(results) ? results : []).filter((item) => !item.ok);
  state.importFailedByOffer = new Map(failed.map((item) => [String(item.offerId || ""), item.error || "unknown_error"]));
  if (!els.importFailures) return;
  const stageLabel = (stage) => ({
    card: "карточка",
    price: "цена",
    stock: "остаток",
  })[stage] || "этап";
  els.importFailures.hidden = failed.length === 0;
  els.importFailures.innerHTML = failed.length
    ? [
        `<strong>Не выгрузились SKU: ${failed.length}</strong>`,
        ...failed.slice(0, 100).map((item) => `<div>${escapeHtml(item.offerId || "-")} - ${escapeHtml(stageLabel(item.stage))} - ${escapeHtml(item.targetName || item.target || "Yandex")} - ${escapeHtml(item.error || "unknown_error")}</div>`),
        failed.length > 100 ? `<div>Показаны первые 100 из ${failed.length}. Полный список есть в журнале операции.</div>` : "",
      ].filter(Boolean).join("")
    : "";
}

function renderCleanupWarnings(warnings) {
  const list = Array.isArray(warnings) ? warnings.filter(Boolean) : [];
  els.cleanupWarnings.hidden = list.length === 0;
  els.cleanupWarnings.innerHTML = list.map((item) => `<div>${escapeHtml(item)}</div>`).join("");
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
      ...(state.importFailedByOffer.has(String(row.offerId || "")) ? [`Ошибка выгрузки: ${state.importFailedByOffer.get(String(row.offerId || ""))}`] : []),
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

function parseCleanupBrands() {
  return String(els.cleanupBrands?.value || "")
    .split(/[\n,;]+/u)
    .map((item) => item.trim())
    .filter(Boolean);
}

function cleanupReason(row) {
  if (row.protected) return `Защищено: ${(row.matchedBrands || []).join(", ")}`;
  if (row.smallVolume) return `Объем меньше 20 мл${row.minVolumeMl ? `: ${row.minVolumeMl} мл` : ""}. Удаляем даже при защищённом бренде.`;
  if (row.archived) return "Товар уже в архиве Яндекса, но всё равно будет удалён из каталога.";
  return "Бренд не найден в названии, описании или характеристиках";
}

function cleanupStatus(row) {
  if (row.protected) return { className: "good", label: "Оставить" };
  if (row.archived) return { className: "danger", label: "Удалить (архив)" };
  return { className: "danger", label: "Удалить" };
}

function visibleCleanupRows() {
  const filter = els.cleanupFilter?.value || "all";
  const query = String(els.cleanupSearch?.value || "").trim().toLowerCase();
  return state.cleanupRows.filter((row) => {
    if (filter === "delete" && row.action !== "delete") return false;
    if (filter === "keep" && !row.protected) return false;
    if (filter === "errors" && !state.cleanupFailedByOffer.has(String(row.offerId || ""))) return false;
    if (filter === "archived" && !row.archived) return false;
    if (!query) return true;
    const text = [row.offerId, row.name, row.shopName, row.vendor, row.stateLabel, row.state, cleanupReason(row)]
      .map((item) => String(item || "").toLowerCase())
      .join(" ");
    return text.includes(query);
  });
}

function renderCleanupFailures(results = []) {
  const failed = (Array.isArray(results) ? results : []).filter((item) => !item.ok);
  state.cleanupFailedByOffer = new Map(failed.map((item) => [String(item.offerId || ""), item.error || "unknown_error"]));
  if (!els.cleanupFailures) return;
  els.cleanupFailures.hidden = failed.length === 0;
  els.cleanupFailures.innerHTML = failed.length
    ? [
        `<strong>Не удалились SKU: ${failed.length}</strong>`,
        ...failed.slice(0, 100).map((item) => `<div>${escapeHtml(item.offerId || "-")} · ${escapeHtml(item.error || "unknown_error")}</div>`),
        failed.length > 100 ? `<div>Показано первые 100 из ${failed.length}. Полный список есть в журнале и ответе операции.</div>` : "",
      ].filter(Boolean).join("")
    : "";
}

function renderCleanup(payload = {}) {
  state.cleanupRows = Array.isArray(payload.rows) ? payload.rows : [];
  state.cleanupSummary = payload.summary || {};
  els.cleanupTotal.textContent = state.cleanupSummary.total || 0;
  els.cleanupProtected.textContent = state.cleanupSummary.protected || 0;
  const toDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
  els.cleanupToArchive.textContent = toDelete;
  els.cleanupAlreadyArchived.textContent = state.cleanupSummary.alreadyArchived || 0;
  els.cleanupArchive.disabled = !(toDelete > 0 && parseCleanupBrands().length);
  if (els.cleanupCsv) els.cleanupCsv.disabled = state.cleanupRows.length === 0;
  renderCleanupWarnings(payload.warnings);

  const filtered = visibleCleanupRows();
  const rows = filtered.slice(0, 1000);
  els.cleanupRowsMeta.textContent = state.cleanupRows.length
    ? `Показано ${rows.length} из ${filtered.length}. Всего: ${state.cleanupRows.length}`
    : "Нет данных";
  if (!rows.length) {
    els.cleanupRows.innerHTML = `<div class="empty-state">Запустите предпросмотр, чтобы увидеть товары Яндекса.</div>`;
    return;
  }
  els.cleanupRows.innerHTML = rows.map((row) => {
    const status = cleanupStatus(row);
    const failedReason = state.cleanupFailedByOffer.get(String(row.offerId || ""));
    const reasons = [cleanupReason(row), failedReason ? `Ошибка удаления: ${failedReason}` : ""].filter(Boolean);
    return `
      <article class="import-row">
        <div class="import-row-image"><div class="import-row-placeholder">ЯМ</div></div>
        <div class="import-row-main">
          <div class="import-row-title">
            <strong>${escapeHtml(row.name || row.offerId || "Без названия")}</strong>
            <span class="import-status ${status.className}">${escapeHtml(status.label)}</span>
          </div>
          <div class="import-row-meta">
            <span>${escapeHtml(row.offerId || "-")}</span>
            <span>${escapeHtml(row.shopName || "Yandex Market")}</span>
            <span>${escapeHtml(row.vendor || "Без бренда")}</span>
            <span>${escapeHtml(row.stateLabel || row.state || "-")}</span>
          </div>
          <div class="import-row-reasons">${reasons.map((reason) => `<span>${escapeHtml(reason)}</span>`).join("")}</div>
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

function csvCell(value) {
  return `"${String(value ?? "").replaceAll('"', '""')}"`;
}

function downloadYandexCleanupCsv() {
  if (!state.cleanupRows.length) {
    els.cleanupStatus.textContent = "Сначала сделайте предпросмотр очистки.";
    return;
  }
  const rows = visibleCleanupRows();
  const header = ["offerId", "название", "магазин", "статус", "причина", "действие"];
  const lines = [
    header.map(csvCell).join(","),
    ...rows.map((row) => {
      const status = cleanupStatus(row);
      return [
        row.offerId || "",
        row.name || "",
        row.shopName || "Yandex Market",
        row.stateLabel || row.state || "",
        cleanupReason(row),
        status.label,
      ].map(csvCell).join(",");
    }),
  ];
  const blob = new Blob([`\uFEFF${lines.join("\n")}`], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `yandex-cleanup-preview-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  els.cleanupStatus.textContent = `CSV сформирован: ${rows.length} строк.`;
}

function renderYandexCleanupHistory(items = []) {
  if (!els.cleanupHistory) return;
  if (!items.length) {
    els.cleanupHistory.innerHTML = `<div class="empty-state">Удалений Яндекса пока не было.</div>`;
    return;
  }
  els.cleanupHistory.innerHTML = items.map((item) => {
    const brands = Array.isArray(item.protectedBrands) && item.protectedBrands.length ? item.protectedBrands.join(", ") : "-";
    const failed = Array.isArray(item.failedOfferIds) && item.failedOfferIds.length
      ? `<span>Не удалились: ${escapeHtml(item.failedOfferIds.slice(0, 10).join(", "))}${item.failedOfferIds.length > 10 ? " ..." : ""}</span>`
      : "";
    return `
      <div class="history-row">
        <div>
          <strong>${escapeHtml(item.user || "system")} · удалено ${escapeHtml(item.deleted || 0)} из ${escapeHtml(item.planned || 0)}</strong>
          <span>Защита: ${escapeHtml(brands)} · ошибок ${escapeHtml(item.failed || 0)} · notDeleted ${escapeHtml(item.notDeleted || 0)}</span>
          ${failed}
        </div>
        <small>${escapeHtml(item.at ? new Date(item.at).toLocaleString("ru-RU") : "-")}</small>
      </div>
    `;
  }).join("");
}

function renderYandexImportHistory(items = []) {
  if (!els.importHistory) return;
  if (!items.length) {
    els.importHistory.innerHTML = `<div class="empty-state">Выгрузок в Яндекс пока не было.</div>`;
    return;
  }
  els.importHistory.innerHTML = items.map((item) => {
    const targets = Array.isArray(item.targets) && item.targets.length
      ? item.targets.map((target) => target.name || target.id || target.businessId).filter(Boolean).join(", ")
      : "Yandex Market";
    const failed = Array.isArray(item.failedOfferIds) && item.failedOfferIds.length
      ? `<span>Ошибки SKU: ${escapeHtml(item.failedOfferIds.slice(0, 10).join(", "))}${item.failedOfferIds.length > 10 ? " ..." : ""}</span>`
      : "";
    return `
      <div class="history-row">
        <div>
          <strong>${escapeHtml(item.user || "system")} · отправлено ${escapeHtml(item.sent || 0)} из ${escapeHtml(item.planned || 0)}</strong>
          <span>${escapeHtml(targets)} - ошибки карточек ${escapeHtml(item.failed || 0)} - цены ${escapeHtml(item.priceSent || 0)}/${escapeHtml(item.priceFailed || 0)} - остатки ${escapeHtml(item.stockSent || 0)}/${escapeHtml(item.stockFailed || 0)}</span>
          <span>Уже были ${escapeHtml(item.skippedExisting || 0)} - блок ${escapeHtml(item.skippedBlocked || 0)}</span>
          ${failed}
        </div>
        <small>${escapeHtml(item.at ? new Date(item.at).toLocaleString("ru-RU") : "-")}</small>
      </div>
    `;
  }).join("");
}

async function loadYandexImportHistory() {
  if (!els.importHistory) return;
  if (els.importHistoryStatus) els.importHistoryStatus.textContent = "Загружаю журнал...";
  if (els.importHistoryRefresh) els.importHistoryRefresh.disabled = true;
  try {
    const payload = await api("/api/ozon-yandex-import/history?limit=20");
    renderYandexImportHistory(payload.history || []);
    if (els.importHistoryStatus) els.importHistoryStatus.textContent = `Событий: ${payload.history?.length || 0}.`;
  } catch (error) {
    if (els.importHistoryStatus) els.importHistoryStatus.textContent = `Ошибка журнала: ${error.message || error}`;
  } finally {
    if (els.importHistoryRefresh) els.importHistoryRefresh.disabled = false;
  }
}

async function loadYandexCleanupHistory() {
  if (!els.cleanupHistory) return;
  if (els.cleanupHistoryStatus) els.cleanupHistoryStatus.textContent = "Загружаю журнал...";
  if (els.cleanupHistoryRefresh) els.cleanupHistoryRefresh.disabled = true;
  try {
    const payload = await api("/api/yandex-cleanup/history?limit=20");
    renderYandexCleanupHistory(payload.history || []);
    if (els.cleanupHistoryStatus) els.cleanupHistoryStatus.textContent = `Событий: ${payload.history?.length || 0}.`;
  } catch (error) {
    if (els.cleanupHistoryStatus) els.cleanupHistoryStatus.textContent = `Ошибка журнала: ${error.message || error}`;
  } finally {
    if (els.cleanupHistoryRefresh) els.cleanupHistoryRefresh.disabled = false;
  }
}

function openYandexCleanupConfirmModal({ summary = {}, protectedBrands = [] } = {}) {
  if (!els.cleanupConfirmModal) return Promise.resolve(false);
  const finalToDelete = Number(summary.toDelete ?? summary.toArchive ?? 0);
  const finalToDeleteNow = Number(summary.deletePlannedNow ?? summary.plannedNow ?? Math.min(finalToDelete, 10000));
  const skippedByLimit = Number(summary.deleteSkippedByLimit ?? 0);
  const deleteLimit = Number(summary.deleteLimit || 10000);
  const finalProtected = Number(summary.protected || 0);
  const finalArchived = Number(summary.alreadyArchived || 0);
  const brands = protectedBrands.length ? protectedBrands.join(", ") : "не указаны";

  els.cleanupConfirmSummary.innerHTML = [
    ["Будет удалено сейчас", finalToDeleteNow],
    ["Всего подходит под удаление", finalToDelete],
    ["Лимит за запуск", deleteLimit],
    ["Останется из-за лимита", skippedByLimit],
    ["Оставлено защитой", finalProtected],
    ["Архивных среди удаления", finalArchived],
  ].map(([label, value]) => `
    <div class="cleanup-confirm-stat">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `).join("") + `
    <div class="cleanup-confirm-brands">
      <span>Бренды-защита</span>
      <strong>${escapeHtml(brands)}</strong>
    </div>
  `;

  els.cleanupConfirmInput.value = "";
  els.cleanupConfirmSubmit.disabled = true;
  els.cleanupConfirmModal.hidden = false;
  document.body.classList.add("modal-open");
  window.setTimeout(() => els.cleanupConfirmInput.focus(), 0);

  return new Promise((resolve) => {
    let done = false;
    const cleanup = (result) => {
      if (done) return;
      done = true;
      els.cleanupConfirmModal.hidden = true;
      document.body.classList.remove("modal-open");
      els.cleanupConfirmInput.removeEventListener("input", onInput);
      els.cleanupConfirmSubmit.removeEventListener("click", onSubmit);
      els.cleanupConfirmCancel.removeEventListener("click", onCancel);
      els.cleanupConfirmClose.removeEventListener("click", onCancel);
      els.cleanupConfirmModal.removeEventListener("click", onBackdrop);
      document.removeEventListener("keydown", onKeydown);
      resolve(result);
    };
    const onInput = () => {
      els.cleanupConfirmSubmit.disabled = els.cleanupConfirmInput.value.trim() !== "УДАЛИТЬ ЯНДЕКС";
    };
    const onSubmit = () => cleanup(els.cleanupConfirmInput.value.trim() === "УДАЛИТЬ ЯНДЕКС");
    const onCancel = () => cleanup(false);
    const onBackdrop = (event) => {
      if (event.target === els.cleanupConfirmModal) cleanup(false);
    };
    const onKeydown = (event) => {
      if (event.key === "Escape") cleanup(false);
      if (event.key === "Enter" && els.cleanupConfirmInput.value.trim() === "УДАЛИТЬ ЯНДЕКС") cleanup(true);
    };
    els.cleanupConfirmInput.addEventListener("input", onInput);
    els.cleanupConfirmSubmit.addEventListener("click", onSubmit);
    els.cleanupConfirmCancel.addEventListener("click", onCancel);
    els.cleanupConfirmClose.addEventListener("click", onCancel);
    els.cleanupConfirmModal.addEventListener("click", onBackdrop);
    document.addEventListener("keydown", onKeydown);
  });
}

async function previewYandexCleanup() {
  const protectedBrands = parseCleanupBrands();
  if (!protectedBrands.length) {
    els.cleanupStatus.textContent = "Укажите хотя бы один бренд, который нельзя удалять.";
    return;
  }
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  els.cleanupPreview.disabled = true;
  els.cleanupArchive.disabled = true;
  state.cleanupFailedByOffer = new Map();
  renderCleanupFailures([]);
  els.cleanupStatus.textContent = "Читаю все статусы Яндекс Маркета и ищу защищённые бренды...";
  try {
    const payload = await api("/api/yandex-cleanup/preview", {
      method: "POST",
      body: { protectedBrands, limit },
    });
    renderCleanup(payload);
    const deleteCount = Number(payload.summary?.toDelete ?? payload.summary?.toArchive ?? 0);
    const skippedByLimit = Number(payload.summary?.deleteSkippedByLimit || 0);
    els.cleanupStatus.textContent = `Проверено: ${payload.summary?.total || 0}. Оставить: ${payload.summary?.protected || 0}. Удалить: ${deleteCount}.${skippedByLimit ? ` За один запуск уйдёт ${payload.summary?.deletePlannedNow || 0}, ещё ${skippedByLimit} останется из-за лимита.` : ""}`;
  } catch (error) {
    els.cleanupStatus.textContent = `Ошибка предпросмотра: ${error.message || error}`;
  } finally {
    els.cleanupPreview.disabled = false;
  }
}

async function archiveYandexCleanup() {
  const protectedBrands = parseCleanupBrands();
  const toDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
  if (!protectedBrands.length || !toDelete) {
    els.cleanupStatus.textContent = "Сначала сделайте предпросмотр очистки.";
    return;
  }
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  els.cleanupPreview.disabled = true;
  els.cleanupArchive.disabled = true;
  els.cleanupStatus.textContent = "Делаю финальную проверку перед удалением...";
  let dryRunPayload;
  try {
    dryRunPayload = await api("/api/yandex-cleanup/delete", {
      method: "POST",
      body: { protectedBrands, limit, dryRun: true },
    });
    renderCleanup(dryRunPayload);
  } catch (error) {
    els.cleanupStatus.textContent = `Ошибка финальной проверки: ${error.message || error}`;
    els.cleanupPreview.disabled = false;
    const afterDryRunToDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
    els.cleanupArchive.disabled = !(afterDryRunToDelete > 0 && parseCleanupBrands().length);
    return;
  }
  const finalSummary = dryRunPayload.summary || {};
  const finalToDelete = Number(finalSummary.toDelete ?? finalSummary.toArchive ?? 0);
  const finalToDeleteNow = Number(finalSummary.deletePlannedNow ?? finalSummary.plannedNow ?? Math.min(finalToDelete, 10000));
  const finalProtected = Number(finalSummary.protected || 0);
  const finalArchived = Number(finalSummary.alreadyArchived || 0);
  const skippedByLimit = Number(finalSummary.deleteSkippedByLimit ?? dryRunPayload.skippedByLimit ?? 0);
  const deleteLimit = Number(finalSummary.deleteLimit || 10000);
  const confirmed = await openYandexCleanupConfirmModal({
    summary: {
      ...finalSummary,
      deletePlannedNow: finalToDeleteNow,
      deleteSkippedByLimit: skippedByLimit,
      deleteLimit,
      protected: finalProtected,
      alreadyArchived: finalArchived,
    },
    protectedBrands,
  });
  if (!confirmed) {
    els.cleanupStatus.textContent = "Удаление отменено после финальной проверки.";
    els.cleanupPreview.disabled = false;
    els.cleanupArchive.disabled = !(finalToDelete > 0 && parseCleanupBrands().length);
    return;
  }
  const confirmationText = "\u0423\u0414\u0410\u041b\u0418\u0422\u042c \u042f\u041d\u0414\u0415\u041a\u0421";
  els.cleanupStatus.textContent = "Удаляю незащищённые товары из каталога Яндекса...";
  try {
    const payload = await api("/api/yandex-cleanup/delete", {
      method: "POST",
      body: { protectedBrands, limit, confirmed: true, confirmationText },
    });
    renderCleanupFailures(payload.results || []);
    els.cleanupStatus.textContent = `Удалено: ${payload.deleted || 0}. Не удалено: ${payload.failed || 0}. Отложено лимитом: ${payload.skippedByLimit || 0}.`;
    await loadYandexCleanupHistory();
    renderCleanup({ rows: state.cleanupRows, summary: state.cleanupSummary });
  } catch (error) {
    els.cleanupStatus.textContent = `Ошибка очистки: ${error.message || error}`;
  } finally {
    els.cleanupPreview.disabled = false;
    const remainingToDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
    els.cleanupArchive.disabled = !(remainingToDelete > 0 && parseCleanupBrands().length);
  }
}

async function loadPreview(refresh) {
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  state.importFailedByOffer = new Map();
  renderImportFailures([]);
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

async function sendYandexImport() {
  const eligible = Number(state.summary.eligible || 0);
  if (!eligible) {
    els.status.textContent = "Нет карточек, которые можно безопасно выгрузить в Яндекс.";
    return;
  }
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  const confirmed = window.confirm([
    "Выгрузить карточки Ozon в Яндекс?",
    `Можно выгружать: ${eligible}`,
    "Сервер ещё раз проверит SKU в Яндексе и пропустит уже существующие.",
    "Выгрузка пойдёт только по карточкам без блокировок и без недостающих полей.",
  ].join("\n"));
  if (!confirmed) return;
  setBusy(true, "Выгружаю карточки Ozon в Яндекс...");
  els.sendYandex.disabled = true;
  try {
    const payload = await api("/api/ozon-yandex-import/send", {
      method: "POST",
      body: { limit, confirmed: true },
    });
    renderImportFailures(payload.results || []);
    if (payload.warnings?.length) renderWarnings(payload.warnings);
    setBusy(false, `Yandex: карточки ${payload.sent || 0}, ошибки карточек ${payload.failed || 0}. Цены ${payload.priceSent || 0}/${payload.priceFailed || 0}. Остатки ${payload.stockSent || 0}/${payload.stockFailed || 0}. Уже были ${payload.skippedExisting || 0}. Осталось на следующий запуск ${payload.skippedByLimit || 0}.`);
    await loadYandexImportHistory();
  } catch (error) {
    setBusy(false, `Ошибка выгрузки в Яндекс: ${error.message || error}`);
  } finally {
    renderSummary();
    renderRows();
  }
}

async function syncYandexStocks() {
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  setBusy(true, "Синхронизирую остатки Ozon → Яндекс...");
  try {
    const payload = await api("/api/ozon-yandex-import/sync-stocks", {
      method: "POST",
      body: { limit },
    });
    setBusy(false, `Остатки отправлены: ${payload.sent || 0}. Пропущено без совпадения в Яндексе: ${payload.skipped || 0}.`);
    if (payload.warnings?.length) renderWarnings(payload.warnings);
  } catch (error) {
    setBusy(false, `Ошибка остатков: ${error.message || error}`);
  }
}

async function archiveBlocked() {
  const blockedCount = Number(state.summary.blocked || 0);
  if (!blockedCount) {
    els.status.textContent = "Заблокированных карточек нет.";
    return;
  }
  const confirmed = window.confirm(`Архивировать заблокированные Ozon-карточки из текущего лимита? Количество: ${blockedCount}.`);
  if (!confirmed) return;
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  setBusy(true, "Архивирую заблокированные карточки в Ozon...");
  try {
    const payload = await api("/api/ozon-yandex-import/archive-blocked", {
      method: "POST",
      body: { confirmed: true, limit },
    });
    setBusy(false, `Архивировано: ${payload.archived || 0}. Ошибок: ${payload.failed || 0}.`);
    await loadPreview(false);
  } catch (error) {
    setBusy(false, `Ошибка архивации: ${error.message || error}`);
  }
}

els.loadWarehouse.addEventListener("click", () => loadPreview(false));
els.loadOzon.addEventListener("click", () => loadPreview(true));
els.syncStocks.addEventListener("click", syncYandexStocks);
els.archiveBlocked.addEventListener("click", archiveBlocked);
els.sendYandex.addEventListener("click", sendYandexImport);
els.filter.addEventListener("change", renderRows);
els.cleanupPreview?.addEventListener("click", previewYandexCleanup);
els.cleanupCsv?.addEventListener("click", downloadYandexCleanupCsv);
els.cleanupArchive?.addEventListener("click", archiveYandexCleanup);
els.cleanupHistoryRefresh?.addEventListener("click", loadYandexCleanupHistory);
els.importHistoryRefresh?.addEventListener("click", loadYandexImportHistory);
els.cleanupFilter?.addEventListener("change", () => renderCleanup({ rows: state.cleanupRows, summary: state.cleanupSummary }));
els.cleanupSearch?.addEventListener("input", () => renderCleanup({ rows: state.cleanupRows, summary: state.cleanupSummary }));
els.cleanupBrands?.addEventListener("input", () => {
  const toDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
  els.cleanupArchive.disabled = !(toDelete > 0 && parseCleanupBrands().length);
});

loadPreview(false).catch(() => {});
loadYandexCleanupHistory().catch(() => {});
loadYandexImportHistory().catch(() => {});
