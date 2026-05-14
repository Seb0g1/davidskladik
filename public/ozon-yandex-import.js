const state = {
  rows: [],
  summary: { total: 0, eligible: 0, blocked: 0, existingInYandex: 0, missingRequired: 0 },
  cleanupRows: [],
  cleanupSummary: { total: 0, protected: 0, toDelete: 0, toArchive: 0, alreadyArchived: 0 },
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
  rows: document.getElementById("importRows"),
  cleanupBrands: document.getElementById("yandexProtectedBrandsInput"),
  cleanupPreview: document.getElementById("previewYandexCleanupButton"),
  cleanupArchive: document.getElementById("archiveYandexCleanupButton"),
  cleanupStatus: document.getElementById("yandexCleanupStatus"),
  cleanupTotal: document.getElementById("yandexCleanupTotalCount"),
  cleanupProtected: document.getElementById("yandexCleanupProtectedCount"),
  cleanupToArchive: document.getElementById("yandexCleanupArchiveCount"),
  cleanupAlreadyArchived: document.getElementById("yandexCleanupAlreadyArchivedCount"),
  cleanupWarnings: document.getElementById("yandexCleanupWarnings"),
  cleanupRows: document.getElementById("yandexCleanupRows"),
  cleanupRowsMeta: document.getElementById("yandexCleanupRowsMeta"),
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

function cleanupStatus(row) {
  if (row.protected) return { className: "good", label: "Оставить" };
  if (row.archived) return { className: "danger", label: "Удалить (архив)" };
  return { className: "danger", label: "Удалить" };
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
  renderCleanupWarnings(payload.warnings);

  const rows = state.cleanupRows.slice(0, 1000);
  els.cleanupRowsMeta.textContent = state.cleanupRows.length
    ? `Показано ${rows.length} из ${state.cleanupRows.length}`
    : "Нет данных";
  if (!rows.length) {
    els.cleanupRows.innerHTML = `<div class="empty-state">Запустите предпросмотр, чтобы увидеть товары Яндекса.</div>`;
    return;
  }
  els.cleanupRows.innerHTML = rows.map((row) => {
    const status = cleanupStatus(row);
    const reasons = row.protected
      ? [`Защищено: ${(row.matchedBrands || []).join(", ")}`]
      : row.smallVolume
        ? [`Объем меньше 20 мл${row.minVolumeMl ? `: ${row.minVolumeMl} мл` : ""}. Удаляем даже при защищённом бренде.`]
        : row.archived
          ? ["Товар уже в архиве Яндекса, но всё равно будет удалён из каталога."]
          : ["Бренд не найден в названии, описании или характеристиках"];
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

async function previewYandexCleanup() {
  const protectedBrands = parseCleanupBrands();
  if (!protectedBrands.length) {
    els.cleanupStatus.textContent = "Укажите хотя бы один бренд, который нельзя удалять.";
    return;
  }
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  els.cleanupPreview.disabled = true;
  els.cleanupArchive.disabled = true;
  els.cleanupStatus.textContent = "Читаю все статусы Яндекс Маркета и ищу защищённые бренды...";
  try {
    const payload = await api("/api/yandex-cleanup/preview", {
      method: "POST",
      body: { protectedBrands, limit },
    });
    renderCleanup(payload);
    els.cleanupStatus.textContent = `Проверено: ${payload.summary?.total || 0}. Оставить: ${payload.summary?.protected || 0}. Удалить: ${payload.summary?.toDelete ?? payload.summary?.toArchive ?? 0}.`;
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
  const confirmed = window.confirm(`Удалить ${toDelete} товаров из каталога Яндекс, кроме защищённых брендов: ${protectedBrands.join(", ")}? Это действие нельзя откатить.`);
  if (!confirmed) return;
  const confirmationText = window.prompt("Для подтверждения введите: УДАЛИТЬ ЯНДЕКС", "");
  if (confirmationText !== "УДАЛИТЬ ЯНДЕКС") {
    els.cleanupStatus.textContent = "Очистка отменена: подтверждение не совпало.";
    return;
  }
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000)));
  els.cleanupPreview.disabled = true;
  els.cleanupArchive.disabled = true;
  els.cleanupStatus.textContent = "Удаляю незащищённые товары из каталога Яндекса...";
  try {
    const payload = await api("/api/yandex-cleanup/delete", {
      method: "POST",
      body: { protectedBrands, limit, confirmed: true, confirmationText },
    });
    els.cleanupStatus.textContent = `Удалено: ${payload.deleted || 0}. Не удалено: ${payload.failed || 0}.`;
    await previewYandexCleanup();
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
els.filter.addEventListener("change", renderRows);
els.cleanupPreview?.addEventListener("click", previewYandexCleanup);
els.cleanupArchive?.addEventListener("click", archiveYandexCleanup);
els.cleanupBrands?.addEventListener("input", () => {
  const toDelete = Number(state.cleanupSummary.toDelete ?? state.cleanupSummary.toArchive ?? 0);
  els.cleanupArchive.disabled = !(toDelete > 0 && parseCleanupBrands().length);
});

loadPreview(false).catch(() => {});
