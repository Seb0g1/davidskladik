const MAIN_TAB_STORAGE_KEY = "magicVibesActiveTab";
const VALID_MAIN_TABS = new Set(["warehouse", "suppliers", "accounts"]);
const WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY = "magicVibesWarehouseAutoFocusAnim";

const state = {
  targets: [],
  warehouse: [],
  filteredWarehouse: [],
  suppliers: [],
  supplierView: "active",
  supplierSearch: "",
  accounts: [],
  hiddenAccounts: [],
  selectedWarehouseProductId: null,
  selectedWarehouseGroupKey: null,
  selectedWarehouseDetailGroup: null,
  warehouseMarketplace: "all",
  ozonStateFilter: "all",
  warehouseAutoOnly: false,
  warehouseLinkFilter: "all",
  warehouseBrandFilter: "",
  warehouseBrands: [],
  warehouseAnimateAutoFocus: localStorage.getItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY) !== "0",
  warehouseViewMode: localStorage.getItem("warehouseViewMode") || "cards",
  warehouseVisibleLimit: 80,
  warehousePageSize: 60,
  warehousePage: 0,
  warehouseRestorePage: 1,
  warehouseHasMore: true,
  warehouseLoadingPage: false,
  warehouseTotalFiltered: 0,
  warehouseRequestToken: 0,
  warehouseScrollTop: 0,
  warehouseLastGroupOrder: [],
  warehouseAutoFocusGroupKey: null,
  enrichedProductIds: new Set(),
  retryQueue: [],
  retryQueueSelectedKeys: new Set(),
  retryQueueSort: "newest",
  retryQueueMarketplace: "all",
  retryQueueSearch: "",
  retryQueueLastRun: null,
};

const elements = {
  logoutButton: document.querySelector("#logoutButton"),
  tabButtons: document.querySelectorAll(".tab-button"),
  tabPanels: document.querySelectorAll(".tab-panel"),
  warehouseForm: document.querySelector("#warehouseForm"),
  warehouseTargetInput: document.querySelector("#warehouseTargetInput"),
  warehouseUsdRateInput: document.querySelector("#warehouseUsdRateInput"),
  warehouseOfferInput: document.querySelector("#warehouseOfferInput"),
  warehouseNameInput: document.querySelector("#warehouseNameInput"),
  warehouseKeywordInput: document.querySelector("#warehouseKeywordInput"),
  warehouseMarkupInput: document.querySelector("#warehouseMarkupInput"),
  bulkMarkupForm: document.querySelector("#bulkMarkupForm"),
  bulkMarkupInput: document.querySelector("#bulkMarkupInput"),
  mergeProductsButton: document.querySelector("#mergeProductsButton"),
  unmergeProductsButton: document.querySelector("#unmergeProductsButton"),
  autoPriceEnableSelectedButton: document.querySelector("#autoPriceEnableSelectedButton"),
  autoPriceDisableSelectedButton: document.querySelector("#autoPriceDisableSelectedButton"),
  autoPriceDisableAllButton: document.querySelector("#autoPriceDisableAllButton"),
  manualProductToggle: document.querySelector("#manualProductToggle"),
  warehouseSyncButton: document.querySelector("#warehouseSyncButton"),
  warehouseRefreshPricesButton: document.querySelector("#warehouseRefreshPricesButton"),
  warehouseDryRunButton: document.querySelector("#warehouseDryRunButton"),
  warehouseRetryQueueButton: document.querySelector("#warehouseRetryQueueButton"),
  warehouseSyncProgress: document.querySelector("#warehouseSyncProgress"),
  syncProgressTitle: document.querySelector("#syncProgressTitle"),
  syncProgressStage: document.querySelector("#syncProgressStage"),
  syncProgressTargets: document.querySelector("#syncProgressTargets"),
  syncProgressBar: document.querySelector("#syncProgressBar"),
  syncProgressMeta: document.querySelector("#syncProgressMeta"),
  syncProgressClose: document.querySelector("#syncProgressClose"),
  syncMiniProgress: document.querySelector("#syncMiniProgress"),
  syncMiniStage: document.querySelector("#syncMiniStage"),
  syncMiniPercent: document.querySelector("#syncMiniPercent"),
  syncMiniBar: document.querySelector("#syncMiniBar"),
  dailySyncStatus: document.querySelector("#dailySyncStatus"),
  dailySyncMeta: document.querySelector("#dailySyncMeta"),
  dailySyncRunButton: document.querySelector("#dailySyncRunButton"),
  syncLogList: document.querySelector("#syncLogList"),
  warehouseSearchInput: document.querySelector("#warehouseSearchInput"),
  ozonStateFilter: document.querySelector("#ozonStateFilter"),
  warehouseAutoPriceOnlyInput: document.querySelector("#warehouseAutoPriceOnlyInput"),
  warehouseLinkFilterInput: document.querySelector("#warehouseLinkFilterInput"),
  warehouseBrandFilterInput: document.querySelector("#warehouseBrandFilterInput"),
  warehouseBrandSuggestions: document.querySelector("#warehouseBrandSuggestions"),
  warehouseAnimateAutoFocusInput: document.querySelector("#warehouseAnimateAutoFocusInput"),
  warehouseMinDiffRubInput: document.querySelector("#warehouseMinDiffRubInput"),
  warehouseMinDiffPctInput: document.querySelector("#warehouseMinDiffPctInput"),
  warehouseStatus: document.querySelector("#warehouseStatus"),
  warehouseNoSupplierAlert: document.querySelector("#warehouseNoSupplierAlert"),
  retryQueuePanel: document.querySelector("#retryQueuePanel"),
  retryQueueMeta: document.querySelector("#retryQueueMeta"),
  retryQueueStats: document.querySelector("#retryQueueStats"),
  retryQueueList: document.querySelector("#retryQueueList"),
  retryQueueSearchInput: document.querySelector("#retryQueueSearchInput"),
  retryQueueMarketplaceFilterInput: document.querySelector("#retryQueueMarketplaceFilterInput"),
  retryQueueSortInput: document.querySelector("#retryQueueSortInput"),
  retryQueueRefreshButton: document.querySelector("#retryQueueRefreshButton"),
  retryQueueRetrySelectedButton: document.querySelector("#retryQueueRetrySelectedButton"),
  retryQueueClearButton: document.querySelector("#retryQueueClearButton"),
  warehouseRateInfo: document.querySelector("#warehouseRateInfo"),
  warehouseCards: document.querySelector("#warehouseCards"),
  warehouseViewButtons: document.querySelectorAll("[data-warehouse-view]"),
  warehouseLoadMoreButton: document.querySelector("#warehouseLoadMoreButton"),
  warehouseVisibleInfo: document.querySelector("#warehouseVisibleInfo"),
  warehouseToolbarHint: document.querySelector("#warehouseToolbarHint"),
  warehouseSelectionLine: document.querySelector("#warehouseSelectionLine"),
  warehouseDetail: document.querySelector("#warehouseDetail"),
  warehouseTotal: document.querySelector("#warehouseTotal"),
  warehouseReady: document.querySelector("#warehouseReady"),
  warehouseChanged: document.querySelector("#warehouseChanged"),
  warehouseNoSupplier: document.querySelector("#warehouseNoSupplier"),
  warehouseOzonArchived: document.querySelector("#warehouseOzonArchived"),
  warehouseOzonInactive: document.querySelector("#warehouseOzonInactive"),
  warehouseOzonOutOfStock: document.querySelector("#warehouseOzonOutOfStock"),
  warehouseSelectChangedButton: document.querySelector("#warehouseSelectChangedButton"),
  warehouseSendButton: document.querySelector("#warehouseSendButton"),
  linkFormTemplate: document.querySelector("#linkFormTemplate"),
  supplierForm: document.querySelector("#supplierForm"),
  supplierIdInput: document.querySelector("#supplierIdInput"),
  supplierNameInput: document.querySelector("#supplierNameInput"),
  supplierNoteInput: document.querySelector("#supplierNoteInput"),
  supplierStopReasonInput: document.querySelector("#supplierStopReasonInput"),
  supplierSaveButton: document.querySelector("#supplierSaveButton"),
  supplierCancelEditButton: document.querySelector("#supplierCancelEditButton"),
  supplierStatus: document.querySelector("#supplierStatus"),
  supplierSearchInput: document.querySelector("#supplierSearchInput"),
  supplierBoard: document.querySelector("#supplierBoard"),
  supplierViewButtons: document.querySelectorAll("[data-supplier-view]"),
  supplierArticleFormTemplate: document.querySelector("#supplierArticleFormTemplate"),
  supplierInactiveModal: document.querySelector("#supplierInactiveModal"),
  supplierInactiveForm: document.querySelector("#supplierInactiveForm"),
  supplierInactiveSupplierId: document.querySelector("#supplierInactiveSupplierId"),
  supplierInactiveCommentInput: document.querySelector("#supplierInactiveCommentInput"),
  supplierInactiveUntilInput: document.querySelector("#supplierInactiveUntilInput"),
  supplierInactiveUnknownInput: document.querySelector("#supplierInactiveUnknownInput"),
  supplierInactiveQuickButtons: document.querySelectorAll("[data-inactive-quick]"),
  supplierInactiveCancel: document.querySelector("#supplierInactiveCancel"),
  supplierInactiveSubmit: document.querySelector("#supplierInactiveSubmit"),
  accountForm: document.querySelector("#accountForm"),
  accountFormTitle: document.querySelector("#accountFormTitle"),
  accountIdInput: document.querySelector("#accountIdInput"),
  accountMarketplaceInput: document.querySelector("#accountMarketplaceInput"),
  accountSaveButton: document.querySelector("#accountSaveButton"),
  accountCancelEditButton: document.querySelector("#accountCancelEditButton"),
  accountStatus: document.querySelector("#accountStatus"),
  accountsBoard: document.querySelector("#accountsBoard"),
  hiddenAccountsBoard: document.querySelector("#hiddenAccountsBoard"),
  reloadAccountsButton: document.querySelector("#reloadAccountsButton"),
  confirmModal: document.querySelector("#confirmModal"),
  confirmTitle: document.querySelector("#confirmTitle"),
  confirmText: document.querySelector("#confirmText"),
  confirmCancel: document.querySelector("#confirmCancel"),
  confirmOk: document.querySelector("#confirmOk"),
};

const WAREHOUSE_URL_PAGE_MAX = 500;
const WAREHOUSE_SCROLL_SESSION_KEY = "magicVibesWarehouseScrollY";

function toPositiveInt(value, fallback = 1) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.floor(n);
}

function setWarehouseMarketplaceUI(value) {
  document.querySelectorAll("[data-marketplace]").forEach((item) => {
    item.classList.toggle("active", item.dataset.marketplace === value);
  });
}

function applyWarehouseStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const q = params.get("q");
  const marketplace = params.get("marketplace");
  const stateCode = params.get("state");
  const linked = params.get("linked");
  const view = params.get("view");
  const page = params.get("page");
  const group = params.get("group");
  const scroll = params.get("s");

  if (q !== null && elements.warehouseSearchInput) elements.warehouseSearchInput.value = q;
  if (marketplace && ["all", "ozon", "yandex"].includes(marketplace)) state.warehouseMarketplace = marketplace;
  if (stateCode && elements.ozonStateFilter?.querySelector(`option[value="${stateCode}"]`)) state.ozonStateFilter = stateCode;
  if (linked && ["all", "linked", "unlinked"].includes(linked)) state.warehouseLinkFilter = linked;
  const brand = params.get("brand");
  if (brand !== null) state.warehouseBrandFilter = String(brand);
  state.warehouseAutoOnly = false;
  if (view === "list" || view === "cards") state.warehouseViewMode = view;
  state.warehouseRestorePage = Math.min(WAREHOUSE_URL_PAGE_MAX, toPositiveInt(page, 1));
  state.warehousePage = 0;
  state.selectedWarehouseGroupKey = group ? String(group) : null;
  state.selectedWarehouseProductId = null;
  state.warehouseScrollTop = Math.max(0, Number(scroll || 0) || 0);
}

function syncWarehouseStateToUrl({ replace = true } = {}) {
  const params = new URLSearchParams();
  const q = elements.warehouseSearchInput?.value?.trim();
  if (q) params.set("q", q);
  if (state.warehouseMarketplace !== "all") params.set("marketplace", state.warehouseMarketplace);
  if (state.ozonStateFilter !== "all") params.set("state", state.ozonStateFilter);
  if (state.warehouseLinkFilter !== "all") params.set("linked", state.warehouseLinkFilter);
  if (state.warehouseBrandFilter) params.set("brand", state.warehouseBrandFilter);
  if (state.warehouseViewMode !== "cards") params.set("view", state.warehouseViewMode);
  if (state.warehousePage > 1) params.set("page", String(state.warehousePage));
  if (state.selectedWarehouseGroupKey) params.set("group", state.selectedWarehouseGroupKey);
  if (state.warehouseScrollTop > 0) params.set("s", String(Math.round(state.warehouseScrollTop)));
  const next = `${window.location.pathname}${params.toString() ? `?${params}` : ""}`;
  if (replace) window.history.replaceState(null, "", next);
  else window.history.pushState(null, "", next);
}

function captureWarehouseScroll() {
  state.warehouseScrollTop = Math.max(0, Number(window.scrollY || window.pageYOffset || 0));
  try {
    sessionStorage.setItem(WAREHOUSE_SCROLL_SESSION_KEY, String(Math.round(state.warehouseScrollTop)));
  } catch (_error) {
    // Ignore private mode/storage quota issues.
  }
}

function resolveWarehouseRestoreScroll() {
  if (state.warehouseScrollTop > 0) return state.warehouseScrollTop;
  try {
    const saved = Number(sessionStorage.getItem(WAREHOUSE_SCROLL_SESSION_KEY) || 0);
    return Number.isFinite(saved) && saved > 0 ? saved : 0;
  } catch (_error) {
    return 0;
  }
}

function restoreWarehouseScroll() {
  const target = resolveWarehouseRestoreScroll();
  if (!target) return;
  window.requestAnimationFrame(() => {
    window.scrollTo({ top: target, behavior: "auto" });
    window.requestAnimationFrame(() => window.scrollTo({ top: target, behavior: "auto" }));
  });
}

if (elements.warehouseSyncProgress) document.body.appendChild(elements.warehouseSyncProgress);
if (elements.syncMiniProgress) document.body.appendChild(elements.syncMiniProgress);

function confirmAction({ title = "Подтвердите действие", text = "Продолжить?", okText = "Подтвердить", danger = true } = {}) {
  if (!elements.confirmModal) return Promise.resolve(window.confirm(text));
  elements.confirmTitle.textContent = title;
  elements.confirmText.textContent = text;
  elements.confirmOk.textContent = okText;
  elements.confirmOk.classList.toggle("danger-button", danger);
  elements.confirmOk.classList.toggle("primary-button", !danger);
  elements.confirmModal.classList.remove("hidden");
  return new Promise((resolve) => {
    const cleanup = (value) => {
      elements.confirmModal.classList.add("hidden");
      elements.confirmCancel.removeEventListener("click", onCancel);
      elements.confirmOk.removeEventListener("click", onOk);
      elements.confirmModal.removeEventListener("click", onBackdrop);
      document.removeEventListener("keydown", onKeydown);
      resolve(value);
    };
    const onCancel = () => cleanup(false);
    const onOk = () => cleanup(true);
    const onBackdrop = (event) => {
      if (event.target === elements.confirmModal) cleanup(false);
    };
    const onKeydown = (event) => {
      if (event.key === "Escape") cleanup(false);
    };
    elements.confirmCancel.addEventListener("click", onCancel);
    elements.confirmOk.addEventListener("click", onOk);
    elements.confirmModal.addEventListener("click", onBackdrop);
    document.addEventListener("keydown", onKeydown);
    elements.confirmOk.focus();
  });
}

function formatNumber(value) {
  return new Intl.NumberFormat("ru-RU").format(Number(value || 0));
}

function formatMoney(value) {
  if (!value) return "-";
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "RUB",
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function formatUsd(value) {
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

function supplierUsdPriceLabel(supplier) {
  const usd = formatUsd(supplier?.price || 0);
  if (supplier?.convertedFromRub && Number(supplier.originalPrice || 0) > 0) {
    return `${usd} (из ${formatMoney(supplier.originalPrice)})`;
  }
  return usd;
}

function formatDate(value) {
  if (!value) return "";
  return new Intl.DateTimeFormat("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizeSupplierName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/\s+/g, " ")
    .trim();
}

/** Доп. строки к цене Ozon из Seller API (акция, старая цена, минимум). */
function ozonCabinetPriceNote(item) {
  if (!item || item.marketplace !== "ozon") return "";
  const bits = [];
  const m = item.ozon?.marketingSellerPrice;
  const cur = Number(item.currentPrice || 0);
  if (m && cur && Math.abs(Number(m) - cur) >= 1) bits.push(`акция селлера ${formatMoney(m)}`);
  if (item.ozon?.marketingPrice && (!m || Math.abs(Number(item.ozon.marketingPrice) - cur) >= 1)) {
    bits.push(`маркетинг ${formatMoney(item.ozon.marketingPrice)}`);
  }
  if (item.ozon?.oldPrice) bits.push(`зачёркнутая ${formatMoney(item.ozon.oldPrice)}`);
  if (item.ozonMinPrice) bits.push(`мин. в кабинете ${formatMoney(item.ozonMinPrice)}`);
  return bits.length ? bits.join(" · ") : "";
}

function marketplaceCurrentLabel(item) {
  if (!item) return "Текущая";
  return item.marketplace === "ozon" ? "Цена в Ozon" : "Текущая";
}

function showToast(message, variant = "warn") {
  const text = String(message || "").trim();
  if (!text) return;
  let root = document.getElementById("toastRoot");
  if (!root) {
    root = document.createElement("div");
    root.id = "toastRoot";
    root.className = "toast-stack";
    document.body.appendChild(root);
  }
  const el = document.createElement("div");
  el.className = `toast toast--${variant === "error" ? "error" : "warn"}`;
  el.textContent = text;
  root.appendChild(el);
  setTimeout(() => el.remove(), 14000);
}

async function api(path, options) {
  const response = await fetch(path, options);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    const missing = Array.isArray(detail.missing) ? ` Не хватает: ${detail.missing.join(", ")}` : "";
    const error = new Error(`${detail.detail || detail.error || "Ошибка запроса"}${missing}`);
    error.status = response.status;
    error.payload = detail;
    throw error;
  }
  return response.json();
}

const pmSuggestControllers = new WeakMap();

function closeAllPmSuggestPanels(exceptWrap) {
  document.querySelectorAll(".pm-suggest-panel").forEach((panel) => {
    const wrap = panel.closest(".pm-autocomplete-wrap");
    if (exceptWrap && wrap === exceptWrap) return;
    panel.hidden = true;
    panel.replaceChildren();
  });
}

function ensurePmSuggestPanel(wrap) {
  let panel = wrap.querySelector(".pm-suggest-panel");
  if (!panel) {
    panel = document.createElement("div");
    panel.className = "pm-suggest-panel";
    panel.hidden = true;
    panel.setAttribute("role", "listbox");
    wrap.appendChild(panel);
  }
  return panel;
}

function dedupeOfferRows(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const key = `${row.article || ""}|${row.partnerName || ""}|${row.name || ""}|${row.rowId || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
    if (out.length >= 30) break;
  }
  return out;
}

function renderPmPartnerPanel(panel, items, input) {
  panel.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "pm-suggest-empty";
    empty.textContent = "Нет поставщиков по этому запросу.";
    panel.appendChild(empty);
    panel.hidden = false;
    return;
  }
  for (const row of items) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "pm-suggest-option";
    btn.setAttribute("role", "option");
    btn.addEventListener("mousedown", (e) => e.preventDefault());
    btn.addEventListener("click", () => {
      input.value = row.name || "";
      closeAllPmSuggestPanels(null);
      input.dispatchEvent(new Event("input", { bubbles: true }));
    });
    const title = document.createElement("span");
    title.className = "pm-suggest-title";
    title.textContent = row.name || "";
    const meta = document.createElement("span");
    meta.className = "pm-suggest-meta";
    meta.textContent = `PartnerID ${row.id}`;
    btn.append(title, meta);
    panel.appendChild(btn);
  }
  panel.hidden = false;
}

function renderPmOfferPanel(panel, rows, input) {
  const form = input.closest("form");
  panel.replaceChildren();
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "pm-suggest-empty";
    empty.textContent = "Нет строк прайса по запросу.";
    panel.appendChild(empty);
    panel.hidden = false;
    return;
  }
  for (const row of rows) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "pm-suggest-option";
    btn.setAttribute("role", "option");
    btn.addEventListener("mousedown", (e) => e.preventDefault());
    btn.addEventListener("click", () => {
      input.value = row.article || "";
      const supplierInput = form?.querySelector('[name="supplierName"]');
      if (supplierInput && !String(supplierInput.value || "").trim() && row.partnerName) {
        supplierInput.value = row.partnerName;
      }
      closeAllPmSuggestPanels(null);
      input.dispatchEvent(new Event("input", { bubbles: true }));
    });
    const title = document.createElement("span");
    title.className = "pm-suggest-title";
    title.textContent = row.article || "";
    const line = document.createElement("span");
    line.className = "pm-suggest-line";
    line.textContent = row.name || "";
    const meta = document.createElement("span");
    meta.className = "pm-suggest-meta";
    const partner = row.partnerName || "—";
    const usd = Number(row.price || 0).toFixed(2);
    meta.textContent = `${partner} · $${usd}`;
    btn.append(title, line, meta);
    panel.appendChild(btn);
  }
  panel.hidden = false;
}

async function runPmSuggestFetch(input) {
  const wrap = input.closest(".pm-autocomplete-wrap");
  if (!wrap) return;
  const kind = input.dataset.pmSuggest;
  if (!kind) return;

  const q = String(input.value || "").trim();
  const panel = ensurePmSuggestPanel(wrap);

  const prev = pmSuggestControllers.get(input);
  if (prev) prev.abort();
  if (!q) {
    panel.hidden = true;
    panel.replaceChildren();
    return;
  }

  closeAllPmSuggestPanels(wrap);

  const controller = new AbortController();
  pmSuggestControllers.set(input, controller);

  try {
    if (kind === "partner") {
      const response = await fetch(`/api/partners/search?q=${encodeURIComponent(q)}&limit=40`, { signal: controller.signal });
      if (response.status === 401) {
        window.location.href = "/login.html";
        return;
      }
      if (!response.ok) throw new Error("partner search");
      const data = await response.json();
      renderPmPartnerPanel(panel, data.items || [], input);
    } else if (kind === "offer") {
      const response = await fetch(`/api/offers?search=${encodeURIComponent(q)}&limit=80`, { signal: controller.signal });
      if (response.status === 401) {
        window.location.href = "/login.html";
        return;
      }
      if (!response.ok) throw new Error("offers search");
      const payload = await response.json();
      const raw = Array.isArray(payload) ? payload : [];
      renderPmOfferPanel(panel, dedupeOfferRows(raw), input);
    }
  } catch (error) {
    if (error.name === "AbortError") return;
    panel.replaceChildren();
    const err = document.createElement("div");
    err.className = "pm-suggest-empty";
    err.textContent = "Не удалось загрузить подсказки.";
    panel.appendChild(err);
    panel.hidden = false;
  }
}

function schedulePmSuggest(input) {
  const wrap = input.closest(".pm-autocomplete-wrap");
  if (!wrap) return;
  clearTimeout(input._pmSuggestTimer);
  input._pmSuggestTimer = setTimeout(() => runPmSuggestFetch(input), 280);
}

document.addEventListener("input", (event) => {
  const el = event.target.closest("[data-pm-suggest]");
  if (!el) return;
  schedulePmSuggest(el);
});

document.addEventListener("focusin", (event) => {
  const el = event.target.closest("[data-pm-suggest]");
  if (!el) return;
  if (String(el.value || "").trim().length) schedulePmSuggest(el);
});

document.addEventListener("click", (event) => {
  if (event.target.closest(".pm-autocomplete-wrap")) return;
  closeAllPmSuggestPanels(null);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeAllPmSuggestPanels(null);
});

function marketLabel(product) {
  return product.marketplace === "yandex" ? "Yandex Market" : "Ozon";
}

function statusLabel(product) {
  if (product.status === "no_supplier") return "Нет поставщика";
  if (product.changed) return "Нужно обновить";
  return "Актуально";
}

function statusClass(product) {
  if (product.status === "no_supplier") return "neutral";
  if (product.changed) return "warn";
  return "ok";
}

function ozonStateLabel(product) {
  if (product.marketplace !== "ozon") return "";
  return product.marketplaceState?.label || "Статус Ozon не загружен";
}

function ozonStateClass(product) {
  const code = product.marketplaceState?.code || "unknown";
  if (code === "active") return "ok";
  if (code === "archived") return "dark";
  if (code === "inactive") return "warn";
  if (code === "out_of_stock") return "danger";
  return "neutral";
}

function ozonStateMeta(product) {
  const state = product.marketplaceState || {};
  const parts = [];
  if (state.stock !== undefined) parts.push(`остаток ${formatNumber(state.stock)}`);
  if (state.visibility) parts.push(`видимость ${state.visibility}`);
  if (state.stateName) parts.push(state.stateName);
  return parts.join(" · ");
}

function marketplaceStateLabel(product) {
  if (product.marketplace === "ozon") return ozonStateLabel(product);
  return product.marketplaceState?.label || "Статус ЯМ не загружен";
}

function marketplaceStateClass(product) {
  return ozonStateClass(product);
}

function marketplaceStateMeta(product) {
  const state = product.marketplaceState || {};
  const parts = [];
  if (state.stock !== undefined) parts.push(`остаток ${formatNumber(state.stock)}`);
  if (state.visibility) parts.push(`видимость ${state.visibility}`);
  if (state.stateName) parts.push(state.stateName);
  return parts.join(" · ");
}

function displayProductName(product) {
  const offerId = String(product.offerId || "").trim().toLowerCase();
  const candidates = [
    product.name,
    product.ozon?.name,
    product.yandex?.name,
    product.keyword,
    product.offerId,
  ];
  const goodName = candidates.find((value) => {
    const text = String(value || "").trim();
    return text && text.toLowerCase() !== offerId && /\s/.test(text);
  });
  return goodName || `Товар ${marketLabel(product)}`;
}

function productGroupKey(product) {
  if (product.manualGroupId) return `manual:${product.manualGroupId}`;
  const offer = String(product.offerId || "").trim().toLowerCase();
  if (offer) return `offer:${offer}`;
  return `name:${displayProductName(product).trim().toLowerCase()}`;
}

function buildWarehouseGroups(products) {
  const map = new Map();
  for (const product of products) {
    const key = productGroupKey(product);
    const group = map.get(key) || {
      key,
      offerId: product.offerId,
      name: displayProductName(product),
      products: [],
    };
    group.products.push(product);
    if (!group.image && productImage(product)) group.image = productImage(product);
    map.set(key, group);
  }

  return Array.from(map.values()).map((group) => {
    const variants = group.products.sort((a, b) => {
      const rank = { ozon: 0, yandex: 1 };
      return (rank[a.marketplace] ?? 9) - (rank[b.marketplace] ?? 9) || String(a.targetName).localeCompare(String(b.targetName));
    });
    const primary = variants[0];
    const brandLabel =
      variants.map((p) => String(p.brand || p.ozon?.vendor || p.yandex?.vendor || "").trim()).find(Boolean) || "";
    const links = variants.flatMap((product) => (product.links || []).map((link) => ({ ...link, productId: product.id })));
    const suppliers = variants.flatMap((product) => product.suppliers || []);
    return {
      ...group,
      brand: brandLabel,
      primary,
      variants,
      links,
      suppliers,
      selectedSupplier: variants.find((product) => product.selectedSupplier)?.selectedSupplier || primary.selectedSupplier,
      selectedSupplierReason: variants.find((product) => product.selectedSupplierReason)?.selectedSupplierReason || primary.selectedSupplierReason || "",
      ready: variants.some((product) => product.ready),
      changed: variants.some((product) => product.changed),
      supplierCount: Math.max(...variants.map((product) => product.supplierCount || 0), 0),
      availableSupplierCount: Math.max(...variants.map((product) => product.availableSupplierCount || 0), 0),
      marketplaceLabels: Array.from(new Set(variants.map((product) => marketLabel(product)))),
      productIds: variants.map((product) => product.id),
    };
  });
}

/** Порядок в каталоге: сначала активные витрины, затем прочие статусы. */
const WAREHOUSE_LISTING_STATE_RANK = {
  active: 0,
  unknown: 1,
  out_of_stock: 2,
  inactive: 3,
  archived: 4,
};

function variantListingRank(product) {
  const code = product.marketplaceState?.code || "unknown";
  return WAREHOUSE_LISTING_STATE_RANK[code] ?? 1;
}

/** Лучший статус среди вариантов карточки: если хотя бы один канал активен — группа вверху списка. */
function groupListingRank(group) {
  const variants = group.variants || group.products || [];
  if (!variants.length) return 99;
  return Math.min(...variants.map(variantListingRank));
}

function sortWarehouseGroups(groups) {
  return [...groups].sort((a, b) => {
    const ra = groupListingRank(a);
    const rb = groupListingRank(b);
    if (ra !== rb) return ra - rb;
    if (Boolean(b.changed) !== Boolean(a.changed)) return a.changed ? -1 : 1;
    if (Boolean(b.ready) !== Boolean(a.ready)) return a.ready ? -1 : 1;
    return String(a.name || "").localeCompare(String(b.name || ""), "ru", { sensitivity: "base" });
  });
}

function getSortedWarehouseGroups() {
  return sortWarehouseGroups(buildWarehouseGroups(state.filteredWarehouse));
}

function ozonUrl(product) {
  if (product.marketplace !== "ozon") return "";
  if (product.productUrl) return product.productUrl;
  const publicId = product.sku || product.ozon?.sku;
  if (publicId) return `https://www.ozon.ru/product/${encodeURIComponent(publicId)}/`;
  const offerId = product.offerId || product.ozon?.offerId;
  return offerId ? `https://seller.ozon.ru/app/products?search=${encodeURIComponent(offerId)}` : "";
}

function marketplaceUrl(product) {
  if (product.marketplace === "ozon") return ozonUrl(product);
  const directUrl = product.productUrl || product.yandex?.url || product.yandex?.marketUrl;
  if (directUrl) return directUrl;
  const query = product.offerId || product.yandex?.offerId || product.name || product.yandex?.name;
  return query ? `https://market.yandex.ru/search?text=${encodeURIComponent(query)}` : "";
}

function productImage(product) {
  const candidates = [
    product.imageUrl,
    product.ozon?.primaryImage,
    ...(Array.isArray(product.ozon?.images) ? product.ozon.images : []),
    ...(Array.isArray(product.yandex?.pictures) ? product.yandex.pictures : []),
  ];
  return candidates.find(Boolean) || "";
}

function needsMediaEnrichment(product) {
  return product.marketplace === "ozon" && (!productImage(product) || !product.sku || !product.productUrl || !product.marketplaceState?.code || product.marketplaceState.code === "unknown");
}

async function enrichVisibleProducts(products) {
  const productIds = products
    .filter((product) => needsMediaEnrichment(product) && !state.enrichedProductIds.has(product.id))
    .slice(0, 80)
    .map((product) => product.id);

  if (!productIds.length) return;
  productIds.forEach((id) => state.enrichedProductIds.add(id));

  try {
    const result = await api("/api/warehouse/products/enrich", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds }),
    });
    if (!result.products?.length) return;
    const byId = new Map(result.products.map((product) => [product.id, product]));
    state.warehouse = state.warehouse.map((product) => byId.get(product.id) || product);
    applyWarehouseFilters();
  } catch (_error) {
    // Media enrichment is a progressive enhancement; the main warehouse stays usable.
  }
}

function hasConfiguredYandexTarget() {
  return state.targets.some((target) => target.marketplace === "yandex" && target.configured !== false);
}

function syncTargetNames() {
  const configured = state.targets.filter((target) => target.configured !== false);
  const hasOzon = configured.some((target) => target.marketplace === "ozon");
  const hasYandex = configured.some((target) => target.marketplace === "yandex");
  const names = [];
  if (hasOzon) names.push("Ozon");
  if (hasYandex) names.push("Yandex Market");
  return names.length ? names : ["Ozon", "Yandex Market"];
}

function updateSyncButtonLabel() {
  const names = syncTargetNames();
  elements.warehouseSyncButton.textContent = `Синхронизировать ${names.map((name) => (name === "Yandex Market" ? "ЯМ" : name)).join(" + ")}`;
  elements.warehouseSyncButton.title = `Синхронизация загрузит товары, цены, статусы, остатки и фото: ${names.join(" + ")}.`;
  if (elements.syncProgressTargets) elements.syncProgressTargets.textContent = names.join(" + ");
}

function setProgress(percent, stage, meta) {
  const safePercent = Math.max(0, Math.min(100, Number(percent || 0)));
  if (elements.syncProgressBar) elements.syncProgressBar.style.width = `${safePercent}%`;
  if (elements.syncMiniBar) elements.syncMiniBar.style.width = `${safePercent}%`;
  if (elements.syncMiniPercent) elements.syncMiniPercent.textContent = `${Math.round(safePercent)}%`;
  const track = elements.warehouseSyncProgress?.querySelector(".progress-track");
  if (track) track.setAttribute("aria-valuenow", String(Math.round(safePercent)));
  if (stage) elements.syncProgressStage.textContent = stage;
  if (stage && elements.syncMiniStage) elements.syncMiniStage.textContent = stage;
  if (meta) elements.syncProgressMeta.textContent = meta;
}

function startSyncProgress(mode = "sync") {
  if (!elements.warehouseSyncProgress) return () => {};
  const targets = syncTargetNames();
  elements.warehouseSyncProgress.classList.remove("hidden");
  elements.warehouseSyncProgress.classList.add("running");
  elements.syncMiniProgress?.classList.add("hidden");
  elements.syncProgressTargets.textContent = targets.join(" + ");
  elements.syncProgressTitle.textContent = mode === "prices" ? "Обновление цен маркетплейсов" : "Синхронизация маркетплейсов";
  const stages = mode === "prices"
    ? [
        [18, "Запрашиваю цены", "Получаю актуальные цены из Ozon и Yandex Market."],
        [48, "Сравниваю с личным складом", "Проверяю текущую, минимальную и новую цены."],
        [78, "Пересчитываю карточки", "Обновляю статусы и список товаров на экране."],
      ]
    : [
        [14, "Подключаю кабинеты", `Будут проверены: ${targets.join(" + ")}.`],
        [34, "Загружаю товары", "Получаю товары из Ozon и Yandex Market без ограничения лимита показа."],
        [58, "Проверяю Ozon", "Подтягиваю статусы, архив, активность, остатки, текущую и минимальную цену."],
        [76, "Проверяю поставщиков", "Сверяю привязанные артикулы PriceMaster и стоп-поставщиков."],
        [90, "Обновляю интерфейс", "Собираю карточки, фильтры и сводку склада."],
      ];
  let index = 0;
  setProgress(6, stages[0][1], stages[0][2]);
  const timer = window.setInterval(() => {
    const stage = stages[Math.min(index, stages.length - 1)];
    setProgress(stage[0], stage[1], stage[2]);
    index += 1;
  }, 1200);
  return (success = true) => {
    window.clearInterval(timer);
    setProgress(success ? 100 : 100, success ? "Готово" : "Не удалось завершить", success ? "Данные обновлены. Фильтры и карточки пересобраны." : "Проверьте сообщение об ошибке ниже.");
    elements.warehouseSyncProgress.classList.toggle("running", false);
    window.setTimeout(() => {
      elements.warehouseSyncProgress?.classList.add("hidden");
      elements.syncMiniProgress?.classList.add("hidden");
      setProgress(0, "Подготовка...", "Загружаю товары, цены, статусы, остатки и изображения.");
    }, success ? 1800 : 3500);
  };
}

function exportText(product, target) {
  const stateItem = product.exports?.[target] || product.exports?.[product.target];
  if (!stateItem?.sentAt) return "Ещё не выгружалось";
  return `Последняя выгрузка: ${formatDate(stateItem.sentAt)}`;
}

function focusWarehouseDetailOnSmallScreen() {
  if (window.matchMedia("(max-width: 1120px)").matches) {
    elements.warehouseDetail.scrollIntoView({ behavior: "smooth", block: "start" });
  }
}

function selectedWarehouseIds() {
  return Array.from(document.querySelectorAll(".warehouse-check:checked")).flatMap((input) =>
    String(input.dataset.productIds || input.value || "")
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean),
  );
}

function selectedWarehouseLocks() {
  const uniqueIds = Array.from(new Set(selectedWarehouseIds().map(String)));
  const byId = new Map((state.warehouse || []).map((item) => [String(item.id), item]));
  return uniqueIds.map((id) => ({
    id,
    expectedUpdatedAt: String(byId.get(id)?.updatedAt || ""),
  }));
}

function conflictOfferPreview(conflicts = []) {
  const offers = conflicts
    .map((item) => String(item?.offerId || "").trim())
    .filter(Boolean)
    .slice(0, 3);
  return offers.length ? offers.join(", ") : "";
}

function ozonStateFilterLabel(code) {
  const map = {
    all: "все статусы Ozon",
    active: "только активные",
    archived: "архив",
    inactive: "неактивные",
    out_of_stock: "нет в наличии",
    unknown: "статус не загружен",
  };
  return map[code] || code;
}

function refreshWarehouseToolbarHints() {
  const hint = elements.warehouseToolbarHint;
  if (!hint) return;
  const total = Number(state.warehouseTotalFiltered || 0);
  const loaded = state.filteredWarehouse.length;
  const groupCount = getSortedWarehouseGroups().length;
  const market =
    state.warehouseMarketplace === "all"
      ? "все площадки"
      : state.warehouseMarketplace === "ozon"
        ? "только Ozon"
        : "только Yandex Market";
  const ozonPart =
    state.warehouseMarketplace !== "yandex"
      ? ` · ${ozonStateFilterLabel(state.ozonStateFilter)}`
      : "";
  const q = elements.warehouseSearchInput?.value?.trim();
  const searchPart = q ? ` · поиск «${q}»` : "";
  const autoPart = "";
  const linkPart = state.warehouseLinkFilter === "all"
    ? ""
    : state.warehouseLinkFilter === "linked"
      ? " · только подвязанные"
      : " · только не подвязанные";
  const brandPart = state.warehouseBrandFilter ? ` · бренд «${state.warehouseBrandFilter}»` : "";

  if (!total) {
    hint.textContent = "Склад пуст — добавьте товары вручную или синхронизируйте кабинеты.";
  } else {
    hint.textContent = `На экране ${formatNumber(groupCount)} карточек (${formatNumber(loaded)} загружено из ${formatNumber(total)}) · ${market}${ozonPart}${searchPart}${autoPart}${linkPart}${brandPart} · сверху активные на Ozon/ЯМ`;
  }

  if (elements.warehouseSelectionLine) {
    const n = selectedWarehouseIds().length;
    elements.warehouseSelectionLine.textContent = n ? `Выбрано для обновления цен: ${formatNumber(n)}` : "";
  }
}

function updateSelection() {
  const warehouseSelected = selectedWarehouseIds().length;
  elements.warehouseSendButton.disabled = warehouseSelected === 0;
  refreshWarehouseToolbarHints();
}

function renderTargets() {
  const manualTargets = state.targets.filter((target) => target.configured !== false);

  elements.warehouseTargetInput.innerHTML = manualTargets.length
    ? manualTargets.map((target) => `<option value="${escapeHtml(target.id)}">${escapeHtml(target.name)}</option>`).join("")
    : `<option value="ozon">Ozon</option>`;
  updateSyncButtonLabel();
}

function applyWarehouseFilters() {
  const previousOrder = Array.isArray(state.warehouseLastGroupOrder) ? state.warehouseLastGroupOrder : [];
  const previousSelectedKey = state.selectedWarehouseGroupKey;
  const previousIndex = previousSelectedKey ? previousOrder.indexOf(previousSelectedKey) : -1;
  state.filteredWarehouse = Array.isArray(state.warehouse) ? state.warehouse.slice() : [];

  const groups = getSortedWarehouseGroups();
  const selectedInFiltered = groups.find((group) => group.key === state.selectedWarehouseGroupKey) || null;
  if (state.selectedWarehouseGroupKey && !selectedInFiltered && state.selectedWarehouseDetailGroup?.key !== state.selectedWarehouseGroupKey) {
    state.selectedWarehouseGroupKey = null;
  }
  if (!state.selectedWarehouseGroupKey && groups.length) {
    if (previousIndex >= 0) {
      const nearestIndex = Math.min(previousIndex, groups.length - 1);
      state.selectedWarehouseGroupKey = groups[nearestIndex]?.key || groups[0].key;
      if (state.selectedWarehouseGroupKey) state.warehouseAutoFocusGroupKey = state.selectedWarehouseGroupKey;
    } else {
      state.selectedWarehouseGroupKey = groups[0].key;
    }
  }
  state.warehouseLastGroupOrder = groups.map((group) => group.key);

  if (!state.filteredWarehouse.some((product) => product.id === state.selectedWarehouseProductId)) {
    state.selectedWarehouseProductId = state.filteredWarehouse[0]?.id || null;
  }

  renderWarehouseCards();
  renderWarehouseDetail(
    groups.find((group) => group.key === state.selectedWarehouseGroupKey)
      || (state.selectedWarehouseDetailGroup?.key === state.selectedWarehouseGroupKey ? state.selectedWarehouseDetailGroup : null),
  );
  syncWarehouseStateToUrl();
}

function renderWarehouse(data) {
  const mode = data.mode || "replace";
  const products = Array.isArray(data.products) ? data.products : [];
  if (mode === "append") {
    const byId = new Map(state.warehouse.map((product) => [product.id, product]));
    products.forEach((product) => byId.set(product.id, product));
    state.warehouse = Array.from(byId.values());
  } else {
    state.warehouse = products;
    state.enrichedProductIds = new Set();
    state.warehouseVisibleLimit = 80;
  }
  state.warehouseHasMore = Boolean(data.hasMore);
  state.warehousePage = Number(data.page || state.warehousePage || 1);
  state.warehouseTotalFiltered = Number(data.total || state.warehouseTotalFiltered || state.warehouse.length);
  if (data.suppliers?.length) state.suppliers = data.suppliers;
  elements.warehouseTotal.textContent = formatNumber(data.totalAll ?? data.total ?? state.warehouse.length);
  elements.warehouseReady.textContent = formatNumber(data.ready || 0);
  elements.warehouseChanged.textContent = formatNumber(data.changed || 0);
  elements.warehouseNoSupplier.textContent = formatNumber(data.withoutSupplier || 0);
  elements.warehouseOzonArchived.textContent = formatNumber(data.ozonArchived || 0);
  elements.warehouseOzonInactive.textContent = formatNumber(data.ozonInactive || 0);
  elements.warehouseOzonOutOfStock.textContent = formatNumber(data.ozonOutOfStock || 0);
  if (data.usdRate) {
    elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(data.usdRate)} RUB/USD`;
  }
  elements.warehouseSelectChangedButton.disabled = !state.warehouse.length;
  if (data.sourceError) {
    elements.warehouseStatus.textContent = `Склад загружен, но PriceMaster сейчас недоступен: ${data.sourceError}`;
    elements.warehouseStatus.classList.add("is-warn");
    elements.warehouseStatus.classList.remove("is-ok");
  } else {
    elements.warehouseStatus.textContent = "Склад загружен. Стоп-поставщики исключаются из выбора автоматически.";
    elements.warehouseStatus.classList.add("is-ok");
    elements.warehouseStatus.classList.remove("is-warn");
  }
  if (Array.isArray(data.noSupplierAlerts) && data.noSupplierAlerts.length) {
    const preview = data.noSupplierAlerts.slice(0, 4).map((item) => item.offerId || item.name || item.id).join(", ");
    elements.warehouseNoSupplierAlert.innerHTML = `Нет активного поставщика: ${escapeHtml(preview)}. <a href="/no-supplier.html">Открыть страницу ошибок</a>`;
    elements.warehouseNoSupplierAlert.classList.remove("hidden");
    elements.warehouseNoSupplierAlert.classList.add("is-warn");
  } else {
    elements.warehouseNoSupplierAlert.classList.add("hidden");
  }
  if (Array.isArray(data.autoArchiveAlerts) && data.autoArchiveAlerts.length) {
    const sample = data.autoArchiveAlerts.slice(0, 4).map((item) => item.offerId || item.name || item.id).join(", ");
    showToast(`Автоархив кандидаты (без подвязок): ${sample}`, "warn");
  }
  if (Array.isArray(data.syncWarnings) && data.syncWarnings.length) {
    data.syncWarnings.forEach((w) => showToast(w, "warn"));
  }
  applyWarehouseFilters();
  renderSuppliers();
  updateSelection();
}

function renderWarehouseCards() {
  const groups = getSortedWarehouseGroups();
  elements.warehouseCards.classList.toggle("list-view", state.warehouseViewMode === "list");
  elements.warehouseViewButtons.forEach((button) => button.classList.toggle("active", button.dataset.warehouseView === state.warehouseViewMode));
  if (!groups.length) {
    const emptyInner =
      state.warehouse.length === 0
        ? `<strong>Склад пуст</strong><span>Добавьте товар вручную или выполните синхронизацию с маркетплейсами.</span>`
        : `<strong>Нет карточек по фильтру</strong><span>Измените поиск или фильтры — на складе есть ${formatNumber(state.warehouse.length)} строк.</span>`;
    elements.warehouseCards.innerHTML = `<div class="empty warehouse-empty-state">${emptyInner}</div>`;
    elements.warehouseLoadMoreButton.classList.add("hidden");
    elements.warehouseVisibleInfo.textContent = "Показано 0 товаров";
    updateSelection();
    return;
  }

  const visibleGroups = groups.slice(0, state.warehouseVisibleLimit);
  elements.warehouseCards.innerHTML = visibleGroups
    .map((group) => {
      const product = group.primary;
      const supplier = group.selectedSupplier;
      const selected = group.key === state.selectedWarehouseGroupKey;
      const url = marketplaceUrl(product);
      const variantLinks = group.variants
        .map((item) => ({ item, url: marketplaceUrl(item) }))
        .filter((entry) => entry.url);
      const image = group.image || productImage(product);
      const productName = group.name;
      const autoFocus = state.warehouseAnimateAutoFocus && state.warehouseAutoFocusGroupKey === group.key;
      return `
        <article class="product-card ${selected ? "selected" : ""} ${autoFocus ? "auto-focus-highlight" : ""}" data-group-key="${escapeHtml(group.key)}">
          <div class="product-image">
            ${
              image
                ? `<img src="${escapeHtml(image)}" alt="${escapeHtml(productName)}" loading="lazy" />`
                : `<div class="product-image-empty">${escapeHtml(marketLabel(product))}</div>`
            }
          </div>
          <div class="product-card-top">
            <input class="warehouse-check" type="checkbox" data-product-ids="${escapeHtml(group.productIds.join(","))}" />
            <span class="market-stack">${group.marketplaceLabels.map((label) => `<span class="market-badge ${label === "Ozon" ? "ozon" : "yandex"}">${escapeHtml(label)}</span>`).join("")}</span>
            <span class="badge ${group.changed ? "warn" : group.ready ? "ok" : "neutral"}">${group.changed ? "Есть изменения" : group.ready ? "Готово" : "Нет поставщика"}</span>
          </div>
          <h3>${escapeHtml(productName)}</h3>
          ${group.brand ? `<p class="product-brand-line muted">${escapeHtml(group.brand)}</p>` : ""}
          <div class="product-meta">
            <span>${escapeHtml(group.offerId || product.offerId)}</span>
            <span>${group.variants.length > 1 ? "Объединено Ozon + ЯМ" : escapeHtml(marketLabel(product))}</span>
          </div>
          <div class="variant-mini-list">
            ${group.variants
              .map(
                (item) => `
                  <div class="variant-mini">
                    <div class="variant-mini-market">
                      <span class="market-badge ${item.marketplace}">${escapeHtml(marketLabel(item))}</span>
                      <span class="badge ${marketplaceStateClass(item)}">${escapeHtml(marketplaceStateLabel(item))}</span>
                    </div>
                    <div><span>${escapeHtml(marketplaceCurrentLabel(item))}</span><strong>${formatMoney(item.currentPrice)}</strong>${item.marketplace === "ozon" && ozonCabinetPriceNote(item) ? `<span class="price-hint">${escapeHtml(ozonCabinetPriceNote(item))}</span>` : ""}</div>
                    <div><span>Новая</span><strong>${formatMoney(item.nextPrice)}</strong></div>
                    <div><span>Наценка</span><strong>${Number(item.markupCoefficient || 0).toFixed(2)}</strong></div>
                  </div>
                `,
              )
              .join("")}
          </div>
          <div class="supplier-mini">
            <span>${supplier ? escapeHtml(supplier.partnerName || supplier.supplierName || "Поставщик") : "Нет доступного поставщика"}</span>
            <small>${group.availableSupplierCount}/${group.supplierCount} поставщиков · ${escapeHtml(group.selectedSupplierReason || "")}</small>
          </div>
          <div class="card-actions">
            <button class="secondary-button compact-button select-product" type="button" data-group-key="${escapeHtml(group.key)}">Открыть</button>
            <a class="text-link" href="/product.html?group=${encodeURIComponent(group.key)}">Страница товара</a>
            ${
              variantLinks.length
                ? variantLinks.map(({ item, url: itemUrl }) => `<a class="text-link" href="${escapeHtml(itemUrl)}" target="_blank" rel="noopener">${item.marketplace === "yandex" ? "Открыть ЯМ" : "Открыть Ozon"}</a>`).join("")
                : url ? `<a class="text-link" href="${escapeHtml(url)}" target="_blank" rel="noopener">Открыть товар</a>` : `<span class="muted">Ссылка появится после синхронизации</span>`
            }
          </div>
        </article>
      `;
    })
    .join("");
  const visibleCount = Math.min(state.warehouseVisibleLimit, groups.length);
  const hasMoreClient = visibleCount < groups.length;
  elements.warehouseVisibleInfo.textContent = `Показано ${formatNumber(visibleCount)} из ${formatNumber(state.warehouseTotalFiltered || groups.length)} строк (загружено ${formatNumber(state.warehouse.length)})`;
  elements.warehouseLoadMoreButton.classList.toggle("hidden", !hasMoreClient && !state.warehouseHasMore);
  elements.warehouseLoadMoreButton.disabled = state.warehouseLoadingPage;
  elements.warehouseLoadMoreButton.textContent = state.warehouseLoadingPage ? "Загрузка..." : "Показать ещё";
  updateSelection();
  enrichVisibleProducts(visibleGroups.flatMap((group) => group.variants));
  if (state.warehouseAutoFocusGroupKey) {
    const key = state.warehouseAutoFocusGroupKey;
    const card = elements.warehouseCards.querySelector(`.product-card[data-group-key="${CSS.escape(key)}"]`);
    if (card) {
      card.scrollIntoView({ behavior: state.warehouseAnimateAutoFocus ? "smooth" : "auto", block: "center", inline: "nearest" });
      if (state.warehouseAnimateAutoFocus) {
        window.setTimeout(() => {
          state.warehouseAutoFocusGroupKey = null;
          const current = elements.warehouseCards.querySelector(`.product-card[data-group-key="${CSS.escape(key)}"]`);
          current?.classList.remove("auto-focus-highlight");
        }, 1400);
      } else {
        state.warehouseAutoFocusGroupKey = null;
      }
    }
  }
}

function mergeWarehouseProduct(product) {
  const index = state.warehouse.findIndex((item) => item.id === product.id);
  if (index >= 0) state.warehouse[index] = product;
  else state.warehouse.push(product);
}

async function ensureWarehouseGroupDetailed(groupKey) {
  const group = getSortedWarehouseGroups().find((item) => item.key === groupKey);
  if (!group) return null;
  const partialIds = (group.variants || []).filter((item) => item.partial).map((item) => item.id);
  if (!partialIds.length) return group;
  const details = await Promise.all(
    partialIds.map((id) => api(`/api/warehouse/products/${encodeURIComponent(id)}/detail`).then((payload) => payload.product)),
  );
  details.filter(Boolean).forEach((product) => mergeWarehouseProduct(product));
  applyWarehouseFilters();
  return getSortedWarehouseGroups().find((item) => item.key === groupKey) || null;
}

function renderWarehouseDetail(group) {
  if (!group) {
    elements.warehouseDetail.innerHTML = `
      <div class="detail-empty">
        <strong>Выберите товар</strong>
        <span>Здесь будут привязки PriceMaster, доступные поставщики и действия по карточке.</span>
      </div>
    `;
    return;
  }
  state.selectedWarehouseDetailGroup = group;

  const product = group.primary || group;
  const variants = group.variants || [product];
  const supplier = group.selectedSupplier || product.selectedSupplier;
  const suppliers = group.suppliers || product.suppliers || [];
  const links = group.links || product.links || [];
  const url = marketplaceUrl(product);
  const ozonLink = ozonUrl(product);
  const yandexVariant = variants.find((item) => item.marketplace === "yandex");
  const ozonVariant = variants.find((item) => item.marketplace === "ozon");
  const yandexLink = yandexVariant ? marketplaceUrl(yandexVariant) : "";
  const image = group.image || productImage(product);
  const productName = group.name || displayProductName(product);
  const groupProductIds = variants.map((item) => item.id);

  elements.warehouseDetail.innerHTML = `
    <div class="detail-head">
      <div>
        <span class="market-stack">${Array.from(new Set(variants.map((item) => marketLabel(item)))).map((label) => `<span class="market-badge ${label === "Ozon" ? "ozon" : "yandex"}">${escapeHtml(label)}</span>`).join("")}</span>
        <span class="state-stack">${variants.map((item) => `<span class="badge ${marketplaceStateClass(item)}">${escapeHtml(marketplaceStateLabel(item))}</span>`).join("")}</span>
        <h2>${escapeHtml(productName)}</h2>
        <p>${escapeHtml(product.offerId)}${variants.length > 1 ? " · объединённая карточка Ozon + ЯМ" : product.productId ? ` · ID ${escapeHtml(product.productId)}` : ""}</p>
      </div>
      <button class="text-button delete-product" type="button" data-product-id="${escapeHtml(product.id)}">Удалить</button>
      <a class="secondary-link-button compact-button" href="/product.html?group=${encodeURIComponent(group.key || productGroupKey(product))}">Страница</a>
    </div>

    <div class="detail-media">
      ${
        image
          ? `<img src="${escapeHtml(image)}" alt="${escapeHtml(productName)}" loading="lazy" />`
          : `<div class="product-image-empty">${escapeHtml(marketLabel(product))}</div>`
      }
    </div>

    <div class="detail-metrics">
      <div><span>${escapeHtml(marketplaceCurrentLabel(product))}</span><strong>${formatMoney(product.currentPrice)}</strong>${product.marketplace === "ozon" && ozonCabinetPriceNote(product) ? `<span class="price-hint">${escapeHtml(ozonCabinetPriceNote(product))}</span>` : ""}</div>
      ${product.marketplace === "ozon" ? `<div><span>Мин. Ozon</span><strong>${formatMoney(product.ozonMinPrice)}</strong></div>` : ""}
      <div><span>Новая</span><strong>${formatMoney(product.nextPrice)}</strong></div>
      <div><span>Наценка</span><strong>${Number(product.markupCoefficient || 0).toFixed(2)}</strong></div>
      <div><span>Ozon send</span><strong>${escapeHtml(product.lastOzonPriceSend?.status || "—")}</strong><small>${escapeHtml(product.lastOzonPriceSend?.at ? formatDate(product.lastOzonPriceSend.at) : "")}${product.lastOzonPriceSend?.detail ? ` · ${escapeHtml(product.lastOzonPriceSend.detail)}` : ""}</small></div>
    </div>

    <section class="detail-section">
      <div class="section-heading compact-heading">
        <div>
          <h3>Маркетплейсы и наценка</h3>
          <p>Цена отправляется автоматически после изменения наценки, курса, поставщика или прайса.</p>
        </div>
      </div>
      <div class="marketplace-variant-list">
        ${variants
          .map(
            (item) => `
              <form class="variant-markup-row" data-product-id="${escapeHtml(item.id)}">
                <input name="expectedUpdatedAt" type="hidden" value="${escapeHtml(item.updatedAt || "")}" />
                <div>
                  <span class="market-badge ${item.marketplace}">${escapeHtml(marketLabel(item))}</span>
                  <span class="badge ${marketplaceStateClass(item)}">${escapeHtml(marketplaceStateLabel(item))}</span>
                  <strong>${formatMoney(item.currentPrice)} → ${formatMoney(item.nextPrice)}</strong>
                  <small>${item.marketplace === "ozon" ? `Мин. Ozon: ${formatMoney(item.ozonMinPrice)}${ozonCabinetPriceNote(item) ? ` · ${escapeHtml(ozonCabinetPriceNote(item))}` : ""}` : "Цена ЯМ хранится отдельно"}</small>
                </div>
                <label>
                  Наценка
                  <input name="markup" type="number" min="0.01" step="0.01" value="${item.markup > 0 ? Number(item.markup).toFixed(2) : ""}" placeholder="По правилам из настроек" data-usd-price="${escapeHtml(item.selectedSupplier?.price || "")}" data-current-price="${escapeHtml(item.currentPrice || "")}" />
                  <small class="markup-live-preview">Предпросмотр: ${formatMoney(item.nextPrice)}</small>
                </label>
                <button class="secondary-button compact-button" type="submit">Сохранить</button>
              </form>
            `,
          )
          .join("")}
      </div>
    </section>

    <section class="detail-section">
      <h3>Состояние на маркетплейсах</h3>
      <div class="state-list">
        ${variants
          .map(
            (item) => `
              <div class="state-detail">
                <strong>${escapeHtml(marketplaceStateLabel(item))}</strong>
                <span>${escapeHtml(marketplaceStateMeta(item) || `Данные появятся после синхронизации с ${marketLabel(item)}.`)}</span>
                ${item.marketplaceState?.stateDescription ? `<p>${escapeHtml(item.marketplaceState.stateDescription)}</p>` : ""}
              </div>
            `,
          )
          .join("")}
      </div>
    </section>

    <section class="detail-section">
      <h3>История цен</h3>
      <div class="history-list">
        ${
          variants.flatMap((item) => (item.priceHistory || []).map((entry) => ({ ...entry, market: marketLabel(item) }))).length
            ? variants
                .flatMap((item) => (item.priceHistory || []).map((entry) => ({ ...entry, market: marketLabel(item) })))
                .sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0))
                .slice(0, 8)
                .map((entry) => `
                  <div class="history-row">
                    <div>
                      <strong>${escapeHtml(entry.market)}: ${formatMoney(entry.oldPrice)} → ${formatMoney(entry.newPrice)}</strong>
                      <span>${escapeHtml(entry.supplierName || "Поставщик не указан")}${entry.supplierArticle ? ` · ${escapeHtml(entry.supplierArticle)}` : ""}${entry.reason ? ` · ${escapeHtml(entry.reason)}` : ""}${entry.status ? ` · ${escapeHtml(entry.status)}` : ""}${entry.error ? ` · ${escapeHtml(entry.error)}` : ""}</span>
                    </div>
                    <small>${formatDate(entry.at)}</small>
                  </div>
                `)
                .join("")
            : '<div class="empty-mini">История появится после первой отправки цен.</div>'
        }
      </div>
    </section>

    <section class="detail-section">
      <h3>Выбранный поставщик</h3>
      ${
        supplier
          ? `<div class="supplier-card">
              <strong>${escapeHtml(supplier.partnerName || supplier.supplierName || "Поставщик")}</strong>
              <span>${escapeHtml(supplier.article)} · ${escapeHtml(supplierUsdPriceLabel(supplier))} · ${formatDate(supplier.docDate)}</span>
              <p>${escapeHtml(supplier.name || "")}</p>
            </div>`
          : '<div class="empty-mini">Нет доступного поставщика. Добавьте привязку или проверьте наличие в PriceMaster.</div>'
      }
    </section>

    <section class="detail-section">
      <h3>Привязки PriceMaster</h3>
      <div class="link-list">
        ${
          links.length
            ? links
                .map(
                  (link) => `
                    <div class="link-item">
                      <div>
                        <strong>${escapeHtml(link.article)}</strong>
                        <span>
                          ${escapeHtml(link.supplierName || "Любой поставщик")}
                          ${link.keyword ? ` · ${escapeHtml(link.keyword)}` : ""}
                          ${link.missingInPriceMaster ? " · нет в PriceMaster" : ""}
                          ${!link.missingInPriceMaster && Number(link.availableCount || 0) === 0 ? " · нет активного остатка" : ""}
                        </span>
                      </div>
                      <button class="text-button delete-link" type="button" data-product-id="${escapeHtml(link.productId || product.id)}" data-link-id="${escapeHtml(link.id)}">Удалить</button>
                    </div>
                  `,
                )
                .join("")
            : '<div class="empty-mini">Связей пока нет.</div>'
        }
      </div>
      ${elements.linkFormTemplate.innerHTML.replace("<form", `<form data-product-id="${escapeHtml(product.id)}" data-product-ids="${escapeHtml(groupProductIds.join(","))}"`)}
    </section>

    <section class="detail-section">
      <h3>Все найденные поставщики</h3>
      <div class="supplier-list">
        ${
          suppliers.length
            ? suppliers
                .slice(0, 10)
                .map(
                  (item) => `
                    <div class="supplier-line ${item.stopped ? "stopped" : ""}">
                      <div>
                        <strong>${escapeHtml(item.partnerName || item.supplierName || "Поставщик")}</strong>
                        <span>${escapeHtml(item.article)} · ${escapeHtml(item.name || "")}</span>
                        ${item.stopped ? `<span class="stop-note">На стопе${item.stopReason ? `: ${escapeHtml(item.stopReason)}` : ""}</span>` : ""}
                      </div>
                      <div class="money">${formatUsd(item.price)}</div>
                    </div>
                  `,
                )
                .join("")
            : '<div class="empty-mini">Нет совпадений PriceMaster.</div>'
        }
      </div>
    </section>

    <section class="detail-section export-section">
      <h3>Выгрузка товара</h3>
      <div class="export-grid">
        <div class="export-tile">
          <strong>Ozon</strong>
          <span>${escapeHtml(exportText(product, "ozon"))}</span>
          <button class="primary-button compact-button export-product" type="button" data-product-id="${escapeHtml(product.id)}" data-target="${escapeHtml(product.marketplace === "ozon" ? product.target : "ozon")}">Выгрузить в Ozon</button>
          ${ozonLink ? `<a class="text-link" href="${escapeHtml(ozonLink)}" target="_blank" rel="noopener">Открыть в Ozon</a>` : '<small>Публичная ссылка появится после синхронизации с SKU Ozon.</small>'}
          <a class="text-link" href="/ozon-product.html?productId=${encodeURIComponent(ozonVariant?.id || product.id)}&offerId=${encodeURIComponent(product.offerId)}&name=${encodeURIComponent(productName)}">Заполнить поля Ozon</a>
        </div>
        <div class="export-tile">
          <strong>Yandex Market</strong>
          <span>${escapeHtml(exportText(product, "yandex"))}</span>
          <button class="secondary-button compact-button export-product" type="button" data-product-id="${escapeHtml(product.id)}" data-target="yandex" ${hasConfiguredYandexTarget() ? "" : "disabled"}>Выгрузить в Яндекс</button>
          ${yandexLink ? `<a class="text-link" href="${escapeHtml(yandexLink)}" target="_blank" rel="noopener">Открыть в ЯМ</a>` : ""}
          <a class="text-link" href="/yandex-product.html?offerId=${encodeURIComponent(product.offerId)}&name=${encodeURIComponent(productName)}&target=${encodeURIComponent(product.marketplace === "yandex" ? product.target : "yandex")}">Заполнить поля ЯМ</a>
          <small>${hasConfiguredYandexTarget() ? "Карточка уйдёт в первый настроенный кабинет ЯМ." : "Добавьте YANDEX_SHOPS_JSON в .env."}</small>
        </div>
      </div>
    </section>
  `;
}

function renderSuppliers() {
  const activeSuppliers = state.suppliers.filter((supplier) => !supplier.stopped);
  const inactiveSuppliers = state.suppliers.filter((supplier) => supplier.stopped);
  const scopedSuppliers = state.supplierView === "inactive" ? inactiveSuppliers : activeSuppliers;
  const query = normalizeSupplierName(state.supplierSearch || "");
  const visibleSuppliers = query
    ? scopedSuppliers.filter((supplier) => {
      const haystack = [
        supplier.name,
        supplier.note,
        supplier.stopReason,
      ]
        .map((value) => normalizeSupplierName(value))
        .join(" ");
      return haystack.includes(query);
    })
    : scopedSuppliers;
  elements.supplierViewButtons?.forEach((button) => {
    const active = button.dataset.supplierView === state.supplierView;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });

  if (!state.suppliers.length) {
    elements.supplierBoard.innerHTML = `<div class="empty">Добавьте поставщика, чтобы управлять стопом и артикулами.</div>`;
    elements.supplierStatus.textContent = "Поставщиков пока нет.";
    return;
  }

  elements.supplierStatus.textContent = `Активных: ${formatNumber(activeSuppliers.length)}. Инактив: ${formatNumber(inactiveSuppliers.length)}.`;
  if (!visibleSuppliers.length) {
    const emptyMessage = query
      ? `По запросу «${escapeHtml(state.supplierSearch)}» ничего не найдено.`
      : (state.supplierView === "inactive" ? "Нет поставщиков в инактиве." : "Нет активных поставщиков.");
    elements.supplierBoard.innerHTML = `<div class="empty">${emptyMessage}</div>`;
    return;
  }

  const soonItems = state.supplierView === "inactive"
    ? visibleSuppliers.filter((supplier) => supplierReturnsSoon(supplier, 14)).slice(0, 6)
    : [];
  const soonBlock = soonItems.length
    ? `
      <section class="detail-section">
        <h3>Скоро вернутся (14 дней)</h3>
        <div class="history-list">
          ${soonItems
            .map((supplier) => `<div class="history-row"><div><strong>${escapeHtml(supplier.name)}</strong><span>${escapeHtml(formatSupplierInactiveInfo(supplier))}</span></div><small>${supplier.inactiveUntil ? formatDate(supplier.inactiveUntil) : "—"}</small></div>`)
            .join("")}
        </div>
      </section>
    `
    : "";

  elements.supplierBoard.innerHTML = `${soonBlock}${visibleSuppliers
    .map(
      (supplier) => `
        <article class="supplier-panel ${supplier.stopped ? "stopped" : ""}" data-supplier-id="${escapeHtml(supplier.id)}">
          <div class="supplier-panel-head">
            <div>
              <h3>
                ${escapeHtml(supplier.name)}
                <span class="supplier-source-badge ${supplier.source === "pricemaster" ? "supplier-source-badge--pm" : "supplier-source-badge--local"}">
                  ${supplier.source === "pricemaster" ? "из PriceMaster" : "локальный"}
                </span>
              </h3>
              <p>${supplier.note ? escapeHtml(supplier.note) : "Без заметки"}</p>
              ${supplier.stopReason ? `<small class="stop-note">Причина стопа: ${escapeHtml(supplier.stopReason)}</small>` : ""}
              ${supplier.stopped ? `<small class="stop-note">${escapeHtml(formatSupplierInactiveInfo(supplier))}</small>` : ""}
            </div>
            <div class="supplier-actions">
              <label class="switch-line">
                <input class="supplier-stop-toggle" type="checkbox" ${supplier.stopped ? "checked" : ""} />
                <span>${supplier.stopped ? "Инактив" : "Активен"}</span>
              </label>
              <button class="secondary-button compact-button edit-supplier" type="button">Изменить</button>
            </div>
          </div>
          <div class="supplier-articles">
            ${
              supplier.articles.length
                ? supplier.articles
                    .map(
                      (article) => `
                        <div class="supplier-article">
                          <div>
                            <strong>${escapeHtml(article.article)}</strong>
                            <span>${article.keyword || "Без ключевого слова"}</span>
                          </div>
                          <div class="supplier-article-actions">
                            <button class="secondary-button compact-button edit-supplier-article" type="button" data-article-id="${escapeHtml(article.id)}">Изменить</button>
                            <button class="text-button delete-supplier-article" type="button" data-article-id="${escapeHtml(article.id)}">Удалить</button>
                          </div>
                        </div>
                      `,
                    )
                    .join("")
                : '<div class="empty-mini">Артикулы еще не добавлены.</div>'
            }
          </div>
          ${elements.supplierArticleFormTemplate.innerHTML.replace("<form", `<form data-supplier-id="${escapeHtml(supplier.id)}"`)}
          <button class="text-button delete-supplier" type="button">Удалить поставщика</button>
        </article>
      `,
    )
    .join("")}`;
}

function formatSupplierInactiveInfo(supplier) {
  if (!supplier?.stopped) return "";
  const comment = String(supplier.inactiveComment || supplier.stopReason || "").trim();
  const dateText = supplier.inactiveUntil
    ? `до ${formatDate(supplier.inactiveUntil)}`
    : (supplier.inactiveUntilUnknown ? "срок неизвестен" : "");
  return [comment || "Инактив", dateText].filter(Boolean).join(" · ");
}

function supplierReturnsSoon(supplier, days = 14) {
  if (!supplier?.stopped || !supplier.inactiveUntil) return false;
  const now = new Date();
  const until = new Date(supplier.inactiveUntil);
  if (Number.isNaN(until.getTime())) return false;
  const diffDays = (until.getTime() - now.getTime()) / (24 * 60 * 60 * 1000);
  return diffDays >= -0.5 && diffDays <= days;
}

function supplierInactiveDefaultDate() {
  const nextMonth = new Date();
  nextMonth.setMonth(nextMonth.getMonth() + 1);
  return nextMonth.toISOString().slice(0, 10);
}

function openSupplierInactiveModal(supplier) {
  if (!elements.supplierInactiveModal || !elements.supplierInactiveForm) {
    return Promise.resolve(null);
  }
  elements.supplierInactiveSupplierId.value = supplier.id;
  elements.supplierInactiveCommentInput.value = supplier.inactiveComment || supplier.stopReason || "";
  elements.supplierInactiveUnknownInput.checked = Boolean(supplier.inactiveUntilUnknown) || !supplier.inactiveUntil;
  elements.supplierInactiveUntilInput.value = supplier.inactiveUntil || supplierInactiveDefaultDate();
  elements.supplierInactiveUntilInput.disabled = elements.supplierInactiveUnknownInput.checked;
  elements.supplierInactiveModal.classList.remove("hidden");
  elements.supplierInactiveCommentInput.focus();

  return new Promise((resolve) => {
    const cleanup = (value) => {
      elements.supplierInactiveModal.classList.add("hidden");
      elements.supplierInactiveForm.removeEventListener("submit", onSubmit);
      elements.supplierInactiveCancel?.removeEventListener("click", onCancel);
      elements.supplierInactiveModal.removeEventListener("click", onBackdrop);
      elements.supplierInactiveUnknownInput?.removeEventListener("change", onUnknownChange);
      elements.supplierInactiveQuickButtons?.forEach((btn) => btn.removeEventListener("click", onQuickDate));
      document.removeEventListener("keydown", onKeydown);
      resolve(value);
    };
    const onUnknownChange = () => {
      elements.supplierInactiveUntilInput.disabled = elements.supplierInactiveUnknownInput.checked;
      if (!elements.supplierInactiveUnknownInput.checked && !elements.supplierInactiveUntilInput.value) {
        elements.supplierInactiveUntilInput.value = supplierInactiveDefaultDate();
      }
    };
    const onCancel = () => cleanup(null);
    const onQuickDate = (event) => {
      const mode = event.currentTarget.dataset.inactiveQuick;
      const base = new Date();
      if (mode === "+7") base.setDate(base.getDate() + 7);
      else if (mode === "+14") base.setDate(base.getDate() + 14);
      else if (mode === "eom") base.setMonth(base.getMonth() + 1, 0);
      else return;
      elements.supplierInactiveUnknownInput.checked = false;
      elements.supplierInactiveUntilInput.disabled = false;
      elements.supplierInactiveUntilInput.value = base.toISOString().slice(0, 10);
    };
    const onBackdrop = (event) => {
      if (event.target === elements.supplierInactiveModal) cleanup(null);
    };
    const onKeydown = (event) => {
      if (event.key === "Escape") cleanup(null);
    };
    const onSubmit = (event) => {
      event.preventDefault();
      const comment = elements.supplierInactiveCommentInput.value.trim();
      const unknown = elements.supplierInactiveUnknownInput.checked;
      const date = unknown ? "" : elements.supplierInactiveUntilInput.value.trim();
      if (!comment) {
        elements.supplierInactiveCommentInput.focus();
        return;
      }
      if (!unknown && !date) {
        elements.supplierInactiveUntilInput.focus();
        return;
      }
      cleanup({ comment, unknown, date });
    };

    elements.supplierInactiveForm.addEventListener("submit", onSubmit);
    elements.supplierInactiveCancel?.addEventListener("click", onCancel);
    elements.supplierInactiveModal.addEventListener("click", onBackdrop);
    elements.supplierInactiveUnknownInput?.addEventListener("change", onUnknownChange);
    elements.supplierInactiveQuickButtons?.forEach((btn) => btn.addEventListener("click", onQuickDate));
    document.addEventListener("keydown", onKeydown);
  });
}

function resetSupplierForm() {
  elements.supplierForm.reset();
  elements.supplierIdInput.value = "";
  elements.supplierSaveButton.textContent = "Добавить поставщика";
  elements.supplierCancelEditButton?.classList.add("hidden");
}

function startSupplierEdit(supplier) {
  if (!supplier) return;
  elements.supplierIdInput.value = supplier.id || "";
  elements.supplierNameInput.value = supplier.name || "";
  elements.supplierNoteInput.value = supplier.note || "";
  elements.supplierStopReasonInput.value = supplier.stopReason || "";
  elements.supplierSaveButton.textContent = "Сохранить поставщика";
  elements.supplierCancelEditButton?.classList.remove("hidden");
  elements.supplierStatus.textContent = "Редактируйте поставщика. Причина стопа будет видна в карточках и логике наличия.";
  elements.supplierForm.scrollIntoView({ behavior: "smooth", block: "start" });
}

function fillSupplierArticleForm(panel, article) {
  const form = panel.querySelector(".supplier-article-form");
  if (!form || !article) return;
  form.elements.id.value = article.id || "";
  form.elements.article.value = article.article || "";
  form.elements.keyword.value = article.keyword || "";
  form.querySelector("button[type='submit']").textContent = "Сохранить артикул";
  form.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

function renderAccounts() {
  if (!elements.accountsBoard) return;
  if (!state.accounts.length) {
    elements.accountsBoard.innerHTML = `<div class="empty">Добавьте первый кабинет Ozon или Yandex Market.</div>`;
    renderHiddenAccounts();
    return;
  }

  elements.accountsBoard.innerHTML = state.accounts
    .map((account) => {
      const isOzon = account.marketplace === "ozon";
      return `
        <article class="account-card ${account.configured ? "configured" : "not-configured"}" data-account-id="${escapeHtml(account.id)}">
          <div class="account-card-head">
            <div>
              <span class="market-badge ${escapeHtml(account.marketplace)}">${isOzon ? "Ozon" : "Yandex Market"}</span>
              <h3>${escapeHtml(account.name)}</h3>
              <p>${account.readOnly ? "Задан в .env" : account.inheritedFromEnv ? "Переопределён из интерфейса" : "Локальная настройка"} · ${account.configured ? "ключи подключены" : "не настроен"}</p>
            </div>
            <div class="account-actions">
              ${account.readOnly ? `<span class="readonly-note">Из .env</span>` : ""}
              <button class="secondary-button compact-button edit-account" type="button">Изменить</button>
              <button class="text-button delete-account" type="button">${account.readOnly ? "Скрыть" : "Удалить"}</button>
            </div>
          </div>
          <dl class="account-meta">
            ${
              isOzon
                ? `<div><dt>Client-Id</dt><dd>${escapeHtml(account.clientId || "-")}</dd></div>`
                : `<div><dt>Business ID</dt><dd>${escapeHtml(account.businessId || "-")}</dd></div>`
            }
            <div><dt>API Key</dt><dd>${escapeHtml(account.apiKey || "-")}</dd></div>
            ${account.campaignId ? `<div><dt>Campaign</dt><dd>${escapeHtml(account.campaignId)}</dd></div>` : ""}
          </dl>
        </article>
      `;
    })
    .join("");
  renderHiddenAccounts();
}

function renderHiddenAccounts() {
  if (!elements.hiddenAccountsBoard) return;
  if (!state.hiddenAccounts.length) {
    elements.hiddenAccountsBoard.innerHTML = `<div class="empty-mini">Скрытых кабинетов нет.</div>`;
    return;
  }
  elements.hiddenAccountsBoard.innerHTML = state.hiddenAccounts
    .map((account) => `
      <div class="hidden-account-row" data-account-id="${escapeHtml(account.id)}">
        <div>
          <span class="market-badge ${escapeHtml(account.marketplace)}">${account.marketplace === "ozon" ? "Ozon" : "Yandex Market"}</span>
          <strong>${escapeHtml(account.name)}</strong>
          <small>${escapeHtml(account.id)}</small>
        </div>
        <button class="secondary-button compact-button restore-account" type="button">Вернуть</button>
      </div>
    `)
    .join("");
}

function updateAccountFormMode() {
  const marketplace = elements.accountMarketplaceInput.value;
  document.querySelectorAll(".account-ozon-field").forEach((item) => item.classList.toggle("hidden", marketplace !== "ozon"));
  document.querySelectorAll(".account-yandex-field").forEach((item) => item.classList.toggle("hidden", marketplace !== "yandex"));
}

function resetAccountForm() {
  elements.accountForm?.reset();
  if (elements.accountIdInput) elements.accountIdInput.value = "";
  if (elements.accountFormTitle) elements.accountFormTitle.textContent = "Добавить кабинет";
  if (elements.accountSaveButton) elements.accountSaveButton.textContent = "Сохранить кабинет";
  elements.accountCancelEditButton?.classList.add("hidden");
  updateAccountFormMode();
}

function startAccountEdit(account) {
  if (!account || !elements.accountForm) return;
  elements.accountIdInput.value = account.id || "";
  elements.accountMarketplaceInput.value = account.marketplace || "ozon";
  elements.accountForm.elements.name.value = account.name || "";
  elements.accountForm.elements.clientId.value = "";
  elements.accountForm.elements.apiKey.value = "";
  elements.accountForm.elements.businessId.value = account.businessId || "";
  elements.accountForm.elements.campaignId.value = account.campaignId || "";
  if (elements.accountFormTitle) elements.accountFormTitle.textContent = "Редактировать кабинет";
  if (elements.accountSaveButton) elements.accountSaveButton.textContent = "Сохранить изменения";
  elements.accountCancelEditButton?.classList.remove("hidden");
  updateAccountFormMode();
  elements.accountStatus.textContent = account.readOnly
    ? "Кабинет из .env будет сохранён как локальное переопределение. Пустые секретные поля сохранят старые ключи."
    : "Можно изменить название и доступы. Пустые секретные поля сохранят старые ключи.";
  elements.accountForm.scrollIntoView({ behavior: "smooth", block: "start" });
}

async function loadAccounts() {
  const data = await api("/api/marketplace-accounts");
  state.accounts = data.accounts || [];
  state.hiddenAccounts = data.hiddenAccounts || [];
  state.targets = data.targets || state.targets;
  renderAccounts();
  renderHiddenAccounts();
  renderTargets();
}

function renderDailySync(status = {}) {
  if (!elements.dailySyncStatus || !elements.dailySyncMeta) return;
  const statusText = status.running
    ? "Обновление выполняется"
    : status.status === "ok"
      ? "Расписание активно"
      : status.status === "failed"
        ? "Последний запуск с ошибкой"
        : "Ожидает первого запуска";
  const last = status.lastRunAt ? formatDate(status.lastRunAt) : "ещё не было";
  const next = status.nextRunAt ? formatDate(status.nextRunAt) : "не запланировано";
  const totals = status.warehouse
    ? ` Склад: ${formatNumber(status.warehouse.total)} товаров, ${formatNumber(status.warehouse.changed)} с изменениями.`
    : "";
  const pricePush = status.warehouse?.pricePush;
  const priceTotals =
    pricePush && (pricePush.sent > 0 || pricePush.failed > 0 || pricePush.skipped > 0 || pricePush.error)
      ? ` Цены MP: отправлено ${formatNumber(pricePush.sent)}, сбоев ${formatNumber(pricePush.failed)}, пропущено ${formatNumber(pricePush.skipped)}.${pricePush.error ? ` Ошибка отправки: ${pricePush.error}` : ""}`
      : "";

  elements.dailySyncStatus.textContent = status.enabled === false
    ? "Выключено"
    : `${statusText}, ${status.time || "11:00"}`;
  elements.dailySyncMeta.textContent = `Последний запуск: ${last}. Следующий: ${next}.${totals}${priceTotals}${status.error ? ` Ошибка: ${status.error}` : ""}`;
  if (elements.syncLogList) {
    const logs = Array.isArray(status.logs) ? status.logs.slice(0, 5) : [];
    elements.syncLogList.innerHTML = logs.length
      ? logs.map((log) => `
          <div class="sync-log-row">
            <div>
              <strong>${escapeHtml(log.status === "ok" ? "Успешно" : log.status === "failed" ? "Ошибка" : "Запуск")}</strong>
              <span>${formatDate(log.at)} · PriceMaster: ${formatNumber(log.priceMasterItems)} / изменений ${formatNumber(log.priceMasterChanges)} · Склад: ${formatNumber(log.warehouseTotal)} / изменилось ${formatNumber(log.warehouseChanged)}${log.pricePushSent != null ? ` · Цены: +${formatNumber(log.pricePushSent)} / сбой ${formatNumber(log.pricePushFailed || 0)} / пропуск ${formatNumber(log.pricePushSkipped || 0)}` : ""}</span>
            </div>
            ${log.error ? `<small>${escapeHtml(log.error)}</small>` : ""}
          </div>
        `).join("")
      : "";
  }
}

async function loadDailySync() {
  const status = await api("/api/daily-sync");
  renderDailySync(status);
  return status;
}

function renderRetryQueue(data = {}) {
  const items = Array.isArray(data.items) ? data.items : [];
  const filtered = items.filter((item) => {
    const marketOk = state.retryQueueMarketplace === "all" || String(item.marketplace || "").toLowerCase() === state.retryQueueMarketplace;
    const q = String(state.retryQueueSearch || "").trim().toLowerCase();
    if (!q) return marketOk;
    const haystack = [
      item.offerId,
      item.target,
      item.error,
      item.marketplace,
      item.queueKey,
    ].join(" ").toLowerCase();
    return marketOk && haystack.includes(q);
  });
  const errorCounts = new Map();
  for (const item of filtered) {
    const key = String(item.error || "unknown");
    errorCounts.set(key, Number(errorCounts.get(key) || 0) + 1);
  }
  const sorted = [...filtered].sort((a, b) => {
    if (state.retryQueueSort === "oldest") return new Date(a.queuedAt || 0) - new Date(b.queuedAt || 0);
    if (state.retryQueueSort === "errors") {
      const ea = Number(errorCounts.get(String(a.error || "unknown")) || 0);
      const eb = Number(errorCounts.get(String(b.error || "unknown")) || 0);
      return eb - ea || (new Date(b.queuedAt || 0) - new Date(a.queuedAt || 0));
    }
    return new Date(b.queuedAt || 0) - new Date(a.queuedAt || 0);
  });
  state.retryQueue = sorted;
  const validKeys = new Set(sorted.map((item) => String(item.queueKey)));
  state.retryQueueSelectedKeys = new Set([...state.retryQueueSelectedKeys].filter((key) => validKeys.has(key)));
  if (!elements.retryQueuePanel || !elements.retryQueueList || !elements.retryQueueMeta) return;
  const hasItems = sorted.length > 0;
  elements.retryQueuePanel.classList.toggle("hidden", !hasItems);
  elements.retryQueueMeta.textContent = hasItems
    ? `В очереди ${formatNumber(sorted.length)} из ${formatNumber(items.length)}. Последнее обновление: ${data.updatedAt ? formatDate(data.updatedAt) : "—"}.`
    : `Нет элементов по текущему фильтру (${formatNumber(items.length)} всего).`;
  if (elements.retryQueueStats) {
    const retried = Number(state.retryQueueLastRun?.retried || 0);
    const failed = Number(state.retryQueueLastRun?.failed || 0);
    elements.retryQueueStats.innerHTML = [
      `<span class="badge ok">success ${formatNumber(retried > 0 ? retried : 0)}</span>`,
      `<span class="badge warn">error ${formatNumber(sorted.length)}</span>`,
      `<span class="badge neutral">retried ${formatNumber(retried)}</span>`,
      failed ? `<small>fail ${formatNumber(failed)}</small>` : "",
    ].join(" ");
  }
  elements.retryQueueRetrySelectedButton.disabled = !state.retryQueueSelectedKeys.size;
  elements.retryQueueList.innerHTML = hasItems
    ? sorted.slice(0, 150).map((item) => `
        <label class="history-row">
          <input class="retry-queue-check" type="checkbox" data-queue-key="${escapeHtml(item.queueKey)}" ${state.retryQueueSelectedKeys.has(String(item.queueKey)) ? "checked" : ""} />
          <div>
            <strong>${escapeHtml(item.offerId || item.id || "offer")}</strong>
            <span>${escapeHtml(item.marketplace || "")} · ${escapeHtml(item.target || "")} · ${formatMoney(item.price)} · попыток ${formatNumber(item.attempts || 1)} · ${escapeHtml(item.error || "ошибка отправки")} · <b class="retry-state ${Number(item.attempts || 1) > 1 ? "retry-state--retried" : "retry-state--error"}">${Number(item.attempts || 1) > 1 ? "retried" : "error"}</b></span>
          </div>
          <small>${item.queuedAt ? formatDate(item.queuedAt) : "—"}</small>
        </label>
      `).join("")
    : '<div class="empty-mini">Очередь пуста.</div>';
}

async function loadRetryQueue() {
  const data = await api("/api/warehouse/prices/retry-queue");
  renderRetryQueue(data);
  return data;
}

async function sendOzonPricesNow(productIds = []) {
  const ids = (Array.isArray(productIds) ? productIds : []).map(String);
  const ozonIds = ids.filter((id) => state.warehouse.some((product) => product.id === id && product.marketplace === "ozon"));
  if (!ozonIds.length) return { sent: 0, skipped: 0, reason: "no_ozon_products" };
  const result = await api("/api/warehouse/prices/send", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      confirmed: true,
      productIds: ozonIds,
      usdRate: Number(elements.warehouseUsdRateInput?.value || 0),
      minDiffRub: 0,
      minDiffPct: 0,
    }),
  });
  return { sent: Number(result.sent || 0), skipped: Array.isArray(result.skipped) ? result.skipped.length : 0 };
}

function currentWarehousePageParams() {
  const params = new URLSearchParams();
  params.set("pageSize", String(state.warehousePageSize));
  if (elements.warehouseUsdRateInput.value) params.set("usdRate", elements.warehouseUsdRateInput.value);
  if (state.warehouseMarketplace !== "all") params.set("marketplace", state.warehouseMarketplace);
  if (state.ozonStateFilter !== "all") params.set("state", state.ozonStateFilter);
  if (state.warehouseLinkFilter !== "all") params.set("linked", state.warehouseLinkFilter);
  if (state.warehouseBrandFilter) params.set("brand", state.warehouseBrandFilter);
  const query = elements.warehouseSearchInput.value.trim();
  if (query) params.set("q", query);
  return params;
}

async function loadWarehousePage({ reset = false, sync = false, refreshPrices = false } = {}) {
  if (!reset && !state.warehouseHasMore) return;
  if (!reset && state.warehouseLoadingPage) return;

  const token = ++state.warehouseRequestToken;
  state.warehouseLoadingPage = true;
  renderWarehouseCards();
  try {
    const params = currentWarehousePageParams();
    params.set("page", String(reset ? 1 : state.warehousePage + 1));
    if (sync) params.set("sync", "true");
    if (refreshPrices) params.set("refreshPrices", "true");
    const data = await api(`/api/warehouse/products/page?${params}`);
    if (token !== state.warehouseRequestToken) return;
    renderWarehouse({
      ...data,
      mode: reset ? "replace" : "append",
      products: data.items || [],
    });
    syncWarehouseStateToUrl();
  } finally {
    if (token === state.warehouseRequestToken) {
      state.warehouseLoadingPage = false;
    }
    renderWarehouseCards();
  }
}

async function loadWarehouse(sync = false, refreshPrices = false) {
  captureWarehouseScroll();
  const stopProgress = sync || refreshPrices ? startSyncProgress(sync ? "sync" : "prices") : null;
  elements.warehouseSyncButton.disabled = sync;
  elements.warehouseRefreshPricesButton.disabled = refreshPrices;
  elements.warehouseStatus.textContent = sync
    ? `Синхронизирую ${syncTargetNames().join(" + ")}: товары, цены, статусы, остатки и изображения...`
    : refreshPrices
      ? `Обновляю цены по ${syncTargetNames().join(" + ")}...`
      : "Обновляю список по фильтрам и курсу…";
  try {
    state.warehousePage = 0;
    state.warehouseHasMore = true;
    state.warehouseTotalFiltered = 0;
    // Do not clear selectedWarehouseGroupKey / selectedWarehouseProductId here:
    // after link save or refresh, applyWarehouseFilters() would fall back to the first card.
    await loadWarehousePage({ reset: true, sync, refreshPrices });
    const requestedPage = Math.max(1, Number(state.warehouseRestorePage || 1));
    for (let page = 2; page <= requestedPage && state.warehouseHasMore; page += 1) {
      // Restore long-list context after refresh by preloading previously opened pages.
      await loadWarehousePage({ reset: false, sync: false, refreshPrices: false });
    }
    state.warehouseRestorePage = 1;
    restoreWarehouseScroll();
    await loadRetryQueue().catch(() => {});
    if (stopProgress) stopProgress(true);
  } catch (error) {
    if (stopProgress) stopProgress(false);
    throw error;
  } finally {
    elements.warehouseSyncButton.disabled = false;
    elements.warehouseRefreshPricesButton.disabled = false;
  }
}

let warehouseRefreshTimer = null;
function queueWarehouseRefresh(delayMs = 160) {
  if (warehouseRefreshTimer) window.clearTimeout(warehouseRefreshTimer);
  warehouseRefreshTimer = window.setTimeout(() => {
    loadWarehouse(false).catch((error) => {
      elements.warehouseStatus.textContent = error.message;
    });
  }, delayMs);
}

let warehouseFilterReloadTimer = null;
function queueWarehouseFilterReload(delayMs = 260) {
  if (warehouseFilterReloadTimer) window.clearTimeout(warehouseFilterReloadTimer);
  warehouseFilterReloadTimer = window.setTimeout(() => {
    loadWarehouse(false).catch((error) => {
      elements.warehouseStatus.textContent = error.message;
      applyWarehouseFilters();
    });
  }, delayMs);
}

function closeWarehouseBrandSuggestions() {
  if (!elements.warehouseBrandSuggestions) return;
  elements.warehouseBrandSuggestions.hidden = true;
  elements.warehouseBrandSuggestions.innerHTML = "";
}

function renderWarehouseBrandSuggestions() {
  const input = elements.warehouseBrandFilterInput;
  const panel = elements.warehouseBrandSuggestions;
  if (!input || !panel) return;
  const q = String(input.value || "").trim().toLowerCase();
  panel.innerHTML = "";
  if (!q) {
    closeWarehouseBrandSuggestions();
    return;
  }

  const matches = (state.warehouseBrands || [])
    .map((brand) => String(brand || "").trim())
    .filter(Boolean)
    .filter((brand) => brand.toLowerCase().includes(q))
    .sort((a, b) => {
      const aa = a.toLowerCase();
      const bb = b.toLowerCase();
      const aStarts = aa.startsWith(q) ? 0 : 1;
      const bStarts = bb.startsWith(q) ? 0 : 1;
      return aStarts - bStarts || a.localeCompare(b, "ru", { sensitivity: "base" });
    })
    .slice(0, 14);

  if (!matches.length) {
    panel.innerHTML = `<div class="pm-suggest-empty">Бренд не найден. Можно искать по части названия.</div>`;
    panel.hidden = false;
    return;
  }

  panel.innerHTML = matches
    .map((brand) => `
      <button class="pm-suggest-option" type="button" role="option" data-brand="${escapeHtml(brand)}">
        <span class="pm-suggest-title">${escapeHtml(brand)}</span>
        <span class="pm-suggest-meta">Фильтр бренда</span>
      </button>
    `)
    .join("");
  panel.hidden = false;
}

async function refreshWarehouseBrandSelect() {
  const input = elements.warehouseBrandFilterInput;
  if (!input) return;
  try {
    const payload = await api("/api/warehouse/brands");
    const brands = Array.isArray(payload.brands) ? payload.brands : [];
    const want = state.warehouseBrandFilter;
    state.warehouseBrands = brands;
    input.value = want || "";
  } catch (_error) {
    state.warehouseBrands = [];
    closeWarehouseBrandSuggestions();
  }
}

async function loadRate(fixedRate) {
  const hasFixed = Number(fixedRate) > 0;
  const savedRate = hasFixed
    ? String(fixedRate)
    : (localStorage.getItem("manualUsdRate") || elements.warehouseUsdRateInput.value || "75.345");
  elements.warehouseUsdRateInput.value = savedRate;
  elements.warehouseUsdRateInput.readOnly = hasFixed;
  elements.warehouseUsdRateInput.title = hasFixed ? "Курс фиксируется в разделе «Настройки»." : "";
  elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(savedRate)} RUB/USD${hasFixed ? " · фиксированный" : ""}`;
  return { rate: Number(savedRate), source: hasFixed ? "settings" : "manual", fetchedAt: new Date().toISOString() };
}

async function loadSettings() {
  const data = await api("/api/marketplaces");
  const fixedRate = data.settings?.fixedUsdRate || data.defaults?.usdRate;
  state.targets = data.targets || [];
  state.accounts = data.accounts || [];
  state.hiddenAccounts = data.hiddenAccounts || [];
  renderTargets();
  renderAccounts();
  renderHiddenAccounts();
  updateAccountFormMode();
  await loadRate(fixedRate);
  const refreshPricesOnFirstLoad = sessionStorage.getItem("mvInitialPriceRefreshDone") !== "1";
  if (refreshPricesOnFirstLoad) sessionStorage.setItem("mvInitialPriceRefreshDone", "1");
  await Promise.all([loadWarehouse(false, refreshPricesOnFirstLoad), loadDailySync()]);
  await refreshWarehouseBrandSelect();
}

function applyMainTab(tab) {
  const safe = VALID_MAIN_TABS.has(tab) ? tab : "warehouse";
  elements.tabButtons.forEach((item) => item.classList.toggle("active", item.dataset.tab === safe));
  elements.tabPanels.forEach((panel) => panel.classList.toggle("active", panel.id === `${safe}Tab`));
}

elements.tabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const tab = button.dataset.tab;
    if (tab) localStorage.setItem(MAIN_TAB_STORAGE_KEY, tab);
    applyMainTab(tab);
  });
});

applyMainTab(localStorage.getItem(MAIN_TAB_STORAGE_KEY) || "warehouse");
applyWarehouseStateFromUrl();
setWarehouseMarketplaceUI(state.warehouseMarketplace);
if (elements.ozonStateFilter) elements.ozonStateFilter.value = state.ozonStateFilter;
if (elements.warehouseAutoPriceOnlyInput) elements.warehouseAutoPriceOnlyInput.checked = state.warehouseAutoOnly;
if (elements.warehouseLinkFilterInput) elements.warehouseLinkFilterInput.value = state.warehouseLinkFilter;
if (elements.warehouseBrandFilterInput) elements.warehouseBrandFilterInput.value = state.warehouseBrandFilter;
if (elements.warehouseAnimateAutoFocusInput) elements.warehouseAnimateAutoFocusInput.checked = state.warehouseAnimateAutoFocus;

document.querySelectorAll("[data-marketplace]").forEach((button) => {
  button.addEventListener("click", () => {
    setWarehouseMarketplaceUI(button.dataset.marketplace);
    state.warehouseMarketplace = button.dataset.marketplace;
    state.warehouseVisibleLimit = 80;
    state.warehousePage = 0;
    state.warehouseRestorePage = 1;
    state.selectedWarehouseGroupKey = null;
    state.warehouseAutoFocusGroupKey = null;
    state.warehouseScrollTop = 0;
    syncWarehouseStateToUrl();
    window.scrollTo({ top: 0, behavior: "auto" });
    queueWarehouseFilterReload();
  });
});

elements.warehouseSearchInput.addEventListener("input", () => {
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  queueWarehouseFilterReload();
});

elements.warehouseAutoPriceOnlyInput?.addEventListener("change", () => {
  state.warehouseAutoOnly = Boolean(elements.warehouseAutoPriceOnlyInput.checked);
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  queueWarehouseFilterReload();
});

elements.warehouseLinkFilterInput?.addEventListener("change", () => {
  state.warehouseLinkFilter = String(elements.warehouseLinkFilterInput.value || "all");
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  queueWarehouseFilterReload();
});

elements.warehouseBrandFilterInput?.addEventListener("input", () => {
  state.warehouseBrandFilter = String(elements.warehouseBrandFilterInput.value || "").trim();
  renderWarehouseBrandSuggestions();
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  queueWarehouseFilterReload(360);
});

elements.warehouseBrandFilterInput?.addEventListener("focus", renderWarehouseBrandSuggestions);

elements.warehouseBrandSuggestions?.addEventListener("mousedown", (event) => {
  event.preventDefault();
});

elements.warehouseBrandSuggestions?.addEventListener("click", (event) => {
  const option = event.target.closest("[data-brand]");
  if (!option) return;
  const brand = String(option.dataset.brand || "").trim();
  elements.warehouseBrandFilterInput.value = brand;
  state.warehouseBrandFilter = brand;
  closeWarehouseBrandSuggestions();
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  queueWarehouseFilterReload(80);
});

document.addEventListener("click", (event) => {
  if (!event.target.closest(".brand-filter-control")) closeWarehouseBrandSuggestions();
});

elements.warehouseAnimateAutoFocusInput?.addEventListener("change", () => {
  state.warehouseAnimateAutoFocus = Boolean(elements.warehouseAnimateAutoFocusInput.checked);
  localStorage.setItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY, state.warehouseAnimateAutoFocus ? "1" : "0");
});

elements.warehouseUsdRateInput?.addEventListener("input", () => {
  if (elements.warehouseUsdRateInput.readOnly) return;
  const value = elements.warehouseUsdRateInput.value;
  localStorage.setItem("manualUsdRate", value);
  elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(value)} RUB/USD`;
});

elements.ozonStateFilter?.addEventListener("change", () => {
  state.ozonStateFilter = elements.ozonStateFilter.value;
  state.warehouseVisibleLimit = 80;
  state.warehousePage = 0;
  state.warehouseRestorePage = 1;
  state.selectedWarehouseGroupKey = null;
  state.warehouseAutoFocusGroupKey = null;
  state.warehouseScrollTop = 0;
  syncWarehouseStateToUrl();
  if (state.ozonStateFilter !== "all" && !state.warehouse.some((product) => product.marketplaceState?.code && product.marketplaceState.code !== "unknown")) {
    elements.warehouseStatus.textContent = "Статусы Ozon + ЯМ обновляются автоматически по расписанию.";
  }
  queueWarehouseFilterReload();
});

elements.manualProductToggle.addEventListener("click", () => {
  elements.warehouseForm.classList.toggle("hidden");
});

elements.warehouseForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/api/warehouse/products", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        target: elements.warehouseTargetInput.value,
        offerId: elements.warehouseOfferInput.value.trim(),
        name: elements.warehouseNameInput.value.trim(),
        keyword: elements.warehouseKeywordInput.value.trim(),
        markup: Number(elements.warehouseMarkupInput.value || 0),
      }),
    });
    elements.warehouseForm.reset();
    elements.warehouseForm.classList.add("hidden");
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.bulkMarkupForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const productIds = selectedWarehouseIds();
  const optimisticLocks = selectedWarehouseLocks();
  const markup = Number(elements.bulkMarkupInput.value || 0);
  if (!productIds.length) {
    elements.warehouseStatus.textContent = "Выберите одну или несколько объединённых карточек.";
    return;
  }
  try {
    const result = await api("/api/warehouse/products/markups/bulk", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds, markup, optimisticLocks }),
    });
    elements.warehouseStatus.textContent = `Наценка ${markup.toFixed(2)} применена: ${formatNumber(result.changed)} товаров Ozon/ЯМ.`;
    elements.bulkMarkupForm.reset();
    queueWarehouseRefresh();
  } catch (error) {
    if (error?.status === 409) {
      const conflictItems = Array.isArray(error?.payload?.conflicts) ? error.payload.conflicts : [];
      const conflicts = conflictItems.length;
      const offerPreview = conflictOfferPreview(conflictItems);
      const suffix = offerPreview ? ` Примеры: ${offerPreview}.` : "";
      elements.warehouseStatus.textContent = `Конфликт bulk-наценки (${formatNumber(conflicts)}).${suffix} Обновляю данные...`;
      showToast(`Часть карточек уже изменена другим менеджером.${suffix}`, "warn");
      queueWarehouseRefresh();
      return;
    }
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.mergeProductsButton?.addEventListener("click", async () => {
  const productIds = selectedWarehouseIds();
  if (productIds.length < 2) {
    elements.warehouseStatus.textContent = "Выберите минимум два товара для ручного объединения.";
    return;
  }
  try {
    const result = await api("/api/warehouse/products/group", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds }),
    });
    elements.warehouseStatus.textContent = `Объединено товаров: ${formatNumber(result.changed)}.`;
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.unmergeProductsButton?.addEventListener("click", async () => {
  const productIds = selectedWarehouseIds();
  if (!productIds.length) {
    elements.warehouseStatus.textContent = "Выберите товары для разъединения.";
    return;
  }
  try {
    const result = await api("/api/warehouse/products/ungroup", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds }),
    });
    elements.warehouseStatus.textContent = `Разъединено товаров: ${formatNumber(result.changed)}.`;
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

async function setAutoPriceForSelected(enabled) {
  const productIds = selectedWarehouseIds();
  const optimisticLocks = selectedWarehouseLocks();
  if (!productIds.length) {
    elements.warehouseStatus.textContent = "Выберите товары для изменения AUTO-режима.";
    return;
  }
  try {
    const result = await api("/api/warehouse/products/auto-price/bulk", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds, enabled, optimisticLocks }),
    });
    elements.warehouseStatus.textContent = `AUTO ${enabled ? "включен" : "выключен"}: ${formatNumber(result.changed)} товаров.`;
    queueWarehouseRefresh();
  } catch (error) {
    if (error?.status === 409) {
      const conflictItems = Array.isArray(error?.payload?.conflicts) ? error.payload.conflicts : [];
      const conflicts = conflictItems.length;
      const offerPreview = conflictOfferPreview(conflictItems);
      const suffix = offerPreview ? ` Примеры: ${offerPreview}.` : "";
      elements.warehouseStatus.textContent = `Конфликт bulk-AUTO (${formatNumber(conflicts)}).${suffix} Обновляю данные...`;
      showToast(`Часть карточек уже изменена другим менеджером.${suffix}`, "warn");
      queueWarehouseRefresh();
      return;
    }
    throw error;
  }
}

elements.autoPriceEnableSelectedButton?.addEventListener("click", () => {
  setAutoPriceForSelected(true).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.autoPriceDisableSelectedButton?.addEventListener("click", () => {
  setAutoPriceForSelected(false).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.autoPriceDisableAllButton?.addEventListener("click", async () => {
  if (!(await confirmAction({ title: "Выключить AUTO у всех?", text: "Отключить автоматическую отправку цен для всех товаров?", okText: "Выключить" }))) return;
  try {
    let result;
    try {
      result = await api("/api/warehouse/products/auto-price/all", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: false }),
      });
    } catch (error) {
      // Backward-compatible fallback when server is not restarted yet.
      if (!/404/.test(String(error.message || ""))) throw error;
      result = await api("/api/warehouse/products/auto-price/bulk", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ productIds: state.warehouse.map((item) => item.id), enabled: false }),
      });
    }
    elements.warehouseStatus.textContent = `AUTO отключен у всех: ${formatNumber(result.changed || state.warehouse.length)} товаров.`;
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.warehouseSyncButton.addEventListener("click", () => {
  loadWarehouse(true).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.warehouseRefreshPricesButton.addEventListener("click", () => {
  state.enrichedProductIds = new Set();
  loadWarehouse(false, true).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.warehouseLoadMoreButton.addEventListener("click", () => {
  const groups = getSortedWarehouseGroups();
  if (state.warehouseVisibleLimit < groups.length) {
    state.warehouseVisibleLimit += 80;
    renderWarehouseCards();
    return;
  }
  loadWarehousePage({ reset: false }).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.warehouseViewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    state.warehouseViewMode = button.dataset.warehouseView === "list" ? "list" : "cards";
    localStorage.setItem("warehouseViewMode", state.warehouseViewMode);
    syncWarehouseStateToUrl();
    renderWarehouseCards();
  });
});

elements.dailySyncRunButton?.addEventListener("click", async () => {
  elements.dailySyncRunButton.disabled = true;
  elements.dailySyncStatus.textContent = "Запускаю обновление...";
  try {
    const status = await api("/api/daily-sync/run", { method: "POST" });
    renderDailySync(status);
    queueWarehouseRefresh();
  } catch (error) {
    elements.dailySyncMeta.textContent = error.message;
  } finally {
    elements.dailySyncRunButton.disabled = false;
  }
});

elements.warehouseCards.addEventListener("click", async (event) => {
  if (event.target.classList.contains("warehouse-check")) return;
  const card = event.target.closest(".product-card");
  const button = event.target.closest(".select-product");
  const groupKey = button?.dataset.groupKey || card?.dataset.groupKey;
  if (!groupKey) return;
  state.selectedWarehouseGroupKey = groupKey;
  state.warehouseAutoFocusGroupKey = null;
  syncWarehouseStateToUrl();
  renderWarehouseCards();
  renderWarehouseDetail(getSortedWarehouseGroups().find((group) => group.key === groupKey));
  try {
    const detailed = await ensureWarehouseGroupDetailed(groupKey);
    if (state.selectedWarehouseGroupKey === groupKey) renderWarehouseDetail(detailed);
  } catch (error) {
    elements.warehouseStatus.textContent = `Не удалось загрузить детали товара: ${error.message}`;
  }
  focusWarehouseDetailOnSmallScreen();
});

elements.warehouseCards.addEventListener("change", (event) => {
  if (event.target.classList.contains("warehouse-check")) updateSelection();
});

elements.warehouseDetail.addEventListener("input", (event) => {
  const input = event.target.closest('.variant-markup-row input[name="markup"]');
  if (!input) return;
  const usd = Number(input.dataset.usdPrice || 0);
  const markup = Number(input.value || 0);
  const rate = Number(elements.warehouseUsdRateInput?.value || 0);
  const next = usd > 0 && markup > 0 && rate > 0 ? Math.round(usd * rate * markup) : 0;
  const current = Number(input.dataset.currentPrice || 0);
  const preview = input.closest("label")?.querySelector(".markup-live-preview");
  if (!preview) return;
  if (next > 0) {
    const delta = current > 0 ? ` (${next > current ? "+" : ""}${Math.round(next - current)} ₽)` : "";
    preview.textContent = `Предпросмотр: ${formatMoney(next)}${delta}`;
  } else {
    preview.textContent = "Предпросмотр: —";
  }
});

elements.warehouseDetail.addEventListener("submit", async (event) => {
  const markupForm = event.target.closest(".variant-markup-row");
  if (markupForm) {
    event.preventDefault();
    const formData = new FormData(markupForm);
    try {
      await api(`/api/warehouse/products/${markupForm.dataset.productId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          markup: Number(formData.get("markup") || 0),
          expectedUpdatedAt: String(formData.get("expectedUpdatedAt") || ""),
        }),
      });
      elements.warehouseStatus.textContent = "Наценка сохранена. Автоотправка цены поставлена в очередь.";
      queueWarehouseRefresh();
    } catch (error) {
      if (error?.status === 409) {
        elements.warehouseStatus.textContent = "Конфликт изменений: товар уже обновлен другим пользователем. Обновляю данные...";
        showToast("Карточка была изменена другим менеджером. Данные перезагружены.", "warn");
        queueWarehouseRefresh();
        return;
      }
      elements.warehouseStatus.textContent = error.message;
    }
    return;
  }
  if (!event.target.classList.contains("inline-link-form")) return;
  event.preventDefault();
  const form = event.target;
  const data = new FormData(form);
  try {
    const productIds = String(form.dataset.productIds || form.dataset.productId || "")
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean);
    const result = await api("/api/warehouse/products/links/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...Object.fromEntries(data.entries()), productIds }),
    });
    if (Array.isArray(result.products)) result.products.forEach((product) => mergeWarehouseProduct(product));
    applyWarehouseFilters();
    renderWarehouseCards();
    const currentGroup = state.selectedWarehouseGroupKey
      ? getSortedWarehouseGroups().find((group) => group.key === state.selectedWarehouseGroupKey)
      : null;
    if (currentGroup) {
      renderWarehouseDetail(currentGroup);
    } else if (state.selectedWarehouseDetailGroup) {
      renderWarehouseDetail(state.selectedWarehouseDetailGroup);
    }
    elements.warehouseStatus.textContent = `Привязка сохранена: ${formatNumber(result.changed || productIds.length)} товар(ов). Цена отправится автоматически.`;
    window.setTimeout(() => {
      if (!document.hidden) loadWarehouse(false).catch(() => {});
    }, 8000);
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.warehouseDetail.addEventListener("click", async (event) => {
  const toggleButton = event.target.closest(".card-auto-toggle");
  if (!toggleButton) return;
  const productIds = String(toggleButton.dataset.groupProductIds || "")
    .split(",")
    .map((id) => id.trim())
    .filter(Boolean);
  if (!productIds.length) return;
  const enable = String(toggleButton.dataset.enabled || "0") !== "1";
  const byId = new Map((state.warehouse || []).map((item) => [String(item.id), item]));
  const optimisticLocks = productIds.map((id) => ({
    id,
    expectedUpdatedAt: String(byId.get(id)?.updatedAt || ""),
  }));

  toggleButton.disabled = true;
  try {
    const result = await api("/api/warehouse/products/auto-price/bulk", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds, enabled: enable, optimisticLocks }),
    });
    elements.warehouseStatus.textContent = `AUTO ${enable ? "включен" : "выключен"} для карточки: ${formatNumber(result.changed)} вариантов.`;
    queueWarehouseRefresh();
  } catch (error) {
    if (error?.status === 409) {
      const conflictItems = Array.isArray(error?.payload?.conflicts) ? error.payload.conflicts : [];
      const offerPreview = conflictOfferPreview(conflictItems);
      const suffix = offerPreview ? ` Примеры: ${offerPreview}.` : "";
      elements.warehouseStatus.textContent = `Конфликт AUTO для карточки.${suffix} Обновляю данные...`;
      showToast(`Часть вариантов уже изменена другим менеджером.${suffix}`, "warn");
      queueWarehouseRefresh();
      return;
    }
    elements.warehouseStatus.textContent = error.message;
  } finally {
    toggleButton.disabled = false;
  }
});

elements.warehouseDetail.addEventListener("click", async (event) => {
  const linkButton = event.target.closest(".delete-link");
  const productButton = event.target.closest(".delete-product");
  const exportButton = event.target.closest(".export-product");
  try {
    if (exportButton) {
      const target = exportButton.dataset.target;
      const targetMeta = state.targets.find((item) => item.id === target);
      const label = targetMeta?.name || (target === "yandex" ? "Yandex Market" : "Ozon");
      if (!(await confirmAction({ title: "Выгрузить товар?", text: `Выгрузить карточку товара в ${label}?`, okText: "Выгрузить", danger: false }))) return;
      exportButton.disabled = true;
      elements.warehouseStatus.textContent = `Выгружаю товар в ${label}...`;
      const result = await api(`/api/warehouse/products/${exportButton.dataset.productId}/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirmed: true, target }),
      });
      elements.warehouseStatus.textContent = `Готово: карточка выгружена в ${label}. Отправлено: ${formatNumber(result.sent || 1)}.`;
      queueWarehouseRefresh();
      return;
    }
    if (linkButton) {
      const result = await api(`/api/warehouse/products/${linkButton.dataset.productId}/links/${linkButton.dataset.linkId}`, { method: "DELETE" });
      if (result.product) {
        mergeWarehouseProduct(result.product);
        applyWarehouseFilters();
        renderWarehouseCards();
        const currentGroup = state.selectedWarehouseGroupKey
          ? getSortedWarehouseGroups().find((group) => group.key === state.selectedWarehouseGroupKey)
          : null;
        renderWarehouseDetail(currentGroup || null);
      }
      elements.warehouseStatus.textContent = "Привязка удалена. Автоцена пересчитается по оставшимся привязкам.";
      window.setTimeout(() => {
        if (!document.hidden) loadWarehouse(false).catch(() => {});
      }, 8000);
    }
    if (productButton && await confirmAction({ title: "Удалить товар?", text: "Удалить товар из личного склада?", okText: "Удалить" })) {
      await api(`/api/warehouse/products/${productButton.dataset.productId}`, { method: "DELETE" });
      queueWarehouseRefresh();
    }
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.warehouseSelectChangedButton.addEventListener("click", () => {
  document.querySelectorAll(".warehouse-check").forEach((input) => {
    const ids = String(input.dataset.productIds || "").split(",");
    input.checked = state.filteredWarehouse.some((product) => ids.includes(product.id) && product.ready && product.changed);
  });
  updateSelection();
});

elements.warehouseSendButton.addEventListener("click", async () => {
  const productIds = selectedWarehouseIds();
  if (!productIds.length) return;
  if (!(await confirmAction({ title: "Обновить цены?", text: `Обновить цены для ${productIds.length} товаров на маркетплейсах?`, okText: "Обновить", danger: false }))) return;

  elements.warehouseSendButton.disabled = true;
  elements.warehouseStatus.textContent = "Отправляю цены на маркетплейсы...";
  try {
    const result = await api("/api/warehouse/prices/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        confirmed: true,
        productIds,
        usdRate: Number(elements.warehouseUsdRateInput.value),
        minDiffRub: Number(elements.warehouseMinDiffRubInput?.value || 0),
        minDiffPct: Number(elements.warehouseMinDiffPctInput?.value || 0),
      }),
    });
    elements.warehouseStatus.textContent = `Готово: отправлено ${formatNumber(result.sent)} цен.`;
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  } finally {
    updateSelection();
  }
});

elements.warehouseDryRunButton?.addEventListener("click", async () => {
  const productIds = selectedWarehouseIds();
  if (!productIds.length) {
    elements.warehouseStatus.textContent = "Выберите товары для dry-run.";
    return;
  }
  elements.warehouseStatus.textContent = "Считаю dry-run...";
  try {
    const result = await api("/api/warehouse/prices/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        confirmed: true,
        dryRun: true,
        productIds,
        usdRate: Number(elements.warehouseUsdRateInput.value),
        minDiffRub: Number(elements.warehouseMinDiffRubInput?.value || 0),
        minDiffPct: Number(elements.warehouseMinDiffPctInput?.value || 0),
      }),
    });
    elements.warehouseStatus.textContent = `Dry-run: готово к отправке ${formatNumber(result.readyToSend || 0)}, пропущено ${formatNumber((result.skipped || []).length)}.`;
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.warehouseRetryQueueButton?.addEventListener("click", async () => {
  if (!(await confirmAction({ title: "Повторить очередь?", text: "Отправить повторно неотправленные цены?", okText: "Повторить", danger: false }))) return;
  elements.warehouseStatus.textContent = "Повторяю очередь отправки...";
  try {
    const result = await api("/api/warehouse/prices/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true }),
    });
    state.retryQueueLastRun = { retried: Number(result.retried || 0), failed: Number(result.failed || 0), at: new Date().toISOString() };
    elements.warehouseStatus.textContent = `Retry: отправлено ${formatNumber(result.retried || 0)}, осталось в очереди ${formatNumber(result.remaining || 0)}.`;
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.retryQueueRefreshButton?.addEventListener("click", () => {
  loadRetryQueue().catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

elements.retryQueueSortInput?.addEventListener("change", () => {
  state.retryQueueSort = elements.retryQueueSortInput.value || "newest";
  renderRetryQueue({ items: state.retryQueue });
});

elements.retryQueueMarketplaceFilterInput?.addEventListener("change", () => {
  state.retryQueueMarketplace = elements.retryQueueMarketplaceFilterInput.value || "all";
  renderRetryQueue({ items: state.retryQueue });
});

elements.retryQueueSearchInput?.addEventListener("input", () => {
  state.retryQueueSearch = elements.retryQueueSearchInput.value || "";
  renderRetryQueue({ items: state.retryQueue });
});

elements.retryQueueList?.addEventListener("change", (event) => {
  const checkbox = event.target.closest(".retry-queue-check");
  if (!checkbox) return;
  const key = String(checkbox.dataset.queueKey || "");
  if (!key) return;
  if (checkbox.checked) state.retryQueueSelectedKeys.add(key);
  else state.retryQueueSelectedKeys.delete(key);
  elements.retryQueueRetrySelectedButton.disabled = !state.retryQueueSelectedKeys.size;
});

elements.retryQueueRetrySelectedButton?.addEventListener("click", async () => {
  if (!state.retryQueueSelectedKeys.size) return;
  elements.warehouseStatus.textContent = "Повторяю выбранные элементы из очереди...";
  try {
    const result = await api("/api/warehouse/prices/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true, queueKeys: Array.from(state.retryQueueSelectedKeys) }),
    });
    state.retryQueueLastRun = { retried: Number(result.retried || 0), failed: Number(result.failed || 0), at: new Date().toISOString() };
    elements.warehouseStatus.textContent = `Retry выбранных: отправлено ${formatNumber(result.retried || 0)}, осталось ${formatNumber(result.remaining || 0)}.`;
    state.retryQueueSelectedKeys.clear();
    queueWarehouseRefresh();
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.retryQueueClearButton?.addEventListener("click", async () => {
  if (!(await confirmAction({ title: "Очистить очередь?", text: "Удалить все элементы retry-очереди?", okText: "Очистить" }))) return;
  try {
    await api("/api/warehouse/prices/retry-queue", { method: "DELETE" });
    state.retryQueueSelectedKeys.clear();
    await loadRetryQueue();
    elements.warehouseStatus.textContent = "Очередь retry очищена.";
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.supplierForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const supplierId = elements.supplierIdInput.value.trim();
  try {
    await api(supplierId ? `/api/suppliers/${encodeURIComponent(supplierId)}` : "/api/suppliers", {
      method: supplierId ? "PATCH" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: elements.supplierNameInput.value.trim(),
        note: elements.supplierNoteInput.value.trim(),
        stopReason: elements.supplierStopReasonInput.value.trim(),
      }),
    });
    resetSupplierForm();
    queueWarehouseRefresh();
  } catch (error) {
    elements.supplierStatus.textContent = error.message;
  }
});

elements.supplierCancelEditButton?.addEventListener("click", () => {
  resetSupplierForm();
  elements.supplierStatus.textContent = "Редактирование поставщика отменено.";
});

elements.supplierViewButtons?.forEach((button) => {
  button.addEventListener("click", () => {
    state.supplierView = button.dataset.supplierView === "inactive" ? "inactive" : "active";
    renderSuppliers();
  });
});

elements.supplierSearchInput?.addEventListener("input", () => {
  state.supplierSearch = elements.supplierSearchInput.value.trim();
  renderSuppliers();
});

elements.supplierBoard.addEventListener("change", async (event) => {
  const toggle = event.target.closest(".supplier-stop-toggle");
  if (!toggle) return;
  const panel = event.target.closest(".supplier-panel");
  const name = panel.querySelector("h3")?.textContent || "поставщика";
  const stopped = toggle.checked;
  const supplier = state.suppliers.find((item) => item.id === panel.dataset.supplierId);

  if (stopped) {
    const modalResult = await openSupplierInactiveModal(supplier || { id: panel.dataset.supplierId });
    if (!modalResult) {
      toggle.checked = false;
      return;
    }
    try {
      await api(`/api/suppliers/${panel.dataset.supplierId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stopped: true,
          stopReason: modalResult.comment,
          inactiveComment: modalResult.comment,
          inactiveUntil: modalResult.unknown ? null : modalResult.date,
          inactiveUntilUnknown: modalResult.unknown,
        }),
      });
      queueWarehouseRefresh();
    } catch (error) {
      toggle.checked = false;
      elements.supplierStatus.textContent = error.message;
    }
    return;
  }

  if (!(await confirmAction({
    title: "Вернуть поставщика в актив?",
    text: `${name}: поставщик снова будет участвовать в выборе цены.`,
    okText: "Вернуть в актив",
    danger: false,
  }))) {
    toggle.checked = true;
    return;
  }

  try {
    await api(`/api/suppliers/${panel.dataset.supplierId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        stopped: false,
        inactiveComment: "",
        inactiveUntil: null,
        inactiveUntilUnknown: false,
      }),
    });
    queueWarehouseRefresh();
  } catch (error) {
    toggle.checked = true;
    elements.supplierStatus.textContent = error.message;
  }
});

elements.supplierBoard.addEventListener("submit", async (event) => {
  if (!event.target.classList.contains("supplier-article-form")) return;
  event.preventDefault();
  const form = event.target;
  const data = new FormData(form);
  try {
    await api(`/api/suppliers/${form.dataset.supplierId}/articles`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(Object.fromEntries(data.entries())),
    });
    form.reset();
    form.querySelector("button[type='submit']").textContent = "Добавить артикул";
    queueWarehouseRefresh();
  } catch (error) {
    elements.supplierStatus.textContent = error.message;
  }
});

elements.supplierBoard.addEventListener("click", async (event) => {
  const panel = event.target.closest(".supplier-panel");
  const deleteSupplier = event.target.closest(".delete-supplier");
  const deleteArticle = event.target.closest(".delete-supplier-article");
  const editSupplier = event.target.closest(".edit-supplier");
  const editArticle = event.target.closest(".edit-supplier-article");
  if (!panel) return;

  try {
    if (editSupplier) {
      startSupplierEdit(state.suppliers.find((supplier) => supplier.id === panel.dataset.supplierId));
      return;
    }
    if (editArticle) {
      const supplier = state.suppliers.find((item) => item.id === panel.dataset.supplierId);
      const article = supplier?.articles?.find((item) => item.id === editArticle.dataset.articleId);
      fillSupplierArticleForm(panel, article);
      return;
    }
    if (deleteSupplier && await confirmAction({ title: "Удалить поставщика?", text: "Поставщик и его локальные артикулы будут удалены.", okText: "Удалить" })) {
      await api(`/api/suppliers/${panel.dataset.supplierId}`, { method: "DELETE" });
      queueWarehouseRefresh();
    }
    if (deleteArticle && await confirmAction({ title: "Удалить артикул?", text: "Артикул поставщика будет удалён из локального списка.", okText: "Удалить" })) {
      await api(`/api/suppliers/${panel.dataset.supplierId}/articles/${deleteArticle.dataset.articleId}`, { method: "DELETE" });
      queueWarehouseRefresh();
    }
  } catch (error) {
    elements.supplierStatus.textContent = error.message;
  }
});

elements.accountMarketplaceInput?.addEventListener("change", updateAccountFormMode);

elements.accountCancelEditButton?.addEventListener("click", () => {
  resetAccountForm();
  elements.accountStatus.textContent = "Редактирование отменено.";
});

elements.reloadAccountsButton?.addEventListener("click", async () => {
  elements.accountStatus.textContent = "Обновляю кабинеты...";
  try {
    await loadAccounts();
    elements.accountStatus.textContent = "Список кабинетов обновлен.";
  } catch (error) {
    elements.accountStatus.textContent = error.message;
  }
});

elements.accountForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = Object.fromEntries(new FormData(elements.accountForm).entries());
  const accountId = String(formData.id || "").trim();
  const isEditing = Boolean(accountId);
  elements.accountStatus.textContent = isEditing ? "Сохраняю изменения кабинета..." : "Сохраняю кабинет...";
  try {
    const result = await api(isEditing ? `/api/marketplace-accounts/${encodeURIComponent(accountId)}` : "/api/marketplace-accounts", {
      method: isEditing ? "PATCH" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(formData),
    });
    state.accounts = result.accounts || [];
    state.hiddenAccounts = result.hiddenAccounts || [];
    state.targets = result.targets || state.targets;
    resetAccountForm();
    renderAccounts();
    renderHiddenAccounts();
    renderTargets();
    elements.accountStatus.textContent = isEditing
      ? "Кабинет обновлен. Изменения доступны в складе и формах товаров."
      : "Кабинет сохранен. Теперь он доступен в складе и формах товаров.";
  } catch (error) {
    elements.accountStatus.textContent = error.message;
  }
});

elements.accountsBoard?.addEventListener("click", async (event) => {
  const editButton = event.target.closest(".edit-account");
  const deleteButton = event.target.closest(".delete-account");
  const card = event.target.closest(".account-card");
  if (!card) return;

  if (editButton) {
    const account = state.accounts.find((item) => item.id === card.dataset.accountId);
    startAccountEdit(account);
    return;
  }

  if (!deleteButton) return;
  const account = state.accounts.find((item) => item.id === card.dataset.accountId);
  const confirmText = account?.readOnly
    ? "Скрыть кабинет из .env в интерфейсе? Его можно будет вернуть в блоке «Скрытые кабинеты»."
    : "Удалить этот кабинет?";
  const confirmed = await confirmAction({
    title: account?.readOnly ? "Скрыть кабинет?" : "Удалить кабинет?",
    text: confirmText,
    okText: account?.readOnly ? "Скрыть" : "Удалить",
  });
  if (!confirmed) return;

  elements.accountStatus.textContent = account?.readOnly ? "Скрываю кабинет..." : "Удаляю кабинет...";
  try {
    const result = await api(`/api/marketplace-accounts/${card.dataset.accountId}`, { method: "DELETE" });
    state.accounts = result.accounts || [];
    state.hiddenAccounts = result.hiddenAccounts || [];
    state.targets = result.targets || state.targets;
    if (elements.accountIdInput?.value === card.dataset.accountId) resetAccountForm();
    renderAccounts();
    renderHiddenAccounts();
    renderTargets();
    elements.accountStatus.textContent = account?.readOnly ? "Кабинет скрыт из интерфейса." : "Кабинет удален.";
  } catch (error) {
    elements.accountStatus.textContent = error.message;
  }
});

elements.hiddenAccountsBoard?.addEventListener("click", async (event) => {
  const button = event.target.closest(".restore-account");
  const row = event.target.closest(".hidden-account-row");
  if (!button || !row) return;
  try {
    elements.accountStatus.textContent = "Возвращаю кабинет...";
    const result = await api(`/api/marketplace-accounts/${encodeURIComponent(row.dataset.accountId)}/restore`, { method: "POST" });
    state.accounts = result.accounts || [];
    state.hiddenAccounts = result.hiddenAccounts || [];
    state.targets = result.targets || state.targets;
    renderAccounts();
    renderHiddenAccounts();
    renderTargets();
    elements.accountStatus.textContent = "Кабинет возвращен.";
  } catch (error) {
    elements.accountStatus.textContent = error.message;
  }
});

elements.syncProgressClose?.addEventListener("click", () => {
  elements.warehouseSyncProgress?.classList.add("hidden");
  elements.syncMiniProgress?.classList.remove("hidden");
});

elements.syncMiniProgress?.addEventListener("click", () => {
  elements.syncMiniProgress.classList.add("hidden");
  elements.warehouseSyncProgress?.classList.remove("hidden");
});

elements.logoutButton.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

function setupIosSelectRoot(root) {
  const select = root.querySelector("select.ios-select-native");
  const trigger = root.querySelector(".ios-select-trigger");
  const menu = root.querySelector(".ios-select-menu");
  const valueEl = root.querySelector(".ios-select-value");
  if (!select || !trigger || !menu || !valueEl) return;

  function syncFromSelect() {
    const opt = select.options[select.selectedIndex];
    valueEl.textContent = opt?.textContent?.trim() || "";
  }

  function close() {
    menu.hidden = true;
    trigger.setAttribute("aria-expanded", "false");
    root.classList.remove("is-open");
  }

  function open() {
    menu.replaceChildren();
    Array.from(select.options).forEach((opt, index) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.setAttribute("role", "option");
      btn.className = "ios-select-option";
      if (opt.selected) btn.classList.add("ios-select-option--active");
      btn.dataset.value = opt.value;
      btn.textContent = opt.textContent;
      btn.addEventListener("mousedown", (e) => e.preventDefault());
      btn.addEventListener("click", () => {
        if (select.selectedIndex !== index) {
          select.selectedIndex = index;
          select.dispatchEvent(new Event("change", { bubbles: true }));
        }
        syncFromSelect();
        close();
      });
      menu.appendChild(btn);
    });
    menu.hidden = false;
    trigger.setAttribute("aria-expanded", "true");
    root.classList.add("is-open");
  }

  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    if (root.classList.contains("is-open")) close();
    else open();
  });

  select.addEventListener("change", syncFromSelect);
  syncFromSelect();
}

function initIosSelects() {
  if (!document.documentElement.dataset.iosSelectBound) {
    document.documentElement.dataset.iosSelectBound = "1";
    document.addEventListener("click", (e) => {
      document.querySelectorAll("[data-ios-select].is-open").forEach((root) => {
        if (!root.contains(e.target)) {
          const trigger = root.querySelector(".ios-select-trigger");
          const menu = root.querySelector(".ios-select-menu");
          if (menu) menu.hidden = true;
          if (trigger) trigger.setAttribute("aria-expanded", "false");
          root.classList.remove("is-open");
        }
      });
    });
    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      document.querySelectorAll("[data-ios-select].is-open").forEach((root) => {
        const trigger = root.querySelector(".ios-select-trigger");
        const menu = root.querySelector(".ios-select-menu");
        if (menu) menu.hidden = true;
        if (trigger) trigger.setAttribute("aria-expanded", "false");
        root.classList.remove("is-open");
        trigger?.focus();
      });
    });
  }
  document.querySelectorAll("[data-ios-select]").forEach((root) => setupIosSelectRoot(root));
}

function initWarehouseInfiniteScroll() {
  if (!elements.warehouseLoadMoreButton || !("IntersectionObserver" in window)) return;
  const observer = new IntersectionObserver((entries) => {
    const entry = entries[0];
    if (!entry?.isIntersecting || state.warehouseLoadingPage) return;
    if (elements.warehouseLoadMoreButton.classList.contains("hidden")) return;
    elements.warehouseLoadMoreButton.click();
  }, { rootMargin: "280px 0px 280px 0px" });
  observer.observe(elements.warehouseLoadMoreButton);
}

function initWarehouseScrollTracking() {
  let timer = null;
  window.addEventListener("scroll", () => {
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(() => {
      captureWarehouseScroll();
      syncWarehouseStateToUrl();
    }, 140);
  }, { passive: true });
  window.addEventListener("beforeunload", captureWarehouseScroll, { passive: true });
}

window.addEventListener("popstate", () => {
  applyWarehouseStateFromUrl();
  setWarehouseMarketplaceUI(state.warehouseMarketplace);
  if (elements.ozonStateFilter) elements.ozonStateFilter.value = state.ozonStateFilter;
  if (elements.warehouseAutoPriceOnlyInput) elements.warehouseAutoPriceOnlyInput.checked = state.warehouseAutoOnly;
  if (elements.warehouseLinkFilterInput) elements.warehouseLinkFilterInput.value = state.warehouseLinkFilter;
  if (elements.warehouseBrandFilterInput) elements.warehouseBrandFilterInput.value = state.warehouseBrandFilter;
  if (elements.warehouseAnimateAutoFocusInput) elements.warehouseAnimateAutoFocusInput.checked = state.warehouseAnimateAutoFocus;
  if (elements.warehouseSearchInput) {
    const params = new URLSearchParams(window.location.search);
    elements.warehouseSearchInput.value = params.get("q") || "";
  }
  loadWarehouse(false).catch((error) => {
    elements.warehouseStatus.textContent = error.message;
  });
});

initIosSelects();
initWarehouseInfiniteScroll();
initWarehouseScrollTracking();

if (elements.supplierInactiveUntilInput) {
  elements.supplierInactiveUntilInput.min = new Date().toISOString().slice(0, 10);
}

loadSettings().catch((error) => {
  elements.warehouseStatus.textContent = error.message;
  elements.supplierStatus.textContent = error.message;
});
