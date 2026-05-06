const state = {
  rows: [],
  filteredRows: [],
  targets: [],
  warehouse: [],
  filteredWarehouse: [],
  suppliers: [],
  accounts: [],
  hiddenAccounts: [],
  selectedWarehouseProductId: null,
  selectedWarehouseGroupKey: null,
  warehouseMarketplace: "all",
  ozonStateFilter: "all",
  warehouseViewMode: localStorage.getItem("warehouseViewMode") || "cards",
  warehouseVisibleLimit: 80,
  enrichedProductIds: new Set(),
};

const elements = {
  logoutButton: document.querySelector("#logoutButton"),
  tabButtons: document.querySelectorAll(".tab-button"),
  tabPanels: document.querySelectorAll(".tab-panel"),
  previewForm: document.querySelector("#previewForm"),
  usdRateInput: document.querySelector("#usdRateInput"),
  ozonMarkupInput: document.querySelector("#ozonMarkupInput"),
  yandexMarkupInput: document.querySelector("#yandexMarkupInput"),
  supplierMarkupInput: document.querySelector("#supplierMarkupInput"),
  limitInput: document.querySelector("#limitInput"),
  searchInput: document.querySelector("#searchInput"),
  targetList: document.querySelector("#targetList"),
  rateInfo: document.querySelector("#rateInfo"),
  refreshRateButton: document.querySelector("#refreshRateButton"),
  statusFilter: document.querySelector("#statusFilter"),
  marketplaceFilter: document.querySelector("#marketplaceFilter"),
  sourceCount: document.querySelector("#sourceCount"),
  rowCount: document.querySelector("#rowCount"),
  changedCount: document.querySelector("#changedCount"),
  selectedCount: document.querySelector("#selectedCount"),
  statusText: document.querySelector("#statusText"),
  selectChangedButton: document.querySelector("#selectChangedButton"),
  sendButton: document.querySelector("#sendButton"),
  previewBody: document.querySelector("#previewBody"),
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
  manualProductToggle: document.querySelector("#manualProductToggle"),
  warehouseSyncButton: document.querySelector("#warehouseSyncButton"),
  warehouseRefreshPricesButton: document.querySelector("#warehouseRefreshPricesButton"),
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
  warehouseStatus: document.querySelector("#warehouseStatus"),
  warehouseRateInfo: document.querySelector("#warehouseRateInfo"),
  warehouseCards: document.querySelector("#warehouseCards"),
  warehouseViewButtons: document.querySelectorAll("[data-warehouse-view]"),
  warehouseLoadMoreButton: document.querySelector("#warehouseLoadMoreButton"),
  warehouseVisibleInfo: document.querySelector("#warehouseVisibleInfo"),
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
  supplierBoard: document.querySelector("#supplierBoard"),
  supplierArticleFormTemplate: document.querySelector("#supplierArticleFormTemplate"),
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

async function api(path, options) {
  const response = await fetch(path, options);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    const missing = Array.isArray(detail.missing) ? ` Не хватает: ${detail.missing.join(", ")}` : "";
    throw new Error(`${detail.detail || detail.error || "Ошибка запроса"}${missing}`);
  }
  return response.json();
}

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
    const links = variants.flatMap((product) => (product.links || []).map((link) => ({ ...link, productId: product.id })));
    const suppliers = variants.flatMap((product) => product.suppliers || []);
    return {
      ...group,
      primary,
      variants,
      links,
      suppliers,
      selectedSupplier: variants.find((product) => product.selectedSupplier)?.selectedSupplier || primary.selectedSupplier,
      ready: variants.some((product) => product.ready),
      changed: variants.some((product) => product.changed),
      supplierCount: Math.max(...variants.map((product) => product.supplierCount || 0), 0),
      availableSupplierCount: Math.max(...variants.map((product) => product.availableSupplierCount || 0), 0),
      marketplaceLabels: Array.from(new Set(variants.map((product) => marketLabel(product)))),
      productIds: variants.map((product) => product.id),
    };
  });
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

function selectedRows() {
  return Array.from(document.querySelectorAll(".row-check:checked")).map((input) => {
    const row = state.filteredRows[Number(input.dataset.index)];
    return { target: row.target, offerId: row.offerId, price: row.nextPrice };
  });
}

function selectedWarehouseIds() {
  return Array.from(document.querySelectorAll(".warehouse-check:checked")).flatMap((input) =>
    String(input.dataset.productIds || input.value || "")
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean),
  );
}

function updateSelection() {
  const selected = selectedRows().length;
  elements.selectedCount.textContent = formatNumber(selected);
  elements.sendButton.disabled = selected === 0;

  const warehouseSelected = selectedWarehouseIds().length;
  elements.warehouseSendButton.disabled = warehouseSelected === 0;
}

function renderTargets() {
  const visibleTargets = state.targets.length ? state.targets : [];
  const manualTargets = state.targets.filter((target) => target.configured !== false);

  elements.targetList.innerHTML = visibleTargets
    .map(
      (target) => `
        <label class="target-option">
          <input type="checkbox" name="target" value="${escapeHtml(target.id)}" checked />
          <span>${escapeHtml(target.name)}</span>
        </label>
      `,
    )
    .join("");

  elements.marketplaceFilter.innerHTML = `
    <option value="all">Все маркетплейсы</option>
    ${visibleTargets.map((target) => `<option value="${escapeHtml(target.id)}">${escapeHtml(target.name)}</option>`).join("")}
  `;

  elements.warehouseTargetInput.innerHTML = manualTargets.length
    ? manualTargets.map((target) => `<option value="${escapeHtml(target.id)}">${escapeHtml(target.name)}</option>`).join("")
    : `<option value="ozon">Ozon</option>`;
  updateSyncButtonLabel();
}

function applyWarehouseFilters() {
  const query = elements.warehouseSearchInput.value.trim().toLowerCase();
  state.filteredWarehouse = state.warehouse.filter((product) => {
    const marketOk = state.warehouseMarketplace === "all" || product.marketplace === state.warehouseMarketplace;
    const ozonStateOk = state.ozonStateFilter === "all"
      || (product.marketplaceState?.code || "unknown") === state.ozonStateFilter;
    const supplier = product.selectedSupplier;
    const searchHaystack = [
      product.name,
      product.offerId,
      product.productId,
      product.keyword,
      supplier?.partnerName,
      supplier?.article,
      ...(product.links || []).map((link) => `${link.article} ${link.supplierName} ${link.keyword}`),
    ]
      .join(" ")
      .toLowerCase();
    return marketOk && ozonStateOk && (!query || searchHaystack.includes(query));
  });

  if (!state.filteredWarehouse.some((product) => product.id === state.selectedWarehouseProductId)) {
    state.selectedWarehouseProductId = state.filteredWarehouse[0]?.id || null;
  }

  renderWarehouseCards();
  renderWarehouseDetail(buildWarehouseGroups(state.filteredWarehouse).find((group) => group.key === state.selectedWarehouseGroupKey));
}

function renderWarehouse(data) {
  state.warehouse = data.products || [];
  state.suppliers = data.suppliers || state.suppliers || [];
  state.warehouseVisibleLimit = 80;
  state.enrichedProductIds = new Set();
  elements.warehouseTotal.textContent = formatNumber(data.total);
  elements.warehouseReady.textContent = formatNumber(data.ready);
  elements.warehouseChanged.textContent = formatNumber(data.changed);
  elements.warehouseNoSupplier.textContent = formatNumber(data.withoutSupplier);
  elements.warehouseOzonArchived.textContent = formatNumber(data.ozonArchived || 0);
  elements.warehouseOzonInactive.textContent = formatNumber(data.ozonInactive || 0);
  elements.warehouseOzonOutOfStock.textContent = formatNumber(data.ozonOutOfStock || 0);
  elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(data.usdRate)} RUB/USD`;
  elements.warehouseSelectChangedButton.disabled = !state.warehouse.length;
  elements.warehouseStatus.textContent = data.sourceError
    ? `Склад загружен, но PriceMaster сейчас недоступен: ${data.sourceError}`
    : "Склад загружен. Стоп-поставщики исключаются из выбора автоматически.";
  applyWarehouseFilters();
  renderSuppliers();
  updateSelection();
}

function renderWarehouseCards() {
  const groups = buildWarehouseGroups(state.filteredWarehouse);
  elements.warehouseCards.classList.toggle("list-view", state.warehouseViewMode === "list");
  elements.warehouseViewButtons.forEach((button) => button.classList.toggle("active", button.dataset.warehouseView === state.warehouseViewMode));
  if (!groups.length) {
    elements.warehouseCards.innerHTML = `<div class="empty">Нет товаров под выбранные фильтры.</div>`;
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
      return `
        <article class="product-card ${selected ? "selected" : ""}" data-group-key="${escapeHtml(group.key)}">
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
                    <div><span>Текущая</span><strong>${formatMoney(item.currentPrice)}</strong></div>
                    <div><span>Новая</span><strong>${formatMoney(item.nextPrice)}</strong></div>
                    <div><span>Наценка</span><strong>${Number(item.markupCoefficient || 0).toFixed(2)}</strong></div>
                  </div>
                `,
              )
              .join("")}
          </div>
          <div class="supplier-mini">
            <span>${supplier ? escapeHtml(supplier.partnerName || supplier.supplierName || "Поставщик") : "Нет доступного поставщика"}</span>
            <small>${group.availableSupplierCount}/${group.supplierCount} поставщиков</small>
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
  elements.warehouseVisibleInfo.textContent = `Показано ${formatNumber(visibleCount)} из ${formatNumber(groups.length)} объединённых карточек`;
  elements.warehouseLoadMoreButton.classList.toggle("hidden", visibleCount >= groups.length);
  updateSelection();
  enrichVisibleProducts(visibleGroups.flatMap((group) => group.variants));
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

  const product = group.primary || group;
  const variants = group.variants || [product];
  const supplier = group.selectedSupplier || product.selectedSupplier;
  const suppliers = group.suppliers || product.suppliers || [];
  const links = group.links || product.links || [];
  const url = marketplaceUrl(product);
  const ozonLink = ozonUrl(product);
  const yandexVariant = variants.find((item) => item.marketplace === "yandex");
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
      <div><span>Текущая</span><strong>${formatMoney(product.currentPrice)}</strong></div>
      ${product.marketplace === "ozon" ? `<div><span>Мин. Ozon</span><strong>${formatMoney(product.ozonMinPrice)}</strong></div>` : ""}
      <div><span>Новая</span><strong>${formatMoney(product.nextPrice)}</strong></div>
      <div><span>Наценка</span><strong>${Number(product.markupCoefficient || 0).toFixed(2)}</strong></div>
    </div>

    <section class="detail-section">
      <h3>Маркетплейсы и наценка</h3>
      <div class="marketplace-variant-list">
        ${variants
          .map(
            (item) => `
              <form class="variant-markup-row" data-product-id="${escapeHtml(item.id)}">
                <div>
                  <span class="market-badge ${item.marketplace}">${escapeHtml(marketLabel(item))}</span>
                  <span class="badge ${marketplaceStateClass(item)}">${escapeHtml(marketplaceStateLabel(item))}</span>
                  <strong>${formatMoney(item.currentPrice)} → ${formatMoney(item.nextPrice)}</strong>
                  <small>${item.marketplace === "ozon" ? `Мин. Ozon: ${formatMoney(item.ozonMinPrice)}` : "Цена ЯМ хранится отдельно"}</small>
                </div>
                <label>
                  Наценка
                  <input name="markup" type="number" min="0.01" step="0.01" value="${Number(item.markupCoefficient || item.markup || 0).toFixed(2)}" />
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
                      <span>${escapeHtml(entry.supplierName || "Поставщик не указан")}${entry.supplierArticle ? ` · ${escapeHtml(entry.supplierArticle)}` : ""}</span>
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
              <span>${escapeHtml(supplier.article)} · ${formatUsd(supplier.price)} · ${formatDate(supplier.docDate)}</span>
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
                        <span>${escapeHtml(link.supplierName || "Любой поставщик")}${link.keyword ? ` · ${escapeHtml(link.keyword)}` : ""}</span>
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
          <a class="text-link" href="/ozon-product.html?offerId=${encodeURIComponent(product.offerId)}&name=${encodeURIComponent(productName)}">Заполнить поля Ozon</a>
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
  if (!state.suppliers.length) {
    elements.supplierBoard.innerHTML = `<div class="empty">Добавьте поставщика, чтобы управлять стопом и артикулами.</div>`;
    elements.supplierStatus.textContent = "Поставщиков пока нет.";
    return;
  }

  const stoppedCount = state.suppliers.filter((supplier) => supplier.stopped).length;
  elements.supplierStatus.textContent = `Поставщиков: ${formatNumber(state.suppliers.length)}. На стопе: ${formatNumber(stoppedCount)}.`;
  elements.supplierBoard.innerHTML = state.suppliers
    .map(
      (supplier) => `
        <article class="supplier-panel ${supplier.stopped ? "stopped" : ""}" data-supplier-id="${escapeHtml(supplier.id)}">
          <div class="supplier-panel-head">
            <div>
              <h3>${escapeHtml(supplier.name)}</h3>
              <p>${supplier.note ? escapeHtml(supplier.note) : "Без заметки"}</p>
              ${supplier.stopReason ? `<small class="stop-note">Причина стопа: ${escapeHtml(supplier.stopReason)}</small>` : ""}
            </div>
            <div class="supplier-actions">
              <label class="switch-line">
                <input class="supplier-stop-toggle" type="checkbox" ${supplier.stopped ? "checked" : ""} />
                <span>${supplier.stopped ? "Стоп" : "Работает"}</span>
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
                            <span>${article.keyword || "Без ключевого слова"} · приоритет ${article.priority}</span>
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
    .join("");
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
  form.elements.priority.value = article.priority || 100;
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

function formatCurrentPrice(row) {
  if (row.currentPrice) return formatMoney(row.currentPrice);
  if (row.currentPriceStatus === "not_found") return "Нет на маркетплейсе";
  if (row.currentPriceStatus === "no_price") return "Цена не задана";
  return "Не получена";
}

function applyFilters() {
  const status = elements.statusFilter.value;
  const marketplace = elements.marketplaceFilter.value;
  state.filteredRows = state.rows.filter((row) => {
    const statusOk =
      status === "all" ||
      (status === "found" && row.currentPriceStatus !== "not_found") ||
      (status === "not_found" && row.currentPriceStatus === "not_found") ||
      (status === "changed" && row.changed);
    const marketplaceOk = marketplace === "all" || row.target === marketplace || row.marketplace === marketplace;
    return statusOk && marketplaceOk;
  });
  renderRows(state.filteredRows);
}

function renderRows(rows) {
  if (!rows.length) {
    elements.previewBody.innerHTML = `<tr><td colspan="9" class="empty">Нет строк для предпросмотра.</td></tr>`;
    updateSelection();
    return;
  }

  elements.previewBody.innerHTML = rows
    .map(
      (row, index) => `
        <tr>
          <td><input class="row-check" data-index="${index}" type="checkbox" ${row.ready ? "" : "disabled"} /></td>
          <td>${escapeHtml(row.targetName)}</td>
          <td>${escapeHtml(row.offerId)}</td>
          <td class="name-cell">${escapeHtml(row.name)}<br /><span>${escapeHtml(row.partnerName || "")}</span></td>
          <td>${formatUsd(row.usdPrice)}</td>
          <td>${Number(row.markupCoefficient || 0).toFixed(2)}</td>
          <td class="money">${formatCurrentPrice(row)}</td>
          <td class="money">${formatMoney(row.nextPrice)}</td>
          <td>${
            row.currentPriceStatus === "not_found"
              ? '<span class="badge neutral">Нет товара</span>'
              : row.currentPriceStatus === "no_price"
                ? '<span class="badge neutral">Нет цены</span>'
                : row.changed
                  ? '<span class="badge warn">Цена отличается</span>'
                  : '<span class="badge ok">Без изменений</span>'
          }</td>
        </tr>
      `,
    )
    .join("");
  updateSelection();
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

  elements.dailySyncStatus.textContent = status.enabled === false
    ? "Выключено"
    : `${statusText}, ${status.time || "11:00"}`;
  elements.dailySyncMeta.textContent = `Последний запуск: ${last}. Следующий: ${next}.${totals}${status.error ? ` Ошибка: ${status.error}` : ""}`;
  if (elements.syncLogList) {
    const logs = Array.isArray(status.logs) ? status.logs.slice(0, 5) : [];
    elements.syncLogList.innerHTML = logs.length
      ? logs.map((log) => `
          <div class="sync-log-row">
            <div>
              <strong>${escapeHtml(log.status === "ok" ? "Успешно" : log.status === "failed" ? "Ошибка" : "Запуск")}</strong>
              <span>${formatDate(log.at)} · PriceMaster: ${formatNumber(log.priceMasterItems)} / изменений ${formatNumber(log.priceMasterChanges)} · Склад: ${formatNumber(log.warehouseTotal)} / изменилось ${formatNumber(log.warehouseChanged)}</span>
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

async function loadWarehouse(sync = false, refreshPrices = false) {
  const stopProgress = sync || refreshPrices ? startSyncProgress(sync ? "sync" : "prices") : null;
  elements.warehouseSyncButton.disabled = sync;
  elements.warehouseRefreshPricesButton.disabled = refreshPrices;
  elements.warehouseStatus.textContent = sync
    ? `Синхронизирую ${syncTargetNames().join(" + ")}: товары, цены, статусы, остатки и изображения...`
    : refreshPrices
      ? `Обновляю цены по ${syncTargetNames().join(" + ")}...`
      : "Обновляю склад...";
  try {
    const params = new URLSearchParams();
    if (sync) params.set("sync", "true");
    if (refreshPrices) params.set("refreshPrices", "true");
    if (elements.warehouseUsdRateInput.value) params.set("usdRate", elements.warehouseUsdRateInput.value);
    const data = await api(`/api/warehouse${params.toString() ? `?${params}` : ""}`);
    if (data.targets?.length) {
      state.targets = data.targets;
      renderTargets();
    }
    renderWarehouse(data);
    if (stopProgress) stopProgress(true);
  } catch (error) {
    if (stopProgress) stopProgress(false);
    throw error;
  } finally {
    elements.warehouseSyncButton.disabled = false;
    elements.warehouseRefreshPricesButton.disabled = false;
  }
}

async function loadRate(force = false) {
  const savedRate = localStorage.getItem("manualUsdRate") || elements.warehouseUsdRateInput.value || elements.usdRateInput.value || "75.345";
  elements.usdRateInput.value = savedRate;
  elements.warehouseUsdRateInput.value = savedRate;
  elements.rateInfo.textContent = `Курс задаётся вручную: ${formatNumber(savedRate)} RUB/USD.`;
  elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(savedRate)} RUB/USD`;
  return { rate: Number(savedRate), source: "manual", fetchedAt: new Date().toISOString() };
}

async function loadSettings() {
  const data = await api("/api/marketplaces");
  state.targets = data.targets || [];
  state.accounts = data.accounts || [];
  state.hiddenAccounts = data.hiddenAccounts || [];
  elements.ozonMarkupInput.value = data.defaults?.ozonMarkup || 1.7;
  elements.yandexMarkupInput.value = data.defaults?.yandexMarkup || 1.6;
  renderTargets();
  renderAccounts();
  renderHiddenAccounts();
  updateAccountFormMode();
  await loadRate(false);
  await Promise.all([loadWarehouse(false), loadDailySync()]);
}

elements.tabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    elements.tabButtons.forEach((item) => item.classList.toggle("active", item === button));
    elements.tabPanels.forEach((panel) => panel.classList.toggle("active", panel.id === `${button.dataset.tab}Tab`));
  });
});

document.querySelectorAll("[data-marketplace]").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll("[data-marketplace]").forEach((item) => item.classList.toggle("active", item === button));
    state.warehouseMarketplace = button.dataset.marketplace;
    state.warehouseVisibleLimit = 80;
    applyWarehouseFilters();
  });
});

elements.warehouseSearchInput.addEventListener("input", () => {
  state.warehouseVisibleLimit = 80;
  applyWarehouseFilters();
});

elements.warehouseUsdRateInput?.addEventListener("input", () => {
  const value = elements.warehouseUsdRateInput.value;
  localStorage.setItem("manualUsdRate", value);
  if (elements.usdRateInput) elements.usdRateInput.value = value;
  elements.warehouseRateInfo.textContent = `Курс: ${formatNumber(value)} RUB/USD`;
});

elements.usdRateInput?.addEventListener("input", () => {
  const value = elements.usdRateInput.value;
  localStorage.setItem("manualUsdRate", value);
  if (elements.warehouseUsdRateInput) elements.warehouseUsdRateInput.value = value;
  elements.rateInfo.textContent = `Курс задаётся вручную: ${formatNumber(value)} RUB/USD.`;
});

elements.ozonStateFilter?.addEventListener("change", () => {
  state.ozonStateFilter = elements.ozonStateFilter.value;
  state.warehouseVisibleLimit = 80;
  if (state.ozonStateFilter !== "all" && !state.warehouse.some((product) => product.marketplaceState?.code && product.marketplaceState.code !== "unknown")) {
    elements.warehouseStatus.textContent = "Для фильтра по статусам нажмите «Синхронизировать», чтобы загрузить архив, активность и остатки Ozon + ЯМ.";
  }
  applyWarehouseFilters();
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
    await loadWarehouse(false);
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  }
});

elements.bulkMarkupForm?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const productIds = selectedWarehouseIds();
  const markup = Number(elements.bulkMarkupInput.value || 0);
  if (!productIds.length) {
    elements.warehouseStatus.textContent = "Выберите одну или несколько объединённых карточек.";
    return;
  }
  try {
    const result = await api("/api/warehouse/products/markups/bulk", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ productIds, markup }),
    });
    elements.warehouseStatus.textContent = `Наценка ${markup.toFixed(2)} применена: ${formatNumber(result.changed)} товаров Ozon/ЯМ.`;
    elements.bulkMarkupForm.reset();
    await loadWarehouse(false);
  } catch (error) {
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
    renderWarehouse(result.warehouse);
    elements.warehouseStatus.textContent = `Объединено товаров: ${formatNumber(result.changed)}.`;
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
    renderWarehouse(result.warehouse);
    elements.warehouseStatus.textContent = `Разъединено товаров: ${formatNumber(result.changed)}.`;
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
  state.warehouseVisibleLimit += 80;
  renderWarehouseCards();
});

elements.warehouseViewButtons.forEach((button) => {
  button.addEventListener("click", () => {
    state.warehouseViewMode = button.dataset.warehouseView === "list" ? "list" : "cards";
    localStorage.setItem("warehouseViewMode", state.warehouseViewMode);
    renderWarehouseCards();
  });
});

elements.dailySyncRunButton?.addEventListener("click", async () => {
  elements.dailySyncRunButton.disabled = true;
  elements.dailySyncStatus.textContent = "Запускаю обновление...";
  try {
    const status = await api("/api/daily-sync/run", { method: "POST" });
    renderDailySync(status);
    await loadWarehouse(false);
  } catch (error) {
    elements.dailySyncMeta.textContent = error.message;
  } finally {
    elements.dailySyncRunButton.disabled = false;
  }
});

elements.warehouseCards.addEventListener("click", (event) => {
  if (event.target.classList.contains("warehouse-check")) return;
  const card = event.target.closest(".product-card");
  const button = event.target.closest(".select-product");
  const groupKey = button?.dataset.groupKey || card?.dataset.groupKey;
  if (!groupKey) return;
  state.selectedWarehouseGroupKey = groupKey;
  renderWarehouseCards();
  renderWarehouseDetail(buildWarehouseGroups(state.filteredWarehouse).find((group) => group.key === groupKey));
  focusWarehouseDetailOnSmallScreen();
});

elements.warehouseCards.addEventListener("change", (event) => {
  if (event.target.classList.contains("warehouse-check")) updateSelection();
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
        body: JSON.stringify({ markup: Number(formData.get("markup") || 0) }),
      });
      elements.warehouseStatus.textContent = "Наценка для маркетплейса сохранена.";
      await loadWarehouse(false);
    } catch (error) {
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
    for (const productId of productIds) {
      await api(`/api/warehouse/products/${productId}/links`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(Object.fromEntries(data.entries())),
      });
    }
    await loadWarehouse(false);
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
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
      await loadWarehouse(false);
      return;
    }
    if (linkButton) {
      await api(`/api/warehouse/products/${linkButton.dataset.productId}/links/${linkButton.dataset.linkId}`, { method: "DELETE" });
      await loadWarehouse(false);
    }
    if (productButton && await confirmAction({ title: "Удалить товар?", text: "Удалить товар из личного склада?", okText: "Удалить" })) {
      await api(`/api/warehouse/products/${productButton.dataset.productId}`, { method: "DELETE" });
      await loadWarehouse(false);
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
      body: JSON.stringify({ confirmed: true, productIds, usdRate: Number(elements.warehouseUsdRateInput.value) }),
    });
    elements.warehouseStatus.textContent = `Готово: отправлено ${formatNumber(result.sent)} цен.`;
    await loadWarehouse(false);
  } catch (error) {
    elements.warehouseStatus.textContent = error.message;
  } finally {
    updateSelection();
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
    await loadWarehouse(false);
  } catch (error) {
    elements.supplierStatus.textContent = error.message;
  }
});

elements.supplierCancelEditButton?.addEventListener("click", () => {
  resetSupplierForm();
  elements.supplierStatus.textContent = "Редактирование поставщика отменено.";
});

elements.supplierBoard.addEventListener("change", async (event) => {
  const toggle = event.target.closest(".supplier-stop-toggle");
  if (!toggle) return;
  const panel = event.target.closest(".supplier-panel");
  const name = panel.querySelector("h3")?.textContent || "поставщика";
  const stopped = toggle.checked;
  if (stopped && !(await confirmAction({
    title: "Поставить поставщика на стоп?",
    text: `${name}: все его артикулы будут считаться недоступными и не попадут в выбор цены.`,
    okText: "Поставить на стоп",
  }))) {
    toggle.checked = false;
    return;
  }

  try {
    await api(`/api/suppliers/${panel.dataset.supplierId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stopped }),
    });
    await loadWarehouse(false);
  } catch (error) {
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
    await loadWarehouse(false);
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
      await loadWarehouse(false);
    }
    if (deleteArticle && await confirmAction({ title: "Удалить артикул?", text: "Артикул поставщика будет удалён из локального списка.", okText: "Удалить" })) {
      await api(`/api/suppliers/${panel.dataset.supplierId}/articles/${deleteArticle.dataset.articleId}`, { method: "DELETE" });
      await loadWarehouse(false);
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

elements.previewForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const targets = Array.from(document.querySelectorAll('input[name="target"]:checked')).map((input) => input.value);
  if (!targets.length) {
    elements.statusText.textContent = "Выберите хотя бы один маркетплейс.";
    return;
  }

  elements.statusText.textContent = "Готовлю предпросмотр. В маркетплейсы ничего не отправляется.";
  elements.selectChangedButton.disabled = true;
  elements.sendButton.disabled = true;

  try {
    const preview = await api("/api/marketplaces/prices/preview", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        usdRate: Number(elements.usdRateInput.value),
        ozonMarkup: Number(elements.ozonMarkupInput.value),
        yandexMarkup: Number(elements.yandexMarkupInput.value),
        supplierMarkups: elements.supplierMarkupInput.value,
        limit: Number(elements.limitInput.value),
        search: elements.searchInput.value.trim(),
        targets,
      }),
    });

    state.rows = preview.rows || [];
    elements.sourceCount.textContent = formatNumber(preview.sourceItems);
    elements.rowCount.textContent = formatNumber(state.rows.length);
    elements.changedCount.textContent = formatNumber(preview.changed);
    elements.statusText.textContent = `Предпросмотр готов. Курс: ${preview.usdRate}. Ozon: ${preview.targetMarkups.ozon}. Яндекс: ${preview.targetMarkups.yandex}. Не найдено: ${formatNumber(preview.notFound)}. Без цены: ${formatNumber(preview.noPrice)}.`;
    elements.selectChangedButton.disabled = state.rows.length === 0;
    applyFilters();
  } catch (error) {
    elements.statusText.textContent = error.message;
    state.rows = [];
    applyFilters();
  }
});

elements.previewBody.addEventListener("change", (event) => {
  if (event.target.classList.contains("row-check")) updateSelection();
});

elements.statusFilter.addEventListener("change", applyFilters);
elements.marketplaceFilter.addEventListener("change", applyFilters);

elements.refreshRateButton.addEventListener("click", async () => {
  try {
    await loadRate(true);
    elements.rateInfo.textContent = "Автозагрузка курса отключена. Введите курс вручную.";
  } catch (error) {
    elements.rateInfo.textContent = error.message;
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

elements.selectChangedButton.addEventListener("click", () => {
  document.querySelectorAll(".row-check").forEach((input) => {
    const row = state.filteredRows[Number(input.dataset.index)];
    input.checked = Boolean(row?.ready && row.changed);
  });
  updateSelection();
});

elements.sendButton.addEventListener("click", async () => {
  const items = selectedRows();
  if (!items.length) return;
  if (!(await confirmAction({ title: "Отправить цены?", text: `Отправить ${items.length} выбранных цен на маркетплейсы?`, okText: "Отправить", danger: false }))) return;

  elements.sendButton.disabled = true;
  elements.statusText.textContent = "Отправляю выбранные цены...";
  try {
    const result = await api("/api/marketplaces/prices/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true, items }),
    });
    elements.statusText.textContent = `Отправка завершена: ${formatNumber(result.sent)} строк.`;
  } catch (error) {
    elements.statusText.textContent = error.message;
  } finally {
    updateSelection();
  }
});

elements.logoutButton.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

loadSettings().catch((error) => {
  elements.statusText.textContent = error.message;
  elements.warehouseStatus.textContent = error.message;
  elements.supplierStatus.textContent = error.message;
});
