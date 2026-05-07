const form = document.querySelector("#ozonProductForm");
const statusBox = document.querySelector("#ozonProductStatus");
const logoutButton = document.querySelector("#logoutButton");
const saveWarehouseDraftButton = document.querySelector("#saveWarehouseDraftButton");
const targetInput = document.querySelector("#ozonTargetInput");
const imageUploadInput = document.querySelector("#ozonImageUpload");
const imageUploadButton = document.querySelector("#ozonImageUploadButton");
const imageUploadStatus = document.querySelector("#ozonImageUploadStatus");
const vendorInput = document.querySelector("#ozonVendorInput");
const vendorSuggestions = document.querySelector("#ozonVendorSuggestions");
const categorySearchInput = document.querySelector("#ozonCategorySearchInput");
const categorySuggestions = document.querySelector("#ozonCategorySuggestions");
const fillAttributesButton = document.querySelector("#ozonFillAttributesButton");
const builderRoot = document.querySelector("[data-product-builder]");
const formTitle = document.querySelector("#ozonFormTitle");
const formLede = document.querySelector("#ozonFormLede");
const submitOzonButton = document.querySelector("#submitOzonButton");
const yandexSection = document.querySelector("#pb-ozon-yandex");
const crosspostBlock = document.querySelector("#ozonCrosspostBlock");
const autoYandexToggle = document.querySelector("#ozonAutoYandexToggle");
const qualityLabel = document.querySelector("#ozonQualityLabel");
const readyStatus = document.querySelector("#ozonReadyStatus");
const readyMeta = document.querySelector("#ozonReadyMeta");
const scrollToMissingFieldButton = document.querySelector("#scrollToMissingFieldButton");
const requiredChecklist = document.querySelector("#ozonRequiredChecklist");

let editingProductId = "";
let vendorSuggestAbort = null;
let vendorActiveIndex = -1;
let categorySuggestAbort = null;
let categoryActiveIndex = -1;
const brandSuggestCache = new Map();
const BRAND_CACHE_TTL_MS = 7 * 60 * 1000;
const REQUIRED_OZON_FIELDS = ["offerId", "name", "description", "categoryId", "price", "width", "height", "depth", "weight"];
const REQUIRED_OZON_FIELD_LABELS = {
  offerId: "Артикул продавца",
  name: "Название",
  description: "Описание",
  categoryId: "Категория Ozon ID",
  price: "Цена",
  width: "Ширина",
  height: "Высота",
  depth: "Глубина",
  weight: "Вес",
};
let preferredYandexTargetId = "";

function isRequiredFieldFilled(name) {
  const input = form.elements[name];
  const raw = String(input?.value || "").trim();
  if (!raw) return false;
  if (["categoryId", "price", "width", "height", "depth", "weight"].includes(name)) return Number(raw) > 0;
  return true;
}

function updateReadinessSidebar() {
  const done = REQUIRED_OZON_FIELDS.filter(isRequiredFieldFilled);
  const missing = REQUIRED_OZON_FIELDS.filter((name) => !done.includes(name));
  if (requiredChecklist) {
    requiredChecklist.querySelectorAll("li[data-field]").forEach((item) => {
      const name = item.dataset.field;
      const ok = done.includes(name);
      item.classList.toggle("is-ready", ok);
      item.classList.toggle("is-missing", !ok);
      const badge = item.querySelector("b");
      if (badge) badge.textContent = ok ? "OK" : "Нужно";
    });
  }
  if (readyStatus) readyStatus.textContent = missing.length ? "Не хватает полей" : "Готово к отправке";
  if (readyMeta) readyMeta.textContent = `Заполнено ${done.length}/${REQUIRED_OZON_FIELDS.length} обязательных полей.`;
  if (qualityLabel) qualityLabel.textContent = missing.length ? "Средний" : "Высокий";
  if (scrollToMissingFieldButton) scrollToMissingFieldButton.disabled = missing.length === 0;
}

function scrollToFirstMissingField() {
  for (const name of REQUIRED_OZON_FIELDS) {
    if (isRequiredFieldFilled(name)) continue;
    const input = form.elements[name];
    if (!input) continue;
    input.classList.add("is-invalid");
    input.scrollIntoView({ behavior: "smooth", block: "center" });
    window.setTimeout(() => input.focus(), 120);
    statusBox.textContent = `Заполните поле: ${REQUIRED_OZON_FIELD_LABELS[name] || name}.`;
    return;
  }
  statusBox.textContent = "Все обязательные поля заполнены.";
}

function vendorOptions() {
  return vendorSuggestions
    ? Array.from(vendorSuggestions.querySelectorAll(".pm-suggest-option"))
    : [];
}

function setVendorActiveIndex(index) {
  const options = vendorOptions();
  if (!options.length) {
    vendorActiveIndex = -1;
    return;
  }
  vendorActiveIndex = Math.max(0, Math.min(index, options.length - 1));
  options.forEach((option, i) => {
    const active = i === vendorActiveIndex;
    option.classList.toggle("pm-suggest-option--active", active);
    option.setAttribute("aria-selected", active ? "true" : "false");
    if (active) option.scrollIntoView({ block: "nearest" });
  });
}

function chooseVendorOption(option) {
  if (!option || !vendorInput) return;
  vendorInput.value = option.dataset.brand || option.textContent.trim();
  vendorSuggestions.hidden = true;
  vendorSuggestions.innerHTML = "";
  vendorActiveIndex = -1;
}

function categoryOptions() {
  return categorySuggestions
    ? Array.from(categorySuggestions.querySelectorAll(".pm-suggest-option"))
    : [];
}

function setCategoryActiveIndex(index) {
  const options = categoryOptions();
  if (!options.length) {
    categoryActiveIndex = -1;
    return;
  }
  categoryActiveIndex = Math.max(0, Math.min(index, options.length - 1));
  options.forEach((option, i) => {
    const active = i === categoryActiveIndex;
    option.classList.toggle("pm-suggest-option--active", active);
    option.setAttribute("aria-selected", active ? "true" : "false");
    if (active) option.scrollIntoView({ block: "nearest" });
  });
}

function chooseCategoryOption(option) {
  if (!option) return;
  const id = option.dataset.categoryId || "";
  const name = option.dataset.categoryName || option.textContent.trim();
  if (form.elements.categoryId) form.elements.categoryId.value = id;
  if (categorySearchInput) categorySearchInput.value = name;
  categorySuggestions.hidden = true;
  categorySuggestions.innerHTML = "";
  categoryActiveIndex = -1;
}

function queryValue(name) {
  return new URLSearchParams(window.location.search).get(name) || "";
}

function setInitialValues() {
  editingProductId = queryValue("productId");
  const offerId = queryValue("offerId");
  const name = queryValue("name");
  if (form.elements.vat) form.elements.vat.value = "0.05";
  if (offerId) form.elements.offerId.value = offerId;
  if (name) {
    form.elements.name.value = name;
    form.elements.description.value = name;
  }
}

function applyOzonDraft(product = {}) {
  const ozon = product.ozon || {};
  const mappings = [
    ["offerId", product.offerId || ozon.offerId || ""],
    ["name", product.name || ozon.name || ""],
    ["description", ozon.description || product.name || ""],
    ["categoryId", ozon.categoryId || ozon.category_id || ""],
    ["price", ozon.price || ""],
    ["oldPrice", ozon.oldPrice || ozon.old_price || ""],
    ["vat", ozon.vat || "0"],
    ["currencyCode", ozon.currencyCode || ozon.currency_code || "RUB"],
    ["vendor", ozon.vendor || product.yandex?.vendor || ""],
    ["width", ozon.width || ""],
    ["height", ozon.height || ""],
    ["depth", ozon.depth || ""],
    ["dimensionUnit", ozon.dimensionUnit || ozon.dimension_unit || "mm"],
    ["weight", ozon.weight || ""],
    ["weightUnit", ozon.weightUnit || ozon.weight_unit || "g"],
    ["barcode", ozon.barcode || ""],
    ["barcodes", Array.isArray(ozon.barcodes) ? ozon.barcodes.join("\n") : (ozon.barcodes || "")],
    ["primaryImage", ozon.primaryImage || ozon.primary_image || ""],
    ["images", Array.isArray(ozon.images) ? ozon.images.join("\n") : (ozon.images || "")],
    ["images360", Array.isArray(ozon.images360) ? ozon.images360.join("\n") : (ozon.images360 || "")],
    ["colorImage", ozon.colorImage || ozon.color_image || ""],
    ["attributesJson", JSON.stringify(ozon.attributes || [], null, 2)],
    ["complexAttributesJson", JSON.stringify(ozon.complexAttributes || ozon.complex_attributes || [], null, 2)],
    ["extraJson", JSON.stringify(ozon.extra || {}, null, 2)],
    ["marketCategoryId", product.yandex?.marketCategoryId || ozon.marketCategoryId || ozon.categoryId || ""],
    ["yandexPictures", Array.isArray(product.yandex?.pictures) ? product.yandex.pictures.join("\n") : (product.yandex?.pictures || "")],
    ["yandexExtraJson", JSON.stringify(product.yandex?.extra || {}, null, 2)],
  ];
  for (const [name, value] of mappings) {
    if (form.elements[name] && value !== undefined && value !== null) form.elements[name].value = value;
  }
}

async function loadExistingProduct() {
  if (!editingProductId) return;
  builderRoot?.classList.add("is-editing-ozon");
  const data = await api(`/api/warehouse/products/${encodeURIComponent(editingProductId)}`);
  applyOzonDraft(data.product || {});
  if (formTitle) formTitle.textContent = "Редактирование карточки Ozon";
  if (formLede) formLede.textContent = "Режим редактирования Ozon: изменяете только Ozon-карточку и отправляете обновление в Ozon.";
  if (submitOzonButton) submitOzonButton.textContent = "Обновить на Ozon";
  if (yandexSection) yandexSection.hidden = true;
  if (crosspostBlock) crosspostBlock.hidden = true;
  statusBox.textContent = "Режим редактирования: изменения этой карточки можно отправить в Ozon кнопкой «Обновить на Ozon».";
  updateReadinessSidebar();
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

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function loadTargets() {
  const data = await api("/api/marketplaces");
  const targets = (data.targets || []).filter((target) => target.marketplace === "ozon" && target.configured !== false);
  preferredYandexTargetId = ((data.targets || []).find((target) => target.marketplace === "yandex" && target.configured !== false)?.id || "");
  const preferredTarget = queryValue("target");

  if (!targets.length) {
    targetInput.innerHTML = `<option value="">Ozon не настроен</option>`;
    targetInput.disabled = true;
    statusBox.textContent = "Добавьте Ozon Client-Id и Api-Key в разделе кабинетов.";
    return;
  }

  targetInput.innerHTML = targets
    .map(
      (target) => `<option value="${escapeHtml(target.id)}" ${target.id === preferredTarget ? "selected" : ""}>${escapeHtml(target.name || "Ozon")}</option>`,
    )
    .join("");
}

function findSavedProduct(warehouse, data) {
  return (warehouse?.products || []).find((product) => (
    product.id === editingProductId
    || (product.target === data.target && product.offerId === data.offerId)
  ));
}

function appendLines(textarea, urls) {
  const current = textarea.value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  textarea.value = [...new Set([...current, ...urls])].join("\n");
}

async function uploadImages(files) {
  const formData = new FormData();
  Array.from(files).forEach((file) => formData.append("images", file));
  const result = await api("/api/uploads/images", {
    method: "POST",
    body: formData,
  });
  return result.files || [];
}

async function importProductImages() {
  const files = imageUploadInput.files;
  if (!files.length) {
    imageUploadStatus.textContent = "Выберите одно или несколько изображений.";
    return;
  }

  imageUploadButton.disabled = true;
  imageUploadStatus.textContent = "Загружаю изображения...";
  try {
    const uploaded = await uploadImages(files);
    const urls = uploaded.map((file) => file.url).filter(Boolean);
    if (!form.elements.primaryImage.value && urls[0]) form.elements.primaryImage.value = urls[0];
    appendLines(form.elements.images, urls);
    imageUploadStatus.textContent = `Импортировано изображений: ${urls.length}.`;
    imageUploadInput.value = "";
  } catch (error) {
    imageUploadStatus.textContent = error.message;
  } finally {
    imageUploadButton.disabled = false;
  }
}

function collectFormData() {
  const data = Object.fromEntries(new FormData(form).entries());
  data.vat = "0.05";
  if (editingProductId) data.id = editingProductId;
  try {
    JSON.parse(data.attributesJson || "[]");
  } catch (_error) {
    throw new Error("Поле attributes JSON содержит невалидный JSON.");
  }
  try {
    JSON.parse(data.complexAttributesJson || "[]");
  } catch (_error) {
    throw new Error("Поле complex_attributes JSON содержит невалидный JSON.");
  }
  try {
    JSON.parse(data.extraJson || "{}");
  } catch (_error) {
    throw new Error("Поле Дополнительные поля Ozon JSON содержит невалидный JSON.");
  }
  try {
    JSON.parse(data.yandexExtraJson || "{}");
  } catch (_error) {
    throw new Error("Поле Дополнительные поля ЯМ JSON содержит невалидный JSON.");
  }
  return data;
}

function highlightInvalidField(input, message) {
  if (!input) return false;
  input.classList.add("is-invalid");
  if (message) input.setAttribute("title", message);
  input.scrollIntoView({ behavior: "smooth", block: "center" });
  input.focus();
  return false;
}

function clearValidationMarks() {
  form.querySelectorAll(".is-invalid").forEach((el) => el.classList.remove("is-invalid"));
}

function validateRequiredFields() {
  clearValidationMarks();
  for (const name of REQUIRED_OZON_FIELDS) {
    const input = form.elements[name];
    const value = String(input?.value || "").trim();
    if (!value) return highlightInvalidField(input, "Заполните обязательное поле");
  }
  if (Number(form.elements.categoryId.value) <= 0) return highlightInvalidField(form.elements.categoryId, "Категория должна быть больше нуля");
  if (Number(form.elements.price.value) <= 0) return highlightInvalidField(form.elements.price, "Цена должна быть больше нуля");
  if (Number(form.elements.width.value) <= 0) return highlightInvalidField(form.elements.width, "Ширина должна быть больше нуля");
  if (Number(form.elements.height.value) <= 0) return highlightInvalidField(form.elements.height, "Высота должна быть больше нуля");
  if (Number(form.elements.depth.value) <= 0) return highlightInvalidField(form.elements.depth, "Глубина должна быть больше нуля");
  if (Number(form.elements.weight.value) <= 0) return highlightInvalidField(form.elements.weight, "Вес должен быть больше нуля");
  return true;
}

function buildWarehousePayload(data) {
  return {
    target: data.target,
    offerId: data.offerId,
    name: data.name,
    ozon: data,
    yandex: {
      offerId: data.offerId,
      name: data.name,
      description: data.description,
      marketCategoryId: data.marketCategoryId || data.categoryId,
      vendor: data.vendor,
      pictures: data.yandexPictures || data.images || data.primaryImage,
      barcodes: data.barcodes || data.barcode,
      price: data.price,
      extra: JSON.parse(data.yandexExtraJson || "{}"),
    },
  };
}

async function saveWarehouseDraft(data) {
  const response = await api("/api/warehouse/products", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(editingProductId ? { ...buildWarehousePayload(data), id: editingProductId } : buildWarehousePayload(data)),
  });
  return { response, product: findSavedProduct(response.warehouse, data) };
}

async function suggestVendors() {
  if (!vendorInput || !vendorSuggestions) return;
  const q = String(vendorInput.value || "").trim();
  const categoryId = String(form.elements.categoryId?.value || "").trim();
  const target = String(form.elements.target?.value || "").trim();
  vendorSuggestions.innerHTML = "";
  vendorSuggestions.hidden = true;
  if (q.length < 2 || !categoryId || !target) return;
  const cacheKey = `${target}|${categoryId}|${q.toLowerCase()}`;
  const cached = brandSuggestCache.get(cacheKey);
  if (cached && Date.now() - cached.at < BRAND_CACHE_TTL_MS) {
    renderVendorSuggestions(cached.brands, q, cached.source || "ozon");
    return;
  }
  if (vendorSuggestAbort) vendorSuggestAbort.abort();
  vendorSuggestAbort = new AbortController();
  try {
    const response = await fetch(
      `/api/ozon/brands/suggest?q=${encodeURIComponent(q)}&categoryId=${encodeURIComponent(categoryId)}&target=${encodeURIComponent(target)}`,
      { signal: vendorSuggestAbort.signal },
    );
    const payload = await response.json().catch(() => ({ brands: [] }));
    const brands = Array.isArray(payload.brands) ? payload.brands.slice(0, 40) : [];
    const source = payload.source === "fallback" ? "fallback" : "ozon";
    brandSuggestCache.set(cacheKey, { at: Date.now(), brands, source });
    renderVendorSuggestions(brands, q, source);
  } catch (error) {
    if (error.name !== "AbortError") {
      vendorSuggestions.innerHTML = "";
      vendorSuggestions.hidden = true;
      vendorActiveIndex = -1;
    }
  }
}

function renderVendorSuggestions(brands, q, source = "ozon") {
  if (!brands.length) {
    vendorSuggestions.innerHTML = `<div class="pm-suggest-empty">Ничего не найдено для «${escapeHtml(q)}».</div>`;
    vendorSuggestions.hidden = false;
    vendorActiveIndex = -1;
    return;
  }
  vendorSuggestions.innerHTML = brands
    .map(
      (brand) => `
        <button class="pm-suggest-option" type="button" role="option" data-brand="${escapeHtml(brand)}">
          <span class="pm-suggest-title">${escapeHtml(brand)}</span>
          <span class="pm-suggest-meta">${source === "fallback" ? "Локальная подсказка" : "Бренд Ozon"}</span>
        </button>
      `,
    )
    .join("");
  vendorSuggestions.hidden = false;
  setVendorActiveIndex(0);
}

async function suggestCategories() {
  if (!categorySearchInput || !categorySuggestions) return;
  const q = String(categorySearchInput.value || "").trim();
  const target = String(form.elements.target?.value || "").trim();
  categorySuggestions.innerHTML = "";
  categorySuggestions.hidden = true;
  if (q.length < 2 || !target) return;
  if (categorySuggestAbort) categorySuggestAbort.abort();
  categorySuggestAbort = new AbortController();
  try {
    const response = await fetch(`/api/ozon/categories/suggest?q=${encodeURIComponent(q)}&target=${encodeURIComponent(target)}`, {
      signal: categorySuggestAbort.signal,
    });
    const payload = await response.json().catch(() => ({ categories: [] }));
    const categories = Array.isArray(payload.categories) ? payload.categories : [];
    if (!categories.length) {
      categorySuggestions.innerHTML = `<div class="pm-suggest-empty">Категория не найдена.</div>`;
      categorySuggestions.hidden = false;
      categoryActiveIndex = -1;
      return;
    }
    categorySuggestions.innerHTML = categories
      .slice(0, 40)
      .map(
        (item) => `
          <button class="pm-suggest-option" type="button" role="option" data-category-id="${escapeHtml(item.id)}" data-category-name="${escapeHtml(item.name)}">
            <span class="pm-suggest-title">${escapeHtml(item.name)}</span>
            <span class="pm-suggest-meta">ID ${escapeHtml(item.id)}</span>
          </button>
        `,
      )
      .join("");
    categorySuggestions.hidden = false;
    setCategoryActiveIndex(0);
  } catch (error) {
    if (error.name !== "AbortError") {
      categorySuggestions.innerHTML = "";
      categorySuggestions.hidden = true;
    }
  }
}

async function fillRequiredAttributesTemplate() {
  const categoryId = String(form.elements.categoryId?.value || "").trim();
  const target = String(form.elements.target?.value || "").trim();
  if (!categoryId || !target) {
    statusBox.textContent = "Сначала выберите кабинет и категорию Ozon.";
    return;
  }
  fillAttributesButton.disabled = true;
  statusBox.textContent = "Загружаю required-атрибуты для категории...";
  try {
    const response = await api(`/api/ozon/categories/${encodeURIComponent(categoryId)}/attributes-template?target=${encodeURIComponent(target)}`);
    const template = Array.isArray(response.template) ? response.template : [];
    form.elements.attributesJson.value = JSON.stringify(template, null, 2);
    statusBox.textContent = template.length
      ? `Подтянуто required-атрибутов: ${template.length}.`
      : "Для категории не найден required-шаблон (можно заполнить вручную).";
  } catch (error) {
    statusBox.textContent = error.message;
  } finally {
    fillAttributesButton.disabled = false;
  }
}

saveWarehouseDraftButton.addEventListener("click", async () => {
  try {
    const data = collectFormData();
    statusBox.textContent = "Сохраняю товар в личный склад...";
    await saveWarehouseDraft(data);
    statusBox.textContent = "Товар сохранен в личный склад.";
  } catch (error) {
    statusBox.textContent = `Проверьте форму: ${error.message}`;
  }
});

imageUploadButton.addEventListener("click", importProductImages);
imageUploadInput.addEventListener("change", () => {
  const count = imageUploadInput.files.length;
  imageUploadStatus.textContent = count ? `Выбрано файлов: ${count}.` : "Файлы ещё не выбраны.";
});
vendorInput?.addEventListener("input", () => {
  window.clearTimeout(vendorInput._suggestTimer);
  vendorInput._suggestTimer = window.setTimeout(suggestVendors, 250);
});
categorySearchInput?.addEventListener("input", () => {
  window.clearTimeout(categorySearchInput._suggestTimer);
  categorySearchInput._suggestTimer = window.setTimeout(suggestCategories, 280);
});
form.elements.categoryId?.addEventListener("input", suggestVendors);
targetInput?.addEventListener("change", suggestVendors);
targetInput?.addEventListener("change", suggestCategories);
fillAttributesButton?.addEventListener("click", fillRequiredAttributesTemplate);
scrollToMissingFieldButton?.addEventListener("click", scrollToFirstMissingField);
form.addEventListener("input", updateReadinessSidebar);
form.addEventListener("change", updateReadinessSidebar);
vendorSuggestions?.addEventListener("mousedown", (event) => {
  if (event.target.closest(".pm-suggest-option")) event.preventDefault();
});
vendorSuggestions?.addEventListener("click", (event) => {
  const option = event.target.closest(".pm-suggest-option");
  if (!option) return;
  chooseVendorOption(option);
});
categorySuggestions?.addEventListener("mousedown", (event) => {
  if (event.target.closest(".pm-suggest-option")) event.preventDefault();
});
categorySuggestions?.addEventListener("click", (event) => {
  const option = event.target.closest(".pm-suggest-option");
  if (!option) return;
  chooseCategoryOption(option);
  suggestVendors();
});
vendorInput?.addEventListener("blur", () => {
  window.setTimeout(() => {
    if (document.activeElement && vendorSuggestions.contains(document.activeElement)) return;
    vendorSuggestions.hidden = true;
  }, 120);
});
vendorInput?.addEventListener("focus", () => {
  if (vendorSuggestions.innerHTML.trim()) vendorSuggestions.hidden = false;
});
categorySearchInput?.addEventListener("focus", () => {
  if (categorySuggestions.innerHTML.trim()) categorySuggestions.hidden = false;
});
vendorInput?.addEventListener("keydown", (event) => {
  if (vendorSuggestions.hidden) return;
  const options = vendorOptions();
  if (!options.length) return;
  if (event.key === "ArrowDown") {
    event.preventDefault();
    setVendorActiveIndex(vendorActiveIndex + 1);
    return;
  }
  if (event.key === "ArrowUp") {
    event.preventDefault();
    setVendorActiveIndex(vendorActiveIndex - 1);
    return;
  }
  if (event.key === "Enter") {
    const option = options[vendorActiveIndex] || options[0];
    if (option) {
      event.preventDefault();
      chooseVendorOption(option);
    }
  }
});
categorySearchInput?.addEventListener("keydown", (event) => {
  if (categorySuggestions.hidden) return;
  const options = categoryOptions();
  if (!options.length) return;
  if (event.key === "ArrowDown") {
    event.preventDefault();
    setCategoryActiveIndex(categoryActiveIndex + 1);
    return;
  }
  if (event.key === "ArrowUp") {
    event.preventDefault();
    setCategoryActiveIndex(categoryActiveIndex - 1);
    return;
  }
  if (event.key === "Enter") {
    const option = options[categoryActiveIndex] || options[0];
    if (option) {
      event.preventDefault();
      chooseCategoryOption(option);
      suggestVendors();
    }
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    if (vendorSuggestions) vendorSuggestions.hidden = true;
    vendorActiveIndex = -1;
    if (categorySuggestions) categorySuggestions.hidden = true;
    categoryActiveIndex = -1;
  }
});
document.addEventListener("click", (event) => {
  const wrap = vendorInput?.closest(".pm-autocomplete-wrap");
  if (!wrap) return;
  if (wrap.contains(event.target)) return;
  vendorSuggestions.hidden = true;
  vendorActiveIndex = -1;
});
document.addEventListener("click", (event) => {
  const wrap = categorySearchInput?.closest(".pm-autocomplete-wrap");
  if (!wrap) return;
  if (wrap.contains(event.target)) return;
  categorySuggestions.hidden = true;
  categoryActiveIndex = -1;
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!validateRequiredFields()) {
    statusBox.textContent = "Заполните обязательные поля: категория, цена, габариты и вес.";
    return;
  }
  let data;

  try {
    data = collectFormData();
  } catch (error) {
    statusBox.textContent = `Проверьте JSON: ${error.message}`;
    return;
  }

  if (!window.confirm(`${editingProductId ? "Обновить" : "Создать"} карточку товара на Ozon? Это отправит данные в Ozon Seller API.`)) return;

  statusBox.textContent = "Отправляю товар в Ozon...";
  try {
    const result = await api("/api/ozon/products/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...data, target: data.target, confirmed: true }),
    });
    const saved = await saveWarehouseDraft(data).catch(() => null);
    let yandexNote = "";
    const shouldAutoSendYandex = !editingProductId && !!autoYandexToggle?.checked;
    if (shouldAutoSendYandex && preferredYandexTargetId && saved?.product) {
      try {
        const yandexResult = await api(`/api/warehouse/products/${encodeURIComponent(saved.product.id)}/export`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ confirmed: true, target: preferredYandexTargetId }),
        });
        yandexNote = ` Яндекс: отправлено ${yandexResult.sent || 1}.`;
      } catch (error) {
        yandexNote = ` Яндекс: не отправлено (${error.message}).`;
      }
    } else if (shouldAutoSendYandex && !preferredYandexTargetId) {
      yandexNote = " Яндекс: нет настроенного кабинета.";
    } else if (!editingProductId) {
      yandexNote = " Яндекс: авто-отправка выключена.";
    }
    statusBox.textContent = `${editingProductId ? "Изменения отправлены" : "Товар отправлен"} в Ozon. Ответ: ${result.ok ? "успешно" : "проверьте кабинет"}.${yandexNote} Карточка сохранена в личный склад.`;
  } catch (error) {
    statusBox.textContent = error.message;
  }
});

logoutButton.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

setInitialValues();
Promise.all([loadTargets(), loadExistingProduct()])
  .catch((error) => {
    statusBox.textContent = error.message;
  });
updateReadinessSidebar();
