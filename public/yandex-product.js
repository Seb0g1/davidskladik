const form = document.querySelector("#yandexProductForm");
const statusBox = document.querySelector("#yandexProductStatus");
const logoutButton = document.querySelector("#logoutButton");
const saveYandexDraftButton = document.querySelector("#saveYandexDraftButton");
const targetInput = document.querySelector("#yandexTargetInput");
const imageUploadInput = document.querySelector("#yandexImageUpload");
const imageUploadButton = document.querySelector("#yandexImageUploadButton");
const imageUploadStatus = document.querySelector("#yandexImageUploadStatus");

function queryValue(name) {
  return new URLSearchParams(window.location.search).get(name) || "";
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
    appendLines(form.elements.pictures, urls);
    imageUploadStatus.textContent = `Импортировано изображений: ${urls.length}.`;
    imageUploadInput.value = "";
  } catch (error) {
    imageUploadStatus.textContent = error.message;
  } finally {
    imageUploadButton.disabled = false;
  }
}

async function loadTargets() {
  const data = await api("/api/marketplaces");
  const targets = (data.targets || []).filter((target) => target.marketplace === "yandex" && target.configured !== false);
  const preferredTarget = queryValue("target");

  if (!targets.length) {
    targetInput.innerHTML = `<option value="">Yandex Market не настроен</option>`;
    targetInput.disabled = true;
    statusBox.textContent = "Добавьте Yandex Business ID и Api-Key в разделе кабинетов.";
    return;
  }

  targetInput.innerHTML = targets
    .map(
      (target) => `<option value="${escapeHtml(target.id)}" ${target.id === preferredTarget ? "selected" : ""}>${escapeHtml(target.name || "Yandex Market")}</option>`,
    )
    .join("");
}

function setInitialValues() {
  const offerId = queryValue("offerId");
  const name = queryValue("name");
  if (offerId) form.elements.offerId.value = offerId;
  if (name) {
    form.elements.name.value = name;
    form.elements.description.value = name;
  }
}

function collectFormData() {
  const data = Object.fromEntries(new FormData(form).entries());
  JSON.parse(data.extraJson || "{}");
  return data;
}

function buildWarehousePayload(data) {
  return {
    target: data.target,
    offerId: data.offerId,
    name: data.name,
    yandex: {
      offerId: data.offerId,
      name: data.name,
      description: data.description,
      marketCategoryId: data.marketCategoryId,
      vendor: data.vendor,
      pictures: data.pictures,
      barcodes: data.barcodes,
      price: data.price,
      extra: JSON.parse(data.extraJson || "{}"),
    },
  };
}

function findSavedProduct(warehouse, data) {
  return (warehouse.products || []).find((product) => product.target === data.target && product.offerId === data.offerId);
}

async function saveWarehouseDraft(data) {
  const result = await api("/api/warehouse/products", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildWarehousePayload(data)),
  });
  const product = findSavedProduct(result.warehouse, data);
  if (!product) throw new Error("Товар сохранен, но не найден в ответе склада.");
  return product;
}

saveYandexDraftButton.addEventListener("click", async () => {
  try {
    const data = collectFormData();
    statusBox.textContent = "Сохраняю товар в личный склад...";
    await saveWarehouseDraft(data);
    statusBox.textContent = "Товар сохранен в личный склад. Его можно выгрузить в Яндекс из этой формы или из карточки склада.";
  } catch (error) {
    statusBox.textContent = `Проверьте форму: ${error.message}`;
  }
});

imageUploadButton.addEventListener("click", importProductImages);
imageUploadInput.addEventListener("change", () => {
  const count = imageUploadInput.files.length;
  imageUploadStatus.textContent = count ? `Выбрано файлов: ${count}.` : "Файлы ещё не выбраны.";
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  let data;

  try {
    data = collectFormData();
  } catch (error) {
    statusBox.textContent = `Проверьте JSON: ${error.message}`;
    return;
  }

  if (!window.confirm("Выгрузить карточку товара в Yandex Market? Это отправит данные в Partner API.")) return;

  statusBox.textContent = "Сохраняю draft и отправляю товар в Yandex Market...";
  try {
    const product = await saveWarehouseDraft(data);
    const result = await api(`/api/warehouse/products/${product.id}/export`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true, target: data.target }),
    });
    statusBox.textContent = `Товар выгружен в Yandex Market. Отправлено: ${result.sent || 1}.`;
  } catch (error) {
    statusBox.textContent = error.message;
  }
});

logoutButton.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

setInitialValues();
loadTargets().catch((error) => {
  statusBox.textContent = error.message;
});
