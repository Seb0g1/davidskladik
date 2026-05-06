const form = document.querySelector("#ozonProductForm");
const statusBox = document.querySelector("#ozonProductStatus");
const logoutButton = document.querySelector("#logoutButton");
const saveWarehouseDraftButton = document.querySelector("#saveWarehouseDraftButton");
const targetInput = document.querySelector("#ozonTargetInput");
const imageUploadInput = document.querySelector("#ozonImageUpload");
const imageUploadButton = document.querySelector("#ozonImageUploadButton");
const imageUploadStatus = document.querySelector("#ozonImageUploadStatus");

function queryValue(name) {
  return new URLSearchParams(window.location.search).get(name) || "";
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
  JSON.parse(data.attributesJson || "[]");
  JSON.parse(data.complexAttributesJson || "[]");
  JSON.parse(data.extraJson || "{}");
  JSON.parse(data.yandexExtraJson || "{}");
  return data;
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
  return api("/api/warehouse/products", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(buildWarehousePayload(data)),
  });
}

saveWarehouseDraftButton.addEventListener("click", async () => {
  try {
    const data = collectFormData();
    statusBox.textContent = "Сохраняю товар в личный склад...";
    await saveWarehouseDraft(data);
    statusBox.textContent = "Товар сохранен в личный склад. Теперь его можно выгрузить отдельно в Ozon или Yandex Market.";
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

  if (!window.confirm("Создать карточку товара на Ozon? Это отправит данные в Ozon Seller API.")) return;

  statusBox.textContent = "Отправляю товар в Ozon...";
  try {
    const result = await api("/api/ozon/products/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...data, target: data.target, confirmed: true }),
    });
    await saveWarehouseDraft(data).catch(() => null);
    statusBox.textContent = `Товар отправлен в Ozon. Ответ получен: ${result.ok ? "успешно" : "проверьте кабинет"}. Draft сохранен в личный склад.`;
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
