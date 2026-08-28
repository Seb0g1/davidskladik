const content = document.querySelector("#productPageContent");
let currentVariants = [];
let aiImageProductId = null;
let aiImageBusy = false;
const title = document.querySelector("#productPageTitle");
const meta = document.querySelector("#productPageMeta");
const aiElements = {
  modal: document.querySelector("#aiImageModal"),
  closeButton: document.querySelector("#aiImageCloseButton"),
  productName: document.querySelector("#aiImageProductName"),
  productMeta: document.querySelector("#aiImageProductMeta"),
  currentPreview: document.querySelector("#aiImageCurrentPreview"),
  preview: document.querySelector("#aiImagePreview"),
  sourceInput: document.querySelector("#aiImageSourceInput"),
  promptInput: document.querySelector("#aiImagePromptInput"),
  status: document.querySelector("#aiImageStatus"),
  generateButton: document.querySelector("#aiImageGenerateButton"),
  approveButton: document.querySelector("#aiImageApproveButton"),
  rejectButton: document.querySelector("#aiImageRejectButton"),
  cancelButton: document.querySelector("#aiImageCancelButton"),
};
const SITE_DOC_TITLE = "Magic Vibes - Склад";

function setDocTitle(pageTitle) {
  document.title = pageTitle ? `${pageTitle} · ${SITE_DOC_TITLE}` : SITE_DOC_TITLE;
}

function showToast(message) {
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
  el.className = "toast toast--warn";
  el.textContent = text;
  root.appendChild(el);
  setTimeout(() => el.remove(), 14000);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatMoney(value) {
  const number = Number(value || 0);
  return number > 0 ? `${new Intl.NumberFormat("ru-RU").format(Math.round(number))} ₽` : "-";
}

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
  if (item.ozonMinPrice) bits.push(`мин. ${formatMoney(item.ozonMinPrice)}`);
  return bits.length ? bits.join(" · ") : "";
}

function formatDate(value) {
  return value ? new Intl.DateTimeFormat("ru-RU", { dateStyle: "short", timeStyle: "short" }).format(new Date(value)) : "-";
}

function marketLabel(product) {
  return product.marketplace === "yandex" ? "Yandex Market" : "Ozon";
}

function productImage(product) {
  return product.imageUrl || product.ozon?.primaryImage || product.ozon?.images?.[0] || product.yandex?.pictures?.[0] || "";
}

function groupKey(product) {
  if (product.manualGroupId) return `manual:${product.manualGroupId}`;
  return product.offerId ? `offer:${String(product.offerId).toLowerCase()}` : `name:${String(product.name || "").toLowerCase()}`;
}

async function api(url, options = {}) {
  const response = await fetch(url, options);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || response.statusText);
  return data;
}

function firstValueFromImageList(value) {
  if (Array.isArray(value)) return value.map((item) => String(item || "").trim()).find(Boolean) || "";
  return String(value || "")
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .find(Boolean) || "";
}

function latestAiImageDraft(product) {
  const drafts = Array.isArray(product?.aiImages) ? product.aiImages : [];
  return drafts[drafts.length - 1] || null;
}

function aiImageStatusLabel(status) {
  if (status === "approved") return "принято";
  if (status === "rejected") return "отменено";
  return "ждет проверки";
}

function aiImageSourceForProduct(product) {
  const ozon = product?.ozon || {};
  return firstValueFromImageList(ozon.primaryImage)
    || firstValueFromImageList(ozon.images)
    || firstValueFromImageList(product?.imageUrl)
    || productImage(product)
    || "";
}

function defaultAiImagePrompt(product) {
  const name = product?.name || product?.offerId || "товар";
  return [
    `Сделай продающее фото для карточки Ozon товара «${name}».`,
    "Сохрани реальный товар с исходного изображения, улучши свет, фон и композицию.",
    "Фон чистый, аккуратный, маркетплейсный; без лишнего текста, логотипов, водяных знаков и недостоверных характеристик.",
    "Товар должен выглядеть натурально, премиально и подходить для главного фото.",
  ].join(" ");
}

function selectedAiImageProduct() {
  return currentVariants.find((item) => String(item.id) === String(aiImageProductId)) || null;
}

function mergeCurrentProduct(product) {
  if (!product?.id) return;
  const index = currentVariants.findIndex((item) => item.id === product.id);
  if (index >= 0) currentVariants[index] = product;
  else currentVariants.push(product);
}

function setAiImageBusy(isBusy, text = "") {
  aiImageBusy = Boolean(isBusy);
  [aiElements.generateButton, aiElements.approveButton, aiElements.rejectButton, aiElements.cancelButton, aiElements.closeButton].forEach((button) => {
    if (button) button.disabled = aiImageBusy;
  });
  if (aiElements.status && text) aiElements.status.textContent = text;
}

function renderAiImageModal(product = selectedAiImageProduct()) {
  if (!aiElements.modal || !product) return;
  const draft = latestAiImageDraft(product);
  const sourceImageUrl = aiElements.sourceInput?.value || aiImageSourceForProduct(product);
  const currentImage = draft?.resultUrl || sourceImageUrl || productImage(product);
  const canReview = draft?.status === "pending" && !aiImageBusy;
  if (aiElements.productName) aiElements.productName.textContent = product.name || product.offerId || "Товар";
  if (aiElements.productMeta) aiElements.productMeta.textContent = `${marketLabel(product)} · ${product.offerId || product.productId || product.id || ""}`;
  if (aiElements.sourceInput && !aiElements.sourceInput.value) aiElements.sourceInput.value = sourceImageUrl || "";
  if (aiElements.promptInput && !aiElements.promptInput.value) aiElements.promptInput.value = defaultAiImagePrompt(product);
  if (aiElements.currentPreview) {
    aiElements.currentPreview.innerHTML = sourceImageUrl
      ? `<img src="${escapeHtml(sourceImageUrl)}" alt="Исходное фото" loading="lazy" />`
      : `<div class="product-image-empty">Добавьте URL исходного фото</div>`;
  }
  if (aiElements.preview) {
    aiElements.preview.innerHTML = currentImage
      ? `<img src="${escapeHtml(currentImage)}" alt="AI-фото Ozon" loading="lazy" />`
      : `<div class="product-image-empty">AI-превью появится здесь</div>`;
  }
  if (aiElements.generateButton) aiElements.generateButton.textContent = draft ? "Переделать" : "Сгенерировать";
  if (aiElements.approveButton) aiElements.approveButton.disabled = !canReview;
  if (aiElements.rejectButton) aiElements.rejectButton.disabled = !canReview;
  if (aiElements.generateButton) aiElements.generateButton.disabled = aiImageBusy;
  if (aiElements.status && !aiImageBusy) {
    aiElements.status.textContent = draft
      ? `Последний черновик: ${aiImageStatusLabel(draft.status)}.`
      : "Черновика пока нет. Проверьте исходное фото и нажмите «Сгенерировать».";
  }
}

async function openAiImageModal(productId) {
  if (!productId || !aiElements.modal) return;
  aiImageProductId = productId;
  if (aiElements.sourceInput) aiElements.sourceInput.value = "";
  if (aiElements.promptInput) aiElements.promptInput.value = "";
  aiElements.modal.classList.remove("hidden");
  document.body.classList.add("modal-open");
  renderAiImageModal();
  setAiImageBusy(true, "Загружаю данные карточки...");
  try {
    const result = await api(`/api/warehouse/products/${encodeURIComponent(productId)}`);
    if (result.product) mergeCurrentProduct(result.product);
    renderAiImageModal(result.product || selectedAiImageProduct());
  } catch (error) {
    if (aiElements.status) aiElements.status.textContent = error.message;
  } finally {
    setAiImageBusy(false);
    renderAiImageModal();
  }
}

function closeAiImageModal() {
  aiElements.modal?.classList.add("hidden");
  document.body.classList.remove("modal-open");
  aiImageProductId = null;
  aiImageBusy = false;
}

async function generateAiImage() {
  const product = selectedAiImageProduct();
  if (!product?.id) return;
  let finalStatus = "";
  setAiImageBusy(true, "Генерирую AI-фото через relay...");
  try {
    const result = await api(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/generate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sourceImageUrl: String(aiElements.sourceInput?.value || aiImageSourceForProduct(product) || "").trim(),
        prompt: String(aiElements.promptInput?.value || "").trim(),
      }),
    });
    if (result.product) mergeCurrentProduct(result.product);
    renderProductPage(currentVariants);
    renderAiImageModal(result.product || selectedAiImageProduct());
    finalStatus = "AI-фото готово. Можно одобрить, отменить или переделать.";
  } catch (error) {
    finalStatus = error.message;
  } finally {
    setAiImageBusy(false);
    renderAiImageModal();
    if (aiElements.status && finalStatus) aiElements.status.textContent = finalStatus;
  }
}

async function reviewAiImage(action) {
  const product = selectedAiImageProduct();
  const draft = latestAiImageDraft(product);
  if (!product?.id || !draft?.id) return;
  let finalStatus = "";
  setAiImageBusy(true, action === "approve" ? "Одобряю фото..." : "Отменяю черновик...");
  try {
    const result = await api(`/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draft.id)}/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (result.product) mergeCurrentProduct(result.product);
    renderProductPage(currentVariants);
    renderAiImageModal(result.product || selectedAiImageProduct());
    finalStatus = action === "approve" ? "Фото одобрено и поставлено главным в карточке." : "Черновик отменен.";
  } catch (error) {
    finalStatus = error.message;
  } finally {
    setAiImageBusy(false);
    renderAiImageModal();
    if (aiElements.status && finalStatus) aiElements.status.textContent = finalStatus;
  }
}

function renderProductPage(variants) {
  const primary = variants[0];
  const image = productImage(primary);
  const ozonProduct = variants.find((item) => item.marketplace === "ozon");
  const productTitle = primary.name || primary.offerId || "Карточка товара";
  const suppliers = variants.flatMap((product) => product.suppliers || []);
  const links = variants.flatMap((product) => product.links || []);
  const history = variants
    .flatMap((product) => (product.priceHistory || []).map((entry) => ({ ...entry, market: marketLabel(product) })))
    .sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0));

  title.textContent = primary.name || primary.offerId || "Карточка товара";
  meta.textContent = `${primary.offerId || "-"} · ${variants.map(marketLabel).join(" + ")}`;
  setDocTitle(primary.name || primary.offerId || "Карточка товара");
  content.innerHTML = `
    <section class="product-page-hero">
      <div class="detail-media-wrap">
        <div class="detail-media">${image ? `<img src="${escapeHtml(image)}" alt="${escapeHtml(primary.name)}" />` : `<div class="product-image-empty">Нет фото</div>`}</div>
        ${
          ozonProduct
            ? `<div class="detail-media-actions">
                <button class="secondary-button compact-button ai-photo-open" type="button" data-product-id="${escapeHtml(ozonProduct.id)}">AI-фото Ozon</button>
                <small>Предпросмотр откроется здесь: можно одобрить, отменить или переделать без перехода на другую страницу.</small>
              </div>`
            : ""
        }
      </div>
      <div class="detail-section">
        <h3>Маркетплейсы</h3>
        <div class="marketplace-variant-list">
          ${variants.map((item) => `
            <div class="variant-markup-row">
              <div>
                <span class="market-badge ${item.marketplace}">${escapeHtml(marketLabel(item))}</span>
                <strong>${item.marketplace === "ozon" ? "В кабинете Ozon: " : ""}${formatMoney(item.currentPrice)} → ${formatMoney(item.nextPrice)}</strong>
                <small>${escapeHtml(item.marketplaceState?.label || "Статус не загружен")}${item.marketplace === "ozon" && ozonCabinetPriceNote(item) ? ` · ${escapeHtml(ozonCabinetPriceNote(item))}` : ""}</small>
              </div>
              <div><strong>Наценка ${Number(item.markupCoefficient || 0).toFixed(2)}</strong></div>
            </div>
          `).join("")}
        </div>
      </div>
    </section>

    <section class="detail-section">
      <h3>Привязки PriceMaster</h3>
      <div class="link-list">
        ${links.length ? links.map((link) => `<div class="link-item"><strong>${escapeHtml(link.article)}</strong><span>${escapeHtml(link.supplierName || "Любой поставщик")}${link.keyword ? ` · ${escapeHtml(link.keyword)}` : ""}</span></div>`).join("") : '<div class="empty-mini">Связей нет.</div>'}
      </div>
    </section>

    <section class="detail-section">
      <h3>Поставщики</h3>
      <div class="supplier-list">
        ${suppliers.length ? suppliers.slice(0, 20).map((supplier) => `<div class="supplier-line ${supplier.stopped ? "stopped" : ""}"><div><strong>${escapeHtml(supplier.partnerName || supplier.supplierName || "Поставщик")}</strong><span>${escapeHtml(supplier.article)} · ${escapeHtml(supplier.name || "")}</span></div><div class="money">$${Number(supplier.price || 0).toFixed(2)}</div></div>`).join("") : '<div class="empty-mini">Поставщики не найдены.</div>'}
      </div>
    </section>

    <section class="detail-section">
      <h3>История цен</h3>
      <div class="history-list">
        ${history.length ? history.slice(0, 30).map((entry) => `<div class="history-row"><div><strong>${escapeHtml(entry.market)}: ${formatMoney(entry.oldPrice)} → ${formatMoney(entry.newPrice)}</strong><span>${escapeHtml(entry.supplierName || "Поставщик не указан")}</span></div><small>${formatDate(entry.at)}</small></div>`).join("") : '<div class="empty-mini">История появится после отправки цен.</div>'}
      </div>
    </section>
  `;
}

async function loadProductPage(refreshPrices) {
  const params = new URLSearchParams(window.location.search);
  const key = params.get("group") || "";
  const id = params.get("id");
  const url = refreshPrices ? "/api/warehouse?refreshPrices=true" : "/api/warehouse";
  const data = await api(url);
  const products = data.products || [];
  const variants = products.filter((product) => groupKey(product) === key || product.id === id);
  if (!variants.length) {
    content.innerHTML = `<div class="empty">Товар не найден.</div>`;
    setDocTitle("Товар не найден");
    return { data, variants: [] };
  }
  currentVariants = variants;
  renderProductPage(variants);
  return { data, variants };
}

async function main() {
  const params = new URLSearchParams(window.location.search);
  const refreshFromUrl = params.get("refreshPrices") === "1" || params.get("refresh") === "1";
  const { data } = await loadProductPage(refreshFromUrl);
  if (Array.isArray(data.syncWarnings) && data.syncWarnings.length) {
    data.syncWarnings.forEach((msg) => showToast(msg));
  }
}

document.getElementById("productRefreshPrices")?.addEventListener("click", async () => {
  const btn = document.getElementById("productRefreshPrices");
  if (btn) btn.disabled = true;
  try {
    const { data } = await loadProductPage(true);
    if (Array.isArray(data.syncWarnings) && data.syncWarnings.length) {
      data.syncWarnings.forEach((msg) => showToast(msg));
    }
  } catch (e) {
    content.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
  } finally {
    if (btn) btn.disabled = false;
  }
});

content.addEventListener("click", async (event) => {
  const button = event.target.closest(".ai-photo-open");
  if (!button) return;
  await openAiImageModal(button.dataset.productId);
});

aiElements.closeButton?.addEventListener("click", closeAiImageModal);
aiElements.cancelButton?.addEventListener("click", closeAiImageModal);
aiElements.modal?.addEventListener("click", (event) => {
  if (event.target === aiElements.modal && !aiImageBusy) closeAiImageModal();
});
aiElements.generateButton?.addEventListener("click", generateAiImage);
aiElements.approveButton?.addEventListener("click", () => reviewAiImage("approve"));
aiElements.rejectButton?.addEventListener("click", () => reviewAiImage("reject"));
aiElements.sourceInput?.addEventListener("input", () => renderAiImageModal());
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !aiElements.modal?.classList.contains("hidden") && !aiImageBusy) closeAiImageModal();
});

main().catch((error) => {
  content.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
  setDocTitle("Ошибка");
});
