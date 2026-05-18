const els = {
  threshold: document.querySelector("#qualityThresholdInput"),
  limit: document.querySelector("#qualityLimitInput"),
  find: document.querySelector("#findLowQualityButton"),
  refresh: document.querySelector("#refreshDraftsButton"),
  status: document.querySelector("#draftStatus"),
  list: document.querySelector("#draftsList"),
};

let rows = [];
const selectedPrimary = new Map();

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: options.body ? { "content-type": "application/json" } : undefined,
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Нужен вход");
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || payload.detail || `HTTP ${response.status}`);
  return payload;
}

function formatDate(value) {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString("ru-RU");
}

function recommendationText(input) {
  if (input === undefined || input === null) return "";
  if (typeof input === "string" || typeof input === "number" || typeof input === "boolean") {
    const value = String(input).trim();
    return value === "[object Object]" ? "" : value;
  }
  if (Array.isArray(input)) return input.map(recommendationText).filter(Boolean).join(": ");
  if (typeof input === "object") {
    return recommendationText(
      input.message
        || input.text
        || input.name
        || input.title
        || input.description
        || input.comment
        || input.reason
        || input.recommendation
        || input.value
        || input.details
        || input.error
        || "",
    );
  }
  return "";
}

function renderQuality(product = {}) {
  const quality = product.cardQuality || {};
  if (!Number.isFinite(Number(quality.contentRating))) return '<span class="ai-draft-pill">Качество не загружено</span>';
  const rating = Number(quality.contentRating);
  const tone = rating < 40 ? "danger" : rating < 70 ? "warn" : "ok";
  return `<span class="ai-draft-pill ai-draft-pill--${tone}">Качество ${escapeHtml(rating)}${quality.averageContentRating ? ` / среднее ${escapeHtml(quality.averageContentRating)}` : ""}</span>`;
}

function renderList(title, items, className = "") {
  const values = (Array.isArray(items) ? items : [])
    .map(recommendationText)
    .map((item) => item.trim())
    .filter(Boolean);
  if (!values.length) return "";
  return `
    <div class="ai-draft-section ${className}">
      <span class="ai-draft-section-title">${escapeHtml(title)}</span>
      <div class="ai-draft-chip-list">
        ${values.map((item) => `<span class="ai-draft-chip">${escapeHtml(item)}</span>`).join("")}
      </div>
    </div>
  `;
}

function latestCreatedAt(row = {}) {
  const dates = [
    row.contentDraft?.createdAt,
    ...(row.imageDrafts || []).map((draft) => draft.createdAt),
  ].filter(Boolean).sort();
  return dates[dates.length - 1] || "";
}

function primaryDraftId(row = {}) {
  const productId = row.product?.id || "";
  const drafts = row.imageDrafts || [];
  return selectedPrimary.get(productId) || drafts[0]?.id || "";
}

function renderImages(row = {}) {
  const product = row.product || {};
  const drafts = row.imageDrafts || [];
  const selectedId = primaryDraftId(row);
  if (!drafts.length) {
    return `
      <div class="ai-draft-media-grid">
        ${product.imageUrl ? `
          <figure class="ai-draft-figure">
            <img src="${escapeHtml(product.imageUrl)}" alt="" loading="lazy" />
            <figcaption>Текущее фото</figcaption>
          </figure>
        ` : ""}
        <div class="ai-draft-image-note">AI-фото еще не созданы. Нажмите “Перегенерировать текст + 5 фото”.</div>
      </div>
    `;
  }
  return `
    <div class="ai-draft-result-grid">
      ${drafts.map((draft, index) => `
        <button class="ai-draft-image-choice ${draft.id === selectedId ? "ai-draft-image-choice--selected" : ""}" type="button" data-primary-product-id="${escapeHtml(product.id)}" data-primary-draft-id="${escapeHtml(draft.id)}">
          <img src="${escapeHtml(draft.resultUrl)}" alt="" loading="lazy" />
          <span>${draft.id === selectedId ? "Главное фото" : `Вариант ${index + 1}`}</span>
        </button>
      `).join("")}
    </div>
  `;
}

function renderDraftText(row = {}) {
  const draft = row.contentDraft;
  if (!draft) {
    return '<div class="ai-draft-description ai-draft-description--prompt">AI-текст еще не создан. Сначала запустите генерацию для этой карточки.</div>';
  }
  return `
    ${draft.name ? `<div class="ai-draft-name">${escapeHtml(draft.name)}</div>` : ""}
    ${draft.vendor ? `<div class="ai-draft-vendor">${escapeHtml(draft.vendor)}</div>` : ""}
    <div class="ai-draft-description">${escapeHtml(draft.description || "")}</div>
    ${renderList("Преимущества", draft.bulletPoints || draft.bullets)}
    ${renderList("SEO", draft.seoKeywords || draft.keywords)}
    ${renderList("Рекомендации Яндекса", draft.recommendations, "ai-draft-section--recommendations")}
  `;
}

function renderRow(row = {}) {
  const product = row.product || {};
  const draftReady = Boolean(row.contentDraft || (row.imageDrafts || []).length);
  const canSend = Boolean(row.contentDraft && (row.imageDrafts || []).length);
  return `
    <article class="ai-draft-card ai-quality-card" data-product-id="${escapeHtml(product.id)}">
      <div class="ai-draft-card-main">
        <div class="ai-draft-topline">
          <div class="ai-draft-title-block">
            <span class="ai-draft-kind">Yandex quality</span>
            <h3>${escapeHtml(product.offerId || "-")} · ${escapeHtml(product.name || "")}</h3>
          </div>
          <div class="ai-draft-meta">
            ${renderQuality(product)}
            ${latestCreatedAt(row) ? `<span class="ai-draft-pill">AI ${escapeHtml(formatDate(latestCreatedAt(row)))}</span>` : ""}
          </div>
        </div>

        ${renderImages(row)}
        ${renderDraftText(row)}
      </div>
      <div class="ai-draft-actions">
        <button class="primary-button compact-button" type="button" data-generate-product-id="${escapeHtml(product.id)}">
          ${draftReady ? "Перегенерировать" : "Перегенерировать текст + 5 фото"}
        </button>
        <button class="secondary-button compact-button" type="button" data-send-product-id="${escapeHtml(product.id)}" ${canSend ? "" : "disabled"}>
          Отправить
        </button>
      </div>
    </article>
  `;
}

function renderRows() {
  els.list.innerHTML = rows.length
    ? rows.map(renderRow).join("")
    : '<div class="empty-state">Нажмите “Найти товары” — сюда попадут карточки с качеством до выбранного порога.</div>';
}

function replaceRow(nextRow) {
  const id = nextRow?.product?.id;
  if (!id) return;
  const index = rows.findIndex((row) => row.product?.id === id);
  if (index >= 0) rows[index] = nextRow;
  else rows.unshift(nextRow);
}

async function findLowQuality() {
  const threshold = Math.max(0, Math.min(100, Number(els.threshold?.value || 40) || 40));
  const limit = Math.max(1, Math.min(50000, Number(els.limit?.value || 30000) || 30000));
  els.status.textContent = "Загружаю качество карточек из Яндекса...";
  els.find.disabled = true;
  try {
    const payload = await api(`/api/warehouse/yandex-quality-candidates?threshold=${encodeURIComponent(threshold)}&limit=${encodeURIComponent(limit)}&resultLimit=500`);
    rows = payload.products || [];
    els.status.textContent = `Найдено ${payload.total || rows.length}; качество загружено ${payload.qualityLoaded || 0}; проверено ${payload.checked || 0}`;
    renderRows();
  } finally {
    els.find.disabled = false;
  }
}

async function generateForProduct(productId) {
  const button = document.querySelector(`[data-generate-product-id="${CSS.escape(productId)}"]`);
  if (button) {
    button.disabled = true;
    button.textContent = "Генерирую...";
  }
  els.status.textContent = "Генерирую новый текст и 5 фото...";
  try {
    const payload = await api(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/generate`, {
      method: "POST",
      body: { imagesCount: 5 },
    });
    replaceRow(payload.row);
    renderRows();
    const errors = Array.isArray(payload.errors) ? payload.errors.length : 0;
    els.status.textContent = errors
      ? `Черновик создан частично: фото ${payload.imageDrafts?.length || 0}/5, ошибок ${errors}`
      : "Готово: создан новый текст и 5 фото.";
  } catch (error) {
    els.status.textContent = `Ошибка генерации: ${error.message}`;
  } finally {
    if (button) button.disabled = false;
  }
}

async function sendProduct(productId) {
  const row = rows.find((item) => item.product?.id === productId);
  if (!row) return;
  const button = document.querySelector(`[data-send-product-id="${CSS.escape(productId)}"]`);
  if (button) {
    button.disabled = true;
    button.textContent = "Отправляю...";
  }
  els.status.textContent = "Отправляю текст и 5 фото в Яндекс...";
  try {
    const payload = await api(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/send`, {
      method: "POST",
      body: {
        contentDraftId: row.contentDraft?.id,
        imageBatchId: row.imageBatchId,
        primaryImageDraftId: primaryDraftId(row),
      },
    });
    replaceRow(payload.row);
    renderRows();
    els.status.textContent = payload.yandexSend?.ok
      ? "Отправлено в Яндекс. Карточка обновится после обработки маркетплейсом."
      : `Сохранено локально, но Яндекс отклонил отправку: ${payload.yandexSend?.error || "неизвестная ошибка"}`;
  } catch (error) {
    els.status.textContent = `Ошибка отправки: ${error.message}`;
  } finally {
    if (button) button.disabled = false;
  }
}

els.find?.addEventListener("click", () => {
  findLowQuality().catch((error) => {
    els.status.textContent = `Ошибка поиска: ${error.message}`;
    els.find.disabled = false;
  });
});

els.refresh?.addEventListener("click", () => {
  findLowQuality().catch((error) => {
    els.status.textContent = `Ошибка обновления: ${error.message}`;
  });
});

els.list?.addEventListener("click", (event) => {
  const primaryButton = event.target.closest("[data-primary-product-id]");
  if (primaryButton) {
    selectedPrimary.set(primaryButton.dataset.primaryProductId, primaryButton.dataset.primaryDraftId);
    renderRows();
    return;
  }
  const generateButton = event.target.closest("[data-generate-product-id]");
  if (generateButton) {
    generateForProduct(generateButton.dataset.generateProductId);
    return;
  }
  const sendButton = event.target.closest("[data-send-product-id]");
  if (sendButton && !sendButton.disabled) {
    sendProduct(sendButton.dataset.sendProductId);
  }
});

renderRows();
