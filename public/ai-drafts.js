const els = {
  statusSelect: document.querySelector("#draftStatusSelect"),
  marketplaceSelect: document.querySelector("#draftMarketplaceSelect"),
  refresh: document.querySelector("#refreshDraftsButton"),
  status: document.querySelector("#draftStatus"),
  list: document.querySelector("#draftsList"),
};

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
    const value = input.message
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
      || "";
    return recommendationText(value);
  }
  return "";
}

function draftKindLabel(type) {
  return type === "image" ? "Фото" : "Описание";
}

function renderQuality(product = {}) {
  const quality = product.cardQuality || {};
  if (!Number.isFinite(Number(quality.contentRating))) return "";
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

function renderImageBlock(row = {}, product = {}, draft = {}) {
  const relatedImageDraft = row.relatedImageDraft || {};
  const resultUrl = draft.resultUrl || relatedImageDraft.resultUrl || "";
  const sourceUrl = draft.sourceImageUrl || relatedImageDraft.sourceImageUrl || product.imageUrl || "";
  if (!sourceUrl && !resultUrl) {
    return row.type === "content"
      ? '<div class="ai-draft-image-note">Фото товара не загружено, поэтому AI-изображение пока не создано.</div>'
      : "";
  }
  const resultCaption = row.type === "image" ? "AI черновик" : "AI фото к этому описанию";
  return `
    <div class="ai-draft-media-grid">
      ${sourceUrl ? `
        <figure class="ai-draft-figure">
          <img src="${escapeHtml(sourceUrl)}" alt="" loading="lazy" />
          <figcaption>${escapeHtml(row.type === "content" ? "Фото товара" : "Исходное фото")}</figcaption>
        </figure>
      ` : ""}
      ${resultUrl ? `
        <figure class="ai-draft-figure ai-draft-figure--result">
          <img src="${escapeHtml(resultUrl)}" alt="" loading="lazy" />
          <figcaption>${escapeHtml(resultCaption)}</figcaption>
        </figure>
      ` : row.type === "content" ? '<div class="ai-draft-image-note">AI-фото для этого товара еще не создано. Проверьте лимит AI-провайдера и параметр image drafts в операции.</div>' : ""}
    </div>
  `;
}

function renderDraft(row = {}) {
  const product = row.product || {};
  const draft = row.draft || {};
  const canReview = draft.status === "pending";
  const approvePath = row.type === "image"
    ? `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draft.id)}/approve`
    : `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-content/${encodeURIComponent(draft.id)}/approve`;
  const rejectPath = row.type === "image"
    ? `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draft.id)}/reject`
    : `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-content/${encodeURIComponent(draft.id)}/reject`;
  const description = row.type === "content"
    ? `<div class="ai-draft-description">${escapeHtml(draft.description || "")}</div>`
    : `<div class="ai-draft-description ai-draft-description--prompt">${escapeHtml(draft.prompt || "")}</div>`;

  return `
    <article class="ai-draft-card">
      <div class="ai-draft-card-main">
        <div class="ai-draft-topline">
          <div class="ai-draft-title-block">
            <span class="ai-draft-kind">${escapeHtml(draftKindLabel(row.type))}</span>
            <h3>${escapeHtml(product.offerId || "-")} · ${escapeHtml(product.name || draft.productName || "")}</h3>
          </div>
          <div class="ai-draft-meta">
            <span class="ai-draft-pill">${escapeHtml(draft.status || "-")}</span>
            <span class="ai-draft-pill">${escapeHtml(formatDate(draft.createdAt))}</span>
            ${renderQuality(product)}
            ${draft.qualityBefore !== undefined ? `<span class="ai-draft-pill">Было ${escapeHtml(draft.qualityBefore)}</span>` : ""}
          </div>
        </div>

        ${renderImageBlock(row, product, draft)}

        ${draft.name ? `<div class="ai-draft-name">${escapeHtml(draft.name)}</div>` : ""}
        ${draft.vendor ? `<div class="ai-draft-vendor">${escapeHtml(draft.vendor)}</div>` : ""}
        ${description}
        ${renderList("Преимущества", draft.bulletPoints || draft.bullets)}
        ${renderList("SEO", draft.seoKeywords || draft.keywords)}
        ${renderList("Рекомендации Яндекса", draft.recommendations, "ai-draft-section--recommendations")}
      </div>
      <div class="ai-draft-actions">
        <button class="primary-button compact-button" type="button" data-review-path="${escapeHtml(approvePath)}" ${canReview ? "" : "disabled"}>Принять</button>
        <button class="secondary-button compact-button" type="button" data-review-path="${escapeHtml(rejectPath)}" ${canReview ? "" : "disabled"}>Отклонить</button>
      </div>
    </article>
  `;
}

async function loadDrafts() {
  const status = encodeURIComponent(els.statusSelect?.value || "");
  const marketplace = encodeURIComponent(els.marketplaceSelect?.value || "");
  els.status.textContent = "Загружаю...";
  const payload = await api(`/api/warehouse/ai-drafts?status=${status}&marketplace=${marketplace}&limit=300`);
  const drafts = payload.drafts || [];
  els.status.textContent = `Найдено: ${payload.total || drafts.length}`;
  els.list.innerHTML = drafts.length
    ? drafts.map(renderDraft).join("")
    : '<div class="empty-state">Черновиков нет.</div>';
}

els.refresh?.addEventListener("click", () => {
  loadDrafts().catch((error) => {
    els.status.textContent = `Ошибка: ${error.message}`;
  });
});

els.statusSelect?.addEventListener("change", () => loadDrafts().catch((error) => { els.status.textContent = `Ошибка: ${error.message}`; }));
els.marketplaceSelect?.addEventListener("change", () => loadDrafts().catch((error) => { els.status.textContent = `Ошибка: ${error.message}`; }));

els.list?.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-review-path]");
  if (!button || button.disabled) return;
  button.disabled = true;
  try {
    const payload = await api(button.dataset.reviewPath, { method: "POST", body: {} });
    let message = "Готово.";
    if (payload.yandexSend) {
      message = payload.yandexSend.ok
        ? (payload.yandexSend.skipped ? "Принято локально." : "Принято и отправлено в Яндекс.")
        : `Принято локально, но Яндекс отклонил обновление: ${payload.yandexSend.error || "неизвестная ошибка"}`;
    }
    await loadDrafts();
    els.status.textContent = message;
  } catch (error) {
    els.status.textContent = `Ошибка: ${error.message}`;
    button.disabled = false;
  }
});

loadDrafts().catch((error) => {
  els.status.textContent = `Ошибка: ${error.message}`;
});
