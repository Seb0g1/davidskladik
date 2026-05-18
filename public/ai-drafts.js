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

function renderQuality(product = {}) {
  const quality = product.cardQuality || {};
  if (!Number.isFinite(Number(quality.contentRating))) return "";
  return `<span>Yandex quality: ${escapeHtml(quality.contentRating)} / avg ${escapeHtml(quality.averageContentRating || "-")}</span>`;
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
  const image = row.type === "image" && draft.resultUrl
    ? `<img class="ai-inline-preview" src="${escapeHtml(draft.resultUrl)}" alt="" loading="lazy" />`
    : "";
  const description = row.type === "content"
    ? `<p>${escapeHtml(draft.description || "")}</p>`
    : `<p>${escapeHtml(draft.prompt || "")}</p>`;
  return `
    <article class="history-item ai-draft-item">
      <div>
        <strong>${escapeHtml(product.offerId || "-")} · ${escapeHtml(product.name || "")}</strong>
        <span>${escapeHtml(row.type)} · ${escapeHtml(draft.status || "-")} · ${escapeHtml(formatDate(draft.createdAt))}</span>
        ${renderQuality(product)}
        ${draft.qualityBefore !== undefined ? `<span>Before: ${escapeHtml(draft.qualityBefore)}</span>` : ""}
        ${image}
        ${draft.name ? `<span>${escapeHtml(draft.name)}</span>` : ""}
        ${draft.vendor ? `<span>${escapeHtml(draft.vendor)}</span>` : ""}
        ${description}
        ${Array.isArray(draft.recommendations) && draft.recommendations.length ? `<span>${draft.recommendations.map(escapeHtml).join(" · ")}</span>` : ""}
      </div>
      <div class="history-actions">
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
