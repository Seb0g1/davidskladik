const els = {
  limit: document.querySelector("#operationLimitInput"),
  sendLimit: document.querySelector("#operationSendLimitInput"),
  startImport: document.querySelector("#startYandexImportJobButton"),
  startStocks: document.querySelector("#startYandexStockJobButton"),
  startHealth: document.querySelector("#startHealthJobButton"),
  refresh: document.querySelector("#refreshOperationsButton"),
  status: document.querySelector("#operationStatus"),
  list: document.querySelector("#operationsList"),
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
    throw new Error("Требуется вход");
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

function statusLabel(status) {
  return {
    queued: "Ожидает",
    running: "В работе",
    completed: "Готово",
    failed: "Ошибка",
  }[status] || status || "-";
}

function jobSummary(job = {}) {
  const result = job.result || {};
  const parts = [];
  if (result.partial) parts.push("частично");
  if (Number.isFinite(Number(result.sent))) parts.push(`карточки ${result.sent}/${result.failed || 0}`);
  if (Number.isFinite(Number(result.priceSent))) parts.push(`цены ${result.priceSent}/${result.priceFailed || 0}`);
  if (Number.isFinite(Number(result.stockSent))) parts.push(`остатки ${result.stockSent}/${result.stockFailed || 0}`);
  if (Number.isFinite(Number(result.skippedByLimit))) parts.push(`следующий запуск ${result.skippedByLimit}`);
  if (result.ok === false && job.error) parts.push(job.error);
  return parts.join(" · ") || job.error || "";
}

function jobWarnings(job = {}) {
  const result = job.result || {};
  return Array.isArray(result.warnings) ? result.warnings.filter(Boolean).slice(0, 5) : [];
}

function jobFailedRows(job = {}) {
  const result = job.result || {};
  return (Array.isArray(result.results) ? result.results : [])
    .filter((item) => item && item.ok === false)
    .slice(0, 8);
}

function renderJobs(jobs = []) {
  if (!jobs.length) {
    els.list.innerHTML = '<div class="empty-state">Задач пока нет.</div>';
    return;
  }
  els.list.innerHTML = jobs.map((job) => `
    <article class="history-item">
      <div>
        <strong>${escapeHtml(job.title || job.type)}</strong>
        <span>${escapeHtml(statusLabel(job.status))} · ${escapeHtml(job.user || "system")} · ${escapeHtml(formatDate(job.createdAt))}</span>
        <span>${escapeHtml(jobSummary(job))}</span>
        ${jobWarnings(job).length ? `<span>${jobWarnings(job).map(escapeHtml).join(" · ")}</span>` : ""}
        ${jobFailedRows(job).length ? `<span>${jobFailedRows(job).map((item) => `${escapeHtml(item.offerId || item.sku || "-")}: ${escapeHtml(item.error || "error")}`).join(" · ")}</span>` : ""}
      </div>
      <div class="sync-status-pill">${Math.round(Number(job.progress || 0))}%</div>
    </article>
  `).join("");
}

async function loadJobs() {
  const payload = await api("/api/operations?limit=80");
  renderJobs(payload.jobs || []);
  return payload.jobs || [];
}

async function startJob(type) {
  const limit = Math.max(1, Math.min(50000, Number(els.limit.value || 30000) || 30000));
  const sendLimit = Math.max(1, Math.min(10000, Number(els.sendLimit.value || 5000) || 5000));
  els.status.textContent = "Ставлю задачу в фон...";
  const payload = type === "yandex-import-send"
    ? { limit, sendLimit }
    : type === "yandex-stock-sync"
      ? { limit }
      : {};
  const result = await api("/api/operations", {
    method: "POST",
    body: { type, payload },
  });
  els.status.textContent = `Задача создана: ${result.job?.title || type}`;
  await loadJobs();
}

els.refresh?.addEventListener("click", () => {
  loadJobs().catch((error) => {
    els.status.textContent = `Не удалось обновить журнал: ${error.message}`;
  });
});

els.startImport?.addEventListener("click", () => {
  startJob("yandex-import-send").catch((error) => {
    els.status.textContent = `Не удалось запустить импорт: ${error.message}`;
  });
});

els.startStocks?.addEventListener("click", () => {
  startJob("yandex-stock-sync").catch((error) => {
    els.status.textContent = `Не удалось запустить остатки: ${error.message}`;
  });
});

els.startHealth?.addEventListener("click", () => {
  startJob("health-deep").catch((error) => {
    els.status.textContent = `Не удалось запустить диагностику: ${error.message}`;
  });
});

loadJobs().catch((error) => {
  els.status.textContent = `Журнал не загрузился: ${error.message}`;
});

setInterval(() => {
  loadJobs().catch(() => {});
}, 5000);
