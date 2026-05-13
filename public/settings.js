const settingsForm = document.querySelector("#settingsForm");
const rulesList = document.querySelector("#markupRulesList");
const addRuleButton = document.querySelector("#addMarkupRuleButton");
const availabilityRulesList = document.querySelector("#availabilityRulesList");
const addAvailabilityRuleButton = document.querySelector("#addAvailabilityRuleButton");
const statusBox = document.querySelector("#settingsStatus");
const logoutButton = document.querySelector("#logoutButton");
const settingsAnimateAutoFocusInput = document.querySelector("#settingsAnimateAutoFocusInput");
const autoSyncEnabledInput = document.querySelector("#autoSyncEnabledInput");
const autoSyncMinutesInput = document.querySelector("#autoSyncMinutesInput");
const manualSyncButton = document.querySelector("#manualSyncButton");
const manualPriceUpdateButton = document.querySelector("#manualPriceUpdateButton");
const manualSyncStatus = document.querySelector("#manualSyncStatus");
const telegramTestButton = document.querySelector("#telegramTestButton");
const telegramReportButton = document.querySelector("#telegramReportButton");
const telegramStatus = document.querySelector("#telegramStatus");
const employeeList = document.querySelector("#employeeList");
const employeeStatus = document.querySelector("#employeeStatus");
const employeeUsernameInput = document.querySelector("#employeeUsernameInput");
const employeePasswordInput = document.querySelector("#employeePasswordInput");
const employeeRoleInput = document.querySelector("#employeeRoleInput");
const employeeAddButton = document.querySelector("#employeeAddButton");
const auditUserInput = document.querySelector("#auditUserInput");
const auditProductInput = document.querySelector("#auditProductInput");
const auditActionInput = document.querySelector("#auditActionInput");
const auditDateFromInput = document.querySelector("#auditDateFromInput");
const auditDateToInput = document.querySelector("#auditDateToInput");
const auditLoadButton = document.querySelector("#auditLoadButton");
const auditList = document.querySelector("#auditList");
const auditStatus = document.querySelector("#auditStatus");
const WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY = "magicVibesWarehouseAutoFocusAnim";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function ruleRow(rule = {}) {
  const row = document.createElement("div");
  row.className = "settings-rule-row";
  row.innerHTML = `
    <label class="settings-rule-field">
      Площадка
      <select name="marketplace" class="settings-rule-input settings-rule-select">
        <option value="all" ${String(rule.marketplace || "all") === "all" ? "selected" : ""}>Все</option>
        <option value="ozon" ${String(rule.marketplace || "") === "ozon" ? "selected" : ""}>Ozon</option>
        <option value="yandex" ${String(rule.marketplace || "") === "yandex" ? "selected" : ""}>Yandex Market</option>
      </select>
    </label>
    <label class="settings-rule-field">
      От цены, USD
      <input name="minUsd" class="settings-rule-input" type="number" min="0" step="0.0001" value="${Number(rule.minUsd || 0)}" required />
    </label>
    <label class="settings-rule-field">
      Коэффициент
      <input name="coefficient" class="settings-rule-input" type="number" min="0.0001" step="0.0001" value="${Number(rule.coefficient || 1)}" required />
    </label>
    <button class="secondary-button compact-button remove-rule" type="button">Удалить</button>
  `;
  return row;
}

function availabilityRuleRow(rule = {}) {
  const row = document.createElement("div");
  row.className = "settings-rule-row";
  row.innerHTML = `
    <label class="settings-rule-field">
      Площадка
      <select name="availabilityMarketplace" class="settings-rule-input settings-rule-select">
        <option value="all" ${String(rule.marketplace || "all") === "all" ? "selected" : ""}>Все</option>
        <option value="ozon" ${String(rule.marketplace || "") === "ozon" ? "selected" : ""}>Ozon</option>
        <option value="yandex" ${String(rule.marketplace || "") === "yandex" ? "selected" : ""}>Yandex Market</option>
      </select>
    </label>
    <label class="settings-rule-field">
      Доступных поставщиков от
      <input name="minAvailableSuppliers" class="settings-rule-input" type="number" min="0" step="1" value="${Number(rule.minAvailableSuppliers || 0)}" required />
    </label>
    <label class="settings-rule-field">
      Поправка к коэффициенту
      <input name="coefficientDelta" class="settings-rule-input" type="number" step="0.0001" value="${Number(rule.coefficientDelta || 0)}" required />
    </label>
    <label class="settings-rule-field">
      Остаток
      <input name="targetStock" class="settings-rule-input" type="number" min="0" step="1" value="${Number(rule.targetStock || 0)}" required />
    </label>
    <button class="secondary-button compact-button remove-availability-rule" type="button">Удалить</button>
  `;
  return row;
}

async function api(path, options) {
  const response = await fetch(path, options);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = [
      payload.error || payload.detail || payload.description || "Ошибка запроса",
      payload.hint,
      payload.telegram?.description,
    ]
      .filter(Boolean)
      .join(" ");
    throw new Error(message);
  }
  return payload;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function manualWarehouseSyncText(status) {
  if (!status) return "Синхронизация запущена в фоне...";
  if (status.status === "running") {
    return `Синхронизация идёт в фоне${status.startedAt ? ` с ${new Date(status.startedAt).toLocaleTimeString("ru-RU")}` : ""}. Страницу можно не держать открытой.`;
  }
  if (status.status === "ok") {
    const total = status.result?.warehouse?.total ?? 0;
    const changed = status.result?.warehouse?.changed ?? 0;
    return `Склад синхронизирован. Товаров: ${total}, изменений цены: ${changed}.`;
  }
  if (status.status === "failed") return `Синхронизация не удалась: ${status.error || "неизвестная ошибка"}`;
  return "Ручной запуск не выполняется.";
}

async function pollManualWarehouseSync() {
  for (;;) {
    await sleep(2500);
    const status = await api("/api/warehouse/sync/status");
    if (manualSyncStatus) manualSyncStatus.textContent = manualWarehouseSyncText(status);
    if (status.status !== "running") return status;
  }
}

function dailySyncText(status) {
  if (!status) return "Обновление цен запущено в фоне...";
  if (status.status === "running") {
    return `Цены и синхронизация обновляются в фоне${status.startedAt ? ` с ${new Date(status.startedAt).toLocaleTimeString("ru-RU")}` : ""}.`;
  }
  if (status.status === "ok") {
    const sent = status.warehouse?.pricePush?.sent ?? status.logs?.[0]?.pricePushSent ?? 0;
    const failed = status.warehouse?.pricePush?.failed ?? status.logs?.[0]?.pricePushFailed ?? 0;
    return `Цены обновлены. Отправлено: ${sent}, ошибок: ${failed}.`;
  }
  if (status.status === "failed") return `Обновление не удалось: ${status.error || "неизвестная ошибка"}`;
  return "Ручной запуск не выполняется.";
}

async function pollDailySync() {
  for (;;) {
    await sleep(2500);
    const status = await api("/api/daily-sync");
    if (manualSyncStatus) manualSyncStatus.textContent = dailySyncText(status);
    if (status.status !== "running") return status;
  }
}

function sourceLabel(source) {
  if (source === "env") return ".env";
  if (source === "env-json") return "APP_USERS_JSON";
  return "локально";
}

function renderUsers(users = []) {
  if (!employeeList) return;
  if (!users.length) {
    employeeList.innerHTML = '<div class="empty-mini">Пока есть только основной пользователь из .env.</div>';
    return;
  }
  employeeList.innerHTML = users
    .map((user) => `
      <div class="settings-user-row ${user.disabled ? "is-disabled" : ""}" data-username="${escapeHtml(user.username)}" data-active="${user.disabled ? "0" : "1"}">
        <div>
          <strong>${escapeHtml(user.username)}</strong>
          <span>${user.role === "admin" ? "Администратор" : "Менеджер"} · ${sourceLabel(user.source)}${user.protected ? " · защищён" : ""}${user.disabled ? " · выключен" : ""}</span>
        </div>
        <div class="form-inline-actions">
          <button class="secondary-button compact-button toggle-user-active" type="button" ${user.protected ? "disabled" : ""}>${user.disabled ? "Включить" : "Выключить"}</button>
          <button class="secondary-button compact-button reset-user-password" type="button" ${user.protected ? "disabled" : ""}>Новый пароль</button>
          <button class="secondary-button compact-button delete-user" type="button" ${user.protected ? "disabled" : ""}>Удалить</button>
        </div>
      </div>
    `)
    .join("");
}

function auditActionLabel(action) {
  const labels = {
    "warehouse.link.save": "Добавление привязки",
    "warehouse.links.bulk_save": "Сохранение привязок",
    "warehouse.link.delete": "Удаление привязки",
    "warehouse.product.update": "Изменение товара",
    "settings.update": "Настройки",
    "users.create": "Создание сотрудника",
    "users.update": "Изменение сотрудника",
    "users.delete": "Удаление сотрудника",
  };
  return labels[action] || action || "Действие";
}

function auditDetailsText(entry = {}) {
  const details = entry.details || {};
  const links = Array.isArray(details.links) ? details.links : [];
  const linkText = links
    .slice(0, 3)
    .map((link) => [link.article, link.supplierName].filter(Boolean).join(" / "))
    .filter(Boolean)
    .join(" · ");
  const parts = [
    details.offerId || "",
    details.name || "",
    details.article || "",
    details.supplierName || "",
    linkText,
    Array.isArray(details.productIds) ? `${details.productIds.length} товаров` : "",
  ].filter(Boolean);
  return parts.join(" · ") || "Без деталей";
}

function renderAudit(audit = []) {
  if (!auditList) return;
  if (!audit.length) {
    auditList.innerHTML = '<div class="empty-mini">По фильтрам ничего не найдено.</div>';
    return;
  }
  auditList.innerHTML = audit.map((entry) => `
    <div class="settings-user-row">
      <div>
        <strong>${escapeHtml(entry.user || "system")} · ${escapeHtml(auditActionLabel(entry.action))}</strong>
        <span>${escapeHtml(auditDetailsText(entry))}</span>
      </div>
      <small>${entry.at ? new Date(entry.at).toLocaleString("ru-RU") : "—"}</small>
    </div>
  `).join("");
}

async function loadAudit() {
  if (!auditList) return;
  if (auditLoadButton) auditLoadButton.disabled = true;
  if (auditStatus) auditStatus.textContent = "Загружаю аудит...";
  try {
    const params = new URLSearchParams();
    params.set("limit", "100");
    if (auditUserInput?.value.trim()) params.set("user", auditUserInput.value.trim());
    if (auditProductInput?.value.trim()) params.set("q", auditProductInput.value.trim());
    if (auditActionInput?.value && auditActionInput.value !== "all") params.set("action", auditActionInput.value);
    if (auditDateFromInput?.value) params.set("dateFrom", auditDateFromInput.value);
    if (auditDateToInput?.value) params.set("dateTo", auditDateToInput.value);
    const data = await api(`/api/audit-log?${params}`);
    renderAudit(data.audit || []);
    if (auditStatus) auditStatus.textContent = `Найдено событий: ${data.audit?.length || 0}.`;
  } catch (error) {
    if (auditStatus) auditStatus.textContent = error.message;
  } finally {
    if (auditLoadButton) auditLoadButton.disabled = false;
  }
}

async function loadUsers() {
  if (!employeeList) return;
  const data = await api("/api/users");
  renderUsers(data.users || []);
  if (employeeStatus) employeeStatus.textContent = "Сотрудники загружены.";
}

function renderRules(rules = []) {
  rulesList.innerHTML = "";
  for (const rule of rules) rulesList.appendChild(ruleRow(rule));
  if (!rules.length) rulesList.appendChild(ruleRow({ marketplace: "all", minUsd: 0, coefficient: 1.7 }));
}

function collectRules() {
  const rows = [...rulesList.querySelectorAll(".settings-rule-row")];
  return rows
    .map((row) => ({
      marketplace: String(row.querySelector('select[name="marketplace"]').value || "all"),
      minUsd: Number(row.querySelector('input[name="minUsd"]').value || 0),
      coefficient: Number(row.querySelector('input[name="coefficient"]').value || 0),
    }))
    .filter((rule) => Number.isFinite(rule.coefficient) && rule.coefficient > 0)
    .sort((a, b) => a.minUsd - b.minUsd);
}

function renderAvailabilityRules(rules = []) {
  if (!availabilityRulesList) return;
  availabilityRulesList.innerHTML = "";
  const rows = rules.length
    ? rules
    : [
        { marketplace: "all", minAvailableSuppliers: 5, coefficientDelta: -0.05, targetStock: 10 },
        { marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 },
      ];
  for (const rule of rows) availabilityRulesList.appendChild(availabilityRuleRow(rule));
}

function collectAvailabilityRules() {
  if (!availabilityRulesList) return [];
  const rows = [...availabilityRulesList.querySelectorAll(".settings-rule-row")];
  return rows
    .map((row) => ({
      marketplace: String(row.querySelector('select[name="availabilityMarketplace"]').value || "all"),
      minAvailableSuppliers: Number(row.querySelector('input[name="minAvailableSuppliers"]').value || 0),
      coefficientDelta: Number(row.querySelector('input[name="coefficientDelta"]').value || 0),
      targetStock: Number(row.querySelector('input[name="targetStock"]').value || 0),
    }))
    .filter((rule) =>
      Number.isFinite(rule.minAvailableSuppliers)
      && rule.minAvailableSuppliers >= 0
      && Number.isFinite(rule.coefficientDelta)
      && Number.isFinite(rule.targetStock)
      && rule.targetStock >= 0,
    )
    .sort((a, b) => b.minAvailableSuppliers - a.minAvailableSuppliers);
}

async function loadSettings() {
  const data = await api("/api/settings");
  const settings = data.settings || {};
  settingsForm.elements.fixedUsdRate.value = settings.fixedUsdRate || 95;
  settingsForm.elements.defaultOzonMarkup.value = settings.defaultMarkups?.ozon || 1.7;
  settingsForm.elements.defaultYandexMarkup.value = settings.defaultMarkups?.yandex || 1.6;
  if (autoSyncEnabledInput) autoSyncEnabledInput.checked = settings.automation?.autoSyncEnabled !== false;
  if (autoSyncMinutesInput) autoSyncMinutesInput.value = settings.automation?.autoSyncMinutes || 30;
  if (telegramStatus) {
    telegramStatus.textContent = data.telegram?.configured
      ? `Telegram подключен. Чат: ${data.telegram.chatId || "задан"}. Ежедневный отчёт: ${data.telegram.dailyReportTime || "22:00"}.`
      : "Telegram не настроен: задайте TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID в .env.";
  }
  renderRules(settings.markupRules || []);
  renderAvailabilityRules(settings.availabilityRules || []);
  if (settingsAnimateAutoFocusInput) {
    settingsAnimateAutoFocusInput.checked = localStorage.getItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY) !== "0";
  }
  await loadUsers();
  statusBox.textContent = "Настройки загружены.";
}

settingsForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const payload = {
    fixedUsdRate: Number(settingsForm.elements.fixedUsdRate.value),
    defaultMarkups: {
      ozon: Number(settingsForm.elements.defaultOzonMarkup.value),
      yandex: Number(settingsForm.elements.defaultYandexMarkup.value),
    },
    automation: {
      autoSyncEnabled: Boolean(autoSyncEnabledInput?.checked),
      autoSyncMinutes: Number(autoSyncMinutesInput?.value || 30),
    },
    markupRules: collectRules(),
    availabilityRules: collectAvailabilityRules(),
  };
  statusBox.textContent = "Сохраняю настройки...";
  try {
    await api("/api/settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    statusBox.textContent = "Настройки сохранены.";
  } catch (error) {
    statusBox.textContent = error.message;
  }
});

addRuleButton?.addEventListener("click", () => {
  rulesList.appendChild(ruleRow({ marketplace: "all", minUsd: 0, coefficient: 1 }));
});

rulesList?.addEventListener("click", (event) => {
  const btn = event.target.closest(".remove-rule");
  if (!btn) return;
  const row = event.target.closest(".settings-rule-row");
  if (row) row.remove();
  if (!rulesList.querySelector(".settings-rule-row")) rulesList.appendChild(ruleRow({ marketplace: "all", minUsd: 0, coefficient: 1.7 }));
});

settingsAnimateAutoFocusInput?.addEventListener("change", () => {
  const enabled = Boolean(settingsAnimateAutoFocusInput.checked);
  localStorage.setItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY, enabled ? "1" : "0");
  statusBox.textContent = `UI-настройка сохранена: авто-фокус ${enabled ? "с анимацией" : "без анимации"}.`;
});

addAvailabilityRuleButton?.addEventListener("click", () => {
  availabilityRulesList?.appendChild(availabilityRuleRow({ marketplace: "all", minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 }));
});

availabilityRulesList?.addEventListener("click", (event) => {
  const btn = event.target.closest(".remove-availability-rule");
  if (!btn) return;
  const row = event.target.closest(".settings-rule-row");
  if (row) row.remove();
  if (!availabilityRulesList.querySelector(".settings-rule-row")) {
    renderAvailabilityRules([]);
  }
});

employeeAddButton?.addEventListener("click", async () => {
  const username = String(employeeUsernameInput?.value || "").trim();
  const password = String(employeePasswordInput?.value || "");
  const role = String(employeeRoleInput?.value || "manager");
  if (!username || !password) {
    if (employeeStatus) employeeStatus.textContent = "Укажите логин и пароль сотрудника.";
    return;
  }
  employeeAddButton.disabled = true;
  if (employeeStatus) employeeStatus.textContent = "Добавляю сотрудника...";
  try {
    const data = await api("/api/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password, role }),
    });
    renderUsers(data.users || []);
    if (employeeUsernameInput) employeeUsernameInput.value = "";
    if (employeePasswordInput) employeePasswordInput.value = "";
    if (employeeRoleInput) employeeRoleInput.value = "manager";
    if (employeeStatus) employeeStatus.textContent = "Сотрудник добавлен.";
  } catch (error) {
    if (employeeStatus) employeeStatus.textContent = error.message;
  } finally {
    employeeAddButton.disabled = false;
  }
});

employeeList?.addEventListener("click", async (event) => {
  const row = event.target.closest(".settings-user-row");
  if (!row) return;
  const username = row.dataset.username || "";
  const deleteButton = event.target.closest(".delete-user");
  const resetButton = event.target.closest(".reset-user-password");
  const toggleActiveButton = event.target.closest(".toggle-user-active");
  try {
    if (toggleActiveButton) {
      const nextActive = row.dataset.active !== "1";
      toggleActiveButton.disabled = true;
      const data = await api(`/api/users/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ active: nextActive }),
      });
      renderUsers(data.users || []);
      if (employeeStatus) employeeStatus.textContent = `Сотрудник ${username} ${nextActive ? "включён" : "выключен"}.`;
    }
    if (deleteButton) {
      deleteButton.disabled = true;
      const data = await api(`/api/users/${encodeURIComponent(username)}`, { method: "DELETE" });
      renderUsers(data.users || []);
      if (employeeStatus) employeeStatus.textContent = `Сотрудник ${username} удалён.`;
    }
    if (resetButton) {
      const password = window.prompt(`Новый пароль для ${username} (минимум 6 символов)`);
      if (!password) return;
      resetButton.disabled = true;
      const data = await api(`/api/users/${encodeURIComponent(username)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });
      renderUsers(data.users || []);
      if (employeeStatus) employeeStatus.textContent = `Пароль сотрудника ${username} обновлён.`;
    }
  } catch (error) {
    if (employeeStatus) employeeStatus.textContent = error.message;
    loadUsers().catch(() => {});
  }
});

auditLoadButton?.addEventListener("click", () => {
  loadAudit();
});

[auditUserInput, auditProductInput, auditActionInput, auditDateFromInput, auditDateToInput].forEach((input) => {
  input?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      loadAudit();
    }
  });
  input?.addEventListener("change", () => {
    if (input === auditActionInput || input === auditDateFromInput || input === auditDateToInput) loadAudit();
  });
});

manualSyncButton?.addEventListener("click", async () => {
  manualSyncButton.disabled = true;
  if (manualPriceUpdateButton) manualPriceUpdateButton.disabled = true;
  if (manualSyncStatus) manualSyncStatus.textContent = "Запускаю синхронизацию склада в фоне...";
  try {
    const result = await api("/api/warehouse/sync/run", { method: "POST" });
    if (manualSyncStatus) manualSyncStatus.textContent = manualWarehouseSyncText(result.status);
    await pollManualWarehouseSync();
  } catch (error) {
    if (manualSyncStatus) manualSyncStatus.textContent = error.message;
  } finally {
    manualSyncButton.disabled = false;
    if (manualPriceUpdateButton) manualPriceUpdateButton.disabled = false;
  }
});

manualPriceUpdateButton?.addEventListener("click", async () => {
  if (manualSyncButton) manualSyncButton.disabled = true;
  manualPriceUpdateButton.disabled = true;
  if (manualSyncStatus) manualSyncStatus.textContent = "Запускаю обновление цен в фоне...";
  try {
    const result = await api("/api/daily-sync/run", { method: "POST" });
    if (manualSyncStatus) manualSyncStatus.textContent = dailySyncText(result.status);
    await pollDailySync();
  } catch (error) {
    if (manualSyncStatus) manualSyncStatus.textContent = error.message;
  } finally {
    if (manualSyncButton) manualSyncButton.disabled = false;
    manualPriceUpdateButton.disabled = false;
  }
});

telegramTestButton?.addEventListener("click", async () => {
  telegramTestButton.disabled = true;
  if (telegramStatus) telegramStatus.textContent = "Отправляю тестовое уведомление...";
  try {
    await api("/api/telegram/test", { method: "POST" });
    if (telegramStatus) telegramStatus.textContent = "Тестовое уведомление отправлено в Telegram.";
  } catch (error) {
    if (telegramStatus) telegramStatus.textContent = error.message;
  } finally {
    telegramTestButton.disabled = false;
  }
});

telegramReportButton?.addEventListener("click", async () => {
  telegramReportButton.disabled = true;
  if (telegramTestButton) telegramTestButton.disabled = true;
  if (telegramStatus) telegramStatus.textContent = "Формирую Excel-отчёт и отправляю в Telegram...";
  try {
    const result = await api("/api/telegram/daily-report/run", { method: "POST" });
    const totals = result.totals || {};
    if (telegramStatus) {
      telegramStatus.textContent = `Отчёт отправлен: цен ${totals.priceUpdated || 0}, привязок ${totals.linkedEvents || 0}, пропавших поставщиков ${totals.suppliersLost || 0}.`;
    }
  } catch (error) {
    if (telegramStatus) telegramStatus.textContent = error.message;
  } finally {
    telegramReportButton.disabled = false;
    if (telegramTestButton) telegramTestButton.disabled = false;
  }
});

logoutButton?.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

loadSettings().catch((error) => {
  statusBox.textContent = error.message;
});
