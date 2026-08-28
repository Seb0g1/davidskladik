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
const aiEnabledInput = document.querySelector("#aiEnabledInput");
const aiProviderIdInput = document.querySelector("#aiProviderIdInput");
const aiBaseUrlInput = document.querySelector("#aiBaseUrlInput");
const aiApiKeyInput = document.querySelector("#aiApiKeyInput");
const aiTextModelInput = document.querySelector("#aiTextModelInput");
const aiImageModelInput = document.querySelector("#aiImageModelInput");
const aiImageSizeInput = document.querySelector("#aiImageSizeInput");
const aiImageQualityInput = document.querySelector("#aiImageQualityInput");
const aiImageFormatInput = document.querySelector("#aiImageFormatInput");
const aiTestButton = document.querySelector("#aiTestButton");
const aiSettingsStatus = document.querySelector("#aiSettingsStatus");
const manualSyncButton = document.querySelector("#manualSyncButton");
const manualPriceUpdateButton = document.querySelector("#manualPriceUpdateButton");
const manualSyncStatus = document.querySelector("#manualSyncStatus");
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
const passwordResetModal = document.querySelector("#passwordResetModal");
const passwordResetTitle = document.querySelector("#passwordResetTitle");
const passwordResetHint = document.querySelector("#passwordResetHint");
const passwordResetInput = document.querySelector("#passwordResetInput");
const passwordResetSubmit = document.querySelector("#passwordResetSubmit");
const passwordResetCancel = document.querySelector("#passwordResetCancel");
const passwordResetClose = document.querySelector("#passwordResetClose");
const WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY = "magicVibesWarehouseAutoFocusAnim";

// window.api инициализируется в /lib/api.js (fetch + 401 + JSON-обработка).
const api = window.api;

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

function renderRules(rules = []) {
  if (!rulesList) return;
  rulesList.innerHTML = "";
  for (const rule of rules) rulesList.appendChild(ruleRow(rule));
  if (!rules.length) rulesList.appendChild(ruleRow({ marketplace: "all", minUsd: 0, coefficient: 1.7 }));
}

function collectRules() {
  if (!rulesList) return [];
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

function renderAiSettings(ai = {}) {
  if (aiEnabledInput) aiEnabledInput.checked = ai.enabled !== false;
  if (aiProviderIdInput) aiProviderIdInput.value = ai.providerId || "codexsale";
  if (aiBaseUrlInput) aiBaseUrlInput.value = ai.baseUrl || "https://codex.sale/v1";
  if (aiApiKeyInput) {
    aiApiKeyInput.value = "";
    aiApiKeyInput.placeholder = ai.apiKeyMasked ? `Сохранён: ${ai.apiKeyMasked}` : "Оставьте пустым, чтобы не менять";
  }
  if (aiTextModelInput) aiTextModelInput.value = ai.textModel || "gpt-5.4-mini";
  if (aiImageModelInput) aiImageModelInput.value = ai.imageModel || "gpt-image-2";
  if (aiImageSizeInput) aiImageSizeInput.value = ai.imageSize || "1024x1024";
  if (aiImageQualityInput) aiImageQualityInput.value = ai.imageQuality || "auto";
  if (aiImageFormatInput) aiImageFormatInput.value = ai.imageFormat || "png";
  if (aiSettingsStatus) {
    const source = ai.source === "settings" ? "ключ сохранён в настройках" : (ai.source === "env" ? "ключ берётся из .env" : "ключ не задан");
    aiSettingsStatus.textContent = `AI: ${ai.enabled === false ? "выключен" : "включён"}, ${source}.`;
  }
}

function collectAiSettings() {
  return {
    enabled: Boolean(aiEnabledInput?.checked),
    providerId: String(aiProviderIdInput?.value || "codexsale").trim(),
    baseUrl: String(aiBaseUrlInput?.value || "").trim(),
    apiKey: String(aiApiKeyInput?.value || "").trim(),
    textModel: String(aiTextModelInput?.value || "gpt-5.4-mini").trim(),
    imageModel: String(aiImageModelInput?.value || "gpt-image-2").trim(),
    imageSize: String(aiImageSizeInput?.value || "1024x1024").trim(),
    imageQuality: String(aiImageQualityInput?.value || "auto").trim(),
    imageFormat: String(aiImageFormatInput?.value || "png").trim(),
  };
}

async function loadSettings() {
  if (!settingsForm || !statusBox) return;
  statusBox.textContent = "Загружаю настройки...";
  let payload;
  try {
    payload = await api("/api/settings");
  } catch (error) {
    if (error.status && [404, 500, 502, 503].includes(error.status)) {
      // На некоторых старых сборках настройки лежали в /api/marketplaces.
      payload = await api("/api/marketplaces");
      payload = { settings: payload.settings || {} };
    } else {
      throw error;
    }
  }
  const settings = payload.settings || {};
  settingsForm.elements.fixedUsdRate.value = settings.fixedUsdRate || 95;
  settingsForm.elements.defaultOzonMarkup.value = settings.defaultMarkups?.ozon || 1.7;
  settingsForm.elements.defaultYandexMarkup.value = settings.defaultMarkups?.yandex || 1.6;
  if (autoSyncEnabledInput) autoSyncEnabledInput.checked = settings.automation?.autoSyncEnabled !== false;
  if (autoSyncMinutesInput) autoSyncMinutesInput.value = settings.automation?.autoSyncMinutes || 30;
  renderAiSettings(settings.ai || {});
  renderRules(settings.markupRules || []);
  renderAvailabilityRules(settings.availabilityRules || []);
  if (settingsAnimateAutoFocusInput) {
    settingsAnimateAutoFocusInput.checked = localStorage.getItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY) !== "0";
  }
  await loadUsers();
  statusBox.textContent = "Настройки загружены.";
}

settingsForm?.addEventListener("submit", async (event) => {
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
    ai: collectAiSettings(),
    markupRules: collectRules(),
    availabilityRules: collectAvailabilityRules(),
  };
  statusBox.textContent = "Сохраняю настройки...";
  try {
    try {
      await api.put("/api/settings", payload);
    } catch (error) {
      // Метод не поддерживается на старом бэке — пробуем POST один раз.
      if (error.status === 404 || error.status === 405) {
        await api.post("/api/settings", payload);
      } else {
        throw error;
      }
    }
    await loadSettings();
    statusBox.textContent =
      "Настройки сохранены. Карточки, где вручную указана наценка, не меняются сами — очистите поле «Наценка» на складе или нажмите «По настройкам».";
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

aiTestButton?.addEventListener("click", async () => {
  aiTestButton.disabled = true;
  if (aiSettingsStatus) aiSettingsStatus.textContent = "Проверяю AI-подключение...";
  try {
    const result = await api("/api/settings/ai/test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ai: collectAiSettings() }),
    });
    if (aiSettingsStatus) {
      aiSettingsStatus.textContent = `AI подключен. Модель: ${result.model || "ok"}, ответ за ${result.latencyMs || 0} мс.`;
    }
  } catch (error) {
    if (aiSettingsStatus) aiSettingsStatus.textContent = `AI не подключен: ${error.message}`;
  } finally {
    aiTestButton.disabled = false;
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
      const password = await openPasswordResetModal(username);
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

employeeList?.addEventListener("change", async (event) => {
  const roleSelect = event.target.closest(".user-role-select");
  if (!roleSelect) return;
  const row = event.target.closest(".settings-user-row");
  const username = row?.dataset.username || "";
  if (!username) return;
  roleSelect.disabled = true;
  try {
    const role = String(roleSelect.value || "manager");
    const data = await api(`/api/users/${encodeURIComponent(username)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ role }),
    });
    renderUsers(data.users || []);
    if (employeeStatus) employeeStatus.textContent = `Роль сотрудника ${username} обновлена.`;
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

logoutButton?.addEventListener("click", async () => {
  await api.post("/api/logout").catch(() => {});
  window.location.href = "/login.html";
});

loadSettings().catch((error) => {
  if (statusBox) statusBox.textContent = error.message;
});
