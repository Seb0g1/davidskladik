const settingsForm = document.querySelector("#settingsForm");
const rulesList = document.querySelector("#markupRulesList");
const addRuleButton = document.querySelector("#addMarkupRuleButton");
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
const WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY = "magicVibesWarehouseAutoFocusAnim";

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

async function api(path, options) {
  const response = await fetch(path, options);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error || payload.detail || "Ошибка запроса");
  return payload;
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
  if (settingsAnimateAutoFocusInput) {
    settingsAnimateAutoFocusInput.checked = localStorage.getItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY) !== "0";
  }
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

manualSyncButton?.addEventListener("click", async () => {
  manualSyncButton.disabled = true;
  if (manualPriceUpdateButton) manualPriceUpdateButton.disabled = true;
  if (manualSyncStatus) manualSyncStatus.textContent = "Синхронизирую склад: товары, статусы, остатки и фото...";
  try {
    const result = await api("/api/warehouse/sync/run", { method: "POST" });
    const total = result?.warehouse?.total ?? 0;
    const changed = result?.warehouse?.changed ?? 0;
    if (manualSyncStatus) manualSyncStatus.textContent = `Склад синхронизирован. Товаров: ${total}, изменений цены: ${changed}.`;
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
  if (manualSyncStatus) manualSyncStatus.textContent = "Запускаю полный цикл обновления цен и отправки на маркетплейсы...";
  try {
    const result = await api("/api/daily-sync/run", { method: "POST" });
    const sent = result?.warehouse?.pricePush?.sent ?? result?.logs?.[0]?.pricePushSent ?? 0;
    const failed = result?.warehouse?.pricePush?.failed ?? result?.logs?.[0]?.pricePushFailed ?? 0;
    if (manualSyncStatus) manualSyncStatus.textContent = `Цены обновлены. Отправлено: ${sent}, ошибок: ${failed}.`;
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
