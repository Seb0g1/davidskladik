const settingsForm = document.querySelector("#settingsForm");
const rulesList = document.querySelector("#markupRulesList");
const addRuleButton = document.querySelector("#addMarkupRuleButton");
const statusBox = document.querySelector("#settingsStatus");
const logoutButton = document.querySelector("#logoutButton");
const settingsAnimateAutoFocusInput = document.querySelector("#settingsAnimateAutoFocusInput");
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
  renderRules(settings.markupRules || []);
  if (settingsAnimateAutoFocusInput) {
    settingsAnimateAutoFocusInput.checked = localStorage.getItem(WAREHOUSE_AUTO_FOCUS_ANIM_STORAGE_KEY) !== "0";
  }
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
    markupRules: collectRules(),
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

logoutButton?.addEventListener("click", async () => {
  await api.post("/api/logout").catch(() => {});
  window.location.href = "/login.html";
});

loadSettings().catch((error) => {
  if (statusBox) statusBox.textContent = error.message;
});
