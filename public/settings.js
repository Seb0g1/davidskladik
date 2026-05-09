const settingsForm = document.querySelector("#settingsForm");
const rulesList = document.querySelector("#markupRulesList");
const addRuleButton = document.querySelector("#addMarkupRuleButton");
const statusBox = document.querySelector("#settingsStatus");
const logoutButton = document.querySelector("#logoutButton");

function ruleRow(rule = {}) {
  const row = document.createElement("div");
  row.className = "settings-rule-row";
  row.innerHTML = `
    <label>
      Площадка
      <select name="marketplace">
        <option value="all" ${String(rule.marketplace || "all") === "all" ? "selected" : ""}>Все</option>
        <option value="ozon" ${String(rule.marketplace || "") === "ozon" ? "selected" : ""}>Ozon</option>
        <option value="yandex" ${String(rule.marketplace || "") === "yandex" ? "selected" : ""}>Yandex Market</option>
      </select>
    </label>
    <label>
      От цены, USD
      <input name="minUsd" type="number" min="0" step="0.0001" value="${Number(rule.minUsd || 0)}" required />
    </label>
    <label>
      Коэффициент
      <input name="coefficient" type="number" min="0.0001" step="0.0001" value="${Number(rule.coefficient || 1)}" required />
    </label>
    <button class="text-button remove-rule" type="button">Удалить</button>
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
  renderRules(settings.markupRules || []);
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

logoutButton?.addEventListener("click", async () => {
  await fetch("/api/logout", { method: "POST" }).catch(() => {});
  window.location.href = "/login.html";
});

loadSettings().catch((error) => {
  statusBox.textContent = error.message;
});
