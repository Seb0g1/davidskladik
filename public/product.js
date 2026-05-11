const content = document.querySelector("#productPageContent");
const title = document.querySelector("#productPageTitle");
const meta = document.querySelector("#productPageMeta");
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

async function api(url) {
  const response = await fetch(url);
  if (response.status === 401) {
    window.location.href = "/login.html";
    throw new Error("Требуется вход");
  }
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || response.statusText);
  return data;
}

function renderProductPage(variants) {
  const primary = variants[0];
  const image = productImage(primary);
  const ozonProduct = variants.find((item) => item.marketplace === "ozon");
  const productTitle = primary.name || primary.offerId || "Карточка товара";
  const aiOzonHref = ozonProduct
    ? `/ozon-product.html?productId=${encodeURIComponent(ozonProduct.id)}&offerId=${encodeURIComponent(ozonProduct.offerId || primary.offerId || "")}&name=${encodeURIComponent(productTitle)}&ai=1`
    : "";
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
          aiOzonHref
            ? `<div class="detail-media-actions">
                <a class="secondary-button compact-button" href="${escapeHtml(aiOzonHref)}">AI-фото Ozon</a>
                <small>Генерация продающего фото по текущему изображению и названию; черновик проверяется перед отправкой в Ozon.</small>
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

main().catch((error) => {
  content.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
  setDocTitle("Ошибка");
});
