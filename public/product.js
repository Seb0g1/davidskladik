const content = document.querySelector("#productPageContent");
const title = document.querySelector("#productPageTitle");
const meta = document.querySelector("#productPageMeta");

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

async function main() {
  const params = new URLSearchParams(window.location.search);
  const key = params.get("group") || "";
  const data = await api("/api/warehouse");
  const products = data.products || [];
  const variants = products.filter((product) => groupKey(product) === key || product.id === params.get("id"));
  if (!variants.length) {
    content.innerHTML = `<div class="empty">Товар не найден.</div>`;
    return;
  }

  const primary = variants[0];
  const image = productImage(primary);
  const suppliers = variants.flatMap((product) => product.suppliers || []);
  const links = variants.flatMap((product) => product.links || []);
  const history = variants
    .flatMap((product) => (product.priceHistory || []).map((entry) => ({ ...entry, market: marketLabel(product) })))
    .sort((a, b) => new Date(b.at || 0) - new Date(a.at || 0));

  title.textContent = primary.name || primary.offerId || "Карточка товара";
  meta.textContent = `${primary.offerId || "-"} · ${variants.map(marketLabel).join(" + ")}`;
  content.innerHTML = `
    <section class="product-page-hero">
      <div class="detail-media">${image ? `<img src="${escapeHtml(image)}" alt="${escapeHtml(primary.name)}" />` : `<div class="product-image-empty">Нет фото</div>`}</div>
      <div class="detail-section">
        <h3>Маркетплейсы</h3>
        <div class="marketplace-variant-list">
          ${variants.map((item) => `
            <div class="variant-markup-row">
              <div>
                <span class="market-badge ${item.marketplace}">${escapeHtml(marketLabel(item))}</span>
                <strong>${formatMoney(item.currentPrice)} → ${formatMoney(item.nextPrice)}</strong>
                <small>${escapeHtml(item.marketplaceState?.label || "Статус не загружен")}</small>
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

main().catch((error) => {
  content.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
});
