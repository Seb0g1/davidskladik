import { Link2, Loader2, Save, Trash2, X } from "lucide-react";
import type { Product, ProductLink } from "../types";

type LinkRow = ProductLink & {
  productId?: string;
  productOfferId?: string;
  productMarketplace?: string;
  productOfferIds?: string[];
  productMarketplaces?: string[];
  groupCount?: number;
};

type LinkEditCardProps = {
  open: boolean;
  link: LinkRow | null;
  product?: Product | null;
  groupLabel?: string;
  saving?: boolean;
  onClose: () => void;
  onSave: (draft: LinkRow) => void;
  onDelete?: (link: LinkRow) => void;
};

function pillForLink(link: LinkRow) {
  if (link.stockOnly || link.pricingMode === "stock_only") return { text: "складская", className: "stock-only" };
  if (link.matchType === "exact" || link.sourceRowId || link.rowId) return { text: "точное совпадение", className: "exact" };
  return { text: "ручная", className: "manual" };
}

export function LinkEditCard({
  open,
  link,
  product,
  groupLabel,
  saving,
  onClose,
  onSave,
  onDelete,
}: LinkEditCardProps) {
  if (!open || !link) return null;

  const status = pillForLink(link);
  const marketplace = product?.marketplace || link.productMarketplace || "marketplace";
  const offerId = product?.offerId || link.productOfferId || "—";
  const productName = product?.name || link.exactName || link.keyword || "Без названия";
  const pmTitle = link.exactName || link.keyword || link.article || "Без названия PriceMaster";

  return (
    <div className="link-edit-backdrop" role="presentation" onClick={onClose}>
      <section
        className="link-edit-card"
        role="dialog"
        aria-labelledby="link-edit-title"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="link-edit-head">
          <div>
            <span className="link-edit-eyebrow">PriceMaster</span>
            <h3 id="link-edit-title">Редактирование привязки</h3>
            {groupLabel ? <p className="link-edit-subtitle">{groupLabel}</p> : null}
          </div>
          <div className="link-edit-head-actions">
            <span className={`pm-source-pill link-edit-status ${status.className}`}>{status.text}</span>
            <button className="icon-action" type="button" onClick={onClose} title="Закрыть">
              <X size={16} />
            </button>
          </div>
        </header>

        <div className="link-edit-compare">
          <article className="link-edit-side">
            <div className="link-edit-thumb">
              {product?.imageUrl ? <img src={product.imageUrl} alt="" /> : <span>SKU</span>}
            </div>
            <div className="link-edit-side-body">
              <strong>{productName}</strong>
              <span>{marketplace} · offer {offerId}</span>
              <div className="link-edit-pills">
                <span className="pm-route-chip">{marketplace}</span>
                <span className="pm-route-chip muted">карточка группы</span>
              </div>
            </div>
          </article>

          <div className="link-edit-bridge" aria-hidden="true">
            <Link2 size={18} />
          </div>

          <article className="link-edit-side is-pm">
            <div className="link-edit-thumb is-pm">
              <span>PM</span>
            </div>
            <div className="link-edit-side-body">
              <strong>{pmTitle}</strong>
              <span>{link.supplierName || "поставщик не указан"}</span>
              <div className="link-edit-pills">
                <span className="pm-source-pill">PriceMaster</span>
                <span className="pm-route-chip">{link.priceCurrency || "USD"}</span>
              </div>
            </div>
          </article>
        </div>

        <div className="pm-link-grid link-edit-grid">
          <span><b>Артикул PM</b>{link.article || link.supplierArticle || "—"}</span>
          <span><b>Row ID</b>{link.sourceRowId || link.rowId || "не сохранен"}</span>
          <span><b>Partner ID</b>{link.partnerId || "не указан"}</span>
          <span><b>Поставщик</b>{link.supplierName || "не указан"}</span>
          <span><b>Обновлено</b>{link.updatedAt || link.createdAt || "—"}</span>
          <span><b>Кто изменил</b>{link.updatedBy || link.createdBy || "system"}</span>
        </div>

        <div className="pm-route-list">
          <span className="pm-route-chip">Общая связь группы: Ozon + Yandex</span>
          <span className="pm-route-chip muted">Строк с этой связью: {link.groupCount || 1}</span>
        </div>

        <div className="link-edit-form">
          <div className="section-subtitle">Параметры привязки</div>
          <div className="draft-grid manual-link-grid">
            <label>
              Артикул PriceMaster
              <input defaultValue={link.article || ""} name="article" />
            </label>
            <label>
              Ключевое слово
              <input defaultValue={link.keyword || link.exactName || ""} name="keyword" />
            </label>
            <label>
              Валюта
              <select defaultValue={link.priceCurrency || "USD"} name="priceCurrency">
                <option value="USD">USD</option>
                <option value="RUB">RUB</option>
              </select>
            </label>
            <label>
              Поставщик
              <input defaultValue={link.supplierName || ""} name="supplierName" />
            </label>
          </div>
          <label className="toggle-line">
            <input type="checkbox" defaultChecked={Boolean(link.stockOnly || link.pricingMode === "stock_only")} name="stockOnly" />
            <span>Складская связь (не берет цену)</span>
          </label>
        </div>

        <footer className="link-edit-footer">
          {onDelete ? (
            <button className="secondary-action danger" type="button" onClick={() => onDelete(link)} disabled={saving}>
              <Trash2 size={16} /> Удалить
            </button>
          ) : (
            <span />
          )}
          <div className="link-edit-footer-actions">
            <button className="secondary-action" type="button" onClick={onClose} disabled={saving}>
              Отмена
            </button>
            <button className="primary-action" type="button" onClick={() => onSave(link)} disabled={saving}>
              {saving ? <Loader2 className="spin" size={16} /> : <Save size={16} />} Сохранить
            </button>
          </div>
        </footer>
      </section>
    </div>
  );
}
