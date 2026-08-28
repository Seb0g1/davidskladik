# Marketplace API Audit

Date: 2026-05-19

This audit covers the marketplace endpoints that directly affect product availability, prices, archive state, stocks, card quality, and AI draft publishing in Davidsklad.

## Yandex Market

Official references:

- Offer mappings: https://yandex.com/dev/market/partner-api/doc/ru/reference/business-assortment/getOfferMappings
- Archive assortment flow: https://yandex.ru/dev/market/partner-api/doc/ru/step-by-step/assortment-archive
- Partner API changelog, including offer unarchive references: https://yandex.ru/dev/market/partner-api/doc/en/changelog/all
- Assortment add/update flow: https://yandex.com/dev/market/partner-api/doc/ru/step-by-step/assortment-add-goods

| Area | Current endpoint | ID scope | Payload rule | Success | Pending | Hard error |
| --- | --- | --- | --- | --- | --- | --- |
| Read offer mappings | `POST /v2/businesses/{businessId}/offer-mappings` | `businessId` | Use either pagination filters such as `archived`, or `offerIds`; do not combine `offerIds` with `archived`. | Items returned and parsed. | Empty/missing item after a recent accepted unarchive. | API `ERROR`, malformed response, auth/config failure. |
| Archive offer mappings | `POST /v2/businesses/{businessId}/offer-mappings/archive` | `businessId` | `{ offerIds: [...] }` only. | Per-offer success from API. | Not used for linked sellable cards. | API item failure or request failure. |
| Unarchive offer mappings | `POST /v2/businesses/{businessId}/offer-mappings/unarchive` | `businessId` | `{ offerIds: [...] }` only. | API accepts offer. | API accepted but follow-up mapping is missing or still reports archived. | API item failure or request failure. |
| Prices | `POST /v2/businesses/{businessId}/offer-prices/updates` | `businessId` | `offers` chunks. | API accepts price update. | Marketplace delayed visibility. | API item failure; queued for retry when appropriate. |
| Stocks | campaign stock endpoints used by `sendYandexStocks...` | `campaignId` | Only configured stock shop/campaign is allowed; current business rule is `128820967` / Magic Stick. | API accepts stock update. | Marketplace delayed visibility. | Wrong campaign, API item error, auth/config failure. |
| Card quality | Content status endpoint used by `getYandexOfferCardsContentStatus` | `businessId` | `offerIds` chunks, optional recommendations. | Quality rows loaded. | Rows missing for offers not visible in quality API. | API request failure. |
| AI content send | Product import/update endpoint used by `sendApprovedYandexProductContent` | `businessId` | Only approved draft content. | API accepts update. | Moderation/visibility delay. | API validation failure. |

Implementation rules:

- `offerIds + archived` must never be sent in one `offer-mappings` request.
- Accepted unarchive followed by stale/missing visibility is `pending`, not failed.
- Linked products must never be archived by no-supplier automation.
- Linked products must not receive `stock=0` when supplier data is temporarily unavailable.
- Duplicates with the same marketplace/target/offerId inherit protection when any sibling is linked or manually sellable.

## Ozon

Official references:

- API overview: https://docs.ozon.com/global/api/intro/
- API documentation index: https://docs.ozon.com/global/api/
- Product upload via API: https://docs.ozon.com/global/api/via-api/
- Product archive behavior: https://docs.ozon.com/global/en/products/upload/created-pdp/archive/

| Area | Current endpoint | Payload rule | Success | Pending | Hard error |
| --- | --- | --- | --- | --- | --- |
| Product list | `/v3/product/list` | Paginated account request. | Products loaded. | Partial marketplace outage; keep stored data. | Auth/config failure. |
| Product info | `/v3/product/info/list` | `offer_id` chunks. | Info loaded and merged. | Missing detail; keep existing state. | Request failure after retry budget. |
| Stocks read | `/v4/product/info/stocks` | `offer_id` chunks. | Stock state loaded. | Missing stock; keep existing state. | Request failure after retry budget. |
| Prices read | `/v5/product/info/prices` | `offer_id` chunks. | Prices loaded. | Missing price; keep existing state. | Request failure after retry budget. |
| Prices send | `/v1/product/import/prices` | Price chunks from approved/calculated payload. | Accepted or queued retry. | Rate/old-price retry cases. | Non-retryable item failure. |
| Stocks send | `/v2/products/stocks` | Warehouse-specific stock rows. | Accepted per item. | Marketplace delayed visibility. | API item failure. |
| Archive | `/v1/product/archive` | `product_id` chunks. | Accepted. | FBO constraints can block archive. | Unexpected API failure. |
| Unarchive | `/v1/product/unarchive` | `product_id` chunks. | Accepted. | Marketplace delayed visibility. | API request failure. |

Implementation rules:

- Expected Ozon archive blocks such as FBO stock conflicts should be reported but not treated as app crashes.
- Stock payloads must target configured warehouses and avoid obsolete warehouse discovery during hot paths.
- Price send failures that are known retry cases go to the retry queue instead of causing repeated immediate sends.

## Current Safety Status

- Search: strict SKU search now prefers primary product identity over supplier-only link matches.
- Archive recovery: accepted Yandex unarchive is protected as pending until marketplace visibility catches up.
- No-supplier automation: linked products and protected duplicate offer groups are excluded from archive/zero-stock automation.
- Postgres indexes: current schema already covers `offerId`, `productId`, `marketplace/target`, `status/updatedAt`, `archived/updatedAt`, `ProductLink.supplierArticle`, and supplier link lookup fields.

