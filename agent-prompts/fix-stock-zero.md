# Agent: Fix Zero Stock for Linked Products (BOD13)

## Problem
Some warehouse products show `stock: 0` despite:
- Having a valid PriceMaster link (подвязка есть)
- The product existing in PriceMaster
- The product being active

Example: product **BOD13** — linked to PriceMaster, exists there, but stock displays as 0 in the warehouse UI.

## Repository
`C:\Users\Seb0g1\Documents\New project`

## What You Need to Do
1. **Trace the stock calculation path** for a linked product:
   - Start from `server/parts/02a-price-master-warehouse-build.js` → `buildWarehouseView()`
   - Find `getPriceMasterMatchesForLinks()` → trace where `stock` field is set on the result
   - Find how PriceMaster's `Quantity` (or equivalent field) maps to the warehouse product's `stock`

2. **Find the PM MySQL schema** for stock:
   - Look in `server/parts/02a-price-master-live-query.js` and `02a-price-master-warehouse-maps.js`
   - Find which SQL field represents stock/quantity (look for `Quantity`, `Count`, `Kolvo`, `ostatok` etc.)

3. **Find why stock can be 0**:
   - Is there a filter that zeroes stock for certain conditions?
   - Is there a mismatch in how the link key is built vs. how it's looked up in the matchMap?
   - Check `server/parts/02a-warehouse-detail-display.js` and `02a-warehouse-page-detail-map.js` for how stock is displayed

4. **Check the link key matching**: In `buildWarehouseView`, links from warehouse products are used to build the matchMap. If the link's key doesn't match the PM row's key, stock will be 0. Look for key construction in:
   - `server/parts/02a-price-master-match-helpers.js`
   - `server/parts/02a-price-master-link-lookup.js`

5. **Fix the root cause** — once found, fix it. Look for:
   - Off-by-one in quantity fields
   - Wrong field name (PM might use `Kolvo` not `Quantity`)
   - Key mismatch (link uses different normalization than PM row key)
   - Stock being overwritten to 0 by the no-supplier automation

## Key Files
- `server/parts/02a-price-master-warehouse-build.js` — main warehouse build
- `server/parts/02a-price-master-match-helpers.js` — PM match key construction
- `server/parts/02a-price-master-live-query.js` — PM MySQL queries
- `server/parts/02a-price-master-warehouse-maps.js` — PM data to warehouse maps
- `server/parts/02a-price-master-warehouse-helpers.js`
- `server/parts/02a-warehouse-page-detail-map.js`
- `server/parts/02d-routes-warehouse-catalog.js` — API endpoint for product detail

## After Fixing
- Run `npm test` — must pass 294/294
- Commit with a descriptive message
