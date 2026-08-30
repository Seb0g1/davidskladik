# Agent: Fix PriceMaster Search for Yandex Market New Products

## Problem
When searching for PriceMaster matches for new Yandex Market products, the search doesn't find all relevant items. The PM search is supposed to find the closest matching product in PriceMaster (MySQL) by name/brand/article, but it misses some products.

## Repository
`C:\Users\Seb0g1\Documents\New project`

## Context
- **PriceMaster (PM)** is a MySQL database with product catalog (`OfferRows` table)
- The warehouse links products from Yandex Market/Ozon to PM rows
- When a new Yandex Market product arrives, the system tries to find the PM match automatically
- The link creates a `ProductLink` in PostgreSQL connecting the warehouse product to a PM row
- The issue: the search fails to find some products that DO exist in PM

## What You Need to Do

### 1. Understand the search flow
- Find where new Yandex products trigger PM matching: `server/parts/02a-pm-word-search.js` and `server/parts/02a-price-master-match-helpers.js`
- Check `server/parts/02d-suppliers-routes-search.js` for the search API
- Check if there's an auto-match endpoint in `server/parts/02d-warehouse-links-routes-manage.js`

### 2. Check the search algorithm
The search likely:
a) Tokenizes the product name into words
b) Builds a SQL LIKE query against PM's product name field
c) Ranks results by similarity

Look for:
- Which PM MySQL field is searched (Name, ShortName, Article, Code?)
- How the search query is built (SQL LIKE? Full-text? Word intersection?)
- Whether the search handles transliteration or partial matches
- Whether there's a minimum score threshold that's too strict

### 3. Check the new product discovery flow
- `server/parts/02f-ozon-new-offer-discovery.js` — discovers new offers
- `server/parts/02a-warehouse-import-ozon.js` and `02a-warehouse-import-yandex-sync.js` — import new products
- After import, does the system auto-search PM? If so, trace that path.

### 4. Check `server/parts/02d-consignment-routes.js`
This file has a `GET /api/consignment/pm-nomenclature/new` route and `checkNewPmNomenclatureItems()` function that may be related.

### 5. Common issues to look for
- Search uses exact tokenization that fails on short/abbreviated names
- Minimum word match threshold too high (e.g., requires 3/5 words but PM name is 2 words)
- Missing fallback to article/code search when name search fails
- The `jaccardSimilarity()` function threshold is too strict

### 6. Fix
Improve the search to be more fuzzy/lenient for new products. Consider:
- Lowering the minimum similarity threshold
- Adding article/SKU code as fallback search
- Adding partial name match fallback
- Making the fuzzy threshold configurable

## Key Files to Read
- `server/parts/02a-pm-word-search.js`
- `server/parts/02a-price-master-match-helpers.js`
- `server/parts/02a-price-master-live-query.js`
- `server/parts/02d-suppliers-routes-search.js`
- `server/parts/02d-consignment-routes.js` (look for jaccardSimilarity)
- `frontend/src/routes/SuppliersPage.tsx` (UI for PM search, see what fields are used)

## After Fixing
- Run `npm test` — must pass 294/294
- Commit with a descriptive message
