#!/usr/bin/env bash
# 2026-09-30 site check: delivery map clusters, lazy map bundle, no prefetch of legal/brand pages,
# cookie banner under the full-screen map; nginx HTTP/2 + HSTS + www redirect.
# Every step backs up and rolls back on its own. Run from the repo root:
#   bash scripts/deploy-20260930-perf.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== 1/3 API: map clusters"
bash scripts/deploy-api-hotfix.sh server/parts/02d-shop-delivery-map.js

echo "== 2/3 shop"
bash scripts/deploy-shop-files.sh app/checkout/page.tsx components/DeliveryMapButton.tsx app/globals.css \
  components/ConsentCheck.tsx components/CookieBanner.tsx components/Footer.tsx components/LegalDoc.tsx \
  app/brands/BrandsSearch.tsx components/BrandsGrid.tsx

echo "== 3/3 nginx: HTTP/2, HSTS, www -> magicvibes.ru"
bash scripts/nginx-magicvibes-h2.sh
