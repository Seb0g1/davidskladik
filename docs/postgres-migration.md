# PostgreSQL migration phase 1

This phase prepares PostgreSQL without switching production logic away from JSON files yet.
PriceMaster MySQL remains the external source for suppliers, articles, prices, and stock.

## Runtime modes

- No `DATABASE_URL`: the app keeps using the current JSON storage.
- `DATABASE_URL` + `DB_MODE=postgres`: PostgreSQL is available for the next migration phases.
- `JSON_FALLBACK_ENABLED=true`: keep JSON fallback during the transition.

## Server setup

```bash
sudo apt update
sudo apt install -y postgresql redis-server
sudo systemctl enable --now postgresql redis-server
sudo -u postgres psql
```

```sql
CREATE USER davidsklad WITH PASSWORD 'replace-with-strong-password';
CREATE DATABASE davidsklad OWNER davidsklad;
\q
```

Add to `.env`:

```env
DATABASE_URL=postgresql://davidsklad:replace-with-strong-password@127.0.0.1:5432/davidsklad
DB_MODE=postgres
JSON_FALLBACK_ENABLED=true
REDIS_URL=redis://127.0.0.1:6379
BULLMQ_ENABLED=true
```

## Apply schema and seed JSON data

```bash
cd /var/www/davidsklad/davidskladik
npm install --omit=dev
npm run db:generate
npm run db:migrate
npm run db:seed-from-json -- --skip-snapshot
```

The first seed migrates:

- `data/app-users.json`
- `data/app-settings.json`
- `data/personal-warehouse.json`
- `data/audit-log.jsonl`
- `data/price-retry-queue.json`

To include the large PriceMaster snapshot later:

```bash
npm run db:seed-from-json
```

For a quick count without writing to PostgreSQL:

```bash
npm run db:seed-from-json -- --dry-run --skip-snapshot
```

## Next phases

1. Switch users/settings/audit/retry queue reads and writes to PostgreSQL with JSON fallback.
2. Move warehouse products and links to PostgreSQL.
3. Move price/stock pushes to BullMQ workers.
4. Add PriceMaster snapshot refresh jobs.
5. Add UI status for queue items and delayed Ozon per-item limit retries.
