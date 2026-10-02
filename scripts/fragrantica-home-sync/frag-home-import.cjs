// Приёмник страниц Фрагрантики, скачанных с домашнего ПК (Cloudflare не пускает серверы).
//   node frag-home-import.cjs queue <limit>   — id+url ароматов без деталей (сначала открытые в UI, потом из PriceMaster)
//   node frag-home-import.cjs save < batch.json — {details:[...], errors:[{id,message}]} → fragrantica_perfumes
process.chdir("/var/www/davidsklad/davidskladik");
require("/var/www/davidsklad/davidskladik/node_modules/dotenv").config({ quiet: true });
const { PrismaClient } = require("/var/www/davidsklad/davidskladik/node_modules/@prisma/client");
const prisma = new PrismaClient();

const searchText = (...parts) => parts.join(" ").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/\s+/g, " ").trim();

async function queue(limit) {
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id, url FROM fragrantica_perfumes
      WHERE detail_at IS NULL AND detail_error IS NULL
      ORDER BY (detail_wanted_at IS NOT NULL) DESC, detail_wanted_at DESC NULLS LAST,
               COALESCE(pm_rows, 0) DESC, year DESC NULLS LAST, id DESC
      LIMIT $1`,
    limit,
  );
  const [stats] = await prisma.$queryRawUnsafe(
    `SELECT count(*) FILTER (WHERE detail_at IS NULL AND detail_error IS NULL)::int AS left,
            count(*) FILTER (WHERE detail_at IS NULL AND detail_error IS NULL AND pm_rows > 0)::int AS pm_left,
            count(*) FILTER (WHERE detail_at IS NOT NULL)::int AS done FROM fragrantica_perfumes`,
  );
  process.stdout.write(JSON.stringify({ stats, items: rows.map((r) => ({ id: Number(r.id), url: r.url })) }));
}

async function save() {
  let input = "";
  for await (const chunk of process.stdin) input += chunk;
  const { details = [], errors = [] } = JSON.parse(input);
  let saved = 0;
  for (const d of details) {
    if (!d?.id || !d?.name) continue;
    await prisma.$executeRawUnsafe(
      `INSERT INTO fragrantica_perfumes (id, url, brand_slug, brand, name, gender, year, search, votes, rating, detail, detail_at, detail_error)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, now(), NULL)
       ON CONFLICT (id) DO UPDATE SET url = EXCLUDED.url, brand_slug = EXCLUDED.brand_slug, brand = EXCLUDED.brand,
         name = EXCLUDED.name, gender = COALESCE(NULLIF(EXCLUDED.gender, ''), fragrantica_perfumes.gender),
         year = COALESCE(EXCLUDED.year, fragrantica_perfumes.year), search = EXCLUDED.search, votes = EXCLUDED.votes,
         rating = EXCLUDED.rating, detail = EXCLUDED.detail, detail_at = now(), detail_error = NULL, detail_wanted_at = NULL, updated_at = now()`,
      d.id, d.url, d.brandSlug || "", d.brand || "", d.name || "", d.gender || "", d.year || null,
      searchText(d.brand, d.name), d.votes || null, d.rating || null, JSON.stringify(d),
    );
    saved += 1;
  }
  for (const e of errors) {
    await prisma.$executeRawUnsafe(
      `UPDATE fragrantica_perfumes SET detail_error = $2, detail_wanted_at = NULL, updated_at = now() WHERE id = $1 AND detail_at IS NULL`,
      e.id, String(e.message || "").slice(0, 500),
    );
  }
  process.stdout.write(JSON.stringify({ saved, errors: errors.length }));
}

(async () => {
  if (process.argv[2] === "queue") await queue(Math.min(2000, Number(process.argv[3]) || 200));
  else if (process.argv[2] === "save") await save();
  else throw new Error("usage: queue <limit> | save");
  await prisma.$disconnect();
})().catch(async (e) => { console.error(e?.message || e); await prisma.$disconnect(); process.exit(1); });
