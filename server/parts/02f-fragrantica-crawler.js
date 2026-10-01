// Фоновый обход fragrantica.ru для каталога «Фрагрантика» (только worker).
//
// Один запрос за шаг, FRAGRANTICA_CRAWL_RPS запросов в секунду (деф. 2). Порядок шагов:
//   1. ароматы, которые открыли в UI без деталей (detail_wanted_at) — сразу;
//   2. индекс брендов /designers-1..N/ — раз в FRAGRANTICA_INDEX_DAYS (деф. 7);
//   3. страницы брендов (сначала крупные), повторно — раз в FRAGRANTICA_BRAND_DAYS (деф. 14);
//   4. страницы ароматов без деталей (сначала крупные бренды и новинки).
// На 403/429/5xx обход замирает с растущей паузой (1 мин → 30 мин), состояние — в fragrantica_state.
// Выключатель: FRAGRANTICA_CRAWL_ENABLED=false; пауза из UI — POST /api/fragrantica/crawler.

const fragranticaCrawlEnabled = process.env.FRAGRANTICA_CRAWL_ENABLED !== "false";
const fragranticaCrawlDelayMs = Math.max(150, Math.round(1000 / Math.max(0.1, Number(process.env.FRAGRANTICA_CRAWL_RPS || 2) || 2)));
const fragranticaIndexDays = Math.max(1, Number(process.env.FRAGRANTICA_INDEX_DAYS || 7) || 7);
const fragranticaBrandDays = Math.max(1, Number(process.env.FRAGRANTICA_BRAND_DAYS || 14) || 14);
const fragranticaIdleDelayMs = 10 * 60_000;

let fragranticaCrawlTimer = null;
let fragranticaCrawlRunning = false;
let fragranticaCrawlStatusWrittenAt = 0;
const fragranticaCrawlStatus = {
  enabled: fragranticaCrawlEnabled,
  delayMs: fragranticaCrawlDelayMs,
  lastStep: null,
  lastStepAt: null,
  lastError: null,
  failures: 0,
  blockedUntil: null,
  counters: { index: 0, brands: 0, details: 0, errors: 0 },
};

async function fragranticaCrawlNextWanted(prisma) {
  const rows = await prisma.$queryRawUnsafe(
    `SELECT id, url FROM fragrantica_perfumes WHERE detail_at IS NULL AND detail_wanted_at IS NOT NULL ORDER BY detail_wanted_at LIMIT 1`,
  );
  return rows[0] || null;
}

async function fragranticaCrawlStepIndex(state) {
  const due = !state.indexDoneAt || Date.now() - Date.parse(state.indexDoneAt) > fragranticaIndexDays * 86_400_000;
  if (!due) return null;
  const page = Math.max(1, Number(state.indexPage) || 1);
  const html = await fetchFragranticaHtml(`https://www.fragrantica.ru/designers-${page}/`);
  const { brands, pages } = parseFragranticaDesignersIndex(html);
  await upsertFragranticaBrands(brands);
  const lastPage = pages.length ? Math.max(...pages) : page;
  fragranticaCrawlStatus.counters.index += 1;
  if (page >= lastPage || !brands.length) {
    await writeFragranticaState("crawler", { indexPage: 1, indexDoneAt: new Date().toISOString(), indexPages: lastPage });
  } else {
    await writeFragranticaState("crawler", { indexPage: page + 1, indexPages: lastPage });
  }
  return { step: "index", page, brands: brands.length };
}

async function fragranticaCrawlStepBrand(prisma) {
  const rows = await prisma.$queryRawUnsafe(
    `SELECT slug, url FROM fragrantica_brands
     WHERE crawled_at IS NULL OR crawled_at < now() - make_interval(days => ${fragranticaBrandDays})
     ORDER BY crawled_at NULLS FIRST, perfume_count DESC NULLS LAST LIMIT 1`,
  );
  const brand = rows[0];
  if (!brand) return null;
  try {
    const html = await fetchFragranticaHtml(brand.url);
    const perfumes = parseFragranticaBrandPage(html, { brandSlug: brand.slug });
    await upsertFragranticaListedPerfumes(perfumes);
    await saveFragranticaBrandInfo(brand.slug, parseFragranticaBrandInfo(html));
    await prisma.$executeRawUnsafe(
      `UPDATE fragrantica_brands SET crawled_at = now(), error = NULL, perfume_count = COALESCE(NULLIF($2, 0), perfume_count) WHERE slug = $1`,
      brand.slug,
      perfumes.length,
    );
    fragranticaCrawlStatus.counters.brands += 1;
    return { step: "brand", slug: brand.slug, perfumes: perfumes.length };
  } catch (error) {
    if (error instanceof FragranticaHttpError && error.status === 404) {
      await prisma.$executeRawUnsafe(`UPDATE fragrantica_brands SET crawled_at = now(), error = $2 WHERE slug = $1`, brand.slug, "404");
      return { step: "brand", slug: brand.slug, missing: true };
    }
    throw error;
  }
}

async function fragranticaCrawlNextDetail(prisma) {
  const rows = await prisma.$queryRawUnsafe(
    `SELECT p.id, p.url FROM fragrantica_perfumes p
     LEFT JOIN fragrantica_brands b ON b.slug = p.brand_slug
     WHERE p.detail_at IS NULL AND p.detail_error IS NULL
     ORDER BY b.perfume_count DESC NULLS LAST, p.year DESC NULLS LAST, p.id DESC LIMIT 1`,
  );
  return rows[0] || null;
}

async function fragranticaCrawlStepDetail(row, reason) {
  try {
    const detail = await fetchAndStoreFragranticaPerfume(row.url);
    fragranticaCrawlStatus.counters.details += 1;
    return { step: reason, id: detail.id };
  } catch (error) {
    if (error instanceof FragranticaHttpError && error.status === 404) {
      await markFragranticaDetailError(row.id, "404");
      return { step: reason, id: row.id, missing: true };
    }
    if (!(error instanceof FragranticaHttpError)) {
      await markFragranticaDetailError(row.id, error?.message || String(error));
      return { step: reason, id: row.id, error: error?.message };
    }
    throw error;
  }
}

async function runFragranticaCrawlStep() {
  const prisma = await requireFragranticaTables();
  const state = await readFragranticaState("crawler");
  if (state.paused) return { step: "paused" };
  const wanted = await fragranticaCrawlNextWanted(prisma);
  if (wanted) return fragranticaCrawlStepDetail(wanted, "wanted");
  const index = await fragranticaCrawlStepIndex(state);
  if (index) return index;
  const brand = await fragranticaCrawlStepBrand(prisma);
  if (brand) return brand;
  const detail = await fragranticaCrawlNextDetail(prisma);
  if (detail) return fragranticaCrawlStepDetail(detail, "detail");
  return { step: "idle" };
}

function scheduleFragranticaCrawl(delayMs = fragranticaCrawlDelayMs) {
  if (!fragranticaCrawlEnabled) return;
  if (fragranticaCrawlTimer) clearTimeout(fragranticaCrawlTimer);
  fragranticaCrawlTimer = setTimeout(async () => {
    let next = fragranticaCrawlDelayMs;
    if (fragranticaCrawlRunning) return scheduleFragranticaCrawl(next);
    fragranticaCrawlRunning = true;
    try {
      const result = await runFragranticaCrawlStep();
      fragranticaCrawlStatus.lastStep = result;
      fragranticaCrawlStatus.lastStepAt = new Date().toISOString();
      fragranticaCrawlStatus.failures = 0;
      fragranticaCrawlStatus.blockedUntil = null;
      if (result.step === "idle" || result.step === "paused") next = result.step === "idle" ? fragranticaIdleDelayMs : 30_000;
    } catch (error) {
      fragranticaCrawlStatus.counters.errors += 1;
      fragranticaCrawlStatus.failures += 1;
      fragranticaCrawlStatus.lastError = error?.message || String(error);
      next = Math.min(30 * 60_000, 60_000 * 2 ** Math.min(5, fragranticaCrawlStatus.failures - 1));
      fragranticaCrawlStatus.blockedUntil = new Date(Date.now() + next).toISOString();
      logger.warn("fragrantica crawl step failed", { detail: fragranticaCrawlStatus.lastError, retryInMs: next });
    } finally {
      fragranticaCrawlRunning = false;
      // The API process shows this on the page; every 10 s is enough.
      if (Date.now() - fragranticaCrawlStatusWrittenAt > 10_000 || next > fragranticaCrawlDelayMs) {
        fragranticaCrawlStatusWrittenAt = Date.now();
        writeFragranticaState("crawler_status", { ...fragranticaCrawlStatus, at: new Date().toISOString() }).catch(() => {});
      }
    }
    scheduleFragranticaCrawl(next);
  }, Math.max(100, Number(delayMs) || fragranticaCrawlDelayMs));
  fragranticaCrawlTimer.unref?.();
}
