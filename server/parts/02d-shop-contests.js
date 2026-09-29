// Shop contests (magicvibes.ru): created in the admin, shown on the home page while active
// and inside their date window. Stored in app settings under "shopContests", like banners.

const SHOP_CONTESTS_KEY = "shopContests";

async function readShopContests() {
  const appSettings = await readAppSettings();
  return Array.isArray(appSettings[SHOP_CONTESTS_KEY]) ? appSettings[SHOP_CONTESTS_KEY] : [];
}

function _contestDate(value) {
  const s = cleanText(value || "");
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

/** Normalises admin input; only known fields are kept. */
function _contestFields(body, existing = {}) {
  const pick = (k, fn) => (body[k] !== undefined ? fn(body[k]) : existing[k]);
  const steps = pick("steps", (v) => (Array.isArray(v) ? v : [])
    .map((s) => ({ title: cleanText(s?.title || "").slice(0, 80), desc: cleanText(s?.desc || "").slice(0, 200) }))
    .filter((s) => s.title || s.desc)
    .slice(0, 5));
  return {
    title: pick("title", (v) => cleanText(v || "").slice(0, 120)) || "",
    badge: pick("badge", (v) => cleanText(v || "").slice(0, 40)) || "",
    description: pick("description", (v) => cleanText(v || "").slice(0, 1000)) || "",
    prize: pick("prize", (v) => cleanText(v || "").slice(0, 200)) || "",
    imageUrl: pick("imageUrl", (v) => cleanText(v || "")) || "",
    linkUrl: pick("linkUrl", (v) => cleanText(v || "")) || "",
    linkText: pick("linkText", (v) => cleanText(v || "").slice(0, 40)) || "",
    rulesUrl: pick("rulesUrl", (v) => cleanText(v || "")) || "",
    startDate: pick("startDate", _contestDate) || "",
    endDate: pick("endDate", _contestDate) || "",
    steps: steps || [],
    active: pick("active", (v) => Boolean(v)) ?? true,
  };
}

/** Moscow calendar date, so a contest ending "2026-09-30" stays visible through that whole day. */
function _todayMsk() {
  return new Date(Date.now() + 3 * 3600 * 1000).toISOString().slice(0, 10);
}

function isContestLive(c, today = _todayMsk()) {
  if (!c.active) return false;
  if (c.startDate && today < c.startDate) return false;
  if (c.endDate && today > c.endDate) return false;
  return true;
}

// ── Public ────────────────────────────────────────────────────────────────────
app.get("/api/shop/contests", shopCors, async (_request, response, next) => {
  try {
    const contests = (await readShopContests())
      .filter((c) => isContestLive(c))
      .sort((a, b) => (a.order ?? 0) - (b.order ?? 0))
      .map(({ order, createdAt, ...c }) => c);
    response.json(contests);
  } catch (error) { next(error); }
});

// ── Admin ─────────────────────────────────────────────────────────────────────
app.get("/api/shop/admin/contests", requireAdmin, async (_request, response, next) => {
  try {
    const today = _todayMsk();
    response.json((await readShopContests()).map((c) => ({ ...c, live: isContestLive(c, today) })));
  } catch (error) { next(error); }
});

app.post("/api/shop/admin/contests", requireAdmin, async (request, response, next) => {
  try {
    const fields = _contestFields(request.body || {});
    if (!fields.title) return response.status(400).json({ error: "Укажите название конкурса" });
    const contests = await readShopContests();
    const contest = { id: nanoid8(), ...fields, order: contests.length, createdAt: new Date().toISOString() };
    contests.push(contest);
    await writeShopData(SHOP_CONTESTS_KEY, contests);
    response.json({ ok: true, contest });
  } catch (error) { next(error); }
});

app.put("/api/shop/admin/contests/:id", requireAdmin, async (request, response, next) => {
  try {
    const contests = await readShopContests();
    const idx = contests.findIndex((c) => c.id === request.params.id);
    if (idx === -1) return response.status(404).json({ error: "Конкурс не найден" });
    const fields = _contestFields(request.body || {}, contests[idx]);
    if (!fields.title) return response.status(400).json({ error: "Укажите название конкурса" });
    contests[idx] = { ...contests[idx], ...fields, id: contests[idx].id };
    await writeShopData(SHOP_CONTESTS_KEY, contests);
    response.json({ ok: true, contest: contests[idx] });
  } catch (error) { next(error); }
});

app.delete("/api/shop/admin/contests/:id", requireAdmin, async (request, response, next) => {
  try {
    const contests = await readShopContests();
    await writeShopData(SHOP_CONTESTS_KEY, contests.filter((c) => c.id !== request.params.id));
    response.json({ ok: true });
  } catch (error) { next(error); }
});
