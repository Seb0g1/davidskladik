"use strict";
// Standalone script: generates fragrance notes for all products without notes.
// Usage: node scripts/run-fragrance-notes-batch.cjs
// Can be killed and re-run safely — skips already-filled products.

require("dotenv").config();
const { PrismaClient } = require("@prisma/client");
const OpenAI = require("openai").default || require("openai");

const prisma = new PrismaClient();
const PAGE = 20;
const DELAY_MS = 250;

async function getOpenAiClient() {
  // Try env first, fall back to DB app_settings
  let apiKey = process.env.OPENAI_API_KEY;
  let baseURL = process.env.OPENAI_BASE_URL || undefined;
  let model = process.env.OPENAI_TEXT_MODEL;

  if (!apiKey) {
    const rows = await prisma.$queryRaw`SELECT value FROM app_settings WHERE key = 'app' LIMIT 1`;
    const appSettings = rows[0]?.value;
    const ai = appSettings?.ai || {};
    apiKey = ai.apiKey;
    baseURL = ai.baseUrl || baseURL;
    model = model || ai.textModel;
  }

  if (!apiKey) throw new Error("OpenAI API key not found in env or DB app_settings");
  return { client: new OpenAI({ apiKey, baseURL }), model: model || "gpt-4o-mini" };
}

async function generateNotes(client, model, brand, name) {
  const prompt = `You are a fragrance expert. Return JSON with the fragrance notes for "${name}" by ${brand}.
JSON format: {"topNotes":[],"middleNotes":[],"baseNotes":[],"accords":[],"gender":"male|female|unisex","seasons":["spring|summer|fall|winter"]}
- topNotes: 2-4 opening notes (e.g. "Bergamot","Lemon")
- middleNotes: 2-4 heart notes
- baseNotes: 2-4 base/dry-down notes
- accords: 2-5 overall accords (e.g. "Woody","Aromatic","Spicy")
- gender: target gender
- seasons: recommended seasons
Return ONLY valid JSON, no other text.`;

  const completion = await client.chat.completions.create({
    model,
    messages: [{ role: "user", content: prompt }],
    max_tokens: 400,
    temperature: 0.2,
    response_format: { type: "json_object" },
  });

  const text = completion.choices[0]?.message?.content || "{}";
  const parsed = JSON.parse(text);
  if (!parsed || !Array.isArray(parsed.topNotes)) return null;
  return {
    topNotes: parsed.topNotes.filter(Boolean).slice(0, 6),
    middleNotes: (parsed.middleNotes || []).filter(Boolean).slice(0, 6),
    baseNotes: (parsed.baseNotes || []).filter(Boolean).slice(0, 6),
    accords: (parsed.accords || []).filter(Boolean).slice(0, 8),
    gender: parsed.gender || "unisex",
    seasons: Array.isArray(parsed.seasons) ? parsed.seasons.filter(Boolean) : [],
    source: "llm",
    generatedAt: new Date().toISOString(),
  };
}

async function main() {
  const { client, model } = await getOpenAiClient();
  console.log(`Using model: ${model}`);

  const countRows = await prisma.$queryRaw`
    SELECT COUNT(*)::int AS count FROM warehouse_products
    WHERE archived = false AND brand IS NOT NULL AND fragrance_notes IS NULL
  `;
  const total = Number(countRows[0]?.count ?? 0);
  console.log(`Total without notes: ${total}`);
  if (!total) { console.log("Nothing to do."); return; }

  let done = 0;
  let errors = 0;
  const startedAt = Date.now();

  while (true) {
    const products = await prisma.$queryRaw`
      SELECT id, brand, name FROM warehouse_products
      WHERE archived = false AND brand IS NOT NULL AND fragrance_notes IS NULL
      ORDER BY id
      LIMIT ${PAGE}
    `;
    if (!products.length) break;

    for (const p of products) {
      try {
        const notes = await generateNotes(client, model, p.brand, p.name);
        if (notes) {
          await prisma.warehouseProduct.update({ where: { id: p.id }, data: { fragranceNotes: notes } });
          done++;
        } else {
          errors++;
          done++;
        }
      } catch (err) {
        errors++;
        done++;
        if (process.env.DEBUG) console.error(`  ERR ${p.brand} ${p.name}: ${err.message}`);
      }

      const elapsed = Math.round((Date.now() - startedAt) / 1000);
      const pct = Math.round((done / total) * 100);
      const eta = done > 0 ? Math.round((total - done) * (elapsed / done)) : "?";
      process.stdout.write(`\r  ${done}/${total} (${pct}%) | errors: ${errors} | elapsed: ${elapsed}s | ETA: ${eta}s   `);

      await new Promise((r) => setTimeout(r, DELAY_MS));
    }
  }

  console.log(`\nDone. ${done} processed, ${errors} errors.`);
  await prisma.$disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
