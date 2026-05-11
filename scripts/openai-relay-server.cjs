/**
 * Минимальный relay для OpenAI Images API: запускайте на VPS в регионе, где OpenAI не блокирует API.
 *
 * На relay-хосте (EU/US и т.п.):
 *   OPENAI_API_KEY=sk-...
 *   OPENAI_RELAY_SECRET=длинный-случайный-секрет
 *   RELAY_PORT=8787
 *   node scripts/openai-relay-server.cjs
 *
 * На основном сервере (например, в РФ) в .env:
 *   OPENAI_RELAY_URL=https://ваш-relay.example.com/v1/openai-image-edit
 *   OPENAI_RELAY_SECRET=тот-же-секрет
 *   (OPENAI_API_KEY на основном сервере не нужен, если используете только relay)
 *
 * Защитите relay HTTPS (nginx/caddy) и ограничьте IP, если возможно.
 */
require("dotenv").config();
const crypto = require("crypto");
const express = require("express");
const OpenAI = require("openai");
const { toFile } = require("openai/uploads");

const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || "").trim();
const RELAY_SECRET = String(process.env.OPENAI_RELAY_SECRET || "").trim();
const PORT = Number(process.env.RELAY_PORT || 8787);

function supportsInputFidelity(model) {
  const normalized = String(model || "").trim().toLowerCase();
  return normalized && normalized !== "gpt-image-2";
}

function normalizeImageModel(model) {
  const normalized = String(model || "").trim();
  if (normalized.toLowerCase() === "gpt-image-1.5-high-fidelity") return "gpt-image-1.5";
  return normalized || "gpt-image-2";
}

function timingSafeEqual(a, b) {
  const first = Buffer.from(String(a));
  const second = Buffer.from(String(b));
  if (first.length !== second.length) return false;
  return crypto.timingSafeEqual(first, second);
}

if (!OPENAI_API_KEY || !RELAY_SECRET) {
  console.error("Нужны OPENAI_API_KEY и OPENAI_RELAY_SECRET в окружении relay-хоста.");
  process.exit(1);
}

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "32mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "openai-image-relay" });
});

app.post("/v1/openai-image-edit", async (req, res) => {
  try {
    const auth = String(req.headers.authorization || "");
    const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
    if (!timingSafeEqual(token, RELAY_SECRET)) {
      return res.status(401).json({ error: "unauthorized", code: "relay_unauthorized" });
    }

    const body = req.body || {};
    const prompt = String(body.prompt || "").trim();
    const sourceImageBase64 = String(body.sourceImageBase64 || "").trim();
    if (!prompt || !sourceImageBase64) {
      return res.status(400).json({ error: "Нужны поля prompt и sourceImageBase64.", code: "relay_bad_request" });
    }

    let buffer;
    try {
      buffer = Buffer.from(sourceImageBase64, "base64");
    } catch (_e) {
      return res.status(400).json({ error: "sourceImageBase64 не является валидным base64.", code: "relay_bad_base64" });
    }
    if (!buffer.length) {
      return res.status(400).json({ error: "Пустой sourceImageBase64.", code: "relay_empty_image" });
    }

    const sourceMimeType = String(body.sourceMimeType || "image/png").split(";")[0].trim().toLowerCase();
    const fileName =
      sourceMimeType.includes("jpeg") || sourceMimeType.includes("jpg")
        ? "source.jpg"
        : sourceMimeType.includes("webp")
          ? "source.webp"
          : "source.png";

    const image = [await toFile(buffer, fileName, { type: sourceMimeType || "image/png" })];
    const references = Array.isArray(body.referenceImages) ? body.referenceImages.slice(0, 4) : [];
    for (let index = 0; index < references.length; index += 1) {
      const ref = references[index] || {};
      const refBase64 = String(ref.base64 || ref.sourceImageBase64 || "").trim();
      if (!refBase64) continue;
      const refMime = String(ref.mimeType || ref.sourceMimeType || "image/png").split(";")[0].trim().toLowerCase();
      const refBuffer = Buffer.from(refBase64, "base64");
      if (!refBuffer.length) continue;
      const refFileName = String(ref.fileName || `reference-${index + 1}.${refMime.includes("webp") ? "webp" : refMime.includes("jpg") || refMime.includes("jpeg") ? "jpg" : "png"}`);
      image.push(await toFile(refBuffer, refFileName, { type: refMime || "image/png" }));
    }
    const client = new OpenAI({ apiKey: OPENAI_API_KEY });
    const model = normalizeImageModel(body.model || "gpt-image-2");
    const editRequest = {
      model,
      image: image.length === 1 ? image[0] : image,
      prompt,
      size: String(body.size || "1536x1024"),
      quality: String(body.quality || "auto"),
      output_format: String(body.output_format || "png"),
    };
    if (supportsInputFidelity(model) && body.input_fidelity) {
      editRequest.input_fidelity = body.input_fidelity === "low" ? "low" : "high";
    }
    const result = await client.images.edit(editRequest);

    const b64 = result?.data?.[0]?.b64_json;
    if (!b64) {
      return res.status(502).json({ error: "Пустой ответ OpenAI.", code: "relay_openai_empty" });
    }
    return res.json({ ok: true, b64_json: b64 });
  } catch (error) {
    const status = Number(error?.status) || Number(error?.statusCode) || 500;
    const safeStatus = status >= 400 && status < 600 ? status : 500;
    return res.status(safeStatus).json({
      error: error?.message || String(error),
      code: error?.code || "relay_error",
      detail: error?.error?.message || error?.message || String(error),
    });
  }
});

app.listen(PORT, () => {
  console.log(`openai-relay listening on http://127.0.0.1:${PORT} (POST /v1/openai-image-edit)`);
});
