// Отдельный процесс рендера фото аромата (пул в 02d-fragrantica-ozon-export.js): JS-часть рендера
// занимает поток целиком, поэтому несколько ароматов параллельно рисуются только в разных процессах.
// Сообщения: { id, source: Uint8Array, styles, perfume, extras } → { id, ok, out | error }.
const renderer = require("./perfume-render/index.cjs");

process.on("message", async (msg) => {
  if (!msg || typeof msg.id === "undefined") return;
  try {
    const out = await renderer.renderPerfumeCard({
      source: Buffer.from(msg.source),
      styles: msg.styles,
      perfume: msg.perfume,
      extras: msg.extras !== false,
    });
    process.send({ id: msg.id, ok: true, out });
  } catch (error) {
    process.send({ id: msg.id, ok: false, error: String(error?.message || error) });
  }
});

process.on("disconnect", () => process.exit(0));
