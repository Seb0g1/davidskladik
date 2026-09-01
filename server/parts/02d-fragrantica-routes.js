// GET /api/fragrantica/lookup?brand=...&name=...
// Returns cached Fragrantica data (notes, accords, gender) for a fragrance.

app.get("/api/fragrantica/lookup", async (request, response, next) => {
  try {
    const { brand, name } = request.query;
    if (!brand || !name) {
      return response.status(400).json({ error: "brand and name are required" });
    }

    const data = await lookupFragranticaData(String(brand).trim(), String(name).trim());
    if (!data) {
      return response.status(404).json({ error: "not found", brand, name });
    }

    response.json(data);
  } catch (error) {
    next(error);
  }
});
