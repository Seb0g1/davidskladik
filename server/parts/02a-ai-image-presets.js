function publicAiImageStudioPresets() {
  return aiImageStudioPresets.map(({ id, label, prompt }) => ({ id, label, prompt }));
}

function normalizeAiImageStudioPresetList(input) {
  const raw = Array.isArray(input) ? input : [];
  const byId = new Map(aiImageStudioPresets.map((preset) => [preset.id, preset]));
  const selected = raw
    .map((item) => {
      if (typeof item === "string") return byId.get(cleanText(item)) || null;
      if (!item || typeof item !== "object") return null;
      const id = cleanText(item.id || item.presetId);
      const existing = byId.get(id);
      if (existing) return existing;
      const prompt = cleanText(item.prompt);
      if (!prompt) return null;
      return {
        id: id || `custom-${crypto.createHash("sha1").update(prompt).digest("hex").slice(0, 8)}`,
        label: cleanText(item.label) || "Custom",
        prompt,
      };
    })
    .filter(Boolean);
  return selected.length ? selected.slice(0, 5) : aiImageStudioPresets.slice(0, 5);
}
