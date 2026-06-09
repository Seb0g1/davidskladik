function splitList(value) {
  if (Array.isArray(value)) return value.map(cleanText).filter(Boolean);
  return String(value || "")
    .split(/\r?\n|,/)
    .map(cleanText)
    .filter(Boolean);
}

function buildOzonManualProductItem(body = {}) {
  const extra = parseJsonField(body.extraJson, {});
  const item = {
    offer_id: cleanText(body.offerId || body.offer_id),
    name: cleanText(body.name),
    description: cleanText(body.description),
    category_id: Number(body.categoryId || body.category_id || 0),
    price: String(roundPrice(body.price)),
    old_price: body.oldPrice ? String(roundPrice(body.oldPrice)) : undefined,
    currency_code: cleanText(body.currencyCode || body.currency_code || "RUB"),
    vat: String(body.vat ?? "0"),
    barcode: cleanText(body.barcode),
    barcodes: splitList(body.barcodes),
    depth: Number(body.depth || 0),
    width: Number(body.width || 0),
    height: Number(body.height || 0),
    dimension_unit: cleanText(body.dimensionUnit || body.dimension_unit || "mm"),
    weight: Number(body.weight || 0),
    weight_unit: cleanText(body.weightUnit || body.weight_unit || "g"),
    primary_image: cleanText(body.primaryImage || body.primary_image),
    images: splitList(body.images),
    images360: splitList(body.images360),
    color_image: cleanText(body.colorImage || body.color_image),
    attributes: parseJsonField(body.attributesJson, []),
    complex_attributes: parseJsonField(body.complexAttributesJson, []),
    ...extra,
  };

  for (const key of Object.keys(item)) {
    if (item[key] === undefined || item[key] === "" || (Array.isArray(item[key]) && !item[key].length)) {
      delete item[key];
    }
  }

  const missing = [];
  if (!item.offer_id) missing.push("offer_id");
  if (!item.name) missing.push("name");
  if (!item.category_id) missing.push("category_id");
  if (!item.price || Number(item.price) <= 0) missing.push("price");
  if (!item.depth) missing.push("depth");
  if (!item.width) missing.push("width");
  if (!item.height) missing.push("height");
  if (!item.weight) missing.push("weight");

  return { item, missing, ready: missing.length === 0 };
}

function buildOzonWarehouseProductItem(product, overrides = {}) {
  const ozon = {
    ...(product.ozon || {}),
    ...(overrides.ozon || {}),
  };
  const body = {
    ...ozon,
    ...overrides,
    offerId: overrides.offerId || ozon.offerId || product.offerId,
    name: overrides.name || ozon.name || product.name,
    categoryId: overrides.categoryId || overrides.category_id || ozon.categoryId || ozon.category_id || ozon.descriptionCategoryId || ozon.description_category_id || ozon.marketCategoryId || ozon.market_category_id,
    attributesJson: overrides.attributesJson ?? ozon.attributes ?? [],
    complexAttributesJson: overrides.complexAttributesJson ?? ozon.complexAttributes ?? [],
    extraJson: overrides.extraJson ?? ozon.extra ?? {},
  };

  return buildOzonManualProductItem(body);
}

function buildOzonAiImagePrompt(product, promptOverride = "", options = {}) {
  const productName = cleanText(product?.name || product?.ozon?.name || product?.offerId || "товар");
  const template = cleanText(promptOverride) || ozonAiImageDefaultPrompt;
  let base = template.includes("{productName}")
    ? template.replaceAll("{productName}", productName)
    : `${template}\n\nНазвание товара: ${productName}`;
  const bottleOnlyInstruction = "Strict marketplace packshot rules: create exactly one standalone image. Do not create a collage, grid, contact sheet, split-screen, multi-panel layout, before/after layout, or multiple variants inside one image. Show one clean perfume bottle only. The full bottle must fit inside the square frame from cap to base, centered, upright, not cropped, with 10-15% empty margin on every side. Do not place the bottle on the far left or far right. Remove or ignore any box, outer packaging, cartons, bags, brochures, accessories, props, hands, faces, and all extra text. Do not create headlines, brand typography, marketing copy, icons, or labels outside the original bottle label. Do not hallucinate packaging, even if the source photo contains a box.";
  base = `${base}\n\n${bottleOnlyInstruction}`;
  const variantIndex = Number(options.variantIndex || 0);
  const variantTotal = Number(options.variantTotal || 0);
  if (!variantIndex || variantTotal <= 1) return base;
  const variantBriefs = [
    "Variant 1: clean white-background packshot, full centered bottle, natural shadow, no text outside the bottle label.",
    "Variant 2: premium soft-shadow packshot, full centered bottle, subtle reflection, safe margins, no text outside the bottle label.",
    "Variant 3: minimal light studio scene, full centered bottle, no props touching the product, no typography.",
    "Variant 4: clean ecommerce packshot with more air, full centered bottle, no logo or headline outside the original label.",
  ];
  return `${base}\n\n${variantBriefs[variantIndex - 1] || variantBriefs[0]}\nThis request generates only variant ${variantIndex} of ${variantTotal}; output exactly one final image, not all variants together. Change only lighting/background subtly. Never crop the bottle and never add text outside the original product label.`;
}
