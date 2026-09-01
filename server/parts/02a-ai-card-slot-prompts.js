// Slot definitions for the 6-image product card generation pipeline.
// Each slot is designed to produce a VISUALLY DISTINCT image via image-edits API.
// Prompts must describe the SCENE AROUND the product — what to add/change — since
// the model keeps the product bottle from the source image.

const CARD_SLOTS = [
  { id: "hero",      name: "Hero shot",     order: 1, requiresProduct: true,  type: "ai"           },
  { id: "dark",      name: "Dark studio",   order: 2, requiresProduct: true,  type: "ai"           },
  { id: "lifestyle", name: "Lifestyle",     order: 3, requiresProduct: true,  type: "ai"           },
  { id: "gift",      name: "Подарочный",    order: 4, requiresProduct: true,  type: "ai"           },
  { id: "detail",    name: "Макро",         order: 5, requiresProduct: true,  type: "ai"           },
  { id: "pyramid",   name: "Пирамида нот",  order: 6, requiresProduct: false, type: "infographic", builder: "pyramid"  },
  { id: "original",  name: "100% Оригинал", order: 7, requiresProduct: false, type: "infographic", builder: "original" },
];

function detectCharacter(name = "", brand = "", accords = []) {
  const text = `${name} ${brand} ${accords.join(" ")}`.toLowerCase();
  if (/\b(oud|уд|aoud|agarwood|agar|woody oud)\b/.test(text)) return "oriental";
  if (/\b(rose|roses|fleur|floral|flower|flora|jasmine|жасмин|тубероза|tuberose|iris|ирис|peony|пион|lily|лилия|цветочный)\b/.test(text)) return "floral";
  if (/\b(aqua|ocean|marine|sea|water|водный|oceanic|fresh)\b/.test(text)) return "fresh";
  if (/\b(citrus|bergamot|lemon|lime|citron|grapefruit|orange|mandarin)\b/.test(text)) return "citrus";
  if (/\b(wood|cedar|sandalwood|vetiver|bois|patchouli|пачули|forest|дерево)\b/.test(text)) return "woody";
  if (/\b(vanilla|caramel|gourmand|sweet|honey|chocolate|sugar|ваниль|сладкий)\b/.test(text)) return "gourmand";
  if (/\b(amber|ambre|мускус|musk|oriental|восточный|spice|spicy|пряный)\b/.test(text)) return "oriental";
  const nicheBrands = ["marc-antoine", "barrois", "mancera", "xerjoff", "amouage", "initio", "nishane", "orto parisi", "bdk", "memo", "ojar", "parle moi"];
  if (nicheBrands.some((b) => text.includes(b))) return "oriental";
  return "floral";
}

function pyramidNotes(fragranceData, character) {
  const defaults = {
    floral:    { top: "bergamot, lemon, pink pepper", mid: "rose, jasmine, peony",            base: "musk, sandalwood, amber" },
    oriental:  { top: "saffron, cardamom, pepper",    mid: "oud wood, rose, incense",         base: "amber, musk, sandalwood" },
    fresh:     { top: "grapefruit, mint, sea salt",   mid: "jasmine, white tea, muguet",      base: "white musk, cedar, amber" },
    citrus:    { top: "bergamot, lemon, mandarin",    mid: "neroli, magnolia, jasmine",        base: "musk, amber, vetiver" },
    woody:     { top: "eucalyptus, pepper, cardamom", mid: "cedarwood, vetiver, patchouli",   base: "sandalwood, oakmoss, amber" },
    gourmand:  { top: "mandarin, grapefruit, pepper", mid: "vanilla orchid, caramel, tonka",  base: "sandalwood, musk, benzoin" },
  };
  const d = defaults[character] || defaults.floral;
  return {
    top:  fragranceData?.topNotes?.slice(0, 3).join(", ")    || d.top,
    mid:  fragranceData?.middleNotes?.slice(0, 3).join(", ") || d.mid,
    base: fragranceData?.baseNotes?.slice(0, 3).join(", ")   || d.base,
  };
}

// Per-character scene definitions — very explicit for image edits
const SCENE = {
  floral: {
    heroBg:      "pure white-to-soft-cream gradient studio background",
    heroLight:   "large softbox from the upper-left, one gentle shadow to the lower-right",
    lifestyle:   "surrounded by scattered fresh rose petals and small white flower blooms on ivory linen fabric, shot from a 35° side angle",
    darkBg:      "deep midnight blue gradient",
    giftProps:   "pale pink satin ribbon, white tissue paper, tiny dried flower buds",
    pyramidBg:   "white marble surface",
    detailBokeh: "blurred soft pink and white gradient bokeh",
  },
  oriental: {
    heroBg:      "pure warm black-to-deep-charcoal gradient studio background",
    heroLight:   "single narrow spotlight from directly above, creating a star-burst reflection on the bottle cap",
    lifestyle:   "placed on a rich gold-veined black marble slab, wisps of smoke curling behind, golden amber side-lighting at 15° angle",
    darkBg:      "absolute jet-black background",
    giftProps:   "deep burgundy velvet fabric, gold metallic ribbon, small resin amber beads",
    pyramidBg:   "deep navy velvet surface",
    detailBokeh: "blurred deep gold and black bokeh with tiny specular highlights",
  },
  fresh: {
    heroBg:      "pure white-to-pale-sky-blue gradient studio background",
    heroLight:   "bright even studio lighting from two sides, crisp white highlights",
    lifestyle:   "placed on a wet pebble surface with small water droplets around it, mist in the background, cool blue-white light from the left",
    darkBg:      "deep ocean blue gradient",
    giftProps:   "aqua-blue ribbon, white cotton tissue, eucalyptus leaves",
    pyramidBg:   "white frosted glass surface",
    detailBokeh: "blurred sky blue and silver bokeh",
  },
  citrus: {
    heroBg:      "pure white-to-warm-cream gradient studio background",
    heroLight:   "bright cheerful studio lighting from upper-right, energetic feel",
    lifestyle:   "surrounded by halved grapefruit, bergamot and lemon slices on white marble, bright natural daylight from the left",
    darkBg:      "warm charcoal-brown gradient",
    giftProps:   "bright orange satin ribbon, kraft paper, dried citrus wheel decorations",
    pyramidBg:   "white marble with scattered citrus zest",
    detailBokeh: "blurred warm orange and yellow gradient bokeh",
  },
  woody: {
    heroBg:      "warm light grey-to-stone gradient studio background",
    heroLight:   "warm directional key light from the right, mimicking late afternoon sun",
    lifestyle:   "placed on a slice of raw cedar wood surrounded by dried moss, pebbles and cedarwood chips, warm earthy side-light",
    darkBg:      "deep dark charcoal brown gradient",
    giftProps:   "dark olive ribbon, kraft paper wrapping, dried cedar bark slice",
    pyramidBg:   "dark wood plank surface",
    detailBokeh: "blurred warm brown and stone grey bokeh",
  },
  gourmand: {
    heroBg:      "warm cream-to-vanilla gradient studio background",
    heroLight:   "warm soft candlelight-style light from the right, cozy feel",
    lifestyle:   "placed next to a vanilla pod, caramel glass jar and cinnamon sticks on a dark walnut surface, warm golden candlelight",
    darkBg:      "deep warm espresso-brown gradient",
    giftProps:   "cream satin ribbon, gold foil paper, vanilla pod and cinnamon stick accents",
    pyramidBg:   "dark walnut wood surface",
    detailBokeh: "blurred warm caramel and cream bokeh",
  },
};

function buildCardSlotPrompts(product, options = {}) {
  const name = cleanText(product?.name || "");
  const brand = cleanText(product?.brand || "");
  const fragranceData = options.fragranceData || null;

  const character = fragranceData?.accords?.length
    ? detectCharacter(name, brand, fragranceData.accords)
    : detectCharacter(name, brand);

  const sc = SCENE[character] || SCENE.floral;
  const notes = pyramidNotes(fragranceData, character);

  // The source image shows the exact bottle. Prompts describe the SCENE AROUND it.
  // Very explicit camera/lighting/set directions force the model to render differently.

  const PROMPTS = {
    hero: [
      `PRODUCT PHOTO: Keep the perfume bottle exactly as shown in the reference — same shape, cap, label, color. `,
      `BACKGROUND: Replace current background with ${sc.heroBg}. Completely clean, no props. `,
      `LIGHTING: ${sc.heroLight}. Realistic crisp shadows. `,
      `COMPOSITION: Bottle dead-center of frame. Bottle fills 70% of frame height. Portrait-style centered composition. `,
      `STYLE: High-end luxury beauty product photography, like Dior or Chanel catalogue. Photorealistic. No text, no people, no extra objects.`,
    ].join(""),

    dark: [
      `PRODUCT PHOTO: Keep the perfume bottle exactly as shown — same shape, label, color. `,
      `BACKGROUND: Replace entirely with ${sc.darkBg}. Nothing else visible. `,
      `LIGHTING: Extremely dramatic single narrow rim-light from the LEFT side only, creating a bright glowing edge on the left and leaving the right in near-darkness. `,
      `SURFACE: Place bottle on a surface that reflects softly — a pool of black lacquered wood or mirror surface. `,
      `COMPOSITION: Bottle slightly off-center to the right, vertical format feel. `,
      `STYLE: Editorial luxury fragrance advertising, dark moody atmosphere. Photorealistic. No text, no people.`,
    ].join(""),

    lifestyle: [
      `PRODUCT PHOTO: Keep the perfume bottle exactly as shown. `,
      `SCENE: ${sc.lifestyle}. `,
      `COMPOSITION: Shot from approximately 30-35° above horizontal (three-quarter view), NOT overhead. `,
      `DEPTH OF FIELD: Background slightly blurred while props in foreground remain sharp. `,
      `STYLE: Premium lifestyle product still-life photography, editorial quality. Photorealistic. No text, no people.`,
    ].join(""),

    gift: [
      `PRODUCT PHOTO: Keep the perfume bottle exactly as shown. `,
      `SCENE: A luxury gift presentation setup. Arrange around the bottle: ${sc.giftProps}. `,
      `BACKGROUND: Softly blurred gradient matching the ribbon color family, with a gentle bokeh sparkle effect. `,
      `LIGHTING: Warm soft diffused light from above, creating festive gift-photography mood. `,
      `COMPOSITION: Slightly angled composition, props partially visible around the central bottle. `,
      `STYLE: Luxury gift photography for premium fragrance e-commerce. Photorealistic. No text, no people.`,
    ].join(""),

    pyramid: [
      `BOTANICAL FLAT-LAY PHOTOGRAPH — no perfume bottle in this image. `,
      `SCENE: A perfect overhead (90° top-down) flat-lay photograph on ${sc.pyramidBg}. `,
      `ARRANGEMENT: Triangle/pyramid composition of real aromatic ingredients: `,
      `TOP of triangle: ${notes.top} (real objects — fruits, flowers, spices). `,
      `MIDDLE of triangle: ${notes.mid}. `,
      `BASE of triangle: ${notes.base}. `,
      `LIGHTING: Flat diffused studio light from directly above, even exposure, soft shadows. `,
      `STYLE: Premium ingredient/botanical flat-lay photography for luxury fragrance brand. Photorealistic, editorial quality. No text, no bottle, no people.`,
    ].join(""),

    detail: [
      `PRODUCT PHOTO: Keep the perfume bottle exactly as shown. `,
      `CAMERA MOVE: Extreme close-up macro shot — crop tightly so the TOP THIRD of the bottle (spray nozzle, cap, and immediately below) fills 90% of the frame. The bottom of the bottle is cropped out. `,
      `FOCUS: Tack-sharp focus on the metal spray nozzle tip and the cap rim. Extreme shallow depth of field — everything below the label fades into smooth bokeh. `,
      `BACKGROUND: ${sc.detailBokeh} fills the entire background. `,
      `LIGHTING: Soft macro-photography lighting revealing glass texture and metal finish. Tiny specular highlight on the nozzle. `,
      `STYLE: Ultra-premium luxury macro product photography. No text, no people.`,
    ].join(""),
  };

  return CARD_SLOTS.map((slot) => ({
    slotId: slot.id,
    slotName: slot.name,
    order: slot.order,
    requiresProduct: slot.requiresProduct,
    prompt: (PROMPTS[slot.id] || "").trim(),
    meta: { character },
  }));
}

global.detectCharacter = detectCharacter;
global.pyramidNotes = pyramidNotes;
global.buildCardSlotPrompts = buildCardSlotPrompts;
