// Deterministic Sharp/SVG infographic card generators for product card pipelines.
// Two builders: fragrance pyramid (notes by layer) and authenticity badge (100% ОРИГИНАЛ).
// No AI API required — output is deterministic for the same inputs.

const PYRAMID_THEMES = {
  floral:   { bgTop: "#fff5f9", bgBottom: "#fce4ef", topColor: "#f9a8c9", midColor: "#e8538c", baseColor: "#9b1a4e", textColor: "#5a0e28", labelColor: "#fff" },
  oriental: { bgTop: "#1a0d00", bgBottom: "#2d1800", topColor: "#f0c060", midColor: "#c07a20", baseColor: "#7a4000", textColor: "#f0d090", labelColor: "#fff" },
  fresh:    { bgTop: "#eaf5ff", bgBottom: "#cce5f5", topColor: "#80c8e8", midColor: "#2890c0", baseColor: "#145878", textColor: "#1a4060", labelColor: "#fff" },
  citrus:   { bgTop: "#fffcf0", bgBottom: "#fff3d8", topColor: "#ffd848", midColor: "#f09010", baseColor: "#c07000", textColor: "#5a3800", labelColor: "#fff" },
  woody:    { bgTop: "#f5efe8", bgBottom: "#e8dcc8", topColor: "#d4b888", midColor: "#8b6040", baseColor: "#4a2c18", textColor: "#2a1808", labelColor: "#fff" },
  gourmand: { bgTop: "#fffbf5", bgBottom: "#fde8d0", topColor: "#f8c870", midColor: "#d47828", baseColor: "#8a3a10", textColor: "#3a1800", labelColor: "#fff" },
};

function escXml(s) {
  return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function noteLines(notesStr, maxChars) {
  const parts = String(notesStr || "").split(",").map((s) => s.trim()).filter(Boolean);
  const lines = [];
  let cur = "";
  for (const p of parts) {
    if (!cur) { cur = p; continue; }
    if ((cur + ", " + p).length <= maxChars) { cur += ", " + p; }
    else { lines.push(cur); cur = p; }
  }
  if (cur) lines.push(cur);
  return lines.slice(0, 2);
}

function buildPyramidSvg(productName, notes, character, S) {
  const theme = PYRAMID_THEMES[character] || PYRAMID_THEMES.floral;
  const cx = S / 2;

  // Pyramid geometry
  const apexX = cx, apexY = Math.round(S * 0.22);
  const bLx = Math.round(S * 0.07), bRx = Math.round(S * 0.93), bY = Math.round(S * 0.83);
  const totalH = bY - apexY;

  // 1/3 and 2/3 dividers
  const d1Lx = Math.round(apexX + (bLx - apexX) / 3);
  const d1Rx = Math.round(apexX + (bRx - apexX) / 3);
  const d1Y  = Math.round(apexY + totalH / 3);
  const d2Lx = Math.round(apexX + (bLx - apexX) * 2 / 3);
  const d2Rx = Math.round(apexX + (bRx - apexX) * 2 / 3);
  const d2Y  = Math.round(apexY + totalH * 2 / 3);

  // Vertical center of each section
  const topCy  = Math.round((apexY + d1Y) / 2);
  const midCy  = Math.round((d1Y + d2Y) / 2);
  const baseCy = Math.round((d2Y + bY) / 2);

  // Section text helper
  function sectionLines(label, nLines, cy, labelFs, noteFs, maxChars) {
    const lineH = Math.round(noteFs * 1.4);
    const totalTextH = labelFs + lineH * nLines.length;
    const startY = Math.round(cy - totalTextH / 2);
    let out = `<text x="${cx}" y="${startY + labelFs}" text-anchor="middle" font-family="Liberation Sans,DejaVu Sans,Arial,sans-serif" font-size="${labelFs}" font-weight="bold" fill="${theme.labelColor}" letter-spacing="1">${escXml(label)}</text>`;
    nLines.forEach((l, i) => {
      out += `\n  <text x="${cx}" y="${startY + labelFs + lineH * (i + 1)}" text-anchor="middle" font-family="Liberation Sans,DejaVu Sans,Arial,sans-serif" font-size="${noteFs}" fill="${theme.labelColor}" opacity="0.88">${escXml(l)}</text>`;
    });
    return out;
  }

  const topNotes  = noteLines(notes.top,  16); // narrow top section
  const midNotes  = noteLines(notes.mid,  28);
  const baseNotes = noteLines(notes.base, 38);

  const titleY  = Math.round(S * 0.085);
  const nameY   = Math.round(S * 0.125);
  const footerY = Math.round(S * 0.930);

  const titleFs  = Math.round(S * 0.034);
  const nameFs   = Math.round(S * 0.019);
  const footerFs = Math.round(S * 0.016);
  const topLabelFs  = Math.round(S * 0.013);
  const topNoteFs   = Math.round(S * 0.011);
  const midLabelFs  = Math.round(S * 0.017);
  const midNoteFs   = Math.round(S * 0.014);
  const baseLabelFs = Math.round(S * 0.019);
  const baseNoteFs  = Math.round(S * 0.015);

  const displayName = productName.length > 45 ? productName.slice(0, 43) + "…" : productName;

  return `<svg width="${S}" height="${S}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${theme.bgTop}"/>
      <stop offset="1" stop-color="${theme.bgBottom}"/>
    </linearGradient>
  </defs>
  <rect width="${S}" height="${S}" fill="url(#bg)"/>
  <text x="${cx}" y="${titleY}" text-anchor="middle" font-family="Liberation Sans,DejaVu Sans,Arial,sans-serif" font-size="${titleFs}" font-weight="bold" letter-spacing="5" fill="${theme.textColor}">ПИРАМИДА АРОМАТА</text>
  <text x="${cx}" y="${nameY}" text-anchor="middle" font-family="Liberation Sans,DejaVu Sans,Arial,sans-serif" font-size="${nameFs}" fill="${theme.textColor}" opacity="0.60">${escXml(displayName)}</text>
  <polygon points="${apexX},${apexY} ${d1Rx},${d1Y} ${d1Lx},${d1Y}" fill="${theme.topColor}"/>
  ${sectionLines("ТОП-НОТЫ", topNotes, topCy, topLabelFs, topNoteFs, 16)}
  <polygon points="${d1Lx},${d1Y} ${d1Rx},${d1Y} ${d2Rx},${d2Y} ${d2Lx},${d2Y}" fill="${theme.midColor}"/>
  ${sectionLines("СЕРДЕЧНЫЕ НОТЫ", midNotes, midCy, midLabelFs, midNoteFs, 28)}
  <polygon points="${d2Lx},${d2Y} ${d2Rx},${d2Y} ${bRx},${bY} ${bLx},${bY}" fill="${theme.baseColor}"/>
  ${sectionLines("БАЗОВЫЕ НОТЫ", baseNotes, baseCy, baseLabelFs, baseNoteFs, 38)}
  <line x1="${d1Lx}" y1="${d1Y}" x2="${d1Rx}" y2="${d1Y}" stroke="${theme.labelColor}" stroke-opacity="0.22" stroke-width="1.5"/>
  <line x1="${d2Lx}" y1="${d2Y}" x2="${d2Rx}" y2="${d2Y}" stroke="${theme.labelColor}" stroke-opacity="0.22" stroke-width="1.5"/>
  <polygon points="${apexX},${apexY} ${bRx},${bY} ${bLx},${bY}" fill="none" stroke="${theme.textColor}" stroke-opacity="0.14" stroke-width="2"/>
  <text x="${cx}" y="${footerY}" text-anchor="middle" font-family="Liberation Sans,DejaVu Sans,Arial,sans-serif" font-size="${footerFs}" fill="${theme.textColor}" opacity="0.32" letter-spacing="4">MAGIC VIBES</text>
</svg>`;
}

function buildOriginalBadgeSvg(S) {
  const cx = S / 2;

  // Shield polygon (8-point pentagon-style shield)
  const sh = {
    tl: `${Math.round(cx - S * 0.28)},${Math.round(S * 0.13)}`,
    tr: `${Math.round(cx + S * 0.28)},${Math.round(S * 0.13)}`,
    rm: `${Math.round(cx + S * 0.31)},${Math.round(S * 0.42)}`,
    rb: `${Math.round(cx + S * 0.18)},${Math.round(S * 0.60)}`,
    bt: `${cx},${Math.round(S * 0.72)}`,
    lb: `${Math.round(cx - S * 0.18)},${Math.round(S * 0.60)}`,
    lm: `${Math.round(cx - S * 0.31)},${Math.round(S * 0.42)}`,
  };
  const shieldPts = `${sh.tl} ${sh.tr} ${sh.rm} ${sh.rb} ${sh.bt} ${sh.lb} ${sh.lm}`;

  const checkY    = Math.round(S * 0.33);
  const pctY      = Math.round(S * 0.52);
  const origY     = Math.round(S * 0.63);
  const divY      = Math.round(S * 0.71);
  const claim1Y   = Math.round(S * 0.79);
  const claim2Y   = Math.round(S * 0.85);
  const claim3Y   = Math.round(S * 0.91);

  const checkFs   = Math.round(S * 0.11);
  const pctFs     = Math.round(S * 0.095);
  const origFs    = Math.round(S * 0.058);
  const claimFs   = Math.round(S * 0.026);

  const gold      = "#c9a227";
  const lightGold = "#f0d080";
  const white     = "#ffffff";

  return `<svg width="${S}" height="${S}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#09091a"/>
      <stop offset="1" stop-color="#1a1228"/>
    </linearGradient>
    <linearGradient id="shield" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${gold}" stop-opacity="0.22"/>
      <stop offset="1" stop-color="${gold}" stop-opacity="0.07"/>
    </linearGradient>
    <linearGradient id="goldText" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${lightGold}"/>
      <stop offset="1" stop-color="${gold}"/>
    </linearGradient>
  </defs>
  <rect width="${S}" height="${S}" fill="url(#bg)"/>
  <circle cx="${cx}" cy="${Math.round(S * 0.42)}" r="${Math.round(S * 0.37)}" fill="none" stroke="${gold}" stroke-opacity="0.07" stroke-width="2"/>
  <circle cx="${cx}" cy="${Math.round(S * 0.42)}" r="${Math.round(S * 0.31)}" fill="none" stroke="${gold}" stroke-opacity="0.05" stroke-width="1"/>
  <polygon points="${shieldPts}" fill="url(#shield)" stroke="${gold}" stroke-opacity="0.45" stroke-width="2"/>
  <text x="${cx}" y="${checkY}" text-anchor="middle" font-family="DejaVu Sans,Arial,sans-serif" font-size="${checkFs}" fill="${gold}" opacity="0.55">✓</text>
  <text x="${cx}" y="${pctY}" text-anchor="middle" font-family="Liberation Sans,Arial,sans-serif" font-size="${pctFs}" font-weight="bold" fill="url(#goldText)">100%</text>
  <text x="${cx}" y="${origY}" text-anchor="middle" font-family="Liberation Sans,Arial,sans-serif" font-size="${origFs}" font-weight="bold" fill="${white}" letter-spacing="7">ОРИГИНАЛ</text>
  <line x1="${Math.round(cx - S * 0.25)}" y1="${divY}" x2="${Math.round(cx + S * 0.25)}" y2="${divY}" stroke="${gold}" stroke-opacity="0.38" stroke-width="1"/>
  <text x="${cx}" y="${claim1Y}" text-anchor="middle" font-family="Liberation Sans,Arial,sans-serif" font-size="${claimFs}" fill="${white}" opacity="0.72">✓ Официальный дистрибьютор</text>
  <text x="${cx}" y="${claim2Y}" text-anchor="middle" font-family="Liberation Sans,Arial,sans-serif" font-size="${claimFs}" fill="${white}" opacity="0.72">✓ Гарантия подлинности</text>
  <text x="${cx}" y="${claim3Y}" text-anchor="middle" font-family="Liberation Sans,Arial,sans-serif" font-size="${claimFs}" fill="${white}" opacity="0.72">✓ Прямые поставки</text>
</svg>`;
}

async function buildFragrancePyramidImageBuffer(product, fragranceData = null, opts = {}) {
  const size = Math.max(512, Math.min(2048, Number(opts.size || 1000) || 1000));
  const name = cleanText(product?.name || product?.ozon?.name || "");
  const brand = cleanText(product?.brand || "");
  const accords = Array.isArray(fragranceData?.accords) ? fragranceData.accords : [];
  const character = detectCharacter(name, brand, accords);
  const notes = pyramidNotes(fragranceData, character);
  const svg = buildPyramidSvg(name, notes, character, size);
  return sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toBuffer();
}

async function buildOriginalBadgeImageBuffer(_product, opts = {}) {
  const size = Math.max(512, Math.min(2048, Number(opts.size || 1000) || 1000));
  const svg = buildOriginalBadgeSvg(size);
  return sharp(Buffer.from(svg)).png({ compressionLevel: 9 }).toBuffer();
}

global.buildFragrancePyramidImageBuffer = buildFragrancePyramidImageBuffer;
global.buildOriginalBadgeImageBuffer = buildOriginalBadgeImageBuffer;
