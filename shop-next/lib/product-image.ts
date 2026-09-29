// Product photo clean-up for marketplace images (Ozon / Yandex):
// finds the product's bounding box (ignoring flat light backgrounds and the
// black rounded-corner artefacts some Ozon photos have), lifts a light-grey
// backdrop to pure white, centres the product on a white fixed-ratio canvas
// and returns a compact WebP. Used by app/img/route.ts.
import sharp from "sharp";

const WHITE = { r: 255, g: 255, b: 255, alpha: 1 };
const PROBE = 320; // analysis resolution

function isLightNeutral(r: number, g: number, b: number) {
  const mn = Math.min(r, g, b), mx = Math.max(r, g, b);
  return mn > 208 && mx - mn < 16;
}

/** Bounding box of non-background pixels on a PROBE-sized copy, plus the backdrop brightness. */
async function analyse(buf: Buffer) {
  const { data, info } = await sharp(buf)
    .resize(PROBE, PROBE, { fit: "inside" })
    .removeAlpha().raw().toBuffer({ resolveWithObject: true });
  const { width: W, height: H } = info;
  const corner = Math.round(Math.min(W, H) * 0.07);
  const rows = new Uint32Array(H), cols = new Uint32Array(W);
  const bgLevels: number[] = [];
  for (let y = 0; y < H; y++) {
    for (let x = 0; x < W; x++) {
      const i = (y * W + x) * 3, r = data[i], g = data[i + 1], b = data[i + 2];
      // corner squares are skipped: Ozon's rounded-corner frames (black, anti-aliased) live there
      if ((x < corner || x >= W - corner) && (y < corner || y >= H - corner)) continue;
      const bg = isLightNeutral(r, g, b);
      if (bg && (x + y) % 7 === 0) bgLevels.push(Math.min(r, g, b));
      if (!bg) { rows[y]++; cols[x]++; }
    }
  }
  const minRow = Math.max(2, W * 0.006), minCol = Math.max(2, H * 0.006);
  let top = 0, bottom = H - 1, left = 0, right = W - 1;
  while (top < H && rows[top] < minRow) top++;
  while (bottom > top && rows[bottom] < minRow) bottom--;
  while (left < W && cols[left] < minCol) left++;
  while (right > left && cols[right] < minCol) right--;
  bgLevels.sort((x, y) => x - y);
  const bgLevel = bgLevels.length > 50 ? bgLevels[Math.floor(bgLevels.length / 2)] : 255;
  return { W, H, top, bottom, left, right, bgLevel, empty: top >= bottom || left >= right };
}

/** width = output px; ratio = height / width of the canvas (cards use 1.12). */
export async function cleanProductImage(input: Buffer, width: number, ratio = 1): Promise<Buffer> {
  const height = Math.round(width * ratio);
  const base = await sharp(input, { failOn: "none" }).rotate().flatten({ background: WHITE }).toBuffer();
  const meta = await sharp(base).metadata();
  const a = await analyse(base);

  let pipeline = sharp(base);
  if (!a.empty) {
    const mw = meta.width ?? a.W, mh = meta.height ?? a.H;
    const sx = mw / a.W, sy = mh / a.H;
    const pad = 2;
    const left = Math.max(0, Math.floor((a.left - pad) * sx));
    const top = Math.max(0, Math.floor((a.top - pad) * sy));
    const right = Math.min(mw, Math.ceil((a.right + 1 + pad) * sx));
    const bottom = Math.min(mh, Math.ceil((a.bottom + 1 + pad) * sy));
    if (right - left > 16 && bottom - top > 16) pipeline = pipeline.extract({ left, top, width: right - left, height: bottom - top });
  }
  // light-grey studio backdrop (e.g. 245) → pure white, product barely changes
  if (a.bgLevel >= 222 && a.bgLevel < 253) {
    const k = 255 / (a.bgLevel + 2);
    pipeline = pipeline.linear([k, k, k], [0, 0, 0]);
  }
  const cropped = await pipeline.toBuffer({ resolveWithObject: true });

  // product fills ~86% of the canvas; limit upscaling of tiny sources
  const boxW = width * 0.86, boxH = height * 0.86;
  const scale = Math.min(boxW / cropped.info.width, boxH / cropped.info.height, 3);
  const w = Math.max(1, Math.round(cropped.info.width * scale));
  const h = Math.max(1, Math.round(cropped.info.height * scale));
  const product = await sharp(cropped.data)
    .resize(w, h, { kernel: "lanczos3" })
    .sharpen(scale > 1.1 ? { sigma: 0.9 } : { sigma: 0.5 })
    .toBuffer();

  return sharp({ create: { width, height, channels: 3, background: WHITE } })
    .composite([{ input: product, left: Math.round((width - w) / 2), top: Math.round((height - h) / 2) }])
    .webp({ quality: 84, effort: 4, smartSubsample: true })
    .toBuffer();
}
