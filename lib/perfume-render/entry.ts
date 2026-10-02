// Bundle entry: the perfume-card renderer of parfumdeclaration (premium backdrop, «Пирамида аромата»,
// «Характеристики», close-up, «Ноты крупно», «Аккорды») built into davidsklad, so photos are made on the
// davidsklad server. Source of truth stays in parfumdeclaration-web/apps/api/src/lib — rebuild with
// `node scripts/build-perfume-render.cjs` (it bundles this file into lib/perfume-render/index.cjs).
import sharp from 'sharp';
import { premiumBackground } from '../../../parfumdeclaration-web/apps/api/src/lib/premium-bg';
import { enhanceSmallPhoto } from '../../../parfumdeclaration-web/apps/api/src/lib/upscale';
import { renderNotesCard, type CardStyle } from '../../../parfumdeclaration-web/apps/api/src/lib/notes-card';
import { renderCloseup, renderSpecsCard, renderTierCard, renderAccordsCard, tierKeys } from '../../../parfumdeclaration-web/apps/api/src/lib/perfume-extra';

export interface RenderInput {
  source: Buffer;
  styles: CardStyle[];
  perfume: any;
  extras?: boolean;
}

/** Same result as POST /api/tools/perfume-card of parfumdeclaration, as Buffers. */
export async function renderPerfumeCard({ source, styles, perfume, extras = true }: RenderInput) {
  const warnings: string[] = [];
  let main = await premiumBackground(source, { width: 1500, height: 2000, quality: 95, chroma444: true }).catch((e: any) => { warnings.push(`Премиум фон: ${e?.message || e}`); return null; });
  if (!main) {
    warnings.push('Фото не на белом фоне — премиум фон не применён');
    main = await enhanceSmallPhoto(source).catch(() => null);
  }
  if (!main) main = await sharp(source, { failOn: 'none' }).rotate().jpeg({ quality: 95, mozjpeg: true, chromaSubsampling: '4:4:4' }).toBuffer();
  const notesByStyle: Record<string, Buffer> = {};
  for (const style of styles) notesByStyle[style] = await renderNotesCard(perfume, style);
  const out: {
    main: Buffer; notesByStyle: Record<string, Buffer>; closeup: Buffer | null; specsByStyle: Record<string, Buffer>;
    tiersByStyle: Record<string, Array<{ tier: string; image: Buffer }>>; accordsByStyle: Record<string, Buffer>; warnings: string[];
  } = { main, notesByStyle, closeup: null, specsByStyle: {}, tiersByStyle: {}, accordsByStyle: {}, warnings };
  if (!extras) return out;
  out.closeup = await renderCloseup(source).catch((e: any) => { warnings.push(`Крупный план: ${e?.message || e}`); return null; });
  const tiers = tierKeys(perfume.notes ?? {});
  for (const style of styles) {
    const specs = await renderSpecsCard(perfume, main, style).catch((e: any) => { warnings.push(`Характеристики: ${e?.message || e}`); return null; });
    if (specs) out.specsByStyle[style] = specs;
    out.tiersByStyle[style] = [];
    for (const tier of tiers) {
      const card = await renderTierCard(perfume, tier, style).catch((e: any) => { warnings.push(`Ноты (${tier}): ${e?.message || e}`); return null; });
      if (card) out.tiersByStyle[style].push({ tier, image: card });
    }
    const accords = await renderAccordsCard(perfume, style).catch((e: any) => { warnings.push(`Аккорды: ${e?.message || e}`); return null; });
    if (accords) out.accordsByStyle[style] = accords;
  }
  return out;
}
