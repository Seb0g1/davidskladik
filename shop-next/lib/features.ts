// Storefront sections switched on/off in davidsklad → Магазин → Настройки → «Разделы сайта».
// Off by default: a failed settings request must not bring a hidden section back.
import { fetchSettings } from "./api";

export interface ShopFeatures { giftBuilder: boolean; vipClub: boolean }

export async function getFeatures(): Promise<ShopFeatures> {
  try {
    const s = await fetchSettings();
    return { giftBuilder: Boolean(s.features?.giftBuilder), vipClub: Boolean(s.features?.vipClub) };
  } catch {
    return { giftBuilder: false, vipClub: false };
  }
}
