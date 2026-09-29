// Общие для оформления и карты доставки (без leaflet — модуль можно импортировать при SSR)
export const CARRIER_LOGO: Record<string, string> = {
  cdek: "/brand/carriers/cdek.svg", yandex: "/brand/carriers/yandex.svg", dostavista: "/brand/carriers/dostavista.svg", ozon: "/brand/carriers/ozon.svg",
};
export const MARK: Record<string, string> = { cdek: "/brand/carriers/cdek.svg", yandex: "/brand/carriers/yandex-mark.svg" };
export const CARRIER_NAME: Record<string, string> = { cdek: "СДЭК", yandex: "Яндекс Доставка", dostavista: "Достависта", ozon: "Ozon" };

export const rubFmt = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;
export const daysText = (o: { daysMin: number | null; daysMax: number | null; sameDay?: boolean; speed?: string }) =>
  o.speed === "fast" ? "сегодня, курьер выедет сразу" : o.speed === "slot" ? "сегодня или завтра, ко времени"
    : o.sameDay ? "сегодня" : o.daysMin == null ? "" : o.daysMin === o.daysMax || o.daysMax == null ? `${o.daysMin} дн.` : `${o.daysMin}–${o.daysMax} дн.`;
export const slotText = (s: { from: string; to: string }) => {
  const f = new Date(s.from), t = new Date(s.to);
  const today = new Date().toDateString() === f.toDateString();
  const hm = (d: Date) => d.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Moscow" });
  return `${today ? "Сегодня" : "Завтра"}, ${hm(f)}–${hm(t)}`;
};

