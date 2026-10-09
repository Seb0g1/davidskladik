import { useEffect, useState } from "react";
import { Loader2, Plus, Trash2 } from "lucide-react";

// Delivery price rules for magicvibes.ru (settings.deliveryRules, computed on the server by
// shopDeliveryQuote in 02d-shop-api-routes.js from the Ozon Доставка tariff table).
export interface DeliveryRules {
  mode?: "fixed" | "ozon";
  senderCluster?: string;
  handlingRub?: number;
  lastMileRub?: number;
  issuePerItemRub?: number;
  returnRatePct?: number;
  returnProcessingRub?: number;
  acquiringPct?: number;
  acquiringBase?: "order" | "delivery" | "none";
  packMlFactor?: number;
  packBaseL?: number;
  defaultItemL?: number;
  extraRub?: number;
  extraPct?: number;
  roundTo?: number;
  minRub?: number;
  maxRub?: number;
  unknownCity?: "max" | "universal";
  cityClusters?: { match: string; cluster: string }[];
  cityRules?: { match: string; addRub?: number | null; fixedRub?: number | null }[];
}

const DEFAULTS: Required<Omit<DeliveryRules, "cityClusters" | "cityRules">> = {
  mode: "fixed", senderCluster: "Москва, МО и Дальние регионы",
  handlingRub: 10, lastMileRub: 25, issuePerItemRub: 30, returnRatePct: 5, returnProcessingRub: 15,
  acquiringPct: 2.2, acquiringBase: "order", packMlFactor: 4, packBaseL: 0.15, defaultItemL: 0.6,
  extraRub: 0, extraPct: 0, roundTo: 10, minRub: 0, maxRub: 0, unknownCity: "max",
};

interface PreviewRow {
  city: string; priceRub: number; cluster: string | null; free: boolean;
  breakdown: { logistics: number; handling: number; lastMile: number; issue: number; returns: number; acquiring: number } | null;
}

function Num({ label, hint, value, step = 1, onChange }: { label: string; hint?: string; value: number; step?: number; onChange: (n: number) => void }) {
  return (
    <div className="mv-field">
      <label>{label}</label>
      <input type="number" min="0" step={step} value={Number.isFinite(value) ? value : 0} onChange={(e) => onChange(Number(e.target.value))} />
      {hint && <span className="sa-note-11-mt4">{hint}</span>}
    </div>
  );
}

export function DeliveryRulesEditor({ value, onChange, flatPrice, freeFrom }: {
  value: DeliveryRules | undefined; onChange: (r: DeliveryRules) => void; flatPrice: number; freeFrom: number;
}) {
  const r = { ...DEFAULTS, ...(value || {}) };
  const cityRules = value?.cityRules || [];
  const cityClusters = value?.cityClusters || [];
  const set = (patch: Partial<DeliveryRules>) => onChange({ ...(value || {}), ...patch });

  const [ml, setMl] = useState("100");
  const [qty, setQty] = useState(1);
  const [goods, setGoods] = useState(5000);
  const [preview, setPreview] = useState<{ rows: PreviewRow[]; clusters: string[]; senders: string[] } | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // server-side preview with the unsaved rules
  useEffect(() => {
    const t = setTimeout(async () => {
      setLoading(true);
      setErr(null);
      try {
        const res = await fetch("/api/shop/admin/delivery-preview", {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            settings: { deliveryRules: { ...(value || {}) }, deliveryPriceRub: flatPrice, freeDeliveryFrom: freeFrom },
            items: [{ volume: `${ml} мл`, quantity: qty }], goodsRub: goods,
          }),
        });
        if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
        setPreview(await res.json());
      } catch (e) {
        setErr(e instanceof Error ? e.message : "Ошибка расчёта");
      } finally {
        setLoading(false);
      }
    }, 400);
    return () => clearTimeout(t);
  }, [value, ml, qty, goods, flatPrice, freeFrom]);

  const clusters = preview?.clusters || [];
  const senders = preview?.senders || [r.senderCluster];

  return (
    <div className="mv-form-section sa-section-sep">
      <h3 className="sa-h3-mb12">🚚 Доставка: правила расчёта</h3>
      <div className="mv-field">
        <label>Как считать стоимость доставки</label>
        <select value={r.mode} onChange={(e) => set({ mode: e.target.value as DeliveryRules["mode"] })}>
          <option value="fixed">Фиксированная цена — «Стоимость доставки» выше ({flatPrice} ₽)</option>
          <option value="ozon">По тарифам Ozon Доставки — с учётом города, объёма, невыкупов и комиссии Ozon Pay</option>
        </select>
        <span className="sa-note-11-mt4">
          «Бесплатная доставка от» работает в обоих режимах. Покупатель видит цену доставки в корзине и при оформлении; в чеке Ozon она отдельной строкой
          (при доставке Ozon — ценой доставки в форме Ozon Pay).
        </span>
      </div>

      {r.mode === "ozon" && (
        <>
          <div className="mv-field-grid">
            <div className="mv-field">
              <label>Кластер отправки (откуда отгружаете)</label>
              <select value={r.senderCluster} onChange={(e) => set({ senderCluster: e.target.value })}>
                {(senders.includes(r.senderCluster) ? senders : [r.senderCluster, ...senders]).map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div className="mv-field">
              <label>Город не распознан</label>
              <select value={r.unknownCity} onChange={(e) => set({ unknownCity: e.target.value as DeliveryRules["unknownCity"] })}>
                <option value="max">Самый дорогой кластер (надёжно покрывает)</option>
                <option value="universal">Универсальный тариф Ozon</option>
              </select>
            </div>
          </div>

          <p className="sa-desc-p">Расходы Ozon (по статье «Расходы на доставку, невыкупы и отмены»):</p>
          <div className="mv-field-grid">
            <Num label="Обработка отправления, ₽" hint="ПВЗ/ППЗ — 10 ₽" value={r.handlingRub} onChange={(n) => set({ handlingRub: n })} />
            <Num label="Доставка до места выдачи, ₽" hint="не больше 25 ₽" value={r.lastMileRub} onChange={(n) => set({ lastMileRub: n })} />
            <Num label="Выдача в ПВЗ, ₽ за товар" hint="30 ₽" value={r.issuePerItemRub} onChange={(n) => set({ issuePerItemRub: n })} />
            <Num label="Невыкупы и отмены, %" hint="закладываем обратную логистику на эту долю" step={0.5} value={r.returnRatePct} onChange={(n) => set({ returnRatePct: n })} />
            <Num label="Обработка невыкупа, ₽ за товар" hint="15 ₽" value={r.returnProcessingRub} onChange={(n) => set({ returnProcessingRub: n })} />
          </div>

          <p className="sa-desc-p">Комиссия Ozon Pay (2,2% карта / Ozon Банк, 0,7% СБП):</p>
          <div className="mv-field-grid">
            <Num label="Комиссия, %" step={0.1} value={r.acquiringPct} onChange={(n) => set({ acquiringPct: n })} />
            <div className="mv-field">
              <label>С какой суммы покрывать комиссию</label>
              <select value={r.acquiringBase} onChange={(e) => set({ acquiringBase: e.target.value as DeliveryRules["acquiringBase"] })}>
                <option value="order">Со всего заказа (товары + доставка)</option>
                <option value="delivery">Только с доставки</option>
                <option value="none">Не закладывать</option>
              </select>
            </div>
          </div>

          <p className="sa-desc-p">Объём упаковки (габаритов в карточках нет, считаем по объёму флакона):</p>
          <div className="mv-field-grid">
            <Num label="Литров на 1 мл флакона ×1000" hint="4 → флакон 100 мл ≈ 0,4 л упаковки" step={0.5} value={r.packMlFactor} onChange={(n) => set({ packMlFactor: n })} />
            <Num label="Плюс к упаковке, л" step={0.05} value={r.packBaseL} onChange={(n) => set({ packBaseL: n })} />
            <Num label="Товар без объёма, л" step={0.1} value={r.defaultItemL} onChange={(n) => set({ defaultItemL: n })} />
          </div>

          <p className="sa-desc-p">Наценка и округление:</p>
          <div className="mv-field-grid">
            <Num label="Наценка, ₽" value={r.extraRub} onChange={(n) => set({ extraRub: n })} />
            <Num label="Наценка, %" step={0.5} value={r.extraPct} onChange={(n) => set({ extraPct: n })} />
            <Num label="Округлять вверх до, ₽" value={r.roundTo} onChange={(n) => set({ roundTo: n })} />
            <Num label="Минимум, ₽" value={r.minRub} onChange={(n) => set({ minRub: n })} />
            <Num label="Максимум, ₽ (0 — нет)" value={r.maxRub} onChange={(n) => set({ maxRub: n })} />
          </div>

          <div className="mv-field sa-field-mt12">
            <label>Правила для городов и кластеров</label>
            <span className="sa-note-11-mt4">Город или кластер (например «Москва» или «Дальний Восток»): фиксированная цена или надбавка. Первое совпадение.</span>
            {cityRules.map((cr, i) => (
              <div key={i} className="mv-form-inline" style={{ marginTop: 6 }}>
                <input value={cr.match} placeholder="Город или кластер" onChange={(e) => set({ cityRules: cityRules.map((x, j) => j === i ? { ...x, match: e.target.value } : x) })} />
                <input type="number" className="sa-w-110" placeholder="Цена, ₽" value={cr.fixedRub ?? ""} onChange={(e) => set({ cityRules: cityRules.map((x, j) => j === i ? { ...x, fixedRub: e.target.value === "" ? null : Number(e.target.value) } : x) })} />
                <input type="number" className="sa-w-110" placeholder="+ ₽" value={cr.addRub ?? ""} onChange={(e) => set({ cityRules: cityRules.map((x, j) => j === i ? { ...x, addRub: e.target.value === "" ? null : Number(e.target.value) } : x) })} />
                <button type="button" className="ghost-button" onClick={() => set({ cityRules: cityRules.filter((_, j) => j !== i) })}><Trash2 size={14} /></button>
              </div>
            ))}
            <button type="button" className="ghost-button" style={{ marginTop: 6 }} onClick={() => set({ cityRules: [...cityRules, { match: "", fixedRub: null, addRub: null }] })}><Plus size={14} /> Правило</button>
          </div>

          <div className="mv-field sa-field-mt12">
            <label>Свой кластер для города</label>
            <span className="sa-note-11-mt4">Если город определился не в тот кластер Ozon — укажите вручную.</span>
            {cityClusters.map((cc, i) => (
              <div key={i} className="mv-form-inline" style={{ marginTop: 6 }}>
                <input value={cc.match} placeholder="Город" onChange={(e) => set({ cityClusters: cityClusters.map((x, j) => j === i ? { ...x, match: e.target.value } : x) })} />
                <select value={cc.cluster} onChange={(e) => set({ cityClusters: cityClusters.map((x, j) => j === i ? { ...x, cluster: e.target.value } : x) })}>
                  {clusters.map((c) => <option key={c} value={c}>{c}</option>)}
                </select>
                <button type="button" className="ghost-button" onClick={() => set({ cityClusters: cityClusters.filter((_, j) => j !== i) })}><Trash2 size={14} /></button>
              </div>
            ))}
            <button type="button" className="ghost-button" style={{ marginTop: 6 }} onClick={() => set({ cityClusters: [...cityClusters, { match: "", cluster: clusters[0] || r.senderCluster }] })}><Plus size={14} /> Город</button>
          </div>
        </>
      )}

      <div className="mv-field sa-field-mt12">
        <label>Проверка цены {loading && <Loader2 size={12} className="spin" />}</label>
        <div className="mv-form-inline">
          <span className="sa-muted-12">Флакон, мл</span>
          <input type="number" className="sa-w-110" value={ml} onChange={(e) => setMl(e.target.value)} />
          <span className="sa-muted-12">шт.</span>
          <input type="number" className="sa-w-110" min={1} value={qty} onChange={(e) => setQty(Math.max(1, Number(e.target.value) || 1))} />
          <span className="sa-muted-12">товары, ₽</span>
          <input type="number" className="sa-w-110" value={goods} onChange={(e) => setGoods(Number(e.target.value) || 0)} />
        </div>
        {err && <span className="sa-note-11-mt4" style={{ color: "#f87171" }}>{err} — сохраните настройки и обновите страницу</span>}
        {preview && (
          <table className="data-table" style={{ marginTop: 8 }}>
            <thead><tr><th>Город</th><th>Кластер</th><th>Доставка</th><th>Логистика</th><th>Обработка + ПВЗ + выдача</th><th>Невыкупы</th><th>Ozon Pay + округл.</th></tr></thead>
            <tbody>
              {preview.rows.map((row) => (
                <tr key={row.city}>
                  <td>{row.city}</td>
                  <td>{row.cluster || "—"}</td>
                  <td><b>{row.free ? "бесплатно" : `${row.priceRub} ₽`}</b></td>
                  <td>{row.breakdown ? `${row.breakdown.logistics} ₽` : "—"}</td>
                  <td>{row.breakdown ? `${row.breakdown.handling + row.breakdown.lastMile + row.breakdown.issue} ₽` : "—"}</td>
                  <td>{row.breakdown ? `${row.breakdown.returns} ₽` : "—"}</td>
                  <td>{row.breakdown ? `${row.breakdown.acquiring} ₽` : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
