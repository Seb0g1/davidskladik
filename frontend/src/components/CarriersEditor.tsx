import { useEffect, useState } from "react";
import { Loader2, CheckCircle2, XCircle, RefreshCw } from "lucide-react";

// Службы доставки magicvibes.ru: СДЭК, Яндекс Доставка, Достависта (settings.carriers,
// считается на сервере в 02d-shop-delivery-carriers.js). Покупатель платит доставку сам.
export interface CarrierSettings {
  sender?: {
    name?: string; contactName?: string; phone?: string; city?: string; address?: string; comment?: string;
    cdekCityCode?: number; cdekShipmentPoint?: string; yandexStationId?: string; yandexStationName?: string;
  };
  cdek?: { enabled?: boolean; pvz?: boolean; postamat?: boolean; courier?: boolean };
  yandex?: { enabled?: boolean; pvz?: boolean; courier?: boolean };
  dostavista?: { enabled?: boolean; regions?: string[]; fast?: boolean; slot?: boolean };
  pricing?: { extraPct?: number; extraRub?: number; roundTo?: number; handlingDays?: number; minRub?: number };
  autoCreateShipment?: boolean;
}

type Conn = { ok: boolean; error?: string; test?: boolean };
type Option = { id: string; title: string; priceRub: number; rawRub: number; daysMin: number | null; daysMax: number | null };
type Point = { id: string; address: string; name?: string; schedule?: string };

const get = async <T,>(url: string, init?: RequestInit): Promise<T> => {
  const r = await fetch(url, { credentials: "same-origin", headers: { "Content-Type": "application/json" }, ...init });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(j.error || r.statusText);
  return j as T;
};

function Num({ label, hint, value, step = 1, onChange }: { label: string; hint?: string; value: number; step?: number; onChange: (n: number) => void }) {
  return (
    <div className="mv-field">
      <label>{label}</label>
      <input type="number" min="0" step={step} value={Number.isFinite(value) ? value : 0} onChange={(e) => onChange(Number(e.target.value))} />
      {hint && <span className="sa-note-11-mt4">{hint}</span>}
    </div>
  );
}
function Txt({ label, hint, value, placeholder, onChange }: { label: string; hint?: string; value: string; placeholder?: string; onChange: (s: string) => void }) {
  return (
    <div className="mv-field">
      <label>{label}</label>
      <input value={value} placeholder={placeholder} onChange={(e) => onChange(e.target.value)} />
      {hint && <span className="sa-note-11-mt4">{hint}</span>}
    </div>
  );
}
function Check({ label, checked, onChange }: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="mv-form-inline" style={{ cursor: "pointer", marginTop: 6 }}>
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} /> <span>{label}</span>
    </label>
  );
}
function ConnBadge({ c, name }: { c?: Conn; name: string }) {
  if (!c) return <span className="pill"><Loader2 size={12} className="spin" /> {name}</span>;
  return (
    <span className={`pill ${c.ok ? "tone-success" : "tone-danger"}`} title={c.error || ""}>
      {c.ok ? <CheckCircle2 size={12} /> : <XCircle size={12} />} {name}{c.ok ? (c.test ? " — ТЕСТОВЫЙ контур" : " — подключено") : ` — ${c.error || "ошибка"}`}
    </span>
  );
}

export function CarriersEditor({ value, onChange }: { value: CarrierSettings | undefined; onChange: (v: CarrierSettings) => void }) {
  const v = value || {};
  const sender = v.sender || {};
  const pricing = { extraPct: 2.5, extraRub: 0, roundTo: 10, handlingDays: 1, minRub: 0, ...(v.pricing || {}) };
  const cdek = { enabled: true, pvz: true, postamat: true, courier: true, ...(v.cdek || {}) };
  const yandex = { enabled: true, pvz: true, courier: true, ...(v.yandex || {}) };
  const dv = { enabled: true, regions: ["москва", "московская"], fast: true, slot: true, ...(v.dostavista || {}) };
  const set = (patch: Partial<CarrierSettings>) => onChange({ ...v, ...patch });
  const setSender = (patch: Partial<NonNullable<CarrierSettings["sender"]>>) => set({ sender: { ...sender, ...patch } });

  const [conn, setConn] = useState<{ cdek?: Conn; yandex?: Conn; dostavista?: Conn }>({});
  const [connErr, setConnErr] = useState<string | null>(null);
  const loadConn = () => {
    setConn({}); setConnErr(null);
    get<{ connection: { cdek: Conn; yandex: Conn; dostavista: Conn } }>("/api/shop/admin/carriers").then((d) => setConn(d.connection)).catch((e) => setConnErr(e.message));
  };
  useEffect(loadConn, []);

  const [points, setPoints] = useState<{ carrier: string; list: Point[] } | null>(null);
  const [pointsLoading, setPointsLoading] = useState(false);
  const loadPoints = async (carrier: "cdek" | "yandex") => {
    setPointsLoading(true);
    try { setPoints({ carrier, list: (await get<{ points: Point[] }>(`/api/shop/admin/carriers/dropoff?carrier=${carrier}&city=${encodeURIComponent(sender.city || "Москва")}`)).points }); }
    catch (e) { setPoints({ carrier, list: [] }); alert(e instanceof Error ? e.message : "Ошибка"); }
    finally { setPointsLoading(false); }
  };

  const [preview, setPreview] = useState<{ city: string; options: Option[] }[] | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const runPreview = async () => {
    setPreviewLoading(true);
    try { setPreview((await get<{ rows: { city: string; options: Option[] }[] }>("/api/shop/admin/carriers/preview", { method: "POST", body: JSON.stringify({ ml: 100, qty: 1, goodsRub: 8000 }) })).rows); }
    catch (e) { alert(e instanceof Error ? e.message : "Ошибка"); }
    finally { setPreviewLoading(false); }
  };

  return (
    <div className="mv-form-section sa-section-sep">
      <h3 className="sa-h3-mb12">📦 Службы доставки: СДЭК, Яндекс, Достависта</h3>
      <p className="sa-note-11-mt4" style={{ marginBottom: 10 }}>
        Покупатель видит при оформлении варианты с ценой и сроком и платит доставку сам. Цена = тариф службы + наценка ниже, округление вверх.
        Ключи API — в .env сервера (CDEK_*, YANDEX_DELIVERY_TOKEN, DOSTAVISTA_*). Настройки сохраняются кнопкой «Сохранить» внизу страницы.
      </p>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 14 }}>
        <ConnBadge c={conn.cdek} name="СДЭК" /><ConnBadge c={conn.yandex} name="Яндекс Доставка" /><ConnBadge c={conn.dostavista} name="Достависта" />
        <button type="button" className="secondary-action" onClick={loadConn}><RefreshCw size={13} /> Проверить</button>
        {connErr && <span className="inline-error">{connErr}</span>}
      </div>

      <h4 style={{ margin: "8px 0" }}>Откуда отправляем</h4>
      <div className="mv-field-grid">
        <Txt label="Отправитель (название)" value={sender.name ?? "Magic Vibes"} onChange={(s) => setSender({ name: s })} />
        <Txt label="Контактное лицо" value={sender.contactName ?? ""} placeholder="Имя, кто отдаёт посылки" onChange={(s) => setSender({ contactName: s })} />
        <Txt label="Телефон отправителя" value={sender.phone ?? ""} placeholder="+7 900 000-00-00" onChange={(s) => setSender({ phone: s })} hint="Нужен курьерам Достависты и СДЭК" />
        <Txt label="Город отправки" value={sender.city ?? "Москва"} onChange={(s) => setSender({ city: s })} />
        <Txt label="Адрес отправки (улица, дом, офис)" value={sender.address ?? ""} placeholder="ул. Примерная, 1, офис 5" onChange={(s) => setSender({ address: s })} hint="Откуда курьер Достависты забирает заказ; без адреса Достависта не показывается" />
        <Txt label="Комментарий для курьера" value={sender.comment ?? ""} placeholder="Подъезд, этаж, как пройти" onChange={(s) => setSender({ comment: s })} />
      </div>

      <h4 style={{ margin: "14px 0 8px" }}>СДЭК</h4>
      <Check label="Показывать СДЭК" checked={cdek.enabled} onChange={(x) => set({ cdek: { ...cdek, enabled: x } })} />
      {cdek.enabled && (
        <>
          <div style={{ display: "flex", gap: 18, flexWrap: "wrap" }}>
            <Check label="Пункты выдачи" checked={cdek.pvz} onChange={(x) => set({ cdek: { ...cdek, pvz: x } })} />
            <Check label="Постаматы" checked={cdek.postamat} onChange={(x) => set({ cdek: { ...cdek, postamat: x } })} />
            <Check label="Курьер до двери" checked={cdek.courier} onChange={(x) => set({ cdek: { ...cdek, courier: x } })} />
          </div>
          <div className="mv-field-grid">
            <Num label="Код города отправки в СДЭК" value={Number(sender.cdekCityCode ?? 44)} onChange={(n) => setSender({ cdekCityCode: n })} hint="44 — Москва, 1097 — Балашиха" />
            <div className="mv-field">
              <label>ПВЗ СДЭК, куда сдаёте посылки</label>
              <div style={{ display: "flex", gap: 6 }}>
                <input value={sender.cdekShipmentPoint ?? ""} placeholder="Код ПВЗ, напр. MSK123" onChange={(e) => setSender({ cdekShipmentPoint: e.target.value.trim() })} />
                <button type="button" className="secondary-action" onClick={() => loadPoints("cdek")}>{pointsLoading && points?.carrier !== "yandex" ? <Loader2 size={13} className="spin" /> : "Выбрать"}</button>
              </div>
              <span className="sa-note-11-mt4">Пусто — СДЭК заберёт посылку курьером с адреса отправки (тариф «от склада»).</span>
            </div>
          </div>
        </>
      )}

      <h4 style={{ margin: "14px 0 8px" }}>Яндекс Доставка (в другой день)</h4>
      <Check label="Показывать Яндекс Доставку" checked={yandex.enabled} onChange={(x) => set({ yandex: { ...yandex, enabled: x } })} />
      {yandex.enabled && (
        <>
          <div style={{ display: "flex", gap: 18, flexWrap: "wrap" }}>
            <Check label="Пункты выдачи и постаматы" checked={yandex.pvz} onChange={(x) => set({ yandex: { ...yandex, pvz: x } })} />
            <Check label="Курьер до двери" checked={yandex.courier} onChange={(x) => set({ yandex: { ...yandex, courier: x } })} />
          </div>
          <div className="mv-field">
            <label>Точка сдачи посылок Яндекса (обязательно для создания отправок)</label>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input value={sender.yandexStationName || sender.yandexStationId || ""} readOnly placeholder="Не выбрана" />
              <button type="button" className="secondary-action" onClick={() => loadPoints("yandex")}>{pointsLoading && points?.carrier !== "cdek" ? <Loader2 size={13} className="spin" /> : "Выбрать"}</button>
            </div>
            <span className="sa-note-11-mt4">Пункт, куда вы приносите заказы для Яндекс Доставки, в городе отправки. Цены покупателю считаются и без неё.</span>
          </div>
        </>
      )}

      {points && (
        <div className="mv-field" style={{ maxHeight: 260, overflow: "auto", border: "1px solid var(--line, #ddd)", borderRadius: 10, padding: 8 }}>
          <label>{points.carrier === "cdek" ? "ПВЗ СДЭК, принимающие посылки" : "Точки сдачи Яндекс Доставки"} — {sender.city || "Москва"} ({points.list.length})</label>
          {points.list.slice(0, 300).map((p) => (
            <button key={p.id} type="button" className="secondary-action" style={{ display: "block", width: "100%", textAlign: "left", marginTop: 4 }}
              onClick={() => {
                if (points.carrier === "cdek") setSender({ cdekShipmentPoint: p.id });
                else setSender({ yandexStationId: p.id, yandexStationName: `${p.name ? p.name + ", " : ""}${p.address}` });
                setPoints(null);
              }}>
              <b>{p.id.length > 12 ? "" : p.id + " · "}</b>{p.name ? `${p.name}, ` : ""}{p.address} {p.schedule ? <span className="sa-muted-11"> · {p.schedule}</span> : null}
            </button>
          ))}
          <button type="button" className="secondary-action" style={{ marginTop: 6 }} onClick={() => setPoints(null)}>Закрыть</button>
        </div>
      )}

      <h4 style={{ margin: "14px 0 8px" }}>Достависта (курьер день в день)</h4>
      <Check label="Показывать Достависту" checked={dv.enabled} onChange={(x) => set({ dostavista: { ...dv, enabled: x } })} />
      {dv.enabled && (
        <div className="mv-field-grid">
          <Txt label="Регионы (через запятую)" value={(dv.regions || []).join(", ")} onChange={(s) => set({ dostavista: { ...dv, regions: s.split(",").map((x) => x.trim().toLowerCase()).filter(Boolean) } })} hint="Город покупателя должен содержать одно из слов" />
          <div className="mv-field">
            <label>Что предлагать покупателю</label>
            <Check label="«Быстрее» — курьер выезжает сразу (пешком или на авто — что дешевле)" checked={dv.fast !== false} onChange={(x) => set({ dostavista: { ...dv, fast: x } })} />
            <Check label="«Ко времени» — покупатель выбирает интервал 4 часа на сегодня или завтра" checked={dv.slot !== false} onChange={(x) => set({ dostavista: { ...dv, slot: x } })} />
            <span className="sa-note-11-mt4">Покупатель сам выбирает вариант на сайте и видит цену каждого.</span>
          </div>
        </div>
      )}

      <h4 style={{ margin: "14px 0 8px" }}>Цена для покупателя</h4>
      <div className="mv-field-grid">
        <Num label="Наценка к тарифу, %" step={0.1} value={pricing.extraPct} onChange={(n) => set({ pricing: { ...pricing, extraPct: n } })} hint="2,5% покрывает комиссию Ozon Pay с суммы доставки" />
        <Num label="Наценка к тарифу, ₽" value={pricing.extraRub} onChange={(n) => set({ pricing: { ...pricing, extraRub: n } })} hint="Упаковка, если нужно" />
        <Num label="Округлять вверх до, ₽" value={pricing.roundTo} onChange={(n) => set({ pricing: { ...pricing, roundTo: n } })} />
        <Num label="Минимальная цена, ₽" value={pricing.minRub} onChange={(n) => set({ pricing: { ...pricing, minRub: n } })} />
        <Num label="Дней на сборку (к сроку службы)" value={pricing.handlingDays} onChange={(n) => set({ pricing: { ...pricing, handlingDays: n } })} />
      </div>
      <Check label="Создавать отправку в службе автоматически сразу после оплаты" checked={Boolean(v.autoCreateShipment)} onChange={(x) => set({ autoCreateShipment: x })} />
      <span className="sa-note-11-mt4">Выключено — кнопка «Создать отправку» в карточке заказа (вкладка «Заказы»).</span>

      <div style={{ marginTop: 14 }}>
        <button type="button" className="secondary-action" onClick={runPreview} disabled={previewLoading}>
          {previewLoading ? <Loader2 size={13} className="spin" /> : null} Проверить цены (сохранённые настройки, флакон 100 мл)
        </button>
        {preview && (
          <div className="sa-scroll-x" style={{ marginTop: 10 }}>
            <table className="sa-table-plain" style={{ width: "100%", fontSize: 12.5, borderCollapse: "collapse" }}>
              <thead><tr><th style={{ textAlign: "left" }}>Город</th><th style={{ textAlign: "left" }}>Вариант</th><th>Тариф</th><th>Покупатель</th><th>Срок</th></tr></thead>
              <tbody>
                {preview.flatMap((r) => (r.options.length ? r.options : [{ id: "none", title: "нет вариантов", priceRub: 0, rawRub: 0, daysMin: null, daysMax: null }]).map((o, i) => (
                  <tr key={r.city + o.id} style={{ borderTop: i === 0 ? "1px solid #ddd" : undefined }}>
                    <td>{i === 0 ? r.city : ""}</td><td>{o.title}</td>
                    <td style={{ textAlign: "right" }}>{o.rawRub ? `${o.rawRub} ₽` : ""}</td>
                    <td style={{ textAlign: "right", fontWeight: 700 }}>{o.priceRub ? `${o.priceRub} ₽` : ""}</td>
                    <td style={{ textAlign: "center" }}>{o.daysMin != null ? `${o.daysMin}–${o.daysMax} дн.` : ""}</td>
                  </tr>
                )))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
