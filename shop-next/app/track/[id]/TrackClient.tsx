"use client";
import { useCallback, useEffect, useState } from "react";
import dynamic from "next/dynamic";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Phone, RefreshCw, Loader2, Check, ExternalLink, MapPin, Clock, Package } from "lucide-react";
import { CARRIER_LOGO, slotText } from "@/lib/delivery";

const TrackMap = dynamic(() => import("@/components/TrackMap"), { ssr: false });
const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";

type Live = {
  stage: "search" | "to_pickup" | "on_way" | "delivered" | "canceled";
  origin: { lat: number; lng: number } | null; eta: string | null; pickedUpAt: string | null; deliveredAt: string | null; trackingUrl: string | null;
  courier: { name: string; phone: string | null; photo: string | null; lat: number | null; lng: number | null; vehicle: string | null } | null;
};
type Track = {
  ok: boolean; orderId: string; shopStatus: string; carrier: string | null; carrierTitle: string | null; method: string | null; kind: string | null;
  daysMin: number | null; daysMax: number | null;
  destination: { lat?: number; lng?: number; address: string; pvzName: string | null };
  slot: { from: string; to: string } | null;
  shipment: { status: string; statusLabel: string | null; number: string | null; trackingUrl: string | null } | null;
  live: Live | null; error?: string;
};

const hm = (iso: string) => new Date(iso).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Moscow" });
const minutesTo = (iso: string) => Math.round((Date.parse(iso) - Date.now()) / 60000);

const STAGES: Record<Live["stage"], { title: string; sub: string }> = {
  search: { title: "Ищем курьера", sub: "Обычно это занимает 5–15 минут" },
  to_pickup: { title: "Курьер едет за заказом", sub: "Заберёт посылку в магазине и сразу поедет к вам" },
  on_way: { title: "Курьер везёт заказ", sub: "Он позвонит перед приездом" },
  delivered: { title: "Заказ доставлен", sub: "Спасибо, что выбрали Magic Vibes!" },
  canceled: { title: "Доставка отменена", sub: "Напишите нам в чат — поможем" },
};

const CSS = `
.tr{min-height:100vh;background:var(--paper);padding-bottom:110px}
.tr-wrap{max-width:1100px;margin:0 auto;padding:clamp(14px,3vw,32px) clamp(12px,3vw,24px);display:grid;grid-template-columns:minmax(0,1fr) 380px;gap:16px}
@media (max-width:900px){.tr-wrap{grid-template-columns:1fr}}
.tr-map{position:relative;border-radius:26px;overflow:hidden;min-height:440px;background:#eee;box-shadow:0 10px 30px rgba(0,0,0,.08)}
@media (max-width:900px){.tr-map{min-height:52vh;order:-1}}
.tr-card{background:#fff;border-radius:26px;padding:20px;display:flex;flex-direction:column;gap:14px;box-shadow:0 10px 30px rgba(0,0,0,.05)}
.tr-top{display:flex;align-items:center;justify-content:space-between;gap:10px}
.tr-top img{height:18px}
.tr-id{font-size:12.5px;color:var(--muted);font-weight:600}
.tr-status h1{margin:0;font-family:var(--font-display);font-weight:800;text-transform:uppercase;font-size:clamp(22px,4vw,28px);line-height:1.05}
.tr-status p{margin:6px 0 0;color:var(--muted);font-size:14px}
.tr-eta{background:#121212;color:#d9f84a;border-radius:18px;padding:14px 16px;display:flex;align-items:center;gap:10px;font-weight:700}
.tr-eta b{font-size:22px;color:#fff}
.tr-steps{display:flex;flex-direction:column;gap:0;margin:2px 0}
.tr-step{display:flex;gap:12px;align-items:flex-start;position:relative;padding-bottom:14px}
.tr-step:not(:last-child):before{content:"";position:absolute;left:11px;top:24px;bottom:0;width:2px;background:rgba(var(--ink-rgb),.1)}
.tr-step.done:not(:last-child):before{background:#121212}
.tr-dot{width:24px;height:24px;border-radius:50%;border:2px solid rgba(var(--ink-rgb),.2);background:#fff;flex-shrink:0;display:grid;place-items:center}
.tr-step.done .tr-dot{background:#121212;border-color:#121212;color:#d9f84a}
.tr-step.now .tr-dot{border-color:#4b3cff;box-shadow:0 0 0 4px rgba(75,60,255,.15)}
.tr-step b{font-size:14px;display:block}.tr-step span{font-size:12.5px;color:var(--muted)}
.tr-courier{display:flex;align-items:center;gap:12px;border:1.5px solid rgba(var(--ink-rgb),.1);border-radius:18px;padding:12px}
.tr-ava{width:48px;height:48px;border-radius:50%;background:#4b3cff center/cover;flex-shrink:0}
.tr-call{margin-left:auto;display:flex;align-items:center;gap:6px;background:#d9f84a;color:#121212;border-radius:999px;padding:10px 14px;font-weight:800;text-decoration:none;font-size:14px}
.tr-row{display:flex;gap:10px;align-items:flex-start;font-size:14px;line-height:1.45}
.tr-row svg{flex-shrink:0;margin-top:2px;color:var(--muted)}
.tr-muted{font-size:12.5px;color:var(--muted);display:flex;align-items:center;gap:8px;justify-content:space-between}
.tr-btn{border:1.5px solid rgba(var(--ink-rgb),.14);background:#fff;border-radius:999px;padding:8px 12px;font:inherit;font-size:13px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;gap:6px;color:var(--ink);text-decoration:none}
.tr-empty{display:grid;place-items:center;height:100%;min-height:440px;text-align:center;color:var(--muted);padding:24px}
`;

export default function TrackClient({ id }: { id: string }) {
  const k = useSearchParams().get("k") || "";
  const [t, setT] = useState<Track | null>(null);
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const [at, setAt] = useState<Date | null>(null);

  const load = useCallback(async () => {
    setBusy(true);
    try {
      const r = await fetch(`${API}/track/${encodeURIComponent(id)}?k=${encodeURIComponent(k)}`, { cache: "no-store" });
      const d: Track = await r.json();
      if (!r.ok || !d.ok) { setErr(d.error || "Заказ не найден"); return; }
      setT(d); setErr(""); setAt(new Date());
    } catch { setErr("Нет связи — пробуем ещё раз…"); }
    finally { setBusy(false); }
  }, [id, k]);

  // курьер в пути — обновляем чаще; вкладка скрыта — не опрашиваем
  const stage = t?.live?.stage;
  const period = stage === "on_way" || stage === "to_pickup" ? 10000 : stage === "delivered" || stage === "canceled" ? 0 : 30000;
  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!period) return;
    const tm = setInterval(() => { if (document.visibilityState === "visible") void load(); }, period);
    const onVis = () => { if (document.visibilityState === "visible") void load(); };
    document.addEventListener("visibilitychange", onVis);
    return () => { clearInterval(tm); document.removeEventListener("visibilitychange", onVis); };
  }, [period, load]);

  if (err && !t) return (
    <div className="tr"><style>{CSS}</style>
      <div className="tr-wrap"><div className="tr-card" style={{ gridColumn: "1/-1" }}>
        <div className="tr-status"><h1>{err}</h1><p>Проверьте ссылку из письма или откройте заказ в <Link href="/account">личном кабинете</Link>.</p></div>
      </div></div>
    </div>
  );
  if (!t) return <div className="tr"><style>{CSS}</style><div className="tr-empty"><Loader2 className="mv-spin" /></div></div>;

  const live = t.live;
  const st = live ? STAGES[live.stage] : null;
  const cour = live?.courier;
  const courierPt = cour?.lat != null && cour?.lng != null ? { lat: cour.lat, lng: cour.lng } : null;
  const dest = t.destination.lat && t.destination.lng ? { lat: t.destination.lat, lng: t.destination.lng } : null;
  const hasMap = !!(dest || courierPt || live?.origin);
  const etaMin = live?.eta && live.stage !== "delivered" ? minutesTo(live.eta) : null;

  // без живого курьера (СДЭК / Яндекс / ещё не передан)
  const plainTitle = !t.shipment
    ? (["pending", "payment_pending", "payment_failed"].includes(t.shopStatus) ? "Ожидает оплаты" : t.shopStatus === "cancelled" ? "Заказ отменён" : "Собираем заказ")
    : t.shipment.statusLabel || "Передан в доставку";
  const steps = live ? [
    { key: "made", title: "Заказ передан в доставку", done: true },
    { key: "courier", title: cour ? `Курьер назначен — ${cour.name}` : "Назначаем курьера", done: live.stage !== "search" },
    { key: "picked", title: "Курьер забрал заказ", sub: live.pickedUpAt ? `в ${hm(live.pickedUpAt)}` : "", done: !!live.pickedUpAt || live.stage === "delivered" },
    { key: "done", title: "Доставлен", sub: live.deliveredAt ? `в ${hm(live.deliveredAt)}` : "", done: live.stage === "delivered" },
  ] : [];
  const nowIdx = steps.findIndex((s) => !s.done);

  return (
    <div className="tr">
      <style>{CSS}</style>
      <div className="tr-wrap">
        <div className="tr-map">
          {hasMap ? <TrackMap origin={live?.origin} destination={dest} courier={courierPt} courierPhoto={cour?.photo} />
            : <div className="tr-empty"><div><Package size={36} /><p>Карта появится, когда заказ передадут курьеру</p></div></div>}
        </div>

        <div className="tr-card">
          <div className="tr-top">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            {t.carrier && CARRIER_LOGO[t.carrier] ? <img src={CARRIER_LOGO[t.carrier]} alt="" /> : <span />}
            <span className="tr-id">Заказ {t.orderId}</span>
          </div>

          <div className="tr-status">
            <h1>{st ? st.title : plainTitle}</h1>
            <p>{st ? st.sub : t.carrierTitle || ""}</p>
          </div>

          {etaMin != null && etaMin > -30 && (
            <div className="tr-eta"><Clock size={20} /><span>Приедет примерно в <b>{hm(live!.eta!)}</b>{etaMin > 0 ? ` · через ${etaMin} мин` : ""}</span></div>
          )}
          {!live && t.slot && <div className="tr-eta"><Clock size={20} /><span>Время доставки: <b>{slotText(t.slot)}</b></span></div>}

          {cour && live?.stage !== "delivered" && (
            <div className="tr-courier">
              <span className="tr-ava" style={cour.photo ? { backgroundImage: `url('${cour.photo.replace(/'/g, "")}')` } : undefined} />
              <div><b style={{ display: "block" }}>{cour.name}</b><span className="tr-id">{cour.vehicle || "Курьер Достависты"}</span></div>
              {cour.phone && <a className="tr-call" href={`tel:${cour.phone}`}><Phone size={15} />Позвонить</a>}
            </div>
          )}

          {steps.length > 0 && (
            <div className="tr-steps">
              {steps.map((s, i) => (
                <div key={s.key} className={`tr-step${s.done ? " done" : ""}${i === nowIdx ? " now" : ""}`}>
                  <span className="tr-dot">{s.done && <Check size={13} />}</span>
                  <div><b>{s.title}</b>{s.sub && <span>{s.sub}</span>}</div>
                </div>
              ))}
            </div>
          )}

          <div className="tr-row"><MapPin size={16} /><span>{t.destination.pvzName ? `${t.destination.pvzName}: ` : ""}{t.destination.address || "Адрес уточняется"}</span></div>
          {t.shipment?.number && <div className="tr-row"><Package size={16} /><span>Трек-номер: <b>{t.shipment.number}</b></span></div>}
          {!live && t.daysMin != null && !["delivered"].includes(t.shopStatus) && <div className="tr-row"><Clock size={16} /><span>Срок доставки: {t.daysMin}–{t.daysMax} дн.</span></div>}

          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {t.shipment?.trackingUrl && <a className="tr-btn" href={t.shipment.trackingUrl} target="_blank" rel="noopener noreferrer"><ExternalLink size={14} />На сайте службы</a>}
            <Link className="tr-btn" href="/account">Мои заказы</Link>
          </div>

          <div className="tr-muted">
            <span>{at ? `Обновлено в ${at.toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit", second: "2-digit" })}` : ""}{err ? ` · ${err}` : ""}</span>
            <button type="button" className="tr-btn" onClick={() => void load()} disabled={busy}>{busy ? <Loader2 size={13} className="mv-spin" /> : <RefreshCw size={13} />}Обновить</button>
          </div>
        </div>
      </div>
    </div>
  );
}
