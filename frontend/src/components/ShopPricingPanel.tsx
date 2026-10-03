import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Copy, Loader2, Percent, Plus, Save, Trash2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { errorMessage, numberValue } from "../lib/common";
import { toast } from "../lib/toast";

// «Отдельные настройки магазина»: у кабинета (например, Ozon AURA) свои базовая наценка, ступени по цене
// закупки и правила наличия. Товары этого кабинета считаются по ним, остальные — по общим правилам выше.

function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  return fetchJson<T>(url, z.custom<T>(() => true), init);
}

type Rule = { minUsd: number; coefficient: number };
type Avail = { minAvailableSuppliers: number; coefficientDelta: number; targetStock: number };
type Profile = {
  enabled: boolean; marketplace: string; label: string; defaultMarkup: number;
  markupRules: Rule[]; availabilityRules: Avail[]; copiedFrom?: string; updatedAt?: string | null;
};
type Shop = { id: string; kind: string; label: string };
type Resp = { shops: Shop[]; profiles: Record<string, Profile> };
type Reprice = { products?: number; queued?: number; error?: string } | null;

const MP: Record<string, string> = { ozon: "Ozon", yandex: "Маркет" };

export function ShopPricingPanel() {
  const queryClient = useQueryClient();
  const data = useQuery({ queryKey: ["target-pricing"], queryFn: () => apiJson<Resp>("/api/settings/target-pricing") });
  const shops = data.data?.shops || [];
  const profiles = data.data?.profiles || {};
  // AURA first when it exists, else the first shop that already has a profile
  const initial = useMemo(() => shops.find((s) => /aura/i.test(s.label))?.id || Object.keys(profiles)[0] || shops[0]?.id || "", [shops, profiles]);
  const [shopId, setShopId] = useState("");
  useEffect(() => { if (!shopId && initial) setShopId(initial); }, [initial, shopId]);
  const shop = shops.find((s) => s.id === shopId);
  const saved = profiles[shopId];
  const [draft, setDraft] = useState<Profile | null>(null);
  useEffect(() => { setDraft(saved ? JSON.parse(JSON.stringify(saved)) : null); }, [shopId, saved]);
  const dirty = Boolean(draft && saved && JSON.stringify(draft) !== JSON.stringify(saved));
  const [percent, setPercent] = useState("5");
  const [confirm, setConfirm] = useState<"" | "copy" | "delete">("");

  const done = (r: { reprice?: Reprice }, text: string) => {
    queryClient.invalidateQueries({ queryKey: ["target-pricing"] });
    queryClient.invalidateQueries({ queryKey: ["settings"] });
    const rp = r.reprice;
    toast.success(rp?.products ? `${text}. Пересчитываем цены: ${rp.products.toLocaleString("ru")} товаров` : text);
    setConfirm("");
  };
  const copy = useMutation({
    mutationFn: () => apiJson<{ reprice?: Reprice }>(`/api/settings/target-pricing/${encodeURIComponent(shopId)}/copy`, mutationBody({})),
    onSuccess: (r) => done(r, "Скопировано с общих настроек — цены пока такие же"),
    onError: (e) => toast.error(errorMessage(e)),
  });
  const save = useMutation({
    mutationFn: () => apiJson<{ reprice?: Reprice; changed?: boolean }>(`/api/settings/target-pricing/${encodeURIComponent(shopId)}`, { method: "PUT", body: JSON.stringify({ profile: draft }) }),
    onSuccess: (r) => done(r, r.changed ? "Сохранено" : "Без изменений"),
    onError: (e) => toast.error(errorMessage(e)),
  });
  const adjust = useMutation({
    mutationFn: (direction: "increase" | "decrease") => apiJson<{ reprice?: Reprice }>(`/api/settings/target-pricing/${encodeURIComponent(shopId)}/adjust-percent`, mutationBody({ direction, percent: numberValue(percent) })),
    onSuccess: (r, direction) => done(r, `Наценки ${direction === "increase" ? "повышены" : "снижены"} на ${percent}%`),
    onError: (e) => toast.error(errorMessage(e)),
  });
  const remove = useMutation({
    mutationFn: () => apiJson<{ reprice?: Reprice }>(`/api/settings/target-pricing/${encodeURIComponent(shopId)}`, { method: "DELETE" }),
    onSuccess: (r) => done(r, "Отдельные настройки удалены — магазин снова по общим"),
    onError: (e) => toast.error(errorMessage(e)),
  });
  const busy = copy.isPending || save.isPending || adjust.isPending || remove.isPending;
  const set = (patch: Partial<Profile>) => setDraft((d) => (d ? { ...d, ...patch } : d));
  const rules = [...(draft?.markupRules || [])].map((r, i) => ({ r, i })).sort((a, b) => a.r.minUsd - b.r.minUsd || a.i - b.i);

  return (
    <div className="settings-panel settings-panel-wide">
      <div className="section-title">
        <div><span>Наценки</span><h3>Отдельные настройки магазина</h3></div>
      </div>
      <p className="settings-hint">
        У магазина могут быть свои базовая наценка, ступени по цене закупки и правила наличия. Товары этого магазина
        считаются по ним, остальные — по общим правилам выше. После сохранения цены этого магазина пересчитываются
        и проходят «Проверку цен», как обычно.
      </p>
      {data.isLoading ? <div className="soft-empty compact"><Loader2 size={14} className="spin" /> Загружаем…</div> : null}
      {data.isError ? <div className="inline-error">{errorMessage(data.error)}</div> : null}

      {shops.length ? (
        <nav className="rules-marketplace-tabs" aria-label="Магазин">
          {shops.map((s) => (
            <button key={s.id} type="button" className={s.id === shopId ? "is-active" : ""} onClick={() => { setShopId(s.id); setConfirm(""); }}>
              {s.label} · {MP[s.kind] || s.kind}
              {profiles[s.id] ? <span className="rules-tab-count">{profiles[s.id].enabled ? "свои" : "выкл"}</span> : null}
            </button>
          ))}
        </nav>
      ) : null}

      {shop && !saved ? (
        <div className="soft-empty compact">
          <p>{shop.label} сейчас считается по общим настройкам {MP[shop.kind] || shop.kind}.</p>
          <button className="primary-action" type="button" disabled={busy} onClick={() => copy.mutate()}>
            {copy.isPending ? <Loader2 size={14} className="spin" /> : <Copy size={14} />} Создать: скопировать с основного {MP[shop.kind] || ""}
          </button>
        </div>
      ) : null}

      {shop && draft ? (
        <>
          <label className="settings-toggle">
            <input type="checkbox" checked={draft.enabled} onChange={(e) => set({ enabled: e.target.checked })} />
            Считать цены {shop.label} по этим настройкам
          </label>
          <div className="settings-form-row">
            <label>
              <span className="field-label">Базовая наценка</span>
              <input type="number" min="0.0001" step="0.0001" value={String(draft.defaultMarkup || "")} onChange={(e) => set({ defaultMarkup: numberValue(e.target.value) })} />
            </label>
          </div>

          <div className="section-title" style={{ marginTop: 12 }}>
            <div><h3>Ступени по цене закупки</h3></div>
            <button className="secondary-action" type="button" onClick={() => set({ markupRules: [...draft.markupRules, { minUsd: 0, coefficient: draft.defaultMarkup || 1 }] })}>
              <Plus size={14} /> Добавить ступень
            </button>
          </div>
          <div className="settings-rule-table markup-rule-table">
            <div className="settings-rule-head"><span>От цены, USD</span><span>Коэффициент</span><span></span></div>
            {rules.map(({ r, i }) => (
              <div className="settings-rule-row" key={`shop-rule-${i}`}>
                <input type="number" min="0" step="0.0001" value={String(r.minUsd)} onChange={(e) => set({ markupRules: draft.markupRules.map((x, j) => (j === i ? { ...x, minUsd: numberValue(e.target.value) } : x)) })} />
                <input type="number" min="0.0001" step="0.0001" value={String(r.coefficient)} onChange={(e) => set({ markupRules: draft.markupRules.map((x, j) => (j === i ? { ...x, coefficient: numberValue(e.target.value, 1) } : x)) })} />
                <button className="icon-action danger" type="button" title="Удалить ступень" onClick={() => set({ markupRules: draft.markupRules.filter((_, j) => j !== i) })}><Trash2 size={15} /></button>
              </div>
            ))}
            {!rules.length ? <div className="soft-empty compact">Ступеней нет — для всех товаров магазина действует базовая наценка.</div> : null}
          </div>

          <div className="section-title" style={{ marginTop: 12 }}>
            <div><h3>Доступность и остатки</h3></div>
            <button className="secondary-action" type="button" onClick={() => set({ availabilityRules: [...draft.availabilityRules, { minAvailableSuppliers: 1, coefficientDelta: 0, targetStock: 3 }] })}>
              <Plus size={14} /> Добавить
            </button>
          </div>
          <div className="settings-rule-table availability-rule-table">
            <div className="settings-rule-head"><span>Поставщиков от</span><span>Поправка</span><span>Остаток</span><span></span></div>
            {draft.availabilityRules.map((r, i) => (
              <div className="settings-rule-row" key={`shop-avail-${i}`}>
                <input type="number" min="0" step="1" value={String(r.minAvailableSuppliers)} onChange={(e) => set({ availabilityRules: draft.availabilityRules.map((x, j) => (j === i ? { ...x, minAvailableSuppliers: Math.round(numberValue(e.target.value, 1)) } : x)) })} />
                <input type="number" step="0.0001" value={String(r.coefficientDelta)} onChange={(e) => set({ availabilityRules: draft.availabilityRules.map((x, j) => (j === i ? { ...x, coefficientDelta: numberValue(e.target.value) } : x)) })} />
                <input type="number" min="0" step="1" value={String(r.targetStock)} onChange={(e) => set({ availabilityRules: draft.availabilityRules.map((x, j) => (j === i ? { ...x, targetStock: Math.round(numberValue(e.target.value, 3)) } : x)) })} />
                <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => set({ availabilityRules: draft.availabilityRules.filter((_, j) => j !== i) })}><Trash2 size={15} /></button>
              </div>
            ))}
            {!draft.availabilityRules.length ? <div className="soft-empty compact">Правил нет — действуют общие правила наличия.</div> : null}
          </div>

          <div className="section-title-actions" style={{ marginTop: 14, flexWrap: "wrap", gap: 8 }}>
            <button className="primary-action" type="button" disabled={busy || !dirty} onClick={() => save.mutate()}>
              {save.isPending ? <Loader2 size={14} className="spin" /> : <Save size={14} />} {dirty ? "Сохранить и пересчитать цены" : "Сохранено"}
            </button>
            <span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
              <Percent size={14} />
              <input type="number" min="0.01" max="90" step="0.5" value={percent} onChange={(e) => setPercent(e.target.value)} style={{ width: 72 }} aria-label="Процент" />
              <button className="secondary-action" type="button" disabled={busy || dirty} title={dirty ? "Сначала сохраните изменения" : ""} onClick={() => adjust.mutate("increase")}>Поднять</button>
              <button className="secondary-action" type="button" disabled={busy || dirty} title={dirty ? "Сначала сохраните изменения" : ""} onClick={() => adjust.mutate("decrease")}>Снизить</button>
            </span>
            {confirm === "copy" ? (
              <span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                Заменить настройки магазина общими?
                <button className="secondary-action" type="button" disabled={busy} onClick={() => copy.mutate()}>Да</button>
                <button className="secondary-action" type="button" onClick={() => setConfirm("")}>Нет</button>
              </span>
            ) : confirm === "delete" ? (
              <span style={{ display: "inline-flex", gap: 6, alignItems: "center" }}>
                Удалить отдельные настройки {shop.label}?
                <button className="secondary-action danger" type="button" disabled={busy} onClick={() => remove.mutate()}>Да, удалить</button>
                <button className="secondary-action" type="button" onClick={() => setConfirm("")}>Нет</button>
              </span>
            ) : (
              <>
                <button className="secondary-action" type="button" disabled={busy} onClick={() => setConfirm("copy")}><Copy size={14} /> Скопировать заново с основного</button>
                <button className="secondary-action danger" type="button" disabled={busy} onClick={() => setConfirm("delete")}><Trash2 size={14} /> Удалить</button>
              </>
            )}
          </div>
          {draft.updatedAt ? <p className="settings-hint">Изменено {new Date(draft.updatedAt).toLocaleString("ru")}{draft.copiedFrom ? ` · создано копией общих настроек ${MP[draft.copiedFrom] || draft.copiedFrom}` : ""}</p> : null}
        </>
      ) : null}
    </div>
  );
}
