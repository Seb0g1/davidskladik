import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, Loader2, Save, Trash2 } from "lucide-react";
import { z } from "zod";
import { fetchJson, patchBody } from "../api";

// Markup for the magicvibes.ru storefront. Stored in shop settings (not in the marketplace
// rules), so site prices never move Ozon / Yandex prices and vice versa.
interface Rule { minUsd: number; coefficient: number }

const ShopSettingsSchema = z.object({
  markup: z.number().optional(),
  markupRules: z.array(z.object({ minUsd: z.number(), coefficient: z.number() })).optional(),
}).passthrough();

export function SiteMarkupRules({ copySources }: { copySources: { label: string; rules: Rule[] }[] }) {
  const qc = useQueryClient();
  const query = useQuery({
    queryKey: ["shop-admin-settings"],
    queryFn: () => fetchJson("/api/shop/admin/settings", ShopSettingsSchema),
  });
  const [markup, setMarkup] = useState<number>(2.2);
  const [rules, setRules] = useState<Rule[]>([]);
  const [dirty, setDirty] = useState(false);

  useEffect(() => {
    if (!query.data || dirty) return;
    setMarkup(query.data.markup ?? 2.2);
    setRules(query.data.markupRules ?? []);
  }, [query.data, dirty]);

  const save = useMutation({
    mutationFn: () => fetchJson("/api/shop/admin/settings", z.unknown(), patchBody({ markup, markupRules: rules })),
    onSuccess: () => { setDirty(false); void qc.invalidateQueries({ queryKey: ["shop-admin-settings"] }); },
  });

  const edit = (next: Rule[]) => { setRules(next); setDirty(true); };
  const sorted = rules.map((rule, index) => ({ rule, index })).sort((a, b) => a.rule.minUsd - b.rule.minUsd || a.index - b.index);

  if (query.isLoading) return <div className="soft-empty compact"><Loader2 size={14} className="spin" /> Загружаю наценки сайта…</div>;
  if (query.isError) return <div className="soft-empty compact">Не удалось загрузить настройки сайта.</div>;

  return (
    <>
      <div className="section-title-actions" style={{ justifyContent: "flex-start", flexWrap: "wrap", gap: 10, margin: "4px 0 12px" }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
          Базовая наценка сайта ×
          <input type="number" min="0.0001" step="0.0001" style={{ width: 110 }} value={String(markup)}
            onChange={(e) => { setMarkup(Number(e.target.value) || 0); setDirty(true); }} />
        </label>
        {copySources.some((s) => s.rules.length) && (
          <select className="copy-from-select" value="" title="Скопировать правила маркетплейса (заменит правила сайта)"
            onChange={(e) => {
              const src = copySources.find((s) => s.label === e.target.value);
              if (src) edit(src.rules.map((r) => ({ minUsd: r.minUsd, coefficient: r.coefficient })));
            }}>
            <option value="">Скопировать с…</option>
            {copySources.filter((s) => s.rules.length).map((s) => <option key={s.label} value={s.label}>{s.label} ({s.rules.length})</option>)}
          </select>
        )}
        <button className="secondary-action" type="button" onClick={() => edit([...rules, { minUsd: 0, coefficient: markup || 1 }])}>Добавить правило сайта</button>
        <button className="primary-action" type="button" disabled={!dirty || save.isPending} onClick={() => save.mutate()}>
          {save.isPending ? <Loader2 size={15} className="spin" /> : save.isSuccess && !dirty ? <Check size={15} /> : <Save size={15} />}
          {save.isSuccess && !dirty ? "Сохранено" : "Сохранить наценки сайта"}
        </button>
      </div>
      <div className="settings-rule-table markup-rule-table">
        <div className="settings-rule-head"><span>От цены, USD</span><span>Коэффициент</span><span></span></div>
        {sorted.map(({ rule, index }) => (
          <div className="settings-rule-row" key={`site-${index}`}>
            <input type="number" min="0" step="0.0001" value={String(rule.minUsd)}
              onChange={(e) => edit(rules.map((r, i) => (i === index ? { ...r, minUsd: Number(e.target.value) || 0 } : r)))} />
            <input type="number" min="0.0001" step="0.0001" value={String(rule.coefficient)}
              onChange={(e) => edit(rules.map((r, i) => (i === index ? { ...r, coefficient: Number(e.target.value) || 1 } : r)))} />
            <button className="icon-action danger" type="button" title="Удалить правило" onClick={() => edit(rules.filter((_, i) => i !== index))}><Trash2 size={15} /></button>
          </div>
        ))}
        {!rules.length && <div className="soft-empty compact">Правил для сайта нет — на magicvibes.ru действует базовая наценка сайта ×{markup}.</div>}
      </div>
      {save.isError && <div className="soft-empty compact">Ошибка сохранения: {String((save.error as Error)?.message || save.error)}</div>}
      <p className="settings-hint">Цена на сайте = цена поставщика × курс USD × коэффициент (рублёвые прайсы — без курса). Правило выбирается по цене поставщика в USD. Изменения видны на сайте в течение нескольких минут, фид для Яндекса обновляется раз в час.</p>
    </>
  );
}
