import { useEffect, useMemo, useRef, useState } from "react";
import { apiJson } from "../api";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckSquare, ChevronDown, ChevronUp, Download, FileCode, FileText, Loader2, RefreshCw, Search, Square, Upload, Tag, ArrowLeftRight, X } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Stat } from "../components/Stat";

type AttrResult = {
  ok?: boolean;
  dryRun?: boolean;
  total?: number;
  updated?: number;
  candidates?: number;
  reason?: string;
  tnvedCode?: string;
  failed?: number;
  errors?: Array<{ count?: number; offerId?: string; error?: string }>;
  categories?: Array<{ key: string; attr: string | null; found: boolean }>;
  sample?: Array<Record<string, unknown>>;
};

function OzonAttributesPanel() {
  const [open, setOpen] = useState(false);
  const [ozonTnved, setOzonTnved] = useState("");
  const [yandexTnved, setYandexTnved] = useState("");
  const [autoCode, setAutoCode] = useState("");
  const queryClient = useQueryClient();

  const settingsQuery = useQuery({
    queryKey: ["app-settings"],
    queryFn: () => apiJson<{ tnved?: { code?: string; autoEnabled?: boolean } }>("/api/settings"),
  });
  const savedCode = settingsQuery.data?.tnved?.code ?? "";
  const autoEnabled = settingsQuery.data?.tnved?.autoEnabled !== false;

  useEffect(() => {
    if (settingsQuery.data) setAutoCode(settingsQuery.data.tnved?.code ?? "");
  }, [settingsQuery.data]);

  const saveAutoCode = useMutation({
    mutationFn: (code: string) =>
      apiJson("/api/settings", { method: "PATCH", body: JSON.stringify({ tnved: { code: code.trim(), autoEnabled: true } }) }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["app-settings"] }),
  });

  const clearMarkingDry = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/ozon/attributes/clear-marking", { method: "POST", body: JSON.stringify({ dryRun: true }) }) });
  const clearMarkingApply = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/ozon/attributes/clear-marking", { method: "POST", body: JSON.stringify({ dryRun: false }) }) });

  const ozonTnvedDry = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/ozon/attributes/backfill-tnved", { method: "POST", body: JSON.stringify({ dryRun: true, tnvedCode: ozonTnved.trim() }) }) });
  const ozonTnvedApply = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/ozon/attributes/backfill-tnved", { method: "POST", body: JSON.stringify({ dryRun: false, tnvedCode: ozonTnved.trim() }) }) });

  const yandexTnvedDry = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/yandex/attributes/backfill-tnved", { method: "POST", body: JSON.stringify({ dryRun: true, tnvedCode: yandexTnved.trim() }) }) });
  const yandexTnvedApply = useMutation({ mutationFn: () => apiJson<AttrResult>("/api/yandex/attributes/backfill-tnved", { method: "POST", body: JSON.stringify({ dryRun: false, tnvedCode: yandexTnved.trim() }) }) });

  const renderResult = (data: AttrResult | undefined, error: Error | null) => {
    if (error) return <div className="inline-error">{error.message}</div>;
    if (!data) return null;
    const rows = data.categories?.filter((c) => !c.found) || [];
    return (
      <div className={`info-strip${data.ok ? " success" : " warn"} compact`} style={{ marginTop: 8 }}>
        {data.dryRun ? (
          <span>Проверка: всего товаров {data.total}, будет обновлено {data.candidates ?? data.updated ?? 0}
            {data.reason ? ` (${data.reason})` : ""}.
          </span>
        ) : (
          <span>Готово: обновлено {data.updated} из {data.total}
            {data.failed ? ` · ошибок: ${data.failed}` : ""}.
          </span>
        )}
        {rows.length > 0 ? <div style={{ marginTop: 4, fontSize: "0.85em", opacity: 0.8 }}>Атрибут не найден в категориях: {rows.map((c) => c.key).join(", ")}</div> : null}
        {data.errors?.length ? <div style={{ marginTop: 4, fontSize: "0.85em", color: "var(--color-error)" }}>Ошибки: {data.errors.slice(0, 3).map((e) => e.error || "неизвестно").join("; ")}</div> : null}
      </div>
    );
  };

  return (
    <div className="table-panel" style={{ marginTop: 24 }}>
      <button
        type="button"
        className="section-title"
        style={{ width: "100%", background: "none", border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px" }}
        onClick={() => setOpen((v) => !v)}
      >
        <div>
          <span>Атрибуты товаров</span>
          <h3>ТН ВЭД и код маркировки для Ozon и Яндекс.Маркет</h3>
        </div>
        {open ? <ChevronUp size={18} /> : <ChevronDown size={18} />}
      </button>

      {open ? (
        <div style={{ padding: "0 16px 16px" }}>

          {/* Автозаполнение ТН ВЭД */}
          <div style={{ marginBottom: 20, padding: "12px 14px", background: "var(--color-bg-alt, #f4f6fa)", borderRadius: 8, border: "1px solid var(--color-border, #e2e6ee)" }}>
            <div style={{ fontWeight: 600, marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
              <RefreshCw size={14} />
              Автозаполнение ТН ВЭД (Ozon · Яндекс · WB)
            </div>
            <div style={{ fontSize: "0.83em", opacity: 0.7, marginBottom: 10 }}>
              Код определяется автоматически из справочника WB по предмету и применяется на всех маркетплейсах без вашего участия.
              Укажите код вручную только если нужен другой (переопределение).
              {savedCode ? (
                <span style={{ display: "block", marginTop: 4, color: "var(--color-success, #16a34a)", fontWeight: 500 }}>
                  Сейчас активен: <code>{savedCode}</code> {autoEnabled ? "✓" : "(отключено)"}
                </span>
              ) : (
                <span style={{ display: "block", marginTop: 4, opacity: 0.6 }}>Не настроено</span>
              )}
            </div>
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              <input
                value={autoCode}
                onChange={(e) => setAutoCode(e.target.value)}
                placeholder="Например: 3303001000"
                style={{ width: 180 }}
              />
              <button
                className="primary-action"
                type="button"
                disabled={!autoCode.trim() || saveAutoCode.isPending}
                onClick={() => saveAutoCode.mutate(autoCode)}
              >
                {saveAutoCode.isPending ? <Loader2 className="spin" size={14} /> : <CheckSquare size={14} />}
                Сохранить и включить
              </button>
              {savedCode && (
                <button
                  className="secondary-action"
                  type="button"
                  disabled={saveAutoCode.isPending}
                  onClick={() => { setAutoCode(""); saveAutoCode.mutate(""); }}
                >
                  Отключить
                </button>
              )}
            </div>
            {saveAutoCode.isSuccess && <div style={{ marginTop: 6, fontSize: "0.85em", color: "var(--color-success, #16a34a)" }}>Сохранено. Следующее обслуживание маркетплейсов применит код автоматически.</div>}
            {saveAutoCode.error && <div className="inline-error" style={{ marginTop: 6 }}>{String((saveAutoCode.error as Error).message)}</div>}
          </div>

          {/* Снять код маркировки Ozon */}
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>
              <X size={14} style={{ verticalAlign: "middle", marginRight: 4 }} />
              Снять «Нужен код маркировки» на Ozon
            </div>
            <div style={{ fontSize: "0.85em", opacity: 0.75, marginBottom: 8 }}>
              Убирает галочку «Нужен код маркировки (Честный знак)» со всех товаров в кабинете Ozon.
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              <button
                className="secondary-action"
                type="button"
                disabled={clearMarkingDry.isPending || clearMarkingApply.isPending}
                onClick={() => clearMarkingDry.mutate()}
              >
                {clearMarkingDry.isPending ? <Loader2 className="spin" size={14} /> : <FileCode size={14} />} Проверить
              </button>
              {clearMarkingDry.data?.candidates ? (
                <button
                  className="primary-action"
                  type="button"
                  disabled={clearMarkingApply.isPending}
                  onClick={() => clearMarkingApply.mutate()}
                >
                  {clearMarkingApply.isPending ? <Loader2 className="spin" size={14} /> : <X size={14} />}
                  Снять маркировку ({clearMarkingDry.data.candidates} тов.)
                </button>
              ) : null}
            </div>
            {renderResult(clearMarkingDry.data, clearMarkingDry.error as Error | null)}
            {renderResult(clearMarkingApply.data, clearMarkingApply.error as Error | null)}
          </div>

          {/* ТНВЭД Ozon */}
          <div style={{ marginBottom: 20 }}>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>
              <FileCode size={14} style={{ verticalAlign: "middle", marginRight: 4 }} />
              ТН ВЭД для Ozon
            </div>
            <div style={{ fontSize: "0.85em", opacity: 0.75, marginBottom: 8 }}>
              Заполняет атрибут «ТН ВЭД» на всех товарах в кабинете Ozon. Код вводится в формате Ozon — например, <code>3303301000</code>.
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              <input
                value={ozonTnved}
                onChange={(e) => setOzonTnved(e.target.value)}
                placeholder="Например: 3303301000"
                style={{ width: 180 }}
              />
              <button
                className="secondary-action"
                type="button"
                disabled={!ozonTnved.trim() || ozonTnvedDry.isPending || ozonTnvedApply.isPending}
                onClick={() => ozonTnvedDry.mutate()}
              >
                {ozonTnvedDry.isPending ? <Loader2 className="spin" size={14} /> : <FileCode size={14} />} Проверить
              </button>
              {ozonTnvedDry.data?.candidates ? (
                <button
                  className="primary-action"
                  type="button"
                  disabled={ozonTnvedApply.isPending}
                  onClick={() => ozonTnvedApply.mutate()}
                >
                  {ozonTnvedApply.isPending ? <Loader2 className="spin" size={14} /> : <Upload size={14} />}
                  Установить ({ozonTnvedDry.data.candidates} тов.)
                </button>
              ) : null}
            </div>
            {renderResult(ozonTnvedDry.data, ozonTnvedDry.error as Error | null)}
            {renderResult(ozonTnvedApply.data, ozonTnvedApply.error as Error | null)}
          </div>

          {/* ТНВЭД Яндекс */}
          <div>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>
              <FileCode size={14} style={{ verticalAlign: "middle", marginRight: 4 }} />
              ТН ВЭД для Яндекс.Маркет
            </div>
            <div style={{ fontSize: "0.85em", opacity: 0.75, marginBottom: 8 }}>
              Отправляет код ТН ВЭД в карточки Яндекс.Маркет через поле <code>customsTariffCode</code>. Например: <code>3303 30 100 0</code>.
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              <input
                value={yandexTnved}
                onChange={(e) => setYandexTnved(e.target.value)}
                placeholder="Например: 3303 30 100 0"
                style={{ width: 180 }}
              />
              <button
                className="secondary-action"
                type="button"
                disabled={!yandexTnved.trim() || yandexTnvedDry.isPending || yandexTnvedApply.isPending}
                onClick={() => yandexTnvedDry.mutate()}
              >
                {yandexTnvedDry.isPending ? <Loader2 className="spin" size={14} /> : <FileCode size={14} />} Проверить
              </button>
              {yandexTnvedDry.data?.candidates ? (
                <button
                  className="primary-action"
                  type="button"
                  disabled={yandexTnvedApply.isPending}
                  onClick={() => yandexTnvedApply.mutate()}
                >
                  {yandexTnvedApply.isPending ? <Loader2 className="spin" size={14} /> : <Upload size={14} />}
                  Установить ({yandexTnvedDry.data.candidates} тов.)
                </button>
              ) : null}
            </div>
            {renderResult(yandexTnvedDry.data, yandexTnvedDry.error as Error | null)}
            {renderResult(yandexTnvedApply.data, yandexTnvedApply.error as Error | null)}
          </div>

        </div>
      ) : null}
    </div>
  );
}

type Candidate = {
  id: string;
  offerId: string;
  name: string;
  vendor?: string;
  imageUrl?: string;
  eligible: boolean;
  existsOnYandex: boolean;
  yandexReady: boolean;
  blockReasons: string[];
  missing?: string[];
};

type CandidatesResponse = { ok: boolean; page: number; pageSize: number; total: number; scanCapped?: boolean; items: Candidate[]; };
type EligibleIdsResponse = { ids: string[]; eligible: number; total: number };
type SyncNamesResponse = { ok: boolean; mismatched: number; sent: number; failed: number; dryRun?: boolean; sample?: { offerId: string; yandexName: string; ozonName: string }[]; errors?: unknown[] };


function statusLabel(item: Candidate): { text: string; tone: string } {
  if (item.existsOnYandex) return { text: "уже на Яндексе", tone: "muted" };
  if (item.eligible) return { text: "готов к импорту", tone: "ok" };
  if (item.blockReasons.length) return { text: item.blockReasons[0], tone: "warn" };
  if (!item.yandexReady) {
    const detail = item.missing?.length ? `: нет ${item.missing.join(", ")}` : "";
    return { text: `нет данных для карточки${detail}`, tone: "warn" };
  }
  return { text: "—", tone: "muted" };
}

export function ImportPage() {
  const queryClient = useQueryClient();
  const [query, setQuery] = useState("");
  const [debounced, setDebounced] = useState("");
  const [brandInput, setBrandInput] = useState("");
  const [brandFilter, setBrandFilter] = useState("");
  const [page, setPage] = useState(1);
  const [onlyEligible, setOnlyEligible] = useState(true);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [syncNamesResult, setSyncNamesResult] = useState<SyncNamesResponse | null>(null);
  const prevRefreshRunning = useRef(false);

  useEffect(() => {
    const timer = window.setTimeout(() => { setDebounced(query.trim()); setPage(1); }, 400);
    return () => window.clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    const timer = window.setTimeout(() => { setBrandFilter(brandInput.trim()); setPage(1); }, 400);
    return () => window.clearTimeout(timer);
  }, [brandInput]);

  const candidatesQuery = useQuery({
    queryKey: ["import-candidates", debounced, brandFilter, page, onlyEligible],
    queryFn: () => apiJson<CandidatesResponse>(
      `/api/ozon-yandex-import/candidates?q=${encodeURIComponent(debounced)}&brand=${encodeURIComponent(brandFilter)}&page=${page}&pageSize=40&onlyEligible=${onlyEligible}`,
    ),
  });
  const refreshStatus = useQuery({
    queryKey: ["import-refresh-status"],
    queryFn: () => apiJson<{ running: boolean; lastResult?: { at?: string; imported?: number; error?: string } }>("/api/ozon-yandex-import/refresh/status"),
    refetchInterval: (q) => (q.state.data?.running ? 4000 : false),
  });

  const refresh = useMutation({
    mutationFn: () => apiJson("/api/ozon-yandex-import/refresh", { method: "POST", body: JSON.stringify({}) }),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ["import-refresh-status"] }); },
  });
  const sendSelected = useMutation({
    mutationFn: () => apiJson<{ sent: number; failed: number; skipped: unknown[] }>("/api/ozon-yandex-import/send-selected", {
      method: "POST",
      body: JSON.stringify({ ids: Array.from(selected) }),
    }),
    onSuccess: () => {
      setSelected(new Set());
      void queryClient.invalidateQueries({ queryKey: ["import-candidates"] });
    },
  });
  const selectByBrand = useMutation({
    mutationFn: (brand: string) => apiJson<EligibleIdsResponse>(
      `/api/ozon-yandex-import/candidates/eligible-ids?brand=${encodeURIComponent(brand)}`,
    ),
    onSuccess: (data) => {
      setSelected((current) => {
        const next = new Set(current);
        data.ids.forEach((id) => next.add(id));
        return next;
      });
    },
  });
  const syncNames = useMutation({
    mutationFn: () => apiJson<SyncNamesResponse>("/api/ozon-yandex-import/sync-names", {
      method: "POST",
      body: JSON.stringify({}),
    }),
    onSuccess: (data) => {
      setSyncNamesResult(data);
      void queryClient.invalidateQueries({ queryKey: ["import-candidates"] });
    },
  });
  const repairDescriptions = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; sent?: number; candidates?: number; apiCalls?: number; apiErrors?: number }>(
      "/api/ozon-yandex-import/repair-yandex-descriptions",
      { method: "POST", body: JSON.stringify({ dryRun: false, limit: 5000 }) },
    ),
  });

  const items = candidatesQuery.data?.items || [];
  const total = candidatesQuery.data?.total || 0;
  const eligibleOnPage = useMemo(() => items.filter((item) => item.eligible && !item.existsOnYandex), [items]);
  const allPageSelected = eligibleOnPage.length > 0 && eligibleOnPage.every((item) => selected.has(item.id));

  const uniqueBrands = useMemo(() => {
    const seen = new Set<string>();
    const brands: string[] = [];
    for (const item of items) {
      const v = item.vendor?.trim();
      if (v && !seen.has(v.toLowerCase())) {
        seen.add(v.toLowerCase());
        brands.push(v);
      }
    }
    return brands.slice(0, 12);
  }, [items]);

  const toggle = (id: string) => {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };
  const togglePage = () => {
    setSelected((current) => {
      const next = new Set(current);
      if (allPageSelected) eligibleOnPage.forEach((item) => next.delete(item.id));
      else eligibleOnPage.forEach((item) => next.add(item.id));
      return next;
    });
  };

  const isRunning = Boolean(refreshStatus.data?.running);
  useEffect(() => {
    if (prevRefreshRunning.current && !isRunning) {
      void queryClient.invalidateQueries({ queryKey: ["import-candidates"] });
    }
    prevRefreshRunning.current = isRunning;
  }, [isRunning, queryClient]);

  const refreshing = refresh.isPending || isRunning;

  return (
    <section className="page-section import-page">
      <PageHeader
        title="Импорт на Яндекс"
        subtitle="Перенос товаров с Ozon на Яндекс.Маркет: обнови список, найди по артикулу, выбери и импортируй."
        action={(
          <div className="row-actions">
            <button className="secondary-action" type="button" disabled={repairDescriptions.isPending} onClick={() => { if (!window.confirm("Добавить описания товарам на Яндексе? Это перезапишет пустые описания.")) return; repairDescriptions.mutate(); }} title="Получить описания из Ozon и отправить на Яндекс для товаров без описания">
              {repairDescriptions.isPending ? <Loader2 className="spin" size={16} /> : <FileText size={16} />} Добавить описания
            </button>
            <button className="secondary-action" type="button" disabled={syncNames.isPending} onClick={() => syncNames.mutate()} title="Найти товары где название на Ozon отличается от Яндекс и исправить">
              {syncNames.isPending ? <Loader2 className="spin" size={16} /> : <ArrowLeftRight size={16} />} Синхронизировать названия
            </button>
            <button className="secondary-action" type="button" disabled={refreshing} onClick={() => refresh.mutate()}>
              {refreshing ? <Loader2 className="spin" size={16} /> : <RefreshCw size={16} />} Обновить с Ozon
            </button>
            <button className="primary-action" type="button" disabled={!selected.size || sendSelected.isPending} onClick={() => sendSelected.mutate()}>
              {sendSelected.isPending ? <Loader2 className="spin" size={16} /> : <Upload size={16} />} Импортировать выбранные ({selected.size})
            </button>
          </div>
        )}
      />
      <section className="dashboard-metrics">
        <Stat label={onlyEligible ? "Готовы к импорту" : "Всего товаров Ozon"} value={total} tone="accent" icon={<Download size={18} />} />
        <Stat label="Выбрано" value={selected.size} tone={selected.size ? "warn" : "success"} icon={<CheckSquare size={18} />} />
      </section>

      {candidatesQuery.data?.scanCapped ? (
        <div className="info-strip warn compact">
          Показаны первые 50 000 товаров. Используй поиск, чтобы найти конкретный артикул.
        </div>
      ) : null}
      {refreshStatus.data?.lastResult?.at ? (
        <div className="info-strip compact">
          Последнее обновление каталога Ozon: {new Date(refreshStatus.data.lastResult.at).toLocaleString("ru-RU")}
          {typeof refreshStatus.data.lastResult.imported === "number" ? ` · ${refreshStatus.data.lastResult.imported} карточек` : ""}
          {refreshStatus.data.lastResult.error ? ` · ошибка: ${refreshStatus.data.lastResult.error}` : ""}
        </div>
      ) : null}

      {repairDescriptions.data ? (
        <div className={`info-strip ${repairDescriptions.data.ok !== false ? "success" : "warn"}`}>
          Описания: найдено {repairDescriptions.data.candidates ?? 0} товаров без описания
          {" · "}отправлено на Яндекс {repairDescriptions.data.sent ?? 0}
          {repairDescriptions.data.apiCalls ? ` · запросов к Ozon: ${repairDescriptions.data.apiCalls}` : ""}
          {repairDescriptions.data.apiErrors ? ` · ошибок: ${repairDescriptions.data.apiErrors}` : ""}
        </div>
      ) : null}
      {repairDescriptions.error ? <div className="inline-error">{String((repairDescriptions.error as Error).message)}</div> : null}

      {syncNamesResult ? (
        <div className={`info-strip ${syncNamesResult.ok ? "success" : "warn"}`}>
          <div>
            Синхронизация названий: расхождений {syncNamesResult.mismatched}
            {" · "}обновлено {syncNamesResult.sent}
            {syncNamesResult.failed ? ` · ошибок: ${syncNamesResult.failed}` : ""}
          </div>
          {syncNamesResult.errors && syncNamesResult.errors.length > 0 ? (
            <ul className="import-skipped">
              {(syncNamesResult.errors as Array<{ offerId?: string; error?: string }>).slice(0, 10).map((e, i) => (
                <li key={e.offerId || i}><b>{e.offerId || "—"}</b>: {e.error || "ошибка"}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
      {syncNames.error ? <div className="inline-error">{String((syncNames.error as Error).message)}</div> : null}

      <div className="filters-row">
        <label className="search-box">
          <Search size={16} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Поиск по артикулу или названию" />
        </label>
        <label className="search-box">
          <Tag size={16} />
          <input value={brandInput} onChange={(event) => setBrandInput(event.target.value)} placeholder="Фильтр по бренду" />
        </label>
        <label className="settings-toggle">
          <input type="checkbox" checked={onlyEligible} onChange={(event) => { setOnlyEligible(event.target.checked); setPage(1); }} />
          Только готовые к импорту
        </label>
      </div>

      {uniqueBrands.length > 0 ? (
        <div className="brand-chips">
          {uniqueBrands.map((b) => (
            <button
              key={b}
              type="button"
              className={`brand-chip${brandFilter.toLowerCase() === b.toLowerCase() ? " active" : ""}`}
              onClick={() => {
                const same = brandFilter.toLowerCase() === b.toLowerCase();
                setBrandInput(same ? "" : b);
                setBrandFilter(same ? "" : b);
              }}
            >
              {b}
            </button>
          ))}
          {brandFilter ? (
            <button
              type="button"
              className="brand-chip select-all"
              disabled={selectByBrand.isPending}
              onClick={() => selectByBrand.mutate(brandFilter)}
              title={`Выбрать все готовые товары бренда «${brandFilter}» по всем страницам`}
            >
              {selectByBrand.isPending ? <Loader2 className="spin" size={12} /> : <CheckSquare size={12} />}
              {" "}Выбрать все «{brandFilter}»
              {selectByBrand.data ? ` (${selectByBrand.data.eligible})` : ""}
            </button>
          ) : null}
        </div>
      ) : null}

      {sendSelected.data ? (
        <div className={`info-strip ${sendSelected.data.sent ? "success" : "warn"}`}>
          <div>Импортировано: {sendSelected.data.sent}{sendSelected.data.failed ? ` · ошибок: ${sendSelected.data.failed}` : ""}{sendSelected.data.skipped?.length ? ` · пропущено: ${sendSelected.data.skipped.length}` : ""}</div>
          {sendSelected.data.sent === 0 && sendSelected.data.skipped?.length > 0 && (
            <div className="inline-warning">Все выбранные товары уже импортированы на Яндекс</div>
          )}
          {sendSelected.data.skipped?.length ? (
            <ul className="import-skipped">
              {(sendSelected.data.skipped as Array<{ offerId?: string; reasons?: string[] }>).slice(0, 20).map((row, index) => (
                <li key={row.offerId || index}><b>{row.offerId || "—"}</b>: {(row.reasons || []).join("; ") || "не готов к выгрузке"}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
      {sendSelected.error ? <div className="inline-error">{String((sendSelected.error as Error).message)}</div> : null}
      {candidatesQuery.error ? <div className="inline-error">{String((candidatesQuery.error as Error).message)}</div> : null}

      <div className="table-panel import-table">
        <div className="table-head import-row">
          <span>
            <button className="checkbox-btn" type="button" onClick={togglePage} title="Выбрать все на странице" disabled={!eligibleOnPage.length}>
              {allPageSelected ? <CheckSquare size={16} /> : <Square size={16} />}
            </button>
          </span>
          <span>Товар</span>
          <span>Артикул</span>
          <span>Бренд</span>
          <span>Статус</span>
        </div>
        {candidatesQuery.isFetching ? <div className="table-note"><Loader2 className="spin" size={14} /> Загружаю…</div> : null}
        {items.map((item) => {
          const status = statusLabel(item);
          const selectable = item.eligible && !item.existsOnYandex;
          return (
            <div className={`table-row import-row${selected.has(item.id) ? " is-selected" : ""}`} key={item.id}>
              <span>
                <button className="checkbox-btn" type="button" disabled={!selectable} onClick={() => toggle(item.id)}>
                  {selected.has(item.id) ? <CheckSquare size={16} /> : <Square size={16} />}
                </button>
              </span>
              <span className="import-product">
                {item.imageUrl ? <img src={item.imageUrl} alt="" loading="lazy" /> : <span className="import-noimg" />}
                <span className="import-name">{item.name || item.offerId}</span>
              </span>
              <span data-label="Артикул">{item.offerId}</span>
              <span data-label="Бренд">
                {item.vendor ? (
                  <button
                    type="button"
                    className="brand-chip inline"
                    onClick={() => { setBrandInput(item.vendor!); setBrandFilter(item.vendor!); setPage(1); }}
                  >
                    {item.vendor}
                  </button>
                ) : "—"}
              </span>
              <span data-label="Статус"><span className={`pill ${status.tone}`}>{status.text}</span></span>
            </div>
          );
        })}
        {!items.length && !candidatesQuery.isFetching ? <div className="empty-state">Товаров по фильтру нет. Нажми «Обновить с Ozon», если добавил новые.</div> : null}
      </div>

      <div className="pager">
        <button className="secondary-action" type="button" disabled={page <= 1} onClick={() => setPage((value) => Math.max(1, value - 1))}>Назад</button>
        <span>Стр. {page} · {total} тов.</span>
        <button className="secondary-action" type="button" disabled={page * 40 >= total} onClick={() => setPage((value) => value + 1)}>Дальше</button>
      </div>

      <OzonAttributesPanel />
    </section>
  );
}
