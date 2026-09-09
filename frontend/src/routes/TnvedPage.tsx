import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Loader2, RefreshCw, Save, Send, Tag, Wrench, XCircle } from "lucide-react";
import { PageHeader } from "../components/PageHeader";

type TnvedCategory = {
  descCatId: number;
  typeId: number;
  count: number;
  displayName: string;
  categoryName: string;
  typeName: string;
  tnvedCode: string;
  offerIds: string[];
};

type CategoriesResponse = {
  categories: TnvedCategory[];
  totalProducts: number;
};

type TnvedProgress = {
  running: boolean;
  phase: "loading_products" | "loading_attributes" | "sending" | null;
  totalProducts: number | null;
  processed: number | null;
  updated: number | null;
  errors: number | null;
  startedAt: string | null;
  completedAt: string | null;
  categoryStats: Array<{ key: string; attrName: string | null; found: boolean; tnvedCode: string }> | null;
};

type ApplyResult = {
  ok: boolean;
  dryRun?: boolean;
  total?: number;
  candidates?: number;
  updated?: number;
  errors?: number;
  failed?: number;
  async?: boolean;
  withCategory?: number;
  withFallback?: number;
  skipped?: number;
  error?: string;
};

type TnvedReport = {
  ozon: {
    assignedCategories: number;
    totalCategories: number;
    totalProducts: number;
    lastApplied: string | null;
    assignments: Array<{ descCatId: number; typeId: number; tnvedCode: string }>;
  };
  yandex: {
    totalProducts: number;
    defaultCode: string;
    lastApplied: string | null;
    withCategory: number | null;
    withFallback: number | null;
    skipped: number | null;
    updatedCount: number | null;
  };
  wb: {
    tnvedCode: string;
    subjectId: number;
    subjectName: string;
  };
};

type YandexTnvedProgress = {
  running: boolean;
  totalProducts: number | null;
  candidates: number | null;
  updated: number | null;
  failed: number | null;
  withCategory: number | null;
  withFallback: number | null;
  skipped: number | null;
  startedAt: string | null;
  completedAt: string | null;
  error: string | null;
};

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const r = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error((data as { error?: string })?.error || `HTTP ${r.status}`);
  return data as T;
}

function phaseLabel(phase: TnvedProgress["phase"]) {
  if (phase === "loading_products") return "Загрузка товаров с Ozon…";
  if (phase === "loading_attributes") return "Поиск атрибута ТН ВЭД в категориях…";
  if (phase === "sending") return "Отправка ТН ВЭД на Ozon…";
  return "Работа…";
}

function renderApplyResult(result: ApplyResult | null, error: Error | null) {
  if (error) return <div className="inline-error">{error.message}</div>;
  if (!result) return null;
  if (result.async) return null;
  if (!result.ok && result.error) return <div className="inline-error">{result.error}</div>;
  if (result.dryRun) {
    return (
      <div className="info-strip success compact">
        Предпросмотр: найдено {result.candidates} из {result.total} товаров.
        {result.withCategory != null ? ` По категории Ozon: ${result.withCategory}` : ""}
        {result.withFallback != null ? ` · По умолчанию: ${result.withFallback}` : ""}
        {result.skipped != null && result.skipped > 0 ? ` · Без кода: ${result.skipped}` : ""}
        . Нажмите «Отправить» для применения.
      </div>
    );
  }
  return (
    <div className={`info-strip ${result.ok ? "success" : "warn"} compact`}>
      {result.ok ? "✓ Применено" : "Завершено с ошибками"}: {result.updated ?? 0} из {result.candidates ?? result.total} товаров.
      {result.withCategory != null ? ` По категории: ${result.withCategory}` : ""}
      {result.withFallback != null ? ` · По умолчанию: ${result.withFallback}` : ""}
      {result.failed != null && result.failed > 0 ? ` · Ошибок: ${result.failed}` : ""}
    </div>
  );
}

export function TnvedPage() {
  const queryClient = useQueryClient();
  const [localCodes, setLocalCodes] = useState<Record<string, string>>({});
  const [saved, setSaved] = useState(false);
  const [previewResult, setPreviewResult] = useState<ApplyResult | null>(null);
  const [yandexPreview, setYandexPreview] = useState<ApplyResult | null>(null);
  const [defaultCode, setDefaultCode] = useState("");
  const [defaultCodeSaved, setDefaultCodeSaved] = useState(false);

  const categoriesQuery = useQuery({
    queryKey: ["ozon-tnved-categories"],
    queryFn: () => apiJson<CategoriesResponse>("/api/ozon/tnved/categories"),
    staleTime: 5 * 60 * 1000,
  });

  const progressQuery = useQuery({
    queryKey: ["ozon-tnved-progress"],
    queryFn: () => apiJson<TnvedProgress>("/api/ozon/tnved/progress"),
    refetchInterval: (query) => (query.state.data?.running ? 2000 : 0),
  });

  const yandexProgressQuery = useQuery({
    queryKey: ["yandex-tnved-progress"],
    queryFn: () => apiJson<YandexTnvedProgress>("/api/yandex/tnved/progress"),
    refetchInterval: (query) => (query.state.data?.running ? 2000 : 0),
  });

  const reportQuery = useQuery({
    queryKey: ["tnved-report"],
    queryFn: () => apiJson<TnvedReport>("/api/tnved/report"),
    staleTime: 60 * 1000,
  });

  const categories = categoriesQuery.data?.categories || [];
  const totalProducts = categoriesQuery.data?.totalProducts ?? 0;
  const progress = progressQuery.data;
  const yandexProgress = yandexProgressQuery.data;
  const report = reportQuery.data;

  useEffect(() => {
    if (!categories.length) return;
    const codes: Record<string, string> = {};
    for (const cat of categories) {
      const key = `${cat.descCatId}:${cat.typeId}`;
      codes[key] = cat.tnvedCode || "";
    }
    setLocalCodes(codes);
  }, [categories]);

  useEffect(() => { if (report?.yandex.defaultCode) setDefaultCode((c) => c || (report?.yandex.defaultCode ?? "")); }, [report]);

  const saveAssignments = useMutation({
    mutationFn: () => {
      const assignments = categories.map((cat) => ({
        descCatId: cat.descCatId,
        typeId: cat.typeId,
        tnvedCode: localCodes[`${cat.descCatId}:${cat.typeId}`] || "",
      }));
      return apiJson<{ ok: boolean; saved: number }>("/api/ozon/tnved/assignments", {
        method: "PUT",
        body: JSON.stringify({ assignments }),
      });
    },
    onSuccess: () => {
      setSaved(true);
      void queryClient.invalidateQueries({ queryKey: ["tnved-report"] });
      setTimeout(() => setSaved(false), 3000);
    },
  });

  const previewApply = useMutation({
    mutationFn: () => apiJson<ApplyResult>("/api/ozon/tnved/apply", {
      method: "POST",
      body: JSON.stringify({ dryRun: true }),
    }),
    onSuccess: (data) => setPreviewResult(data),
  });

  const applyMutation = useMutation({
    mutationFn: () => apiJson<ApplyResult>("/api/ozon/tnved/apply", {
      method: "POST",
      body: JSON.stringify({ dryRun: false }),
    }),
    onSuccess: () => {
      setPreviewResult(null);
      void queryClient.invalidateQueries({ queryKey: ["ozon-tnved-progress"] });
      void queryClient.invalidateQueries({ queryKey: ["tnved-report"] });
    },
  });

  const yandexPreviewMutation = useMutation({
    mutationFn: () => apiJson<ApplyResult>("/api/yandex/tnved/apply", {
      method: "POST",
      body: JSON.stringify({ dryRun: true }),
    }),
    onSuccess: (data) => setYandexPreview(data),
  });

  const yandexApplyMutation = useMutation({
    mutationFn: () => apiJson<ApplyResult>("/api/yandex/tnved/apply", {
      method: "POST",
      body: JSON.stringify({ dryRun: false }),
    }),
    onSuccess: () => {
      setYandexPreview(null);
      void queryClient.invalidateQueries({ queryKey: ["tnved-report"] });
      void queryClient.invalidateQueries({ queryKey: ["yandex-tnved-progress"] });
    },
  });

  const [sweepResult, setSweepResult] = useState<string | null>(null);

  const sweepMutation = useMutation({
    mutationFn: () => apiJson<{ ok: boolean; message?: string; error?: string }>("/api/ozon/tnved/sweep/run", {
      method: "POST",
    }),
    onSuccess: (data) => setSweepResult(data.message || (data.ok ? "Sweep запущен" : (data.error ?? "Ошибка"))),
    onError: (err: Error) => setSweepResult(`Ошибка: ${err.message}`),
  });

  const saveDefaultCodeMutation = useMutation({
    mutationFn: (code: string) => apiJson<{ ok: boolean; code: string }>("/api/settings/tnved-code", {
      method: "PUT",
      body: JSON.stringify({ code }),
    }),
    onSuccess: () => {
      setDefaultCodeSaved(true);
      void queryClient.invalidateQueries({ queryKey: ["tnved-report"] });
      setTimeout(() => setDefaultCodeSaved(false), 3000);
    },
  });

  const [showOnlyWithoutCode, setShowOnlyWithoutCode] = useState(false);

  const hasAssignments = categories.some((cat) => (localCodes[`${cat.descCatId}:${cat.typeId}`] || "").trim());
  const isApplying = progress?.running || applyMutation.isPending;

  const withCodeProducts = categories.filter((c) => (localCodes[`${c.descCatId}:${c.typeId}`] || "").trim()).reduce((s, c) => s + c.count, 0);
  const withoutCodeProducts = totalProducts - withCodeProducts;
  const coveragePct = totalProducts > 0 ? Math.round((withCodeProducts / totalProducts) * 100) : 0;
  const visibleCategories = showOnlyWithoutCode
    ? [...categories].filter((c) => !(localCodes[`${c.descCatId}:${c.typeId}`] || "").trim()).sort((a, b) => b.count - a.count)
    : [...categories].sort((a, b) => b.count - a.count);
  const isYandexApplying = yandexProgress?.running || yandexApplyMutation.isPending;

  const progressPct = progress?.running && progress.totalProducts && progress.processed != null
    ? Math.round((progress.processed / progress.totalProducts) * 100)
    : null;

  return (
    <section className="page-section">
      <PageHeader
        title="Коды ТН ВЭД"
        subtitle="Назначение кодов ТН ВЭД по категориям — Ozon и Яндекс.Маркет"
        action={(
          <div className="row-actions">
            <button
              className="secondary-action"
              type="button"
              disabled={!hasAssignments || previewApply.isPending || isApplying}
              onClick={() => previewApply.mutate()}
            >
              {previewApply.isPending ? <Loader2 className="spin" size={16} /> : <Tag size={16} />} Предпросмотр Ozon
            </button>
            <button
              className="primary-action"
              type="button"
              disabled={!hasAssignments || isApplying || (!saved && previewResult === null)}
              onClick={() => {
                if (window.confirm(`Применить коды ТН ВЭД к ${totalProducts} товарам на Ozon?`)) applyMutation.mutate();
              }}
            >
              {isApplying ? <Loader2 className="spin" size={16} /> : <Send size={16} />}
              {isApplying ? "Применяется…" : "Отправить на Ozon"}
            </button>
          </div>
        )}
      />

      {/* Отчёт по покрытию */}
      {report ? (
        <div className="settings-grid tnved-status-grid">
          <div className="settings-panel tnved-mp-panel">
            <div className="tnved-mp-label">Ozon</div>
            <div className="tnved-mp-status">
              {report.ozon.assignedCategories > 0
                ? <CheckCircle2 size={14} className="tnved-mp-icon-ok" />
                : <XCircle size={14} className="tnved-mp-icon-fail" />}
              <strong>{report.ozon.assignedCategories} / {report.ozon.totalCategories} категорий с кодом</strong>
              <span className="muted tnved-mp-count">· {report.ozon.totalProducts} товаров</span>
            </div>
            {report.ozon.lastApplied ? (
              <div className="muted tnved-mp-note">
                Последнее применение: {new Date(report.ozon.lastApplied).toLocaleString("ru-RU")}
              </div>
            ) : (
              <div className="muted tnved-mp-note">Ещё не применялось</div>
            )}
          </div>
          <div className="settings-panel tnved-mp-panel">
            <div className="tnved-mp-label">Яндекс.Маркет</div>
            <div className="tnved-mp-status">
              {report.yandex.defaultCode
                ? <CheckCircle2 size={14} className="tnved-mp-icon-ok" />
                : <XCircle size={14} className="tnved-mp-icon-fail" />}
              <strong>{report.yandex.defaultCode ? `Код по умолчанию: ${report.yandex.defaultCode}` : "Код не задан"}</strong>
              <span className="muted tnved-mp-count">· {report.yandex.totalProducts} товаров</span>
            </div>
            {report.yandex.lastApplied ? (
              <>
                <div className="muted tnved-mp-note">
                  Последнее применение: {new Date(report.yandex.lastApplied).toLocaleString("ru-RU")}
                  {report.yandex.updatedCount != null ? ` · Отправлено: ${report.yandex.updatedCount}` : ""}
                </div>
                {(report.yandex.withCategory != null || report.yandex.withFallback != null) ? (
                  <div className="muted tnved-mp-note">
                    {report.yandex.withCategory != null ? `По категории: ${report.yandex.withCategory}` : ""}
                    {report.yandex.withFallback != null ? ` · По умолчанию: ${report.yandex.withFallback}` : ""}
                    {report.yandex.skipped != null && report.yandex.skipped > 0 ? ` · Без кода: ${report.yandex.skipped}` : ""}
                  </div>
                ) : null}
              </>
            ) : (
              <div className="muted tnved-mp-note">
                Устанавливается по категориям Ozon + код по умолчанию · Ещё не применялось
              </div>
            )}
          </div>
          <div className="settings-panel tnved-mp-panel">
            <div className="tnved-mp-label">Wildberries</div>
            <div className="tnved-mp-status">
              {report.wb.tnvedCode
                ? <CheckCircle2 size={14} className="tnved-mp-icon-ok" />
                : <XCircle size={14} className="tnved-mp-icon-fail" />}
              <strong>
                {report.wb.tnvedCode ? `Код: ${report.wb.tnvedCode}` : "Код не задан (берётся из справочника WB)"}
              </strong>
            </div>
            {report.wb.subjectName ? (
              <div className="muted tnved-mp-note">
                Предмет: {report.wb.subjectName} (#{report.wb.subjectId})
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {/* Прогресс применения Ozon */}
      {(progress?.running || progress?.completedAt) ? (
        <div className="settings-panel tnved-progress-panel">
          <div className="tnved-progress-head">
            {progress.running ? <Loader2 className="spin" size={14} /> : null}
            <strong className="tnved-progress-title">
              {progress.running ? phaseLabel(progress.phase) : (progress.errors ? "Завершено с ошибками" : "Успешно применено")}
            </strong>
            {progress.completedAt ? (
              <span className="muted tnved-progress-time">в {new Date(progress.completedAt).toLocaleTimeString("ru-RU")}</span>
            ) : null}
          </div>
          <div className="progress-line">
            {progressPct !== null ? (
              <span style={{ width: `${progressPct}%` }} />
            ) : (
              <span style={{ width: "100%", animation: "pulse 1.5s ease-in-out infinite" }} />
            )}
            <span className="tnved-progress-text">
              {progress.running
                ? (progress.processed != null && progress.totalProducts
                  ? `${progress.processed} из ${progress.totalProducts} товаров`
                  : phaseLabel(progress.phase))
                : `Обновлено: ${progress.updated ?? 0} · Ошибок: ${progress.errors ?? 0}`}
            </span>
          </div>
        </div>
      ) : null}

      {/* Предпросмотр Ozon */}
      {previewResult?.dryRun ? (
        <div className="info-strip success">
          Предпросмотр: будет обновлено {previewResult.candidates} из {previewResult.total} товаров.
          Нажмите «Отправить на Ozon» для применения.
        </div>
      ) : null}
      {previewApply.error ? <div className="inline-error">{String((previewApply.error as Error).message)}</div> : null}
      {applyMutation.error ? <div className="inline-error">{String((applyMutation.error as Error).message)}</div> : null}

      <div className="settings-grid">
        {/* Ozon категории */}
        <section className="settings-panel settings-panel-wide">
          <div className="section-title">
            <div>
              <span>Ozon</span>
              <h3>Назначение ТН ВЭД по категориям и типам</h3>
            </div>
            <div className="row-actions">
              <button
                className="secondary-action"
                type="button"
                disabled={categoriesQuery.isFetching}
                onClick={() => void queryClient.invalidateQueries({ queryKey: ["ozon-tnved-categories"] })}
              >
                <RefreshCw size={16} /> Обновить
              </button>
              <button
                className="primary-action"
                type="button"
                disabled={saveAssignments.isPending || categoriesQuery.isFetching}
                onClick={() => {
                  const invalidCodes = Object.entries(localCodes).filter(([, code]) => code && !/^\d{10}$/.test((code as string).replace(/\s/g, "")));
                  if (invalidCodes.length > 0) {
                    alert(`Некорректные коды ТН ВЭД: ${invalidCodes.map(([k]) => k).join(", ")}. Код должен содержать 10 цифр.`);
                    return;
                  }
                  saveAssignments.mutate();
                }}
              >
                {saveAssignments.isPending ? <Loader2 className="spin" size={16} /> : <Save size={16} />}
                {saved ? "Сохранено ✓" : "Сохранить коды"}
              </button>
            </div>
          </div>

          {categoriesQuery.isFetching ? (
            <div className="table-note">
              <Loader2 className="spin" size={14} /> Загружаю категории с Ozon… (может занять 1–2 минуты)
            </div>
          ) : categoriesQuery.error ? (
            <div className="inline-error">{String((categoriesQuery.error as Error).message)}</div>
          ) : !categories.length ? (
            <div className="table-note">Категории не найдены. Настройте Ozon API в кабинетах.</div>
          ) : (
            <>
              {/* Прогресс покрытия кодами */}
              <div className="tnved-coverage-box">
                <div className="tnved-coverage-head">
                  <span className="tnved-coverage-stat">
                    Покрытие: <span style={{ color: withoutCodeProducts === 0 ? "var(--success)" : "var(--warn)" }}>{withCodeProducts}</span> из {totalProducts} товаров с кодом ({coveragePct}%)
                  </span>
                  {withoutCodeProducts > 0 && (
                    <button
                      type="button"
                      className={`${showOnlyWithoutCode ? "primary-action" : "secondary-action"} tnved-filter-btn`}
                      onClick={() => setShowOnlyWithoutCode((v) => !v)}
                    >
                      {showOnlyWithoutCode ? "Показать все" : `Только без кода (${withoutCodeProducts} тов.)`}
                    </button>
                  )}
                </div>
                <div className="tnved-bar">
                  <div className="tnved-bar-fill" style={{ width: `${coveragePct}%`, background: withoutCodeProducts === 0 ? "var(--success)" : "var(--primary)" }} />
                </div>
                {withoutCodeProducts > 0 && (
                  <div className="tnved-coverage-note">
                    Без кода: {withoutCodeProducts} товаров в {categories.filter((c) => !(localCodes[`${c.descCatId}:${c.typeId}`] || "").trim()).length} категориях
                  </div>
                )}
              </div>
              <p className="form-hint">
                Введите коды ТН ВЭД и нажмите «Сохранить коды», затем «Отправить на Ozon».
                Сохранённые коды также используются при отправке на Яндекс.Маркет. Таблица отсортирована по количеству товаров.
              </p>
              <div className="tnved-table-scroll">
                <table className="data-table tnved-table">
                  <thead>
                    <tr>
                      <th>Категория / Тип продукта</th>
                      <th className="tnved-th-count">Товаров</th>
                      <th className="tnved-th-code">Код ТН ВЭД</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleCategories.map((cat) => {
                      const key = `${cat.descCatId}:${cat.typeId}`;
                      const code = localCodes[key] ?? "";
                      return (
                        <tr key={key}>
                          <td>
                            <div className="tnved-cat-name">{cat.displayName || `Категория ${cat.descCatId}`}</div>
                            <div className="muted tnved-cat-id">
                              catId:{cat.descCatId} typeId:{cat.typeId}
                              {cat.offerIds.length ? ` · пр: ${cat.offerIds[0]}` : ""}
                            </div>
                          </td>
                          <td className="tnved-td-right">{cat.count}</td>
                          <td>
                            <input
                              type="text"
                              className="field-input tnved-code-input"
                              placeholder="например 3303001000"
                              value={code}
                              maxLength={20}
                              onChange={(e) => {
                                setSaved(false);
                                setLocalCodes((prev) => ({ ...prev, [key]: e.target.value }));
                              }}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              {saveAssignments.error ? (
                <div className="inline-error">{String((saveAssignments.error as Error).message)}</div>
              ) : null}
            </>
          )}
        </section>

        {/* Справочник кодов */}
        <section className="settings-panel">
          <div className="section-title compact-title">
            <div><span>Справочник</span><h3>Актуальные коды ЕАЭС 2025</h3></div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Парфюмерия (3303) — Духи и туалетная вода</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div className="tnved-ref-highlight">
              <strong>3303001000</strong> — Духи и туалетная вода: — духи (parfums)
            </div>
            <div>3303009000 — Духи и туалетная вода: — туалетная вода</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Косметика и макияж (3304)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3304100000 — Средства для макияжа губ</div>
            <div>3304200000 — Средства для макияжа глаз</div>
            <div>3304300000 — Средства для маникюра или педикюра</div>
            <div>3304910000 — Средства для макияжа и ухода за кожей: — — пудра, включая компактную</div>
            <div>3304990000 — Прочие: кремы, лосьоны, тональный</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Средства для волос (3305)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3305100000 — Средства для волос: — шампуни</div>
            <div>3305200000 — Средства для волос: — средства для перманентной завивки или распрямления волос</div>
            <div>3305300000 — Средства для волос: — лаки для волос</div>
            <div>3305900001 — Средства для волос: — прочие: — — лосьоны для волос</div>
            <div>3305900009 — Средства для волос: — прочие: — — прочие</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Гигиена полости рта (3306)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3306100000 — Средства для гигиены полости рта: — средства для чистки зубов</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Прочие туалетные средства (3307)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3307100000 — Средства до, во время или после бритья</div>
            <div>3307200000 — Дезодоранты и антиперспиранты индивидуального назначения</div>
            <div>3307300000 — Соли и прочие средства для ванн</div>
            <div>3307490000 — Прочие: — — прочие (ароматические палочки, диффузоры)</div>
            <div>3307900008 — Прочие туалетные средства: — прочие: — — прочие (дезодоранты для помещений)</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Мыло и средства для мытья кожи (3401)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3401300000 — Поверхностно-активные средства для мытья кожи в виде жидкости или крема (гели для душа, жидкое мыло)</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Моющие и чистящие средства (3402)</strong>
          </div>
          <div className="form-hint tnved-ref-codes">
            <div>3402909000 — Прочие поверхностно-активные средства: — — моющие средства и чистящие средства</div>
          </div>
          <div className="form-hint tnved-ref-label">
            <strong>Свечи (3406)</strong>
          </div>
          <div className="form-hint tnved-ref-codes" style={{ marginBottom: 0 }}>
            <div>3406000000 — Свечи, тонкие восковые свечки и аналогичные изделия</div>
          </div>

          {/* Яндекс.Маркет */}
          <div className="section-title compact-title tnved-section-mt">
            <div><span>Яндекс.Маркет</span><h3>Отправить ТН ВЭД на Яндекс</h3></div>
          </div>
          <p className="form-hint">
            Код определяется по категории Ozon (если товар связан). Для остальных — код по умолчанию.
            Яндекс.Маркет товаров: {report?.yandex.totalProducts ?? "…"}
          </p>
          <div className="tnved-yandex-input-row">
            <input
              type="text"
              className="field-input tnved-yandex-input"
              placeholder="3303001000"
              value={defaultCode}
              maxLength={20}
              onChange={(e) => { setDefaultCode(e.target.value.replace(/\s/g, "")); setDefaultCodeSaved(false); }}
            />
            <button
              className="secondary-action"
              type="button"
              disabled={saveDefaultCodeMutation.isPending || !defaultCode.trim()}
              onClick={() => saveDefaultCodeMutation.mutate(defaultCode.trim())}
            >
              {saveDefaultCodeMutation.isPending ? <Loader2 className="spin" size={14} /> : <Save size={14} />}
              {defaultCodeSaved ? "Сохранено ✓" : "Сохранить код"}
            </button>
          </div>
          {saveDefaultCodeMutation.error ? (
            <div className="inline-error tnved-error-mb">{String((saveDefaultCodeMutation.error as Error).message)}</div>
          ) : null}
          <div className="row-actions tnved-yandex-actions">
            <button
              className="secondary-action"
              type="button"
              disabled={yandexPreviewMutation.isPending || isYandexApplying}
              onClick={() => yandexPreviewMutation.mutate()}
            >
              {yandexPreviewMutation.isPending ? <Loader2 className="spin" size={14} /> : <Tag size={14} />} Предпросмотр
            </button>
            <button
              className="primary-action"
              type="button"
              disabled={isYandexApplying || yandexPreviewMutation.isPending}
              onClick={() => {
                if (window.confirm(`Отправить коды ТН ВЭД на ${report?.yandex.totalProducts ?? "все"} товаров Яндекс.Маркет?`)) {
                  yandexApplyMutation.mutate();
                }
              }}
            >
              {isYandexApplying ? <Loader2 className="spin" size={14} /> : <Send size={14} />}
              {isYandexApplying ? "Отправляется…" : "Отправить на Яндекс"}
            </button>
          </div>
          {renderApplyResult(yandexPreview, yandexPreviewMutation.error as Error | null)}
          {yandexApplyMutation.error ? <div className="inline-error">{String((yandexApplyMutation.error as Error).message)}</div> : null}

          {/* Прогресс применения Яндекс */}
          {(yandexProgress?.running || yandexProgress?.completedAt) ? (
            <div className="settings-panel tnved-yandex-progress">
              <div className="tnved-progress-head tnved-yandex-progress-head">
                {yandexProgress.running ? <Loader2 className="spin" size={13} /> : null}
                <strong className="tnved-progress-time">
                  {yandexProgress.running
                    ? "Отправка ТН ВЭД на Яндекс.Маркет…"
                    : yandexProgress.error
                      ? `Ошибка: ${yandexProgress.error}`
                      : yandexProgress.failed
                        ? "Завершено с ошибками"
                        : "Успешно применено на Яндекс"}
                </strong>
                {yandexProgress.completedAt ? (
                  <span className="muted tnved-mp-note">в {new Date(yandexProgress.completedAt).toLocaleTimeString("ru-RU")}</span>
                ) : null}
              </div>
              <div className="progress-line">
                <span style={{ width: yandexProgress.running ? "100%" : `${yandexProgress.candidates && yandexProgress.updated != null ? Math.round((yandexProgress.updated / yandexProgress.candidates) * 100) : 100}%`, animation: yandexProgress.running ? "pulse 1.5s ease-in-out infinite" : undefined }} />
                <span className="tnved-progress-text--sm">
                  {yandexProgress.running
                    ? `${yandexProgress.candidates ?? "…"} товаров в очереди`
                    : `Отправлено: ${yandexProgress.updated ?? 0}${yandexProgress.failed ? ` · Ошибок: ${yandexProgress.failed}` : ""}${yandexProgress.withCategory != null ? ` · По категории: ${yandexProgress.withCategory}` : ""}${yandexProgress.withFallback != null ? ` · По умолчанию: ${yandexProgress.withFallback}` : ""}`}
                </span>
              </div>
            </div>
          ) : null}

          {/* Sweep: исправить пустые и неверные коды */}
          <div className="section-title compact-title tnved-section-mt">
            <div><span>Sweep</span><h3>Исправить коды на Ozon</h3></div>
          </div>
          <p className="form-hint">
            Автосвип (каждые 2 ч) проставляет правильный код по категории: пустые и неверные
            (напр. 3303009000 вместо 3303001000 для Парфюмерии). Запустить вручную:
          </p>
          <button
            className="primary-action"
            type="button"
            disabled={sweepMutation.isPending}
            onClick={() => {
              setSweepResult(null);
              sweepMutation.mutate();
            }}
          >
            {sweepMutation.isPending ? <Loader2 className="spin" size={14} /> : <Wrench size={14} />} Запустить исправление
          </button>
          {sweepResult ? (
            <div className={`info-strip ${sweepMutation.isError ? "warn" : "success"} compact tnved-sweep-result`}>
              {sweepResult}
            </div>
          ) : null}
        </section>
      </div>
    </section>
  );
}
