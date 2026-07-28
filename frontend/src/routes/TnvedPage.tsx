import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, Loader2, RefreshCw, Save, Send, Tag, XCircle } from "lucide-react";
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
  };
  wb: {
    tnvedCode: string;
    subjectId: number;
    subjectName: string;
  };
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
      {result.ok ? "✓ Применено" : "Завершено с ошибками"}: {result.updated ?? 0} из {result.total} товаров.
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

  const reportQuery = useQuery({
    queryKey: ["tnved-report"],
    queryFn: () => apiJson<TnvedReport>("/api/tnved/report"),
    staleTime: 60 * 1000,
  });

  const categories = categoriesQuery.data?.categories || [];
  const totalProducts = categoriesQuery.data?.totalProducts ?? 0;
  const progress = progressQuery.data;
  const report = reportQuery.data;

  useEffect(() => {
    if (categories.length && !Object.keys(localCodes).length) {
      const codes: Record<string, string> = {};
      for (const cat of categories) {
        const key = `${cat.descCatId}:${cat.typeId}`;
        codes[key] = cat.tnvedCode || "";
      }
      setLocalCodes(codes);
    }
  }, [categories]);

  useEffect(() => {
    if (report?.yandex.defaultCode && !defaultCode) {
      setDefaultCode(report.yandex.defaultCode);
    }
  }, [report]);

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
    },
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

  const hasAssignments = categories.some((cat) => (localCodes[`${cat.descCatId}:${cat.typeId}`] || "").trim());
  const isApplying = progress?.running || applyMutation.isPending;

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
        <div className="settings-grid" style={{ marginBottom: 0 }}>
          <div className="settings-panel" style={{ padding: "12px 16px" }}>
            <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 4 }}>Ozon</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {report.ozon.assignedCategories > 0
                ? <CheckCircle2 size={14} style={{ color: "var(--success)" }} />
                : <XCircle size={14} style={{ color: "var(--danger)" }} />}
              <span style={{ fontWeight: 600 }}>
                {report.ozon.assignedCategories} / {report.ozon.totalCategories} категорий с кодом
              </span>
              <span className="muted" style={{ fontSize: 12 }}>· {report.ozon.totalProducts} товаров</span>
            </div>
            {report.ozon.lastApplied ? (
              <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>
                Последнее применение: {new Date(report.ozon.lastApplied).toLocaleString("ru-RU")}
              </div>
            ) : (
              <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>Ещё не применялось</div>
            )}
          </div>
          <div className="settings-panel" style={{ padding: "12px 16px" }}>
            <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 4 }}>Яндекс.Маркет</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {report.yandex.defaultCode
                ? <CheckCircle2 size={14} style={{ color: "var(--success)" }} />
                : <XCircle size={14} style={{ color: "var(--danger)" }} />}
              <span style={{ fontWeight: 600 }}>
                {report.yandex.defaultCode ? `Код по умолчанию: ${report.yandex.defaultCode}` : "Код не задан"}
              </span>
              <span className="muted" style={{ fontSize: 12 }}>· {report.yandex.totalProducts} товаров</span>
            </div>
            <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>
              Устанавливается по категориям Ozon + код по умолчанию
            </div>
          </div>
          <div className="settings-panel" style={{ padding: "12px 16px" }}>
            <div style={{ fontSize: 12, color: "var(--muted)", marginBottom: 4 }}>Wildberries</div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              {report.wb.tnvedCode
                ? <CheckCircle2 size={14} style={{ color: "var(--success)" }} />
                : <XCircle size={14} style={{ color: "var(--danger)" }} />}
              <span style={{ fontWeight: 600 }}>
                {report.wb.tnvedCode
                  ? `Код: ${report.wb.tnvedCode}`
                  : "Код не задан (берётся из справочника WB)"}
              </span>
            </div>
            {report.wb.subjectName ? (
              <div className="muted" style={{ fontSize: 11, marginTop: 2 }}>
                Предмет: {report.wb.subjectName} (#{report.wb.subjectId})
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {/* Прогресс применения Ozon */}
      {(progress?.running || progress?.completedAt) ? (
        <div className="settings-panel" style={{ marginBottom: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            {progress.running ? <Loader2 className="spin" size={14} /> : null}
            <strong style={{ fontSize: 13 }}>
              {progress.running ? phaseLabel(progress.phase) : (progress.errors ? "Завершено с ошибками" : "Успешно применено")}
            </strong>
            {progress.completedAt ? (
              <span className="muted" style={{ fontSize: 12 }}>в {new Date(progress.completedAt).toLocaleTimeString("ru-RU")}</span>
            ) : null}
          </div>
          <div className="progress-line">
            {progressPct !== null ? (
              <span style={{ width: `${progressPct}%` }} />
            ) : (
              <span style={{ width: "100%", animation: "pulse 1.5s ease-in-out infinite" }} />
            )}
            <span style={{ position: "relative", zIndex: 1, fontSize: 12 }}>
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
                onClick={() => saveAssignments.mutate()}
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
              <p className="form-hint">
                Всего товаров: {totalProducts} в {categories.length} категориях/типах.
                Введите коды ТН ВЭД и нажмите «Сохранить коды», затем «Отправить на Ozon».
                Сохранённые коды также используются при отправке на Яндекс.Маркет.
              </p>
              <div style={{ overflowX: "auto" }}>
                <table className="data-table" style={{ minWidth: 600 }}>
                  <thead>
                    <tr>
                      <th>Категория / Тип продукта</th>
                      <th style={{ width: 80, textAlign: "right" }}>Товаров</th>
                      <th style={{ width: 180 }}>Код ТН ВЭД</th>
                    </tr>
                  </thead>
                  <tbody>
                    {categories.map((cat) => {
                      const key = `${cat.descCatId}:${cat.typeId}`;
                      const code = localCodes[key] ?? "";
                      return (
                        <tr key={key}>
                          <td>
                            <div style={{ fontWeight: 500 }}>{cat.displayName || `Категория ${cat.descCatId}`}</div>
                            <div className="muted" style={{ fontSize: 11 }}>
                              catId:{cat.descCatId} typeId:{cat.typeId}
                              {cat.offerIds.length ? ` · пр: ${cat.offerIds[0]}` : ""}
                            </div>
                          </td>
                          <td style={{ textAlign: "right" }}>{cat.count}</td>
                          <td>
                            <input
                              type="text"
                              className="field-input"
                              placeholder="например 3303009000"
                              value={code}
                              maxLength={20}
                              style={{ width: "100%", fontFamily: "monospace" }}
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
          <div className="form-hint" style={{ marginBottom: 12, lineHeight: 1.7 }}>
            <strong>Парфюмерия (3303)</strong>
          </div>
          <div className="form-hint" style={{ fontFamily: "monospace", lineHeight: 2, marginBottom: 12 }}>
            <div style={{ background: "var(--highlight, rgba(0,180,80,.08))", borderRadius: 4, padding: "2px 6px", display: "inline-block" }}>
              <strong>3303009000</strong> — Туалетная и парфюмерная вода
            </div>
            <div>3303001000 — Духи (parfums, &gt;20% масла)</div>
          </div>
          <div className="form-hint" style={{ marginBottom: 4, lineHeight: 1.7 }}>
            <strong>Косметика и макияж (3304)</strong>
          </div>
          <div className="form-hint" style={{ fontFamily: "monospace", lineHeight: 2, marginBottom: 12 }}>
            <div>3304100000 — Средства для макияжа губ</div>
            <div>3304200000 — Средства для макияжа глаз</div>
            <div>3304300000 — Лаки для ногтей</div>
            <div>3304910000 — Пудры (компактные и рассыпчатые)</div>
            <div>3304990000 — Прочие: кремы, лосьоны, тональный</div>
          </div>
          <div className="form-hint" style={{ marginBottom: 4, lineHeight: 1.7 }}>
            <strong>Средства для волос (3305)</strong>
          </div>
          <div className="form-hint" style={{ fontFamily: "monospace", lineHeight: 2, marginBottom: 12 }}>
            <div>3305100000 — Шампуни</div>
            <div>3305200000 — Средства для перманентной завивки</div>
            <div>3305300000 — Лак для волос</div>
            <div>3305900000 — Прочие: кондиционеры, маски</div>
          </div>
          <div className="form-hint" style={{ marginBottom: 4, lineHeight: 1.7 }}>
            <strong>Прочая парфюмерия (3307)</strong>
          </div>
          <div className="form-hint" style={{ fontFamily: "monospace", lineHeight: 2 }}>
            <div>3307200000 — Дезодоранты и антиперспиранты</div>
            <div>3307900000 — Прочие косметические товары</div>
          </div>

          {/* Яндекс.Маркет */}
          <div className="section-title compact-title" style={{ marginTop: 20 }}>
            <div><span>Яндекс.Маркет</span><h3>Отправить ТН ВЭД на Яндекс</h3></div>
          </div>
          <p className="form-hint">
            Код определяется по категории Ozon (если товар связан). Для остальных — код по умолчанию.
            Яндекс.Маркет товаров: {report?.yandex.totalProducts ?? "…"}
          </p>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
            <input
              type="text"
              className="field-input"
              placeholder="3303009000"
              value={defaultCode}
              maxLength={20}
              style={{ fontFamily: "monospace", width: 160, flexShrink: 0 }}
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
            <div className="inline-error" style={{ marginBottom: 8 }}>{String((saveDefaultCodeMutation.error as Error).message)}</div>
          ) : null}
          <div className="row-actions" style={{ marginTop: 4 }}>
            <button
              className="secondary-action"
              type="button"
              disabled={yandexPreviewMutation.isPending || yandexApplyMutation.isPending}
              onClick={() => yandexPreviewMutation.mutate()}
            >
              {yandexPreviewMutation.isPending ? <Loader2 className="spin" size={14} /> : <Tag size={14} />} Предпросмотр
            </button>
            <button
              className="primary-action"
              type="button"
              disabled={yandexApplyMutation.isPending || yandexPreviewMutation.isPending}
              onClick={() => {
                if (window.confirm(`Отправить коды ТН ВЭД на ${report?.yandex.totalProducts ?? "все"} товаров Яндекс.Маркет?`)) {
                  yandexApplyMutation.mutate();
                }
              }}
            >
              {yandexApplyMutation.isPending ? <Loader2 className="spin" size={14} /> : <Send size={14} />} Отправить на Яндекс
            </button>
          </div>
          {renderApplyResult(yandexPreview, yandexPreviewMutation.error as Error | null)}
          {renderApplyResult(yandexApplyMutation.data ?? null, yandexApplyMutation.error as Error | null)}
        </section>
      </div>
    </section>
  );
}
