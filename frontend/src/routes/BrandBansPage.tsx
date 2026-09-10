import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArchiveX, ChevronDown, ChevronRight, Eye, Loader2, Plus, Trash2, X } from "lucide-react";
import { PageHeader } from "../components/PageHeader";

type BrandBan = {
  id: string;
  displayBrand: string;
  normalizedBrand: string;
  note?: string;
  bannedAt: string;
  bannedOfferIds?: string[];
};

type BrandBansResponse = { bans: BrandBan[] };

type BrandSuggestion = {
  displayBrand: string;
  normalizedBrand: string;
  marketplaces: string[];
};

type BrandsSearchResponse = { brands: BrandSuggestion[] };

type PreviewProduct = {
  id: string;
  name: string;
  offerId: string;
  marketplace: string;
  brand: string;
};

type PreviewResponse = {
  ban: BrandBan;
  total: number;
  wbCount?: number;
  suggestions?: string[];
  wbSuggestions?: string[];
  products: PreviewProduct[];
};

type ApplyResponse = {
  ok: boolean;
  archived: number;
  stockZeroed?: number;
  total: number;
  failed: number;
  wbCards?: number;
  wbZeroed?: number;
  wbError?: string;
  ozonApiExtra?: number;
  ozonApiError?: string;
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

function formatDate(iso: string) {
  return new Date(iso).toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit", year: "numeric" });
}

function mpLabel(marketplace: string) {
  return marketplace === "yandex" ? "ЯМ" : "Ozon";
}

function MpBadge({ marketplace }: { marketplace: string }) {
  const isYandex = marketplace === "yandex";
  return (
    <span className={`brand-mp-badge${isYandex ? " brand-mp-badge--ym" : " brand-mp-badge--ozon"}`}>
      {isYandex ? "ЯМ" : "Ozon"}
    </span>
  );
}

function OfferIdsPanel({ ban, onUpdated }: { ban: BrandBan; onUpdated: () => void }) {
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const currentIds: string[] = ban.bannedOfferIds ?? [];

  const updateMutation = useMutation({
    mutationFn: (offerIds: string[]) =>
      apiJson(`/api/brand-bans/${ban.id}/offer-ids`, { method: "PATCH", body: JSON.stringify({ offerIds }) }),
    onSuccess: onUpdated,
  });

  const addId = () => {
    const trimmed = input.trim();
    if (!trimmed || currentIds.includes(trimmed)) { setInput(""); return; }
    updateMutation.mutate([...currentIds, trimmed]);
    setInput("");
  };

  const removeId = (id: string) => {
    updateMutation.mutate(currentIds.filter((x) => x !== id));
  };

  return (
    <div className="brand-offer-panel">
      <button className="brand-offer-toggle" onClick={() => setOpen((v) => !v)}>
        {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        Артикулы
        {currentIds.length > 0 && <span className="brand-offer-count">{currentIds.length}</span>}
      </button>
      {open && (
        <div className="brand-offer-body">
          <div className="brand-offer-chips">
            {currentIds.map((id) => (
              <span key={id} className="brand-offer-chip">
                {id}
                <button className="brand-offer-chip-remove" onClick={() => removeId(id)} disabled={updateMutation.isPending}>
                  <X size={11} />
                </button>
              </span>
            ))}
            {currentIds.length === 0 && <span className="muted-note text-xs">Нет запрещённых артикулов</span>}
          </div>
          <div className="brand-offer-add">
            <input
              className="pm-chip-input brand-offer-input"
              placeholder="Артикул (offer_id)"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addId(); } }}
            />
            <button className="secondary-action compact" disabled={!input.trim() || updateMutation.isPending} onClick={addId}>
              {updateMutation.isPending ? <Loader2 size={12} className="spin" /> : <Plus size={12} />}
              Добавить
            </button>
          </div>
          {updateMutation.isError && (
            <div className="inline-error text-xs">
              {String((updateMutation.error as Error)?.message || "Ошибка")}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function BrandAutocomplete({
  value,
  onChange,
  onSelect,
}: {
  value: string;
  onChange: (v: string) => void;
  onSelect: (brand: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [activeIdx, setActiveIdx] = useState(0);
  const wrapRef = useRef<HTMLDivElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [debouncedQ, setDebouncedQ] = useState("");

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setDebouncedQ(value), 280);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [value]);

  const { data, isFetching } = useQuery<BrandsSearchResponse>({
    queryKey: ["brands-search", debouncedQ],
    queryFn: () => apiJson(`/api/brand-bans/brands-search?q=${encodeURIComponent(debouncedQ)}`),
    enabled: debouncedQ.length >= 2,
    staleTime: 30_000,
  });

  const brands = data?.brands || [];

  useEffect(() => {
    setActiveIdx(0);
    setOpen(brands.length > 0);
  }, [brands]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const handleSelect = (brand: BrandSuggestion) => {
    onSelect(brand.displayBrand);
    setOpen(false);
  };

  return (
    <div ref={wrapRef} className="brand-autocomplete">
      <input
        className="pm-chip-input brand-autocomplete-input"
        placeholder="Название бренда (напр. Chanel)"
        value={value}
        onChange={(e) => { onChange(e.target.value); setOpen(true); }}
        onFocus={() => { if (brands.length > 0) setOpen(true); }}
        onKeyDown={(e) => {
          if (!open) return;
          if (e.key === "ArrowDown") { e.preventDefault(); setActiveIdx((i) => Math.min(i + 1, brands.length - 1)); }
          else if (e.key === "ArrowUp") { e.preventDefault(); setActiveIdx((i) => Math.max(i - 1, 0)); }
          else if (e.key === "Enter" && brands[activeIdx]) { e.preventDefault(); handleSelect(brands[activeIdx]); }
          else if (e.key === "Escape") setOpen(false);
        }}
      />
      {isFetching && (
        <span className="brand-autocomplete-spinner">
          <Loader2 size={12} className="spin" />
        </span>
      )}
      {open && brands.length > 0 && (
        <div className="brand-autocomplete-dropdown">
          {brands.map((b, i) => (
            <div
              key={b.normalizedBrand}
              onMouseDown={() => handleSelect(b)}
              onMouseEnter={() => setActiveIdx(i)}
              className={`brand-autocomplete-item${i === activeIdx ? " is-active" : ""}`}
            >
              <span className="brand-autocomplete-name">{b.displayBrand}</span>
              <span className="brand-autocomplete-badges">
                {b.marketplaces.map((mp) => (
                  <MpBadge key={mp} marketplace={mp} />
                ))}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function BrandBansPage() {
  const qc = useQueryClient();
  const [brandInput, setBrandInput] = useState("");
  const [noteInput, setNoteInput] = useState("");
  const [openPreview, setOpenPreview] = useState<string | null>(null);
  const [previews, setPreviews] = useState<Record<string, PreviewResponse>>({});
  const [applyResults, setApplyResults] = useState<Record<string, ApplyResponse>>({});

  const { data, isLoading } = useQuery<BrandBansResponse>({
    queryKey: ["brand-bans"],
    queryFn: () => apiJson("/api/brand-bans"),
  });

  const addMutation = useMutation({
    mutationFn: (payload: { brand: string; note: string }) =>
      apiJson("/api/brand-bans", { method: "POST", body: JSON.stringify(payload) }),
    onSuccess: () => {
      setBrandInput("");
      setNoteInput("");
      void qc.invalidateQueries({ queryKey: ["brand-bans"] });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => apiJson(`/api/brand-bans/${id}`, { method: "DELETE" }),
    onSuccess: (_, id) => {
      void qc.invalidateQueries({ queryKey: ["brand-bans"] });
      setOpenPreview((prev) => (prev === id ? null : prev));
      setPreviews((prev) => { const n = { ...prev }; delete n[id]; return n; });
      setApplyResults((prev) => { const n = { ...prev }; delete n[id]; return n; });
    },
  });

  const previewMutation = useMutation({
    mutationFn: (id: string) => apiJson<PreviewResponse>(`/api/brand-bans/${id}/preview`),
    onSuccess: (data, id) => {
      setPreviews((prev) => ({ ...prev, [id]: data }));
      setApplyResults((prev) => { const n = { ...prev }; delete n[id]; return n; });
      setOpenPreview(id);
    },
  });

  const applyMutation = useMutation({
    mutationFn: (id: string) => apiJson<ApplyResponse>(`/api/brand-bans/${id}/apply`, { method: "POST" }),
    onSuccess: (data, id) => {
      setApplyResults((prev) => ({ ...prev, [id]: data }));
      setOpenPreview(null);
    },
  });

  const bans = data?.bans || [];

  return (
    <section className="page-section">
      <PageHeader
        title="Запрет брендов"
        subtitle="Добавьте бренд — все его товары будут сняты с продажи на Ozon, Яндекс Маркете и WB"
      />

      <div className="table-panel">
        <div className="section-title">
          <div>
            <h3>Добавить бренд в запрет</h3>
          </div>
        </div>
        <div className="brand-add-row">
          <BrandAutocomplete
            value={brandInput}
            onChange={setBrandInput}
            onSelect={(brand) => {
              setBrandInput(brand);
            }}
          />
          <input
            className="pm-chip-input brand-note-input"
            placeholder="Причина (необязательно)"
            value={noteInput}
            onChange={(e) => setNoteInput(e.target.value)}
          />
          <button
            className="secondary-action"
            disabled={!brandInput.trim() || addMutation.isPending}
            onClick={() => addMutation.mutate({ brand: brandInput.trim(), note: noteInput.trim() })}
          >
            {addMutation.isPending ? <Loader2 size={14} className="spin" /> : <Plus size={14} />}
            Добавить запрет
          </button>
        </div>
        {addMutation.isError && (
          <div className="inline-error brand-add-error">
            {String((addMutation.error as Error)?.message || "Ошибка добавления")}
          </div>
        )}
      </div>

      {isLoading && (
        <div className="soft-empty">
          <Loader2 className="spin" size={16} /> Загрузка…
        </div>
      )}

      {!isLoading && bans.length === 0 && (
        <div className="soft-empty">Запрещённых брендов нет. Добавьте первый выше.</div>
      )}

      {bans.length > 0 && (
        <div className="table-panel">
          <div className="section-title">
            <div>
              <h3>Список запрещённых брендов</h3>
            </div>
          </div>
          {bans.map((ban) => {
            const preview = previews[ban.id];
            const result = applyResults[ban.id];
            const isPreviewOpen = openPreview === ban.id;
            const previewLoading = previewMutation.isPending && previewMutation.variables === ban.id;
            const applyLoading = applyMutation.isPending && applyMutation.variables === ban.id;
            const deleteLoading = deleteMutation.isPending && deleteMutation.variables === ban.id;

            return (
              <div key={ban.id} className="brand-ban-item">
                <article className="job-row brand-ban-row">
                  <div>
                    <strong>{ban.displayBrand}</strong>
                    <span>
                      Добавлен {formatDate(ban.bannedAt)}
                      {ban.note ? ` · ${ban.note}` : ""}
                    </span>
                  </div>
                  <div className="brand-ban-actions">
                    <button
                      className="secondary-action compact"
                      title="Посмотреть какие товары попадут под запрет"
                      disabled={previewLoading}
                      onClick={() => {
                        if (isPreviewOpen) {
                          setOpenPreview(null);
                        } else {
                          previewMutation.mutate(ban.id);
                        }
                      }}
                    >
                      {previewLoading ? <Loader2 size={13} className="spin" /> : <Eye size={13} />}
                      {isPreviewOpen ? "Скрыть" : "Просмотр"}
                    </button>
                    <button
                      className="secondary-action compact danger"
                      title="Заархивировать все товары бренда на Ozon и Яндекс Маркете"
                      disabled={applyLoading}
                      onClick={() => {
                        if (confirm(`Снять с продажи все товары бренда «${ban.displayBrand}» на Ozon, Яндекс Маркете и обнулить остатки WB?`)) {
                          applyMutation.mutate(ban.id);
                        }
                      }}
                    >
                      {applyLoading ? <Loader2 size={13} className="spin" /> : <ArchiveX size={13} />}
                      Снять с продажи
                    </button>
                    <button
                      className="icon-action"
                      title="Удалить запрет"
                      disabled={deleteLoading}
                      onClick={() => {
                        if (confirm(`Удалить запрет бренда «${ban.displayBrand}»? Ранее заархивированные товары останутся архивными — для восстановления используйте страницу склада.`)) {
                          deleteMutation.mutate(ban.id);
                        }
                      }}
                    >
                      {deleteLoading ? <Loader2 size={13} className="spin" /> : <Trash2 size={14} />}
                    </button>
                  </div>
                </article>

                <OfferIdsPanel ban={ban} onUpdated={() => void qc.invalidateQueries({ queryKey: ["brand-bans"] })} />

                {result && (
                  <div className="brand-result-wrap">
                    <div className={`info-strip compact ${result.ok ? "success" : ""}`}>
                      {result.ok
                        ? `Готово: заархивировано ${result.archived} из ${result.total} товаров (Ozon/ЯМ).`
                        : `Частично: заархивировано ${result.archived} из ${result.total}. Ошибок: ${result.failed}.`}
                      {(result.ozonApiExtra ?? 0) > 0 ? ` Ozon API: +${result.ozonApiExtra} доп.` : null}
                      {result.ozonApiError ? ` Ozon API ошибка: ${result.ozonApiError}.` : null}
                      {(result.wbCards ?? 0) > 0
                        ? ` WB: обнулено остатков ${result.wbZeroed ?? 0} SKU (${result.wbCards} карточек).`
                        : result.wbCards === 0 ? " WB: совпадений не найдено." : null}
                      {result.wbError ? ` WB ошибка: ${result.wbError}.` : null}
                    </div>
                  </div>
                )}

                {isPreviewOpen && preview && (
                  <div className="brand-preview-wrap">
                    <div className="info-strip brand-preview-strip">
                      <strong>
                        {preview.total === 0 && (preview.wbCount ?? 0) === 0
                          ? `Нет точных совпадений для бренда «${ban.displayBrand}».`
                          : `Найдено: ${preview.total} товар${preview.total === 1 ? "" : preview.total < 5 ? "а" : "ов"} Ozon/ЯМ${(preview.wbCount ?? 0) > 0 ? `, ${preview.wbCount} карточек WB` : preview.wbCount === 0 ? ", WB: 0" : ""}`}
                        {preview.wbCount === -1 ? " (WB недоступен)" : null}
                      </strong>
                      {((preview.suggestions?.length ?? 0) > 0 || (preview.wbSuggestions?.length ?? 0) > 0) && (
                        <div className="brand-preview-similar">
                          <span className="muted-note">Похожие бренды в системе: </span>
                          {[...(preview.suggestions ?? []), ...(preview.wbSuggestions ?? []).map((s) => `WB: ${s}`)].join(" · ")}
                        </div>
                      )}
                      {preview.products.length > 0 && (
                        <div className="brand-preview-products">
                          {preview.products.slice(0, 60).map((p) => (
                            <div key={p.id} className="brand-preview-product">
                              <span className="brand-preview-mp">[{mpLabel(p.marketplace)}]</span>
                              {p.name || p.offerId}
                            </div>
                          ))}
                          {preview.total > 60 && (
                            <div className="brand-preview-product muted-note">…и ещё {preview.total - 60}</div>
                          )}
                        </div>
                      )}
                      {preview.total > 0 && (
                        <span>Нажмите «Снять с продажи» чтобы заархивировать все на всех площадках.</span>
                      )}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}
