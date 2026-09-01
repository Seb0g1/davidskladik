import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Bot, Check, ImagePlus, Loader2, RefreshCw, Send, X } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { AiDraftsSchema, AiImagesResponseSchema, MutationProductResponseSchema, YandexQualityCandidatesSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { Stat } from "../components/Stat";
import { asRecord, errorMessage, numberValue } from "../lib/common";

export function AiDraftsPage() {
  const [status, setStatus] = useState("pending");
  const [threshold, setThreshold] = useState(40);
  const [limit, setLimit] = useState(300);
  const [generatingProductId, setGeneratingProductId] = useState("");
  const [lastGenerateProductId, setLastGenerateProductId] = useState("");
  const queryClient = useQueryClient();
  const draftsQuery = useQuery({
    queryKey: ["ai-drafts", status],
    queryFn: () => fetchJson(`/api/warehouse/ai-drafts?status=${encodeURIComponent(status)}&marketplace=yandex&limit=300`, AiDraftsSchema),
  });
  const candidatesQuery = useQuery({
    queryKey: ["quality-candidates", threshold, limit],
    queryFn: () => fetchJson(`/api/warehouse/yandex-quality-candidates?cached=1&threshold=${threshold}&limit=${limit}&resultLimit=300`, YandexQualityCandidatesSchema),
  });
  const generate = useMutation({
    mutationFn: async (productId: string) => {
      setGeneratingProductId(productId);
      setLastGenerateProductId(productId);
      return fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/generate`, AiImagesResponseSchema, mutationBody({ count: 5, imagesCount: 5 }));
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] });
      void queryClient.invalidateQueries({ queryKey: ["quality-candidates"] });
    },
    onSettled: () => setGeneratingProductId(""),
  });
  const send = useMutation({
    mutationFn: (productId: string) => fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/yandex-quality-draft/send`, MutationProductResponseSchema, mutationBody({})),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] });
      void queryClient.invalidateQueries({ queryKey: ["quality-candidates"] });
    },
  });

  const reviewImage = useMutation({
    mutationFn: ({ productId, draftId, action }: { productId: string; draftId: string; action: "approve" | "reject" }) =>
      fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/ai-images/${encodeURIComponent(draftId)}/${action}`, MutationProductResponseSchema, mutationBody({})),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] }),
  });

  const sendImage = useMutation({
    mutationFn: ({ productId, draftId }: { productId: string; draftId: string }) =>
      fetchJson(`/api/warehouse/products/${encodeURIComponent(productId)}/ai-images/${encodeURIComponent(draftId)}/send`, MutationProductResponseSchema, mutationBody({ marketplace: "yandex" })),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ["ai-drafts"] }),
  });
  const drafts = draftsQuery.data?.drafts || [];
  const candidates = candidatesQuery.data?.products || [];
  return (
    <section className="page-section ai-drafts-page">
      <PageHeader title="AI drafts" subtitle="Карточки качества ниже порога, генерация текста и 5 фото, ручная отправка на маркетплейс." action={<button className="secondary-action" onClick={() => { candidatesQuery.refetch(); draftsQuery.refetch(); }}><RefreshCw size={16} /> Обновить</button>} />
      <section className="control-grid">
        <label>Статус<SelectField ariaLabel="Статус" value={status} onChange={setStatus} options={[{ value: "pending", label: "На проверке" }, { value: "approved", label: "Одобрено" }, { value: "rejected", label: "Отклонено" }, { value: "", label: "Все" }]} /></label>
        <label>Качество до<input type="number" value={threshold} onChange={(event) => setThreshold(numberValue(event.target.value, 40))} /></label>
        <label>Лимит проверки<input type="number" value={limit} onChange={(event) => setLimit(numberValue(event.target.value, 300))} /></label>
      </section>
      <section className="dashboard-metrics">
        <Stat label="Проверено" value={candidatesQuery.data?.checked || 0} tone="accent" icon={<Bot size={18} />} />
        <Stat label="Ниже порога" value={candidatesQuery.data?.total || 0} tone={candidatesQuery.data?.total ? "warn" : "success"} icon={<AlertTriangle size={18} />} />
        <Stat label="Черновики" value={draftsQuery.data?.total || 0} tone="success" icon={<ImagePlus size={18} />} />
      </section>
      {generate.isPending && (
        <div className="progress-line"><span style={{ width: "58%" }} />Генерация текста и 5 фото через image endpoint...</div>
      )}
      {(generate.error || send.error) && (
        <div className="inline-error">
          {errorMessage(generate.error || send.error)}
          {generate.error ? <button className="secondary-action inline-retry" type="button" disabled={!lastGenerateProductId || generate.isPending} onClick={() => lastGenerateProductId && generate.mutate(lastGenerateProductId)}>Повторить</button> : null}
        </div>
      )}
      <section className="table-panel">
        <div className="section-title"><div><span>Кандидаты</span><h3>Качество карточки до {threshold}</h3></div></div>
        {candidatesQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю кандидатов...</div>}
        {candidates.length > 80 && (
          <div className="inline-warning">Показаны 80 из {candidatesQuery.data?.total ?? candidates.length} кандидатов</div>
        )}
        {candidates.slice(0, 80).map((row) => {
          const product = asRecord(row.product || row);
          const quality = asRecord(row.cardQuality || product.cardQuality);
          const productId = String(product.id || row.id || "");
          const hasDraft = draftsQuery.data?.drafts.some((d) => {
            const dp = asRecord(d.product);
            return String(dp.id || dp.offerId) === productId || String(dp.offerId) === String(product.offerId);
          });
          return (
            <article className="job-row" key={productId || String(product.offerId)}>
              <div>
                <strong>{String(product.offerId || product.name || productId)}</strong>
                <span>{String(product.name || "Без названия")} · качество {String(quality.contentRating || row.quality || "-")}</span>
              </div>
              <div className="row-actions">
                <button className="secondary-action" onClick={() => generate.mutate(productId)} disabled={generate.isPending || !productId}>
                  {generate.isPending && generatingProductId === productId ? <Loader2 className="spin" size={16} /> : <ImagePlus size={16} />} Текст + 5 фото
                </button>
                <button className="primary-action" onClick={() => send.mutate(productId)} disabled={send.isPending || !productId || !hasDraft} title={!hasDraft ? "Сначала сгенерируйте черновик" : undefined}>Отправить</button>
              </div>
            </article>
          );
        })}
      </section>
      <section className="table-panel">
        <div className="section-title"><div><span>Черновики</span><h3>На ручной проверке</h3></div></div>
        {draftsQuery.isLoading && <div className="soft-empty"><Loader2 className="spin" size={16} /> Загружаю черновики...</div>}
        <div className="draft-card-grid">
          {drafts.map((row) => {
            const product = asRecord(row.product);
            const draft = asRecord(row.draft);
            const related = asRecord(row.relatedImageDraft);
            const imageUrl = String(draft.resultUrl || related.resultUrl || product.imageUrl || "");
            const imageDrafts = Array.isArray(row.imageDrafts) ? row.imageDrafts : [];
            return (
              <article className="ai-review-card" key={`${product.id}-${draft.id}`}>
                <div className="ai-review-image">
                  {imageDrafts.length > 1 ? (
                    <div style={{ display: "flex", gap: 4, overflowX: "auto", flexWrap: "wrap" }}>
                      {imageDrafts.map((imgDraft, idx) => {
                        const img = asRecord(imgDraft);
                        const imgUrl = String(img.resultUrl || "");
                        const imgId = String(img.id || idx);
                        const imgStatus = String(img.status || "pending");
                        const productId = String(product.id || product.offerId || "");
                        return imgUrl ? (
                          <div key={idx} style={{ position: "relative", flexShrink: 0 }}>
                            <img src={imgUrl} alt="" style={{ height: 72, width: 72, objectFit: "cover", borderRadius: 4, opacity: imgStatus === "rejected" ? 0.35 : 1, border: imgStatus === "approved" ? "2px solid rgba(34,197,94,0.7)" : "1px solid rgba(255,255,255,0.1)" }} />
                            <div style={{ position: "absolute", top: 2, right: 2, display: "flex", gap: 2 }}>
                              {imgStatus !== "approved" && (
                                <button type="button" onClick={() => reviewImage.mutate({ productId, draftId: imgId, action: "approve" })} disabled={reviewImage.isPending} title="Одобрить" style={{ width: 18, height: 18, borderRadius: 3, border: "none", background: "rgba(34,197,94,0.9)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", padding: 0 }}>
                                  <Check size={10} />
                                </button>
                              )}
                              {imgStatus !== "rejected" && (
                                <button type="button" onClick={() => reviewImage.mutate({ productId, draftId: imgId, action: "reject" })} disabled={reviewImage.isPending} title="Отклонить" style={{ width: 18, height: 18, borderRadius: 3, border: "none", background: "rgba(239,68,68,0.85)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", padding: 0 }}>
                                  <X size={10} />
                                </button>
                              )}
                              {imgStatus === "approved" && (
                                <button type="button" onClick={() => sendImage.mutate({ productId, draftId: imgId })} disabled={sendImage.isPending} title="Отправить на Yandex" style={{ width: 18, height: 18, borderRadius: 3, border: "none", background: "rgba(168,85,247,0.9)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", padding: 0 }}>
                                  <Send size={10} />
                                </button>
                              )}
                            </div>
                          </div>
                        ) : null;
                      })}
                    </div>
                  ) : imageUrl ? <img src={imageUrl} alt="" /> : <Bot size={22} />}
                </div>
                <div>
                  <strong>{String(product.offerId || product.name || product.id)}</strong>
                  <span>{String(row.type || "draft")} · {String(draft.status || "pending")} · качество {String(asRecord(product.cardQuality).contentRating || "-")}</span>
                  <span>Фото-черновики: {String(imageDrafts.length || (imageUrl ? 1 : 0))}/5</span>
                  <p>{String(draft.description || draft.text || draft.prompt || "Черновик без текста").slice(0, 360)}</p>
                </div>
              </article>
            );
          })}
        </div>
        {!draftsQuery.isLoading && !drafts.length && <div className="soft-empty">Черновиков пока нет.</div>}
      </section>
    </section>
  );
}
