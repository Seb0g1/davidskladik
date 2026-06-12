import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Bot, ImagePlus, Loader2, RefreshCw } from "lucide-react";
import { fetchJson, mutationBody } from "../api";
import { AiDraftsSchema, AiImagesResponseSchema, MutationProductResponseSchema, YandexQualityCandidatesSchema } from "../types";
import { PageHeader } from "../components/PageHeader";
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
  const drafts = draftsQuery.data?.drafts || [];
  const candidates = candidatesQuery.data?.products || [];
  return (
    <section className="page-section ai-drafts-page">
      <PageHeader title="AI drafts" subtitle="Карточки качества ниже порога, генерация текста и 5 фото, ручная отправка на маркетплейс." action={<button className="secondary-action" onClick={() => { candidatesQuery.refetch(); draftsQuery.refetch(); }}><RefreshCw size={16} /> Обновить</button>} />
      <section className="control-grid">
        <label>Статус<select value={status} onChange={(event) => setStatus(event.target.value)}><option value="pending">На проверке</option><option value="approved">Одобрено</option><option value="rejected">Отклонено</option><option value="">Все</option></select></label>
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
        {candidates.slice(0, 80).map((row) => {
          const product = asRecord(row.product || row);
          const quality = asRecord(row.cardQuality || product.cardQuality);
          const productId = String(product.id || row.id || "");
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
                <button className="primary-action" onClick={() => send.mutate(productId)} disabled={send.isPending || !productId}>Отправить</button>
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
                <div className="ai-review-image">{imageUrl ? <img src={imageUrl} alt="" /> : <Bot size={22} />}</div>
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
