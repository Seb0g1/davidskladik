import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, ImagePlus, Loader2, Send, Sparkles, X } from "lucide-react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { AiImage, Product, ProductSchema, SettingsResponseSchema } from "../types";
import { asRecord, errorMessage, updateCachedProducts } from "../lib/common";

const SLOT_LABELS = ["Hero shot", "Dark studio", "Lifestyle", "Подарочный", "Макро", "Пирамида нот", "100% Оригинал"];
const SLOT_COUNT = 7;

const CardGenerateResponseSchema = z.object({
  ok: z.boolean().optional(),
  batchId: z.coerce.string().optional().default(""),
  productId: z.coerce.string().optional().default(""),
  jobs: z.array(z.object({ jobId: z.coerce.string(), slotId: z.coerce.string().optional().default(""), status: z.coerce.string().optional().default("queued") })).optional().default([]),
}).passthrough();

const ProductResponseSchema = z.object({
  ok: z.boolean().optional(),
  product: ProductSchema.optional().nullable(),
}).passthrough();

function countRunning(jobs: { status: string }[]): number {
  return jobs.filter((j) => ["queued", "running"].includes(j.status)).length;
}

interface ImageSlotProps {
  draft?: AiImage;
  index: number;
  slotLabel: string;
  onApprove?: () => void;
  onReject?: () => void;
  onSend?: () => void;
  actionPending?: boolean;
}

function ImageSlot({ draft, index, slotLabel, onApprove, onReject, onSend, actionPending }: ImageSlotProps) {
  const [hovered, setHovered] = useState(false);
  const hasImage = Boolean(draft?.resultUrl);
  const status = draft?.status || "";
  const isApproved = status === "approved";
  const isRejected = status === "rejected";

  return (
    <div
      style={{ position: "relative", width: "100%", paddingBottom: "100%", borderRadius: 8, overflow: "hidden", border: `1px solid ${isApproved ? "rgba(34,197,94,0.5)" : isRejected ? "rgba(239,68,68,0.3)" : "rgba(255,255,255,0.08)"}`, background: hasImage ? "transparent" : "#1a1a2e", cursor: hasImage ? "pointer" : "default" }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 4 }}>
        {hasImage ? (
          <img src={draft!.resultUrl} alt={slotLabel} style={{ width: "100%", height: "100%", objectFit: "cover", opacity: isRejected ? 0.35 : 1 }} />
        ) : (
          <>
            <Sparkles size={16} style={{ color: "rgba(255,255,255,0.2)" }} />
            <span style={{ fontSize: 10, color: "rgba(255,255,255,0.2)", textAlign: "center", padding: "0 4px" }}>{slotLabel || `AI ${index + 1}`}</span>
          </>
        )}

        {hasImage && (
          <div style={{ position: "absolute", bottom: 0, left: 0, right: 0, background: "linear-gradient(transparent, rgba(0,0,0,0.7))", padding: "16px 4px 4px", fontSize: 9, color: "rgba(255,255,255,0.7)", textAlign: "center" }}>
            {slotLabel}
          </div>
        )}

        {isApproved && (
          <div style={{ position: "absolute", top: 4, left: 4, background: "rgba(34,197,94,0.9)", borderRadius: 4, width: 18, height: 18, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <Check size={11} color="#fff" />
          </div>
        )}
        {isRejected && (
          <div style={{ position: "absolute", top: 4, left: 4, background: "rgba(239,68,68,0.8)", borderRadius: 4, width: 18, height: 18, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <X size={11} color="#fff" />
          </div>
        )}

        {hasImage && hovered && !actionPending && (
          <div style={{ position: "absolute", top: 4, right: 4, display: "flex", flexDirection: "column", gap: 3 }}>
            {!isApproved && onApprove && (
              <button type="button" onClick={(e) => { e.stopPropagation(); onApprove(); }} title="Одобрить" style={{ width: 22, height: 22, borderRadius: 4, border: "none", background: "rgba(34,197,94,0.9)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}>
                <Check size={12} />
              </button>
            )}
            {!isRejected && onReject && (
              <button type="button" onClick={(e) => { e.stopPropagation(); onReject(); }} title="Отклонить" style={{ width: 22, height: 22, borderRadius: 4, border: "none", background: "rgba(239,68,68,0.85)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}>
                <X size={12} />
              </button>
            )}
            {isApproved && onSend && (
              <button type="button" onClick={(e) => { e.stopPropagation(); onSend(); }} title="Отправить на маркетплейс" style={{ width: 22, height: 22, borderRadius: 4, border: "none", background: "rgba(168,85,247,0.9)", color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center" }}>
                <Send size={11} />
              </button>
            )}
          </div>
        )}

        {actionPending && (
          <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center" }}>
            <Loader2 className="spin" size={18} color="#fff" />
          </div>
        )}
      </div>
    </div>
  );
}

function StubSlot({ url, label }: { url: string; label: string }) {
  return (
    <div style={{ position: "relative", width: "100%", paddingBottom: "100%", background: url ? "transparent" : "#12121a", borderRadius: 8, overflow: "hidden", border: "1px solid rgba(255,255,255,0.05)" }}>
      <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 4 }}>
        {url ? (
          <img src={url} alt={label} style={{ width: "100%", height: "100%", objectFit: "cover" }} />
        ) : (
          <>
            <ImagePlus size={16} style={{ color: "rgba(255,255,255,0.12)" }} />
            <span style={{ fontSize: 10, color: "rgba(255,255,255,0.18)", textAlign: "center", padding: "0 6px" }}>{label}</span>
          </>
        )}
      </div>
    </div>
  );
}

export function AiCardImagePanel({ product, onSaved }: { product: Product; onSaved: () => void }) {
  const queryClient = useQueryClient();
  const [activeBatchId, setActiveBatchId] = useState("");
  const [activeJobs, setActiveJobs] = useState<{ jobId: string; status: string }[]>([]);
  const [freshDrafts, setFreshDrafts] = useState<AiImage[]>(product.aiImages || []);
  const [baseImageCount, setBaseImageCount] = useState(0);
  const [generateError, setGenerateError] = useState("");
  const [actionDraftId, setActionDraftId] = useState("");
  const [stub1, setStub1] = useState("");
  const [stub2, setStub2] = useState("");
  const [stubsOpen, setStubsOpen] = useState(false);
  const [stubsSaved, setStubsSaved] = useState(false);

  useEffect(() => {
    setFreshDrafts(product.aiImages || []);
    setActiveBatchId("");
    setActiveJobs([]);
    setBaseImageCount(0);
    setGenerateError("");
  }, [product.id]);

  const settingsQuery = useQuery({
    queryKey: ["settings"],
    queryFn: () => fetchJson("/api/settings", SettingsResponseSchema),
  });

  useEffect(() => {
    const settings = settingsQuery.data?.settings;
    if (!settings) return;
    const stubs = asRecord(settings.cardImageStubs);
    if (stubs.stub1Url) setStub1(String(stubs.stub1Url));
    if (stubs.stub2Url) setStub2(String(stubs.stub2Url));
  }, [settingsQuery.data]);

  const stubsMutation = useMutation({
    mutationFn: () =>
      fetchJson("/api/settings", SettingsResponseSchema, mutationBody({ cardImageStubs: { stub1Url: stub1.trim(), stub2Url: stub2.trim() } })),
    onSuccess: () => {
      setStubsSaved(true);
      setTimeout(() => setStubsSaved(false), 2000);
    },
  });

  const anyRunning = activeJobs.length > 0 && countRunning(activeJobs) > 0;

  const productPollQuery = useQuery({
    queryKey: ["warehouse", "card-image-product-poll", product.id, activeBatchId],
    enabled: anyRunning,
    queryFn: () => fetchJson(`/api/warehouse/products/${encodeURIComponent(product.id)}`, ProductResponseSchema),
    refetchInterval: anyRunning ? 3000 : false,
  });

  useEffect(() => {
    const p = productPollQuery.data?.product;
    if (!p) return;
    if (p.aiImages?.length) {
      setFreshDrafts(p.aiImages);
      updateCachedProducts(queryClient, { product: p });
    }
    const newCount = (p.aiImages?.length ?? 0) - baseImageCount;
    const running = activeJobs.filter((j) => ["queued", "running"].includes(j.status)).length;
    if (newCount >= SLOT_COUNT || (newCount > 0 && running === 0)) {
      setActiveJobs([]);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    }
  }, [productPollQuery.data, queryClient, onSaved, baseImageCount]);

  const generateMutation = useMutation({
    mutationFn: async () => {
      setGenerateError("");
      return fetchJson(
        `/api/warehouse/products/${encodeURIComponent(product.id)}/card-images/generate`,
        CardGenerateResponseSchema,
        mutationBody({})
      );
    },
    onSuccess: (payload) => {
      if (payload.batchId) setActiveBatchId(payload.batchId);
      if (payload.jobs?.length) {
        setBaseImageCount(freshDrafts.length);
        setActiveJobs(payload.jobs.map((j) => ({ jobId: j.jobId, status: j.status })));
      }
    },
    onError: (err) => setGenerateError(errorMessage(err)),
  });

  const reviewMutation = useMutation({
    mutationFn: async ({ draftId, action }: { draftId: string; action: "approve" | "reject" }) => {
      setActionDraftId(draftId);
      return fetchJson(
        `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draftId)}/${action}`,
        ProductResponseSchema,
        mutationBody({ expectedUpdatedAt: product.updatedAt || "" }),
      );
    },
    onSuccess: (payload) => {
      if (payload.product?.aiImages?.length) setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
    onSettled: () => setActionDraftId(""),
  });

  const sendMutation = useMutation({
    mutationFn: async ({ draftId }: { draftId: string }) => {
      setActionDraftId(draftId);
      const marketplace = String(product.marketplace || "").toLowerCase().includes("yandex") ? "yandex" : "ozon";
      return fetchJson(
        `/api/warehouse/products/${encodeURIComponent(product.id)}/ai-images/${encodeURIComponent(draftId)}/send`,
        ProductResponseSchema,
        mutationBody({ marketplace }),
      );
    },
    onSuccess: (payload) => {
      if (payload.product?.aiImages?.length) setFreshDrafts(payload.product.aiImages);
      updateCachedProducts(queryClient, payload);
      void queryClient.invalidateQueries({ queryKey: ["warehouse"] });
      onSaved();
    },
    onSettled: () => setActionDraftId(""),
  });

  const busy = generateMutation.isPending || anyRunning;
  // When a batch is active: show only current-batch drafts sorted by slotOrder.
  // After generation: show the most recent SLOT_COUNT drafts.
  const batchDrafts = activeBatchId
    ? [...freshDrafts.filter((d) => d.batchId === activeBatchId)].sort((a, b) => (((a as Record<string, unknown>).slotOrder as number) || 0) - (((b as Record<string, unknown>).slotOrder as number) || 0))
    : freshDrafts.slice(-SLOT_COUNT);
  const aiSlots = Array.from({ length: SLOT_COUNT }, (_, i) => batchDrafts[i]);
  const doneCount = activeBatchId
    ? freshDrafts.filter((d) => d.batchId === activeBatchId && d.resultUrl).length
    : freshDrafts.filter((d) => d?.resultUrl).length;
  const approvedCount = freshDrafts.filter((d) => d?.status === "approved").length;

  return (
    <section className="detail-section">
      <div className="section-title">
        <div>
          <span>AI Карточка</span>
          <h3>Фотографии для карточки товара</h3>
        </div>
        <button type="button" className="primary-action" disabled={busy} onClick={() => generateMutation.mutate()}>
          {busy ? <Loader2 className="spin" size={14} /> : <Sparkles size={14} />}
          {" "}{busy ? (doneCount > 0 ? `Генерирую… ${doneCount}/${SLOT_COUNT}` : "Генерирую…") : "Сгенерировать карточку"}
        </button>
      </div>

      {generateError ? <p className="inline-error">{generateError}</p> : null}
      {(reviewMutation.error || sendMutation.error) ? <p className="inline-error">{errorMessage(reviewMutation.error || sendMutation.error)}</p> : null}

      {doneCount > 0 && (
        <p style={{ fontSize: 11, color: "rgba(255,255,255,0.45)", marginTop: 4, marginBottom: 0 }}>
          Наведите на фото: <span style={{ color: "rgba(34,197,94,0.85)" }}>✓ одобрить</span> · <span style={{ color: "rgba(239,68,68,0.8)" }}>✗ отклонить</span> · <span style={{ color: "rgba(168,85,247,0.9)" }}>→ отправить</span>
          {approvedCount > 0 ? ` · Одобрено: ${approvedCount}` : ""}
        </p>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8, marginTop: 8 }}>
        {aiSlots.map((draft, i) => (
          <ImageSlot
            key={draft?.id || i}
            draft={draft}
            index={i}
            slotLabel={SLOT_LABELS[i] || `Слот ${i + 1}`}
            onApprove={draft?.resultUrl && draft.status !== "approved" ? () => reviewMutation.mutate({ draftId: draft.id, action: "approve" }) : undefined}
            onReject={draft?.resultUrl && draft.status !== "rejected" ? () => reviewMutation.mutate({ draftId: draft.id, action: "reject" }) : undefined}
            onSend={draft?.status === "approved" ? () => sendMutation.mutate({ draftId: draft.id }) : undefined}
            actionPending={actionDraftId === draft?.id && (reviewMutation.isPending || sendMutation.isPending)}
          />
        ))}
        <StubSlot url={stub1} label="Заглушка 1" />
        <StubSlot url={stub2} label="Заглушка 2" />
      </div>

      {busy && (
        <p style={{ fontSize: 12, color: "rgba(255,255,255,0.45)", marginTop: 8 }}>
          <Loader2 className="spin" size={11} /> Генерирую фотографии — это займёт несколько минут
        </p>
      )}

      <div style={{ marginTop: 12 }}>
        <button type="button" className="secondary-action" style={{ fontSize: 12 }} onClick={() => setStubsOpen((v) => !v)}>
          {stubsOpen ? "Скрыть заглушки" : "Настроить заглушки (100% оригинал и т.д.)"}
        </button>
      </div>

      {stubsOpen && (
        <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 8 }}>
          <p style={{ fontSize: 11, color: "rgba(255,255,255,0.45)", margin: 0 }}>
            Маркетинговые слайды в конце галереи: «100% ОРИГИНАЛ», «СТОЙКОСТЬ 48 ЧАСОВ» и т.д. Вставьте URL готового изображения 1000×1000 px.
          </p>
          <label style={{ fontSize: 12, color: "rgba(255,255,255,0.6)" }}>
            Заглушка 1 (URL)
            <input type="url" value={stub1} onChange={(e) => setStub1(e.target.value)} placeholder="https://…" style={{ display: "block", width: "100%", marginTop: 4, padding: "4px 8px", borderRadius: 4, background: "#1a1a2e", border: "1px solid rgba(255,255,255,0.12)", color: "#fff", fontSize: 12 }} />
          </label>
          <label style={{ fontSize: 12, color: "rgba(255,255,255,0.6)" }}>
            Заглушка 2 (URL)
            <input type="url" value={stub2} onChange={(e) => setStub2(e.target.value)} placeholder="https://…" style={{ display: "block", width: "100%", marginTop: 4, padding: "4px 8px", borderRadius: 4, background: "#1a1a2e", border: "1px solid rgba(255,255,255,0.12)", color: "#fff", fontSize: 12 }} />
          </label>
          <button type="button" className="primary-action" style={{ alignSelf: "flex-start" }} onClick={() => stubsMutation.mutate()} disabled={stubsMutation.isPending}>
            {stubsMutation.isPending ? <Loader2 className="spin" size={12} /> : null}
            {" "}{stubsSaved ? "Сохранено!" : "Сохранить"}
          </button>
        </div>
      )}
    </section>
  );
}
