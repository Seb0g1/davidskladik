import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2, Plus, Trash2, X } from "lucide-react";

export type ReplyTemplate = { id: string; title: string; text: string };

const EMOJI_ROW = ["🙏", "❤️", "😊", "🌸", "✨", "👍", "🎁", "🥰", "💫", "🤝", "📦", "🚚"];

async function apiJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "same-origin",
    ...(init || {}),
    headers: { ...(init?.body ? { "Content-Type": "application/json" } : {}), ...(init?.headers || {}) },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error((data as any)?.error || `HTTP ${response.status}`);
  return data as T;
}

export function TemplatesDrawer({
  open,
  onClose,
  title,
  description,
  apiBase,
  queryKey,
  templates,
  onInsert,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  description?: string;
  apiBase: string;
  queryKey: unknown[];
  templates: ReplyTemplate[];
  onInsert?: (text: string) => void;
}) {
  const queryClient = useQueryClient();
  const [draftTitle, setDraftTitle] = useState("");
  const [draftText, setDraftText] = useState("");

  const refresh = () => void queryClient.invalidateQueries({ queryKey });

  const save = useMutation({
    mutationFn: () => apiJson(apiBase, { method: "POST", body: JSON.stringify({ title: draftTitle, text: draftText }) }),
    onSuccess: () => { setDraftTitle(""); setDraftText(""); refresh(); },
  });
  const remove = useMutation({
    mutationFn: (id: string) => apiJson(`${apiBase}/${encodeURIComponent(id)}`, { method: "DELETE" }),
    onSuccess: refresh,
  });

  return (
    <>
      <button className={`templates-drawer-backdrop${open ? " is-open" : ""}`} type="button" aria-label="Закрыть шаблоны" onClick={onClose} tabIndex={open ? 0 : -1} />
      <aside className={`templates-drawer${open ? " is-open" : ""}`} aria-hidden={!open}>
        <div className="templates-drawer-head">
          <div>
            <span className="eyebrow">Шаблоны</span>
            <h3>{title}</h3>
          </div>
          <button className="icon-action" type="button" onClick={onClose} title="Закрыть"><X size={16} /></button>
        </div>
        {description ? <p className="section-note">{description}</p> : null}
        <div className="templates-drawer-list">
          {templates.map((template) => (
            <div className="template-card" key={template.id}>
              <div className="template-card-head">
                <strong>{template.title}</strong>
                <button type="button" className="icon-action" title="Удалить шаблон" onClick={() => remove.mutate(template.id)} disabled={remove.isPending}>
                  {remove.isPending ? <Loader2 className="spin" size={13} /> : <Trash2 size={13} />}
                </button>
              </div>
              <p>{template.text}</p>
              {onInsert ? (
                <button className="secondary-action compact" type="button" onClick={() => onInsert(template.text)}>Вставить в ответ</button>
              ) : null}
            </div>
          ))}
          {!templates.length ? <div className="empty-state compact">Шаблонов пока нет.</div> : null}
        </div>
        <div className="templates-drawer-form">
          <span className="section-subtitle">Новый шаблон</span>
          <input placeholder="Название шаблона" value={draftTitle} onChange={(event) => setDraftTitle(event.target.value)} />
          <textarea rows={3} placeholder="Текст шаблона (эмодзи приветствуются 😊)" value={draftText} onChange={(event) => setDraftText(event.target.value)} />
          <div className="emoji-row">
            {EMOJI_ROW.map((emoji) => (
              <button key={emoji} type="button" onClick={() => setDraftText((current) => current + emoji)}>{emoji}</button>
            ))}
          </div>
          {save.error ? <div className="inline-error">{String((save.error as Error).message)}</div> : null}
          <button className="primary-action" type="button" disabled={!draftTitle.trim() || !draftText.trim() || save.isPending} onClick={() => save.mutate()}>
            {save.isPending ? <Loader2 className="spin" size={15} /> : <Plus size={15} />} Сохранить шаблон
          </button>
        </div>
      </aside>
    </>
  );
}
