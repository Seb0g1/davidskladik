import { Loader2, MessageSquareReply } from "lucide-react";

interface Template { id: string; title: string; text: string }

interface ReplyBoxProps {
  text: string;
  setText: (value: string) => void;
  templates: Template[];
  onSubmit: () => void;
  isPending: boolean;
  error?: Error | null;
  emojiRow?: string[];
  placeholder?: string;
  confirmMessage?: string;
}

const DEFAULT_EMOJI = ["🙏", "😊", "✨", "👍", "🤝", "💬"];
// Сколько шаблонов показывать кнопками; остальные — в выпадающем списке.
const QUICK_TEMPLATES = 5;

export function ReplyBox({
  text,
  setText,
  templates,
  onSubmit,
  isPending,
  error,
  emojiRow = DEFAULT_EMOJI,
  placeholder = "Текст ответа",
  confirmMessage,
}: ReplyBoxProps) {
  const handleSubmit = () => {
    if (!text.trim() || isPending) return;
    if (confirmMessage && !window.confirm(confirmMessage)) return;
    onSubmit();
  };
  const insertTemplate = (tpl: Template) => setText(text ? `${text}\n${tpl.text}` : tpl.text);
  const quick = templates.slice(0, QUICK_TEMPLATES);
  const rest = templates.slice(QUICK_TEMPLATES);

  return (
    <div className="review-reply-box">
      {templates.length > 0 ? (
        <div className="reply-templates">
          {quick.map((tpl) => (
            <button key={tpl.id} type="button" className="reply-template-chip" title={tpl.text} onClick={() => insertTemplate(tpl)}>{tpl.title}</button>
          ))}
          {rest.length ? (
            <select
              defaultValue=""
              onChange={(e) => {
                const tpl = rest.find((t) => t.id === e.target.value);
                if (tpl) insertTemplate(tpl);
                e.target.value = "";
              }}
            >
              <option value="" disabled>Ещё шаблоны…</option>
              {rest.map((t) => <option key={t.id} value={t.id}>{t.title}</option>)}
            </select>
          ) : null}
        </div>
      ) : null}
      <div className="emoji-row">
        {emojiRow.map((emoji) => (
          <button key={emoji} type="button" onClick={() => setText(text + emoji)}>{emoji}</button>
        ))}
      </div>
      <textarea
        rows={4}
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            handleSubmit();
          }
        }}
        placeholder={`${placeholder} (Ctrl+Enter — отправить)`}
      />
      <div className="reply-char-count">{text.length}/5000</div>
      {error ? <div className="inline-error">{String((error as Error).message)}</div> : null}
      <button
        className="primary-action"
        type="button"
        disabled={!text.trim() || isPending}
        onClick={handleSubmit}
        title="Отправить (Ctrl+Enter)"
      >
        {isPending ? <Loader2 className="spin" size={15} /> : <MessageSquareReply size={15} />} Отправить ответ
      </button>
    </div>
  );
}
