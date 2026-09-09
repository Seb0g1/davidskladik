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
    if (!text.trim()) return;
    if (confirmMessage && !window.confirm(confirmMessage)) return;
    onSubmit();
  };

  return (
    <div className="review-reply-box">
      {templates.length > 0 ? (
        <select
          defaultValue=""
          onChange={(e) => {
            const tpl = templates.find((t) => t.id === e.target.value);
            if (tpl) setText(text ? `${text}\n${tpl.text}` : tpl.text);
            e.target.value = "";
          }}
        >
          <option value="" disabled>Вставить шаблон…</option>
          {templates.map((t) => <option key={t.id} value={t.id}>{t.title}</option>)}
        </select>
      ) : null}
      <div className="emoji-row">
        {emojiRow.map((emoji) => (
          <button key={emoji} type="button" onClick={() => setText(text + emoji)}>{emoji}</button>
        ))}
      </div>
      <textarea rows={4} value={text} onChange={(e) => setText(e.target.value)} placeholder={placeholder} />
      <div className="reply-char-count">{text.length}/5000</div>
      {error ? <div className="inline-error">{String((error as Error).message)}</div> : null}
      <button
        className="primary-action"
        type="button"
        disabled={!text.trim() || isPending}
        onClick={handleSubmit}
      >
        {isPending ? <Loader2 className="spin" size={15} /> : <MessageSquareReply size={15} />} Отправить ответ
      </button>
    </div>
  );
}
