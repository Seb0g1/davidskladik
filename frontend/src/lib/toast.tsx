import { CheckCircle2, Info, X, XCircle } from "lucide-react";
import { ReactNode, useEffect, useRef, useSyncExternalStore } from "react";

// Всплывающие уведомления: сообщения об успехе/ошибке появляются в углу и исчезают сами,
// не сдвигая страницу (раньше это были полосы внутри вёрстки).
export type ToastTone = "success" | "error" | "info";
type ToastAction = { label: string; onClick: () => void };
type ToastItem = { id: number; tone: ToastTone; message: ReactNode; leaving?: boolean; action?: ToastAction; durationMs: number };

let items: ToastItem[] = [];
let seq = 0;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((listener) => listener());

function dismiss(id: number) {
  items = items.map((item) => (item.id === id ? { ...item, leaving: true } : item));
  emit();
  window.setTimeout(() => {
    items = items.filter((item) => item.id !== id);
    emit();
  }, 220);
}

export function toast(message: ReactNode, tone: ToastTone = "success", durationMs?: number, action?: ToastAction) {
  const id = ++seq;
  const duration = durationMs ?? (tone === "error" ? 7000 : 3800);
  // Одинаковое сообщение подряд не плодим: повторный клик просто продлевает показ.
  if (typeof message === "string") {
    const same = items.find((item) => !item.leaving && item.message === message);
    if (same) dismiss(same.id);
  }
  items = [...items.slice(-4), { id, tone, message, action, durationMs: duration }];
  emit();
  window.setTimeout(() => dismiss(id), duration);
  return id;
}

// Удаление с «Отменить»: действие выполняется только через несколько секунд, пока тост
// висит на экране. Кнопка «Отменить» в тосте его отменяет — быстрее, чем диалог «Вы уверены?».
const pendingCommits = new Map<number, () => void>();
export function undoable(message: ReactNode, commit: () => void, options: { delayMs?: number; onUndo?: () => void } = {}) {
  const delayMs = options.delayMs ?? 6000;
  let done = false;
  const key = ++seq;
  const run = () => {
    if (done) return;
    done = true;
    pendingCommits.delete(key);
    commit();
  };
  pendingCommits.set(key, run);
  const toastId = toast(message, "info", delayMs, {
    label: "Отменить",
    onClick: () => {
      if (done) return;
      done = true;
      pendingCommits.delete(key);
      options.onUndo?.();
    },
  });
  window.setTimeout(run, delayMs);
  return toastId;
}
// Закрытие вкладки не должно терять удаление, которое ещё ждёт своего таймера.
if (typeof window !== "undefined") {
  window.addEventListener("pagehide", () => { for (const run of Array.from(pendingCommits.values())) run(); });
}

toast.success = (message: ReactNode) => toast(message, "success");
toast.error = (message: ReactNode) => toast(message, "error");
toast.info = (message: ReactNode) => toast(message, "info");

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function Toaster() {
  const list = useSyncExternalStore(subscribe, () => items);
  return (
    <div className="toast-stack" aria-live="polite">
      {list.map((item) => (
        <div key={item.id} className={`toast toast-${item.tone}${item.leaving ? " is-leaving" : ""}`} role={item.tone === "error" ? "alert" : "status"} onClick={() => dismiss(item.id)}>
          <span className="toast-icon">
            {item.tone === "success" ? <CheckCircle2 size={17} /> : item.tone === "error" ? <XCircle size={17} /> : <Info size={17} />}
          </span>
          <div className="toast-body">{item.message}</div>
          {item.action ? (
            <button type="button" className="toast-action" onClick={(event) => { event.stopPropagation(); item.action?.onClick(); dismiss(item.id); }}>{item.action.label}</button>
          ) : null}
          <button type="button" className="toast-close" aria-label="Закрыть" onClick={(event) => { event.stopPropagation(); dismiss(item.id); }}><X size={14} /></button>
          <span className="toast-timer" style={{ animationDuration: `${item.durationMs}ms` }} />
        </div>
      ))}
    </div>
  );
}

/** Показывает тост при появлении в разметке — замена бывших полос «success-strip». */
export function FlashToast({ children, tone = "success" }: { children: ReactNode; tone?: ToastTone }) {
  // Ref переживает скрытие страницы (keep-alive), поэтому при возврате на раздел тост
  // не показывается повторно.
  const shown = useRef(false);
  useEffect(() => {
    if (shown.current) return;
    shown.current = true;
    toast(children, tone);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return null;
}
