import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Bell, CheckCheck, MessageCircle, Package, Star, HelpCircle } from "lucide-react";

type NotificationRow = {
  id: string;
  type: string;
  marketplace: string;
  title: string;
  body?: string | null;
  url?: string | null;
  readAt?: string | null;
  createdAt?: string;
};

export type NotificationSoundSettings = {
  enabled: boolean;
  sound: "ding" | "chime" | "pop";
  types: Record<string, boolean>;
};

const SOUND_SETTINGS_KEY = "davidsklad.notifications";

export function readNotificationSoundSettings(): NotificationSoundSettings {
  try {
    const parsed = JSON.parse(localStorage.getItem(SOUND_SETTINGS_KEY) || "{}");
    return {
      enabled: parsed.enabled !== false,
      sound: ["ding", "chime", "pop"].includes(parsed.sound) ? parsed.sound : "ding",
      types: { order: true, chat: true, review: true, question: true, ...(parsed.types || {}) },
    };
  } catch {
    return { enabled: true, sound: "ding", types: { order: true, chat: true, review: true, question: true } };
  }
}

export function writeNotificationSoundSettings(settings: NotificationSoundSettings) {
  localStorage.setItem(SOUND_SETTINGS_KEY, JSON.stringify(settings));
}

export function playNotificationSound(kind: NotificationSoundSettings["sound"]) {
  try {
    const ctx = new (window.AudioContext || (window as any).webkitAudioContext)();
    const presets: Record<string, Array<[number, number, number]>> = {
      // [frequency, startOffset, duration]
      ding: [[880, 0, 0.18], [1320, 0.12, 0.22]],
      chime: [[523, 0, 0.25], [659, 0.1, 0.25], [784, 0.2, 0.35]],
      pop: [[300, 0, 0.08], [600, 0.05, 0.1]],
    };
    for (const [freq, offset, duration] of presets[kind] || presets.ding) {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.0001, ctx.currentTime + offset);
      gain.gain.exponentialRampToValueAtTime(0.25, ctx.currentTime + offset + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + offset + duration);
      osc.connect(gain).connect(ctx.destination);
      osc.start(ctx.currentTime + offset);
      osc.stop(ctx.currentTime + offset + duration + 0.05);
    }
    window.setTimeout(() => void ctx.close().catch(() => {}), 1500);
  } catch {
    /* audio unavailable */
  }
}

function typeIcon(type: string) {
  if (type === "order") return <Package size={14} />;
  if (type === "chat") return <MessageCircle size={14} />;
  if (type === "review") return <Star size={14} />;
  if (type === "question") return <HelpCircle size={14} />;
  return <Bell size={14} />;
}

function typeLabel(type: string) {
  if (type === "order") return "Заказ";
  if (type === "chat") return "Чат";
  if (type === "review") return "Отзыв";
  if (type === "question") return "Вопрос";
  return "Событие";
}

export function NotificationsBell() {
  const [rows, setRows] = useState<NotificationRow[]>([]);
  const [unread, setUnread] = useState(0);
  const [open, setOpen] = useState(false);
  const [toast, setToast] = useState<NotificationRow | null>(null);
  const [dropdownPos, setDropdownPos] = useState<{ top: number; right: number } | null>(null);
  const bellRef = useRef<HTMLButtonElement | null>(null);
  const dropdownRef = useRef<HTMLDivElement | null>(null);

  const refresh = () => {
    fetch("/api/notifications?limit=30", { credentials: "same-origin" })
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => {
        if (!data?.ok) return;
        setRows(data.rows || []);
        setUnread(Number(data.unread || 0));
      })
      .catch(() => {});
  };

  const openDropdown = () => {
    if (!bellRef.current) return;
    const rect = bellRef.current.getBoundingClientRect();
    setDropdownPos({
      top: rect.bottom + 8,
      right: window.innerWidth - rect.right,
    });
    setOpen(true);
  };

  useEffect(() => {
    refresh();
    const source = new EventSource("/api/notifications/stream");
    source.addEventListener("notification", (event) => {
      try {
        const row = JSON.parse((event as MessageEvent).data) as NotificationRow;
        const settings = readNotificationSoundSettings();
        if (settings.enabled && settings.types[row.type] !== false) {
          playNotificationSound(settings.sound);
          setToast(row);
          window.setTimeout(() => setToast((current) => (current?.id === row.id ? null : current)), 7000);
        }
        refresh();
      } catch {
        /* ignore malformed event */
      }
    });
    const fallback = window.setInterval(refresh, 60_000);
    const onDocClick = (event: MouseEvent) => {
      const target = event.target as Node;
      const inBell = bellRef.current?.contains(target);
      const inDropdown = dropdownRef.current?.contains(target);
      if (!inBell && !inDropdown) setOpen(false);
    };
    document.addEventListener("click", onDocClick);
    return () => {
      source.close();
      window.clearInterval(fallback);
      document.removeEventListener("click", onDocClick);
    };
  }, []);

  const markAllRead = () => {
    fetch("/api/notifications/read", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ all: true }),
    }).then(refresh).catch(() => {});
  };

  return (
    <div className="notify-wrap">
      <button
        className="notify-bell"
        type="button"
        ref={bellRef}
        onClick={() => (open ? setOpen(false) : openDropdown())}
        title="Уведомления"
      >
        <Bell size={18} />
        {unread > 0 ? <span className="notify-badge">{unread > 99 ? "99+" : unread}</span> : null}
      </button>
      {open && dropdownPos ? createPortal(
        <div
          className="notify-dropdown"
          ref={dropdownRef}
          style={{ position: "fixed", top: dropdownPos.top, right: dropdownPos.right }}
        >
          <div className="notify-head">
            <strong>Уведомления</strong>
            <button type="button" onClick={markAllRead} title="Отметить все прочитанными">
              <CheckCheck size={14} /> прочитано
            </button>
          </div>
          <div className="notify-list">
            {rows.length ? rows.map((row) => (
              <div className={`notify-item${row.readAt ? "" : " is-unread"}`} key={row.id}>
                <span className={`notify-type notify-type-${row.type}`}>{typeIcon(row.type)} {typeLabel(row.type)} · {row.marketplace === "ozon" ? "Ozon" : row.marketplace === "wb" ? "WB" : "Яндекс"}</span>
                <strong>{row.title}</strong>
                {row.body ? <small>{row.body}</small> : null}
                <small className="notify-time">{row.createdAt ? new Date(row.createdAt).toLocaleString("ru-RU") : ""}</small>
              </div>
            )) : <div className="notify-empty">Пока тихо.</div>}
          </div>
        </div>,
        document.body,
      ) : null}
      {toast ? createPortal(
        <div className="notify-toast" onClick={() => setToast(null)}>
          <span className={`notify-type notify-type-${toast.type}`}>{typeIcon(toast.type)} {typeLabel(toast.type)}</span>
          <strong>{toast.title}</strong>
          {toast.body ? <small>{toast.body}</small> : null}
        </div>,
        document.body,
      ) : null}
    </div>
  );
}
