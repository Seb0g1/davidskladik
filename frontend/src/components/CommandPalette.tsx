import { ArrowRight, Clock, CornerDownLeft, Keyboard, LogOut, Palette, PanelLeft, RefreshCw, Search, Star } from "lucide-react";
import { ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { AppRoute, NavItem } from "../lib/nav";

export type PaletteAction = { id: string; label: string; hint?: string; icon: ReactNode; keywords?: string; run: () => void };

// Набор на английской раскладке вместо русской («crkfl» → «склад»).
const EN = "qwertyuiop[]asdfghjkl;'zxcvbnm,.`";
const RU = "йцукенгшщзхъфывапролджэячсмитьбюё";
function switchLayout(text: string) {
  return text.split("").map((char) => {
    const index = EN.indexOf(char);
    return index >= 0 ? RU[index] : char;
  }).join("");
}

function score(haystack: string, needle: string) {
  if (!needle) return 1;
  const text = haystack.toLowerCase();
  if (text.startsWith(needle)) return 100;
  if (text.split(/[\s/·-]+/).some((word) => word.startsWith(needle))) return 70;
  if (text.includes(needle)) return 40;
  // Буквы по порядку («скл» ~ «Склад», «авк» ~ «Автокорзина»).
  let position = 0;
  for (const char of needle) {
    position = text.indexOf(char, position);
    if (position < 0) return 0;
    position += 1;
  }
  return 10;
}

type Entry = { id: string; group: string; label: string; hint?: string; icon: ReactNode; run: () => void; score: number };

export function CommandPalette({
  open,
  onClose,
  items,
  recent,
  favorites,
  actions,
  onNavigate,
}: {
  open: boolean;
  onClose: () => void;
  items: NavItem[];
  recent: AppRoute[];
  favorites: AppRoute[];
  actions: PaletteAction[];
  onNavigate: (href: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const listRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    setQuery("");
    setActive(0);
    window.setTimeout(() => inputRef.current?.focus(), 0);
  }, [open]);

  const entries = useMemo<Entry[]>(() => {
    const raw = query.trim().toLowerCase();
    const needles = raw ? Array.from(new Set([raw, switchLayout(raw)])) : [""];
    const best = (text: string) => Math.max(...needles.map((needle) => score(text, needle)));
    const result: Entry[] = [];
    if (!raw) {
      const seen = new Set<AppRoute>();
      for (const route of [...favorites, ...recent]) {
        const item = items.find((candidate) => candidate.route === route);
        if (!item || seen.has(route)) continue;
        seen.add(route);
        result.push({ id: `nav-${route}`, group: favorites.includes(route) ? "Избранное" : "Недавние", label: item.label, icon: favorites.includes(route) ? <Star size={15} /> : <Clock size={15} />, run: () => onNavigate(item.href), score: 1 });
      }
      for (const item of items) {
        if (seen.has(item.route)) continue;
        result.push({ id: `nav-${item.route}`, group: "Разделы", label: item.label, icon: item.icon, run: () => onNavigate(item.href), score: 1 });
      }
      for (const action of actions) result.push({ id: action.id, group: "Действия", label: action.label, hint: action.hint, icon: action.icon, run: action.run, score: 1 });
      return result;
    }
    for (const item of items) {
      const value = Math.max(best(item.label) * 2, best(item.keywords || ""));
      if (value > 0) result.push({ id: `nav-${item.route}`, group: "Разделы", label: item.label, icon: item.icon, run: () => onNavigate(item.href), score: value });
    }
    for (const action of actions) {
      const value = Math.max(best(action.label) * 2, best(action.keywords || ""));
      if (value > 0) result.push({ id: action.id, group: "Действия", label: action.label, hint: action.hint, icon: action.icon, run: action.run, score: value });
    }
    result.sort((a, b) => b.score - a.score);
    if (items.some((item) => item.route === "warehouse")) {
      const text = query.trim();
      result.push({
        id: "search-warehouse",
        group: "Поиск",
        label: `Найти на складе: «${text}»`,
        hint: "артикул, название, SKU",
        icon: <Search size={15} />,
        run: () => onNavigate(`/app/warehouse?q=${encodeURIComponent(text)}`),
        score: 0,
      });
    }
    return result;
  }, [query, items, recent, favorites, actions, onNavigate]);

  useEffect(() => setActive(0), [query]);
  useEffect(() => {
    listRef.current?.querySelector<HTMLElement>(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [active]);

  if (!open) return null;

  const runEntry = (entry: Entry | undefined) => {
    if (!entry) return;
    onClose();
    entry.run();
  };

  const onKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "ArrowDown" || (event.key === "Tab" && !event.shiftKey)) {
      event.preventDefault();
      setActive((value) => (entries.length ? (value + 1) % entries.length : 0));
    } else if (event.key === "ArrowUp" || (event.key === "Tab" && event.shiftKey)) {
      event.preventDefault();
      setActive((value) => (entries.length ? (value - 1 + entries.length) % entries.length : 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      runEntry(entries[active]);
    } else if (event.key === "Escape") {
      event.preventDefault();
      onClose();
    }
  };

  let lastGroup = "";
  return (
    <div className="cmdk-overlay" onMouseDown={onClose}>
      <div className="cmdk" role="dialog" aria-modal="true" aria-label="Быстрый переход" onMouseDown={(event) => event.stopPropagation()} onKeyDown={onKeyDown}>
        <label className="cmdk-input">
          <Search size={18} />
          <input ref={inputRef} autoFocus value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Раздел, действие или товар для поиска…" spellCheck={false} autoComplete="off" />
          <kbd>Esc</kbd>
        </label>
        <div className="cmdk-list" ref={listRef}>
          {entries.map((entry, index) => {
            const header = entry.group !== lastGroup ? <div className="cmdk-group" key={`g-${entry.group}`}>{entry.group}</div> : null;
            lastGroup = entry.group;
            return (
              <div key={entry.id}>
                {header}
                <button
                  type="button"
                  data-index={index}
                  className={`cmdk-item${index === active ? " is-active" : ""}`}
                  onMouseMove={() => { if (index !== active) setActive(index); }}
                  onClick={() => runEntry(entry)}
                >
                  <span className="cmdk-item-icon">{entry.icon}</span>
                  <span className="cmdk-item-label">{entry.label}</span>
                  {entry.hint ? <span className="cmdk-item-hint">{entry.hint}</span> : null}
                  {index === active ? <CornerDownLeft size={14} className="cmdk-enter" /> : <ArrowRight size={14} className="cmdk-arrow" />}
                </button>
              </div>
            );
          })}
          {!entries.length ? <div className="cmdk-empty">Ничего не найдено</div> : null}
        </div>
        <div className="cmdk-foot">
          <span><kbd>↑</kbd><kbd>↓</kbd> выбор</span>
          <span><kbd>Enter</kbd> открыть</span>
          <span><kbd>/</kbd> поиск на странице</span>
          <span><kbd>Alt</kbd>+<kbd>1…9</kbd> избранное</span>
        </div>
      </div>
    </div>
  );
}

export const paletteIcons = { Keyboard, LogOut, Palette, PanelLeft, RefreshCw };
