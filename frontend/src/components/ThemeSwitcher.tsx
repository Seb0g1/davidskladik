import { useEffect, useRef, useState } from "react";
import { Check, Palette } from "lucide-react";

export type ThemeName = "blue" | "pink" | "red" | "yellow" | "green" | "violet" | "white";

const THEME_KEY = "davidsklad.theme";

const THEMES: Array<{ id: ThemeName; label: string; swatch: string }> = [
  { id: "blue", label: "Синий", swatch: "#0a84ff" },
  { id: "pink", label: "Розовый", swatch: "#ff5ca8" },
  { id: "red", label: "Красный", swatch: "#ff5656" },
  { id: "yellow", label: "Жёлтый", swatch: "#f5bc24" },
  { id: "green", label: "Зелёный", swatch: "#31d07b" },
  { id: "violet", label: "Фиолетовый", swatch: "#9b86ff" },
  { id: "white", label: "Белый", swatch: "#ffffff" },
];

export function readTheme(): ThemeName {
  try {
    const saved = localStorage.getItem(THEME_KEY);
    if (THEMES.some((theme) => theme.id === saved)) return saved as ThemeName;
  } catch {
    /* ignore */
  }
  return "blue";
}

export function applyTheme(theme: ThemeName) {
  document.documentElement.setAttribute("data-theme", theme);
}

export function writeTheme(theme: ThemeName) {
  try {
    localStorage.setItem(THEME_KEY, theme);
  } catch {
    /* ignore */
  }
  applyTheme(theme);
}

export function ThemeSwitcher() {
  const [open, setOpen] = useState(false);
  const [theme, setTheme] = useState<ThemeName>(() => readTheme());
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    applyTheme(theme);
  }, []);

  useEffect(() => {
    const onDocClick = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  const choose = (next: ThemeName) => {
    setTheme(next);
    writeTheme(next);
    setOpen(false);
  };

  const current = THEMES.find((item) => item.id === theme) || THEMES[0];

  return (
    <div className="theme-switch-wrap" ref={wrapRef}>
      <button className="theme-switch-toggle" type="button" onClick={() => setOpen((value) => !value)} title="Цветовая тема">
        <Palette size={16} />
        <span className="theme-switch-dot" style={{ background: current.swatch }} />
      </button>
      {open ? (
        <div className="theme-switch-dropdown">
          <div className="theme-switch-head">Цветовая тема</div>
          <div className="theme-switch-grid">
            {THEMES.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`theme-switch-option${item.id === theme ? " is-active" : ""}`}
                onClick={() => choose(item.id)}
                title={item.label}
              >
                <span className="theme-switch-swatch" style={{ background: item.swatch, color: item.id === "white" ? "#0a84ff" : "#ffffff" }}>
                  {item.id === theme ? <Check size={13} /> : null}
                </span>
                <span>{item.label}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
