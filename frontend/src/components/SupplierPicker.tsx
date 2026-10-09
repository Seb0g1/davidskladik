import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Check, ChevronDown, Search, X } from "lucide-react";

// Per-supplier colour, so neighbouring supplier cards in the picking list never look
// alike and the picker dot matches the card stripe. Colours go by alphabetical position
// in the full supplier list, so two suppliers next to each other always differ.
const SUPPLIER_PALETTE = ["#4aa3ff", "#f5a524", "#31d07b", "#f5606f", "#b47cff", "#22c7d6", "#f472b6", "#a3e635", "#ff8a4c", "#8b9cff"];
let paletteOrder = new Map<string, number>();

export function setSupplierColorOrder(names: string[]) {
  const sorted = Array.from(new Set(names.filter(Boolean))).sort((a, b) => a.localeCompare(b, "ru", { sensitivity: "base" }));
  paletteOrder = new Map(sorted.map((name, index) => [name, index]));
}

export function supplierColor(name: string) {
  let index = paletteOrder.get(name);
  if (index === undefined) {
    let hash = 0;
    for (const ch of String(name || "")) hash = (hash * 31 + ch.charCodeAt(0)) | 0;
    index = Math.abs(hash);
  }
  return SUPPLIER_PALETTE[index % SUPPLIER_PALETTE.length];
}

// A phone keyboard is often left on the English layout: "byyf" should still find "Инна".
const LATIN_TO_CYR: Record<string, string> = {
  q: "й", w: "ц", e: "у", r: "к", t: "е", y: "н", u: "г", i: "ш", o: "щ", p: "з", "[": "х", "]": "ъ",
  a: "ф", s: "ы", d: "в", f: "а", g: "п", h: "р", j: "о", k: "л", l: "д", ";": "ж", "'": "э",
  z: "я", x: "ч", c: "с", v: "м", b: "и", n: "т", m: "ь", ",": "б", ".": "ю", "`": "ё",
};

const normalize = (value: string) => String(value || "").toLowerCase().replace(/ё/g, "е").replace(/\s+/g, " ").trim();
const toCyrillic = (value: string) => normalize(Array.from(value.toLowerCase()).map((ch) => LATIN_TO_CYR[ch] ?? ch).join(""));

// 0 = name starts with the query, 1 = a word starts with it, 2 = substring, -1 = no match.
function matchRank(name: string, query: string) {
  if (!query) return 0;
  const text = normalize(name);
  if (text.startsWith(query)) return 0;
  if (text.split(/[\s\-_.()«»"]+/).some((word) => word.startsWith(query))) return 1;
  return text.includes(query) ? 2 : -1;
}

export function SupplierPicker({
  value,
  suppliers,
  counts = {},
  onChange,
  allLabel = "Все поставщики",
  className = "",
}: {
  value: string;
  suppliers: string[];
  counts?: Record<string, number>;
  onChange: (next: string) => void;
  allLabel?: string;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const listRef = useRef<HTMLDivElement | null>(null);

  const options = useMemo(() => {
    const plain = normalize(query);
    const cyr = toCyrillic(query);
    const ranked = suppliers
      .map((name) => {
        const a = matchRank(name, plain);
        const b = cyr !== plain ? matchRank(name, cyr) : -1;
        const rank = a < 0 ? b : b < 0 ? a : Math.min(a, b);
        return { name, rank };
      })
      .filter((item) => item.rank >= 0)
      .sort((left, right) => left.rank - right.rank || left.name.localeCompare(right.name, "ru", { sensitivity: "base" }))
      .map((item) => item.name);
    // "All suppliers" stays on top only while nothing is typed: Enter after typing picks the best match.
    return query.trim() ? ranked : ["", ...ranked];
  }, [query, suppliers]);

  useEffect(() => {
    if (!open) return;
    setQuery("");
    const index = ["", ...suppliers].indexOf(value);
    setActive(index >= 0 ? index : 0);
    // Focus after the sheet mounts so the phone keyboard opens immediately.
    const timer = window.setTimeout(() => inputRef.current?.focus(), 30);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { window.clearTimeout(timer); document.body.style.overflow = prevOverflow; };
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { setActive(0); }, [query]);

  useEffect(() => {
    if (!open) return;
    listRef.current?.querySelector<HTMLElement>(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [active, open]);

  const choose = (next: string) => {
    onChange(next);
    setOpen(false);
  };

  const totalCount = Object.values(counts).reduce((sum, n) => sum + (Number(n) || 0), 0);

  return (
    <>
      <button
        type="button"
        className={`supplier-picker-trigger${value ? " is-filtered" : ""} ${className}`.trim()}
        onClick={() => setOpen(true)}
        aria-haspopup="dialog"
        title="Выбрать поставщика"
      >
        {value ? <span className="supplier-dot" style={{ background: supplierColor(value) }} /> : <Search size={15} />}
        <span className="supplier-picker-value">{value || allLabel}</span>
        {value ? (
          <span
            role="button"
            tabIndex={-1}
            className="supplier-picker-clear"
            title="Показать всех"
            onClick={(event) => { event.stopPropagation(); onChange(""); }}
          >
            <X size={14} />
          </span>
        ) : <ChevronDown size={15} className="supplier-picker-caret" />}
      </button>
      {/* Portal: the page container is its own stacking context, so the sheet would slide under the top bar. */}
      {open ? createPortal(
        <div className="supplier-picker-overlay" onMouseDown={(event) => { if (event.target === event.currentTarget) setOpen(false); }}>
          <div className="supplier-picker-sheet" role="dialog" aria-label="Выбор поставщика">
            <div className="supplier-picker-search">
              <Search size={17} />
              <input
                ref={inputRef}
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Введите имя поставщика…"
                autoComplete="off"
                autoCorrect="off"
                spellCheck={false}
                enterKeyHint="go"
                onKeyDown={(event) => {
                  if (event.key === "ArrowDown") { event.preventDefault(); setActive((i) => Math.min(options.length - 1, i + 1)); }
                  else if (event.key === "ArrowUp") { event.preventDefault(); setActive((i) => Math.max(0, i - 1)); }
                  else if (event.key === "Enter") { event.preventDefault(); if (options[active] !== undefined) choose(options[active]); }
                  else if (event.key === "Escape") { event.preventDefault(); if (query) setQuery(""); else setOpen(false); }
                }}
              />
              <button type="button" className="supplier-picker-close" onClick={() => setOpen(false)} title="Закрыть (Esc)">
                <X size={18} />
              </button>
            </div>
            <div className="supplier-picker-list" ref={listRef} role="listbox">
              {options.map((name, index) => {
                const count = name ? counts[name] : totalCount;
                return (
                  <button
                    type="button"
                    key={name || "__all"}
                    data-index={index}
                    role="option"
                    aria-selected={name === value}
                    className={`supplier-picker-option${index === active ? " is-active" : ""}${name === value ? " is-selected" : ""}`}
                    onMouseEnter={() => setActive(index)}
                    onClick={() => choose(name)}
                  >
                    {name
                      ? <span className="supplier-dot" style={{ background: supplierColor(name) }} />
                      : <span className="supplier-dot is-all" />}
                    <span className="supplier-picker-name">{name || allLabel}</span>
                    {count ? <span className="supplier-picker-count">{count}</span> : null}
                    {name === value ? <Check size={15} /> : null}
                  </button>
                );
              })}
              {!options.length ? <div className="supplier-picker-empty">Поставщик «{query}» не найден</div> : null}
            </div>
          </div>
        </div>,
        document.body,
      ) : null}
    </>
  );
}
