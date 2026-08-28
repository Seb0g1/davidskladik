import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, RefreshCw, X } from "lucide-react";

export function BrandPicker({
  value,
  options,
  loading,
  onChange,
  onRefresh,
  refreshing,
  canRefresh,
}: {
  value: string;
  options: string[];
  loading?: boolean;
  onChange: (next: string) => void;
  onRefresh?: () => void;
  refreshing?: boolean;
  canRefresh?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState(value);
  const [active, setActive] = useState(0);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => { setQuery(value); }, [value]);
  useEffect(() => {
    const onDoc = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const base = needle ? options.filter((brand) => brand.toLowerCase().includes(needle)) : options;
    return base.slice(0, 200);
  }, [query, options]);

  const choose = (brand: string) => {
    onChange(brand);
    setQuery(brand);
    setOpen(false);
  };

  return (
    <div className="brand-combo" ref={wrapRef}>
      <input
        className="brand-combo-input"
        value={query}
        placeholder="Бренд"
        onFocus={() => setOpen(true)}
        onChange={(event) => { setQuery(event.target.value); setOpen(true); setActive(0); }}
        onKeyDown={(event) => {
          if (event.key === "ArrowDown") { event.preventDefault(); setActive((index) => Math.min(filtered.length - 1, index + 1)); }
          else if (event.key === "ArrowUp") { event.preventDefault(); setActive((index) => Math.max(0, index - 1)); }
          else if (event.key === "Enter") { event.preventDefault(); if (filtered[active]) choose(filtered[active]); else onChange(query); setOpen(false); }
          else if (event.key === "Escape") setOpen(false);
        }}
      />
      <div className="brand-combo-meta">
        {loading ? <><Loader2 className="spin" size={11} /> загружаю бренды</> : <span>{options.length} брендов</span>}
        {value ? (
          <button type="button" className="icon-action" title="Сбросить бренд" onClick={() => choose("")}><X size={12} /></button>
        ) : null}
        {canRefresh && onRefresh ? (
          <button type="button" className="icon-action" title="Обновить список брендов" onClick={onRefresh} disabled={refreshing}>
            {refreshing ? <Loader2 className="spin" size={12} /> : <RefreshCw size={12} />}
          </button>
        ) : null}
      </div>
      {open ? (
        <div className="brand-combo-pop">
          {value ? (
            <button type="button" className="brand-combo-option" onClick={() => choose("")}>Все бренды</button>
          ) : null}
          {filtered.map((brand, index) => (
            <button
              type="button"
              key={brand}
              className={`brand-combo-option${index === active ? " is-active" : ""}`}
              onMouseEnter={() => setActive(index)}
              onClick={() => choose(brand)}
            >
              {brand}
            </button>
          ))}
          {!filtered.length ? <div className="brand-combo-empty">Бренд не найден</div> : null}
        </div>
      ) : null}
    </div>
  );
}
