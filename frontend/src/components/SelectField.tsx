import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";

export type SelectOption = { value: string; label: string };

// Custom dropdown that replaces native <select> so the open option list is themed
// (the browser-styled <option> popup was the "фильтры некрасивые / исправь до конца"
// complaint). Keyboard-accessible (Up/Down/Enter/Escape), closes on outside click.
export function SelectField({
  value,
  options,
  onChange,
  title,
  className = "",
  ariaLabel,
  disabled = false,
}: {
  value: string;
  options: SelectOption[];
  onChange: (next: string) => void;
  title?: string;
  className?: string;
  ariaLabel?: string;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  const selected = options.find((option) => option.value === value) || options[0];
  const selectedLabel = selected ? selected.label : "";

  useEffect(() => {
    if (!open) return;
    const index = options.findIndex((option) => option.value === value);
    setActive(index >= 0 ? index : 0);
  }, [open, value, options]);

  useEffect(() => {
    const onDoc = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const choose = (next: string) => {
    onChange(next);
    setOpen(false);
  };

  return (
    <div className={`select-field${open ? " is-open" : ""}${disabled ? " is-disabled" : ""} ${className}`.trim()} ref={wrapRef}>
      <button
        type="button"
        className="select-field-trigger"
        title={title}
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={ariaLabel || title}
        onClick={() => { if (!disabled) setOpen((value) => !value); }}
        onKeyDown={(event) => {
          if (disabled) return;
          if (event.key === "ArrowDown") { event.preventDefault(); setOpen(true); setActive((index) => Math.min(options.length - 1, index + 1)); }
          else if (event.key === "ArrowUp") { event.preventDefault(); setOpen(true); setActive((index) => Math.max(0, index - 1)); }
          else if (event.key === "Enter" || event.key === " ") { event.preventDefault(); if (open && options[active]) choose(options[active].value); else setOpen(true); }
          else if (event.key === "Escape") setOpen(false);
        }}
      >
        <span className="select-field-value">{selectedLabel}</span>
        <ChevronDown size={15} className="select-field-caret" />
      </button>
      {open ? (
        <div className="select-field-pop" role="listbox">
          {options.map((option, index) => (
            <button
              type="button"
              key={option.value}
              role="option"
              aria-selected={option.value === value}
              className={`select-field-option${index === active ? " is-active" : ""}${option.value === value ? " is-selected" : ""}`}
              onMouseEnter={() => setActive(index)}
              onClick={() => choose(option.value)}
            >
              <span>{option.label}</span>
              {option.value === value ? <Check size={14} /> : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
