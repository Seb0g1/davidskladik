import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { X } from "lucide-react";
import { fetchJson } from "../api";
import { PmWordSuggestSchema } from "../types";
import { pmSearchStore, usePmChips } from "../lib/pmSearchStore";

const MIN_PREFIX_LEN = 1;

interface PmChipInputProps {
  onQueryChange: (query: string) => void;
  placeholder?: string;
  disabled?: boolean;
  inputClassName?: string;
}

export function PmChipInput({ onQueryChange, placeholder = "Введите слово…", disabled, inputClassName }: PmChipInputProps) {
  const chips = usePmChips();
  const [inputVal, setInputVal] = useState("");
  const [focused, setFocused] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    onQueryChange(chips.join(" "));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chips]);

  const prefix = inputVal.trim().toLowerCase();
  const wordSuggest = useQuery({
    queryKey: ["pm-words", prefix],
    queryFn: () => fetchJson(`/api/pricemaster/words?prefix=${encodeURIComponent(prefix)}`, PmWordSuggestSchema),
    enabled: prefix.length >= MIN_PREFIX_LEN,
    staleTime: 5 * 60_000,
  });

  const commitWord = (word: string) => {
    const words = word.trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) return;
    for (const w of words) pmSearchStore.addChip(w);
    setInputVal("");
    inputRef.current?.focus();
  };

  const removeChip = (idx: number) => {
    pmSearchStore.removeChip(idx);
    inputRef.current?.focus();
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if ((e.key === " " || e.key === "Enter") && inputVal.trim()) {
      e.preventDefault();
      commitWord(inputVal);
      return;
    }
    if (e.key === "Backspace" && !inputVal && chips.length) {
      removeChip(chips.length - 1);
    }
  };

  const handlePaste = (e: React.ClipboardEvent<HTMLInputElement>) => {
    const text = e.clipboardData.getData("text");
    if (text.trim()) {
      e.preventDefault();
      commitWord(text);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setInputVal(val);
    onQueryChange([...chips, val.trim()].filter(Boolean).join(" "));
  };

  const showDropdown = focused && prefix.length >= MIN_PREFIX_LEN && (wordSuggest.data?.words?.length ?? 0) > 0;

  return (
    <div
      className={`pm-chip-wrap${focused ? " is-focused" : ""}${disabled ? " is-disabled" : ""}`}
      onClick={() => inputRef.current?.focus()}
    >
      {chips.map((chip, i) => (
        <span key={`${chip}:${i}`} className="pm-chip">
          {chip}
          <button
            type="button"
            className="pm-chip-remove"
            tabIndex={-1}
            onMouseDown={(e) => { e.preventDefault(); removeChip(i); }}
          >
            <X size={10} />
          </button>
        </span>
      ))}
      {chips.length > 1 && (
        <button
          type="button"
          className="pm-chip-clear-all"
          tabIndex={-1}
          title="Сбросить все фильтры"
          onMouseDown={(e) => { e.preventDefault(); pmSearchStore.clear(); inputRef.current?.focus(); }}
        >
          <X size={12} />
        </button>
      )}

      <input
        ref={inputRef}
        className={`pm-chip-input${inputClassName ? ` ${inputClassName}` : ""}`}
        value={inputVal}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        onPaste={handlePaste}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        placeholder={chips.length ? "" : placeholder}
        disabled={disabled}
        autoComplete="off"
      />

      {showDropdown && (
        <div className="pm-word-dropdown">
          {(wordSuggest.data?.words ?? []).map((w) => (
            <button
              key={w}
              type="button"
              className="pm-word-option"
              onMouseDown={(e) => { e.preventDefault(); commitWord(w); }}
            >
              {w}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
