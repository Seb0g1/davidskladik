import { useSyncExternalStore } from "react";

// Module-level shared state for PM chip search — all PmChipInput instances
// across all pages read/write here, so chips persist during navigation.

let _chips: string[] = [];
const _listeners = new Set<() => void>();

function notify() { _listeners.forEach((fn) => fn()); }

export const pmSearchStore = {
  getChips: (): string[] => _chips,
  setChips: (chips: string[]): void => { _chips = chips; notify(); },
  addChip: (word: string): void => {
    const w = word.trim().toLowerCase();
    if (!w || _chips.includes(w)) return;
    _chips = [..._chips, w];
    notify();
  },
  removeChip: (idx: number): void => {
    _chips = _chips.filter((_, i) => i !== idx);
    notify();
  },
  clear: (): void => { _chips = []; notify(); },
  subscribe: (listener: () => void): (() => void) => {
    _listeners.add(listener);
    return () => _listeners.delete(listener);
  },
};

export function usePmChips(): string[] {
  return useSyncExternalStore(pmSearchStore.subscribe, pmSearchStore.getChips);
}
