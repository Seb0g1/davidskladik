import { useSyncExternalStore } from "react";

function createPmSearchStore() {
  let _chips: string[] = [];
  const _listeners = new Set<() => void>();
  function notify() { _listeners.forEach((fn) => fn()); }
  return {
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
}

// Each named store is isolated — pages that use different keys don't share chips.
const _stores = new Map<string, ReturnType<typeof createPmSearchStore>>();

export function getPmSearchStore(key = "global") {
  if (!_stores.has(key)) _stores.set(key, createPmSearchStore());
  return _stores.get(key)!;
}

// Default global store (used by WarehousePage linking modal).
export const pmSearchStore = getPmSearchStore("global");

export function usePmChips(key = "global"): string[] {
  const store = getPmSearchStore(key);
  return useSyncExternalStore(store.subscribe, store.getChips);
}
