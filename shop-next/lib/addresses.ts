// Saved delivery addresses, marketplace-style: pickup points / courier addresses used before.
// Signed-in buyers get them from their orders (API); everyone also keeps a local copy,
// so a guest's second checkout doesn't start from a blank map either.
export interface SavedAddress {
  id: string;
  type: "pickup" | "courier";
  pvzId?: string | null;
  name?: string;
  address: string;
  city?: string;
  postalCode?: string;
  lastUsed?: string;
  /** СДЭК / Яндекс / Достависта и вариант (cdek_pvz, yandex_courier…); нет — пункт Ozon или старый курьер */
  carrier?: string | null;
  method?: string | null;
  region?: string;
  lat?: number;
  lng?: number;
  flat?: string;
  entrance?: string;
  floor?: string;
  intercom?: string;
  addrComment?: string;
}
export interface SavedContact { firstName: string; lastName?: string; phone: string; email: string }

const KEY = "mv_addresses_v1";
const HIDDEN = "mv_addresses_hidden_v1";
const CONTACT = "mv_contact_v1";
const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop/auth";

const read = <T,>(k: string, fallback: T): T => {
  try { const v = localStorage.getItem(k); return v ? (JSON.parse(v) as T) : fallback; } catch { return fallback; }
};
const write = (k: string, v: unknown) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* private mode */ } };

export function addressKey(a: Pick<SavedAddress, "type" | "pvzId" | "address">) {
  return a.type === "pickup" && a.pvzId ? `pvz:${a.pvzId}` : `${a.type}:${a.address.trim().toLowerCase()}`;
}

/** "Балашиха, Россия, Московская Область, Балашиха, улица Бояринова, 19" → "Балашиха, Московская область, улица Бояринова, 19" */
export function prettyAddress(a: Pick<SavedAddress, "address" | "city">): string {
  const seen = new Set<string>();
  const parts: string[] = [];
  for (const raw of [a.city || "", ...a.address.split(",")]) {
    const p = raw.trim().replace(/Область/g, "область");
    const k = p.toLowerCase().replace(/ё/g, "е");
    if (!p || k === "россия" || seen.has(k)) continue;
    seen.add(k);
    parts.push(p);
  }
  return parts.join(", ");
}

/** same place written differently (duplicated city, "Россия", case) → one key */
function placeKey(a: SavedAddress): string {
  const segs = prettyAddress(a).toLowerCase().replace(/ё/g, "е").split(",").map((x) => x.trim())
    .filter((x) => !/(область|край|республика|округ)$/.test(x));
  return `${a.type}|${segs.join("|")}`;
}

export function localAddresses(): SavedAddress[] { return read<SavedAddress[]>(KEY, []); }

export function rememberAddress(a: Omit<SavedAddress, "id" | "lastUsed">) {
  if (!a.address) return;
  const id = addressKey(a);
  const list = localAddresses().filter((x) => x.id !== id);
  write(KEY, [{ ...a, id, lastUsed: new Date().toISOString() }, ...list].slice(0, 8));
  const pk = placeKey({ ...a, id } as SavedAddress);
  write(HIDDEN, read<string[]>(HIDDEN, []).filter((h) => h !== id && h !== pk));
}

/** hides the address and every other spelling of the same place */
export function forgetAddress(a: SavedAddress) {
  const pk = placeKey(a);
  write(KEY, localAddresses().filter((x) => x.id !== a.id && placeKey(x) !== pk));
  write(HIDDEN, [...new Set([...read<string[]>(HIDDEN, []), a.id, pk])]);
}

export function rememberContact(c: SavedContact) { write(CONTACT, c); }
export function localContact(): SavedContact | null { return read<SavedContact | null>(CONTACT, null); }

/** local + server addresses, newest first, minus the ones the buyer removed */
export async function loadAddresses(token?: string | null): Promise<{ addresses: SavedAddress[]; contact: SavedContact | null }> {
  let server: SavedAddress[] = [];
  let contact: SavedContact | null = localContact();
  if (token) {
    try {
      const r = await fetch(`${API}/addresses`, { headers: { Authorization: `Bearer ${token}` } });
      const d = await r.json();
      if (d.ok) { server = d.addresses || []; contact = contact || d.contact || null; }
    } catch { /* offline: local only */ }
  }
  const hidden = new Set(read<string[]>(HIDDEN, []));
  const byId = new Map<string, SavedAddress>();
  for (const a of [...localAddresses(), ...server]) {
    const id = a.id || addressKey(a);
    if (hidden.has(id) || hidden.has(placeKey(a))) continue;
    const prev = byId.get(id);
    if (!prev || String(a.lastUsed || "") > String(prev.lastUsed || "")) byId.set(id, { ...prev, ...a, id, name: a.name || prev?.name });
  }
  const sorted = [...byId.values()].sort((a, b) => String(b.lastUsed || "").localeCompare(String(a.lastUsed || "")));
  // one entry per place: the newest spelling / pickup point id wins
  const places = new Set<string>();
  const addresses = sorted.filter((a) => {
    const k = placeKey(a);
    if (places.has(k)) return false;
    places.add(k);
    return true;
  }).slice(0, 8);
  return { addresses, contact };
}
