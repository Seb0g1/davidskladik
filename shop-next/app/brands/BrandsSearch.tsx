"use client";
// Brand index like on marketplaces: popular brands as photo tiles, a search box, a sticky
// A–Z / А–Я letter bar and the full list grouped by letter.
import { useMemo, useState } from "react";
import Link from "next/link";
import { Search, X } from "lucide-react";
import { brandHref } from "@/lib/landings";
import { productImg } from "@/lib/img";

export interface BrandRow { name: string; count: number; inStock?: number; minPrice?: number; image?: string | null }

function ruPlural(n: number, one: string, few: string, many: string) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return few;
  return many;
}
const letterOf = (name: string) => {
  const c = name.trim()[0]?.toUpperCase() || "#";
  return /[0-9]/.test(c) ? "0–9" : /[A-ZА-ЯЁ]/.test(c) ? (c === "Ё" ? "Е" : c) : "#";
};
const rub = (n: number) => `${n.toLocaleString("ru-RU")} ₽`;

export default function BrandsSearch({ brands }: { brands: BrandRow[]; total: number; totalItems: number }) {
  const [q, setQ] = useState("");
  const term = q.trim().toLowerCase();

  const popular = useMemo(() => [...brands].filter((b) => b.image).sort((a, b) => b.count - a.count).slice(0, 12), [brands]);
  const filtered = useMemo(() => (term ? brands.filter((b) => b.name.toLowerCase().includes(term)) : brands), [brands, term]);
  const groups = useMemo(() => {
    const m = new Map<string, BrandRow[]>();
    for (const b of filtered) { const k = letterOf(b.name); if (!m.has(k)) m.set(k, []); m.get(k)!.push(b); }
    const order = (k: string) => (k === "0–9" ? "0" : /[A-Z]/.test(k) ? `1${k}` : k === "#" ? "9" : `2${k}`);
    return [...m.entries()].sort((a, b) => order(a[0]).localeCompare(order(b[0]), "ru"));
  }, [filtered]);
  const letters = groups.map(([k]) => k);

  return (
    <div className="mv-brands">
      {!term && popular.length > 0 && (
        <section className="mv-brands-pop">
          <h2>Популярные бренды</h2>
          <div className="mv-brands-tiles">
            {popular.map((b) => (
              <Link key={b.name} prefetch={false} href={brandHref(b.name)} className="mv-brand-tile">
                <span className="mv-brand-tile-img">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img src={productImg(b.image, 320)} alt="" loading="lazy" />
                </span>
                <b>{b.name}</b>
                <em>{b.count} {ruPlural(b.count, "аромат", "аромата", "ароматов")}{b.minPrice ? ` · от ${rub(b.minPrice)}` : ""}</em>
              </Link>
            ))}
          </div>
        </section>
      )}

      <div className="mv-brands-bar">
        <label className="mv-brands-search">
          <Search size={16} aria-hidden />
          <input value={q} onChange={(e) => setQ(e.target.value)} placeholder={`Найти среди ${brands.length} брендов`} aria-label="Найти бренд" />
          {q && <button type="button" onClick={() => setQ("")} aria-label="Очистить"><X size={15} /></button>}
        </label>
        <nav className="mv-brands-letters" aria-label="Алфавит">
          {letters.map((l) => <a key={l} href={`#brand-${l}`}>{l}</a>)}
        </nav>
      </div>

      {groups.length === 0 && <p className="mv-brands-empty">Не нашли «{q}». Попробуйте другое написание — например, латиницей.</p>}

      <div className="mv-brands-groups">
        {groups.map(([letter, list]) => (
          <section key={letter} id={`brand-${letter}`} className="mv-brands-group">
            <h3>{letter}</h3>
            <ul>
              {list.map((b) => (
                <li key={b.name}>
                  <Link prefetch={false} href={brandHref(b.name)}>
                    <span>{b.name}</span>
                    <em>{b.count}</em>
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        ))}
      </div>
    </div>
  );
}
