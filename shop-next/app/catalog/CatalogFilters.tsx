"use client";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback } from "react";
import type { ShopCategory } from "@/lib/types";

interface Props {
  brands: { name: string; count: number }[];
  categories: ShopCategory[];
  activeBrand?: string;
  activeCategory?: string;
  activeInStock?: boolean;
  activeQ?: string;
}

const S = {
  border: "rgba(255,252,245,0.08)",
  accent: "#C9A96E",
  muted: "rgba(244,239,230,0.45)",
  text: "#F4EFE6",
};

export default function CatalogFilters({ brands, categories, activeBrand, activeCategory, activeInStock, activeQ }: Props) {
  const router = useRouter();
  const sp = useSearchParams();

  const navigate = useCallback((updates: Record<string, string | undefined>) => {
    const params = new URLSearchParams(sp.toString());
    params.delete("page");
    Object.entries(updates).forEach(([k, v]) => {
      if (v === undefined) params.delete(k);
      else params.set(k, v);
    });
    router.push(`/catalog?${params.toString()}`);
  }, [router, sp]);

  const topBrands = brands.slice(0, 30);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 28 }}>
      {/* Search */}
      <div>
        <div style={{ fontSize: 9, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 12 }}>Поиск</div>
        <form onSubmit={e => { e.preventDefault(); const val = (e.currentTarget.elements.namedItem("q") as HTMLInputElement).value; navigate({ q: val || undefined, brand: undefined, category: undefined }); }} style={{ display: "flex", gap: 6 }}>
          <input name="q" defaultValue={activeQ ?? ""} placeholder="Название, бренд..." style={{ flex: 1, background: "#1D1C18", border: `1px solid ${S.border}`, borderRadius: 6, padding: "7px 10px", color: S.text, fontSize: 12, outline: "none" }} />
          <button type="submit" style={{ background: "rgba(201,162,94,0.15)", border: `1px solid rgba(201,162,94,0.3)`, borderRadius: 6, padding: "7px 10px", color: S.accent, cursor: "pointer", fontSize: 12 }}>→</button>
        </form>
      </div>

      {/* In stock toggle */}
      <div>
        <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer", fontSize: 13, color: S.muted }}>
          <input type="checkbox" checked={activeInStock ?? false} onChange={e => navigate({ inStock: e.target.checked ? "true" : undefined })} style={{ accentColor: S.accent }} />
          Только в наличии
        </label>
      </div>

      {/* Categories */}
      {categories.length > 0 && (
        <div>
          <div style={{ fontSize: 9, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 12 }}>Категории</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <button onClick={() => navigate({ category: undefined })} style={{ textAlign: "left", background: "none", border: "none", padding: "5px 0", fontSize: 12, color: !activeCategory ? S.text : S.muted, cursor: "pointer", fontWeight: !activeCategory ? 500 : 400 }}>Все категории</button>
            {categories.map(c => (
              <button key={c.id} onClick={() => navigate({ category: c.name, brand: undefined })} style={{ textAlign: "left", background: "none", border: "none", padding: "5px 0", fontSize: 12, color: activeCategory === c.name ? S.accent : S.muted, cursor: "pointer" }}>{c.icon} {c.name}</button>
            ))}
          </div>
        </div>
      )}

      {/* Brands */}
      {topBrands.length > 0 && (
        <div>
          <div style={{ fontSize: 9, letterSpacing: "0.22em", textTransform: "uppercase", color: S.accent, marginBottom: 12 }}>Бренды</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, maxHeight: 320, overflowY: "auto" }}>
            {topBrands.map(b => (
              <button key={b.name} onClick={() => navigate({ brand: activeBrand === b.name ? undefined : b.name, category: undefined })} style={{ textAlign: "left", background: "none", border: "none", padding: "4px 0", fontSize: 12, color: activeBrand === b.name ? S.accent : S.muted, cursor: "pointer", display: "flex", justifyContent: "space-between" }}>
                <span>{b.name}</span>
                {b.count > 0 && <span style={{ fontSize: 10, opacity: 0.5 }}>{b.count}</span>}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Reset */}
      {(activeBrand || activeCategory || activeQ || activeInStock) && (
        <button onClick={() => router.push("/catalog")} style={{ alignSelf: "flex-start", fontSize: 11, color: S.muted, background: "none", border: `1px solid ${S.border}`, borderRadius: 6, padding: "6px 12px", cursor: "pointer", letterSpacing: "0.08em" }}>
          ✕ Сбросить фильтры
        </button>
      )}
    </div>
  );
}
