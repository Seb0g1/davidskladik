"use client";
import { useState, useRef } from "react";
import Link from "next/link";
import { Search, Sparkles, ShoppingBag, Check, ArrowRight } from "lucide-react";
import { useCart } from "@/components/CartContext";
import { aiSearch } from "@/lib/client";
import { toProductSlug } from "@/lib/slug";

const S = {
  bg:         "#09090b",
  surface:    "#111113",
  border:     "rgba(255,255,255,0.07)",
  borderGold: "rgba(201,162,94,0.3)",
  text:       "#f2ede6",
  muted:      "rgba(242,237,230,0.5)",
  accent:     "#c9a25e",
};

const EXAMPLES = [
  "Тёплый, как ваниль, но не приторный",
  "Свежий морской аромат для офиса",
  "Восточный мускусный на вечер",
  "Цветочный нежный, женственный, весенний",
  "Брутальный и стойкий, дерево и кожа",
  "Лёгкий цитрусовый для повседневного",
];

type Product = { id: string; offerId: string; name: string; brand: string; priceRub: number; images: string[]; inStock: boolean; _matchTerm?: string };

function ResultCard({ product: p }: { product: Product }) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    add(p as Parameters<typeof add>[0], 1);
    setAdded(true);
    setTimeout(() => setAdded(false), 1800);
  }

  return (
    <Link href={`/product/${toProductSlug(p.name, p.offerId)}`} style={{ textDecoration: "none" }}>
      <div style={{
        background: "rgba(17,17,19,0.9)", border: `1px solid ${S.border}`,
        borderRadius: 12, overflow: "hidden", display: "flex", flexDirection: "column",
        transition: "border-color 0.25s, transform 0.25s", cursor: "pointer",
      }}
        onMouseEnter={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = "rgba(201,162,94,0.4)"; el.style.transform = "translateY(-4px)"; }}
        onMouseLeave={e => { const el = e.currentTarget as HTMLElement; el.style.borderColor = S.border; el.style.transform = ""; }}
      >
        <div style={{ aspectRatio: "1", background: "#161512", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }}>
          {p.images[0]
            // eslint-disable-next-line @next/next/no-img-element
            ? <img src={p.images[0]} alt={p.name} style={{ width: "100%", height: "100%", objectFit: "contain" }} />
            : <span style={{ fontSize: 40, color: "rgba(242,237,230,0.1)" }}>{p.brand?.[0] ?? "?"}</span>
          }
        </div>
        <div style={{ padding: "14px 16px", display: "flex", flexDirection: "column", gap: 6, flex: 1 }}>
          {p.brand && <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.12em", color: S.accent, textTransform: "uppercase" }}>{p.brand}</div>}
          <div style={{ fontSize: 13, fontWeight: 600, color: S.text, lineHeight: 1.3 }}>{p.name}</div>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: "auto", paddingTop: 10 }}>
            <span style={{ fontSize: 16, fontWeight: 700, color: S.text }}>{p.priceRub.toLocaleString("ru-RU")} ₽</span>
            {p.inStock && (
              <button onClick={handleAdd} style={{
                background: added ? "rgba(74,222,128,0.15)" : "rgba(201,162,94,0.12)",
                border: `1px solid ${added ? "rgba(74,222,128,0.3)" : "rgba(201,162,94,0.25)"}`,
                color: added ? "#4ade80" : S.accent,
                borderRadius: 8, padding: "7px 12px", cursor: "pointer",
                fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", gap: 5,
                transition: "all 0.2s",
              }}>
                {added ? <><Check size={13} />Добавлено</> : <><ShoppingBag size={13} />В корзину</>}
              </button>
            )}
          </div>
        </div>
      </div>
    </Link>
  );
}

export default function AiFinderClient() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<Product[] | null>(null);
  const [label, setLabel] = useState("");
  const [terms, setTerms] = useState<string[]>([]);
  const [matchedNotes, setMatchedNotes] = useState<string[]>([]);
  const [matchedAccords, setMatchedAccords] = useState<string[]>([]);
  const [error, setError] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  async function search(q = query) {
    const trimmed = q.trim();
    if (!trimmed || loading) return;
    setLoading(true);
    setError("");
    setResults(null);
    setTerms([]); setMatchedNotes([]); setMatchedAccords([]); setLabel("");
    try {
      const data = await aiSearch(trimmed);
      setResults(data.products);
      setLabel(data.label);
      setTerms(data.terms);
      setMatchedNotes(data.notes ?? []);
      setMatchedAccords(data.accords ?? []);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Ошибка поиска");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      {/* Search box */}
      <div style={{
        background: S.surface, border: `1px solid ${S.borderGold}`,
        borderRadius: 12, overflow: "hidden",
        boxShadow: "0 0 0 4px rgba(201,162,94,0.04), 0 8px 32px rgba(0,0,0,0.4)",
        marginBottom: 20,
      }}>
        <textarea
          ref={textareaRef}
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) search(); }}
          placeholder='Например: "Тёплый восточный аромат, немного ванили, стойкий, для зимних вечеров"'
          rows={3}
          style={{
            width: "100%", padding: "20px 24px 8px",
            background: "transparent", border: "none", outline: "none", resize: "none",
            color: S.text, fontSize: 15, lineHeight: 1.6, fontFamily: "inherit",
            boxSizing: "border-box",
          }}
        />
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 16px 16px" }}>
          <span className="hidden md:inline" style={{ fontSize: 11, color: S.muted }}>Ctrl+Enter для поиска</span>
          <button
            onClick={() => search()}
            disabled={loading || !query.trim()}
            style={{
              display: "flex", alignItems: "center", gap: 8,
              background: query.trim() && !loading ? S.accent : "rgba(201,162,94,0.2)",
              color: query.trim() && !loading ? "#09090b" : S.muted,
              border: "none", borderRadius: 8, cursor: query.trim() ? "pointer" : "not-allowed",
              padding: "10px 22px", fontWeight: 600, fontSize: 13, letterSpacing: "0.04em",
              transition: "all 0.2s", fontFamily: "inherit",
            }}
          >
            {loading
              ? <><div style={{ width: 14, height: 14, border: "2px solid currentColor", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />Подбираю…</>
              : <><Search size={14} />Найти аромат</>
            }
          </button>
        </div>
      </div>

      {/* Example chips */}
      {!results && !loading && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center", marginBottom: 16 }}>
          {EXAMPLES.map(ex => (
            <button key={ex} onClick={() => { setQuery(ex); search(ex); }}
              style={{ background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)", borderRadius: 20, padding: "6px 14px", cursor: "pointer", fontSize: 12, color: S.muted, transition: "all 0.15s", fontFamily: "inherit" }}
              onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.45)"; el.style.color = S.text; }}
              onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = "rgba(255,255,255,0.08)"; el.style.color = S.muted; }}
            >{ex}</button>
          ))}
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div style={{ textAlign: "center", padding: "40px 24px" }}>
          <div style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 16 }}>
            <div style={{ width: 48, height: 48, border: "2px solid rgba(201,162,94,0.2)", borderTopColor: S.accent, borderRadius: "50%", animation: "spin 1s linear infinite" }} />
            <p style={{ fontSize: 14, color: S.muted }}>ИИ анализирует описание и ищет подходящие ароматы…</p>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div style={{ maxWidth: 560, margin: "0 auto 24px" }}>
          <div style={{ background: "rgba(248,113,113,0.06)", border: "1px solid rgba(248,113,113,0.2)", borderRadius: 8, padding: "14px 18px", fontSize: 13, color: "#f87171" }}>{error}</div>
        </div>
      )}

      {/* Results */}
      {results !== null && !loading && (
        <div>
          <div style={{ marginBottom: 24 }}>
            <h2 style={{ fontFamily: "'Cormorant Garamond',Georgia,serif", fontStyle: "italic", fontWeight: 300, fontSize: "clamp(20px,3vw,28px)", color: S.text, marginBottom: 8 }}>
              {label || "Подобранные ароматы"}
            </h2>
            {(matchedNotes.length > 0 || matchedAccords.length > 0) && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: terms.length > 0 ? 8 : 0 }}>
                <span style={{ fontSize: 12, color: S.muted, marginRight: 4, alignSelf: "center" }}>Ноты:</span>
                {matchedNotes.map(n => (
                  <span key={n} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 12, background: "rgba(201,162,94,0.1)", border: "1px solid rgba(201,162,94,0.3)", color: S.accent, letterSpacing: "0.04em" }}>✦ {n}</span>
                ))}
                {matchedAccords.map(a => (
                  <span key={a} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 12, background: "rgba(139,92,246,0.08)", border: "1px solid rgba(139,92,246,0.25)", color: "#a78bfa", letterSpacing: "0.04em" }}>{a}</span>
                ))}
              </div>
            )}
            {terms.length > 0 && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                <span style={{ fontSize: 12, color: S.muted, marginRight: 4 }}>По запросам:</span>
                {terms.map(t => (
                  <span key={t} style={{ fontSize: 11, padding: "3px 10px", borderRadius: 12, background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.2)", color: S.accent, letterSpacing: "0.04em" }}>{t}</span>
                ))}
              </div>
            )}
          </div>

          {results.length === 0 ? (
            <div style={{ textAlign: "center", padding: "48px 24px", color: S.muted }}>
              <div style={{ fontSize: 40, marginBottom: 16 }}>🔍</div>
              <p style={{ fontSize: 15 }}>По вашему описанию ничего не нашлось.<br />Попробуйте другие слова.</p>
              <button onClick={() => { setResults(null); setQuery(""); textareaRef.current?.focus(); }}
                style={{ marginTop: 20, background: S.accent, color: "#09090b", border: "none", borderRadius: 8, padding: "10px 22px", cursor: "pointer", fontWeight: 600, fontSize: 13, fontFamily: "inherit" }}>
                Попробовать снова
              </button>
            </div>
          ) : (
            <>
              <div className="product-grid">
                {results.map(p => <ResultCard key={p.offerId} product={p} />)}
              </div>
              <div style={{ marginTop: 32, textAlign: "center" }}>
                <Link href="/catalog" style={{ display: "inline-flex", alignItems: "center", gap: 8, color: S.accent, textDecoration: "none", fontSize: 14, fontWeight: 500 }}>
                  Смотреть весь каталог <ArrowRight size={16} />
                </Link>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
