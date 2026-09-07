import { useState, useRef } from "react";
import { Link } from "react-router-dom";
import { Search, Sparkles, ShoppingBag, ArrowRight } from "lucide-react";
import { useCart } from "../CartContext";
import type { ShopProduct } from "../types";
import { api } from "../api";

const S = {
  bg:      "#09090b",
  surface: "#111113",
  border:  "rgba(255,255,255,0.07)",
  borderGold: "rgba(201,162,94,0.3)",
  text:    "#f2ede6",
  muted:   "rgba(242,237,230,0.5)",
  accent:  "#c9a25e",
};

const EXAMPLES = [
  "Тёплый, как ваниль, но не приторный",
  "Свежий морской аромат для офиса",
  "Восточный мускусный на вечер",
  "Цветочный нежный, женственный, весенний",
  "Брутальный и стойкий, дерево и кожа",
  "Лёгкий цитрусовый для повседневного",
];

type Result = ShopProduct & { _matchTerm?: string };

export default function FindPage() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<Result[] | null>(null);
  const [label, setLabel] = useState("");
  const [terms, setTerms] = useState<string[]>([]);
  const [matchedNotes, setMatchedNotes] = useState<string[]>([]);
  const [matchedAccords, setMatchedAccords] = useState<string[]>([]);
  const [error, setError] = useState("");
  const { add } = useCart();
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  async function search(q = query) {
    const trimmed = q.trim();
    if (!trimmed || loading) return;
    setLoading(true);
    setError("");
    setResults(null);
    setTerms([]);
    setMatchedNotes([]);
    setMatchedAccords([]);
    setLabel("");
    try {
      const data = await api.aiSearch(trimmed);
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
    <div style={{ background: S.bg, minHeight: "100vh", paddingBottom: 64 }}>
      {/* Hero */}
      <div style={{
        maxWidth: 780, margin: "0 auto",
        padding: "clamp(48px,8vw,96px) clamp(20px,5vw,40px) clamp(32px,5vw,56px)",
        textAlign: "center",
      }}>
        <div style={{
          display: "inline-flex", alignItems: "center", gap: 8,
          background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.25)",
          borderRadius: 20, padding: "5px 14px", marginBottom: 24,
        }}>
          <Sparkles size={12} style={{ color: S.accent }} />
          <span style={{ fontSize: 11, letterSpacing: "0.15em", color: S.accent, textTransform: "uppercase" }}>
            AI-подбор
          </span>
        </div>

        <h1 style={{
          fontFamily: "'Cormorant Garamond', Georgia, serif",
          fontStyle: "italic", fontWeight: 300,
          fontSize: "clamp(36px,6vw,64px)",
          color: S.text, lineHeight: 1.05, marginBottom: 16,
        }}>
          Опишите<br />свой аромат
        </h1>
        <p style={{ fontSize: "clamp(13px,1.5vw,16px)", color: S.muted, lineHeight: 1.7, maxWidth: "42ch", margin: "0 auto 36px" }}>
          Расскажите словами — тёплый, свежий, древесный, восточный, для вечера… ИИ найдёт подходящие ароматы из нашего каталога.
        </p>

        {/* Search box */}
        <div style={{
          background: S.surface, border: `1px solid ${S.borderGold}`,
          borderRadius: 12, overflow: "hidden",
          boxShadow: "0 0 0 4px rgba(201,162,94,0.04), 0 8px 32px rgba(0,0,0,0.4)",
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
              color: S.text, fontSize: 15, lineHeight: 1.6,
              fontFamily: "inherit",
            }}
          />
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 16px 16px" }}>
            <span style={{ fontSize: 11, color: S.muted }}>Ctrl+Enter для поиска</span>
            <button
              onClick={() => search()}
              disabled={loading || !query.trim()}
              style={{
                display: "flex", alignItems: "center", gap: 8,
                background: query.trim() && !loading ? S.accent : "rgba(201,162,94,0.2)",
                color: query.trim() && !loading ? "#09090b" : S.muted,
                border: "none", borderRadius: 8, cursor: query.trim() ? "pointer" : "not-allowed",
                padding: "10px 22px", fontWeight: 600, fontSize: 13, letterSpacing: "0.04em",
                transition: "all 0.2s",
              }}
            >
              {loading ? (
                <>
                  <div style={{ width: 14, height: 14, border: `2px solid currentColor`, borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
                  Подбираю…
                </>
              ) : (
                <><Search size={14} /> Найти аромат</>
              )}
            </button>
          </div>
        </div>

        {/* Example chips */}
        {!results && !loading && (
          <div style={{ marginTop: 20, display: "flex", flexWrap: "wrap", gap: 8, justifyContent: "center" }}>
            {EXAMPLES.map(ex => (
              <button
                key={ex}
                onClick={() => { setQuery(ex); search(ex); }}
                style={{
                  background: "rgba(255,255,255,0.04)", border: "1px solid rgba(255,255,255,0.08)",
                  borderRadius: 20, padding: "6px 14px", cursor: "pointer",
                  fontSize: 12, color: S.muted, transition: "all 0.15s",
                }}
                onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = S.accent + "55"; (e.currentTarget as HTMLElement).style.color = S.text; }}
                onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.08)"; (e.currentTarget as HTMLElement).style.color = S.muted; }}
              >
                {ex}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Loading */}
      {loading && (
        <div style={{ textAlign: "center", padding: "32px 24px" }}>
          <div style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 16 }}>
            <div style={{
              width: 48, height: 48, border: `2px solid rgba(201,162,94,0.2)`,
              borderTopColor: S.accent, borderRadius: "50%",
              animation: "spin 1s linear infinite",
            }} />
            <p style={{ fontSize: 14, color: S.muted }}>ИИ анализирует описание и ищет подходящие ароматы…</p>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div style={{ maxWidth: 560, margin: "0 auto", padding: "0 24px" }}>
          <div style={{ background: "rgba(248,113,113,0.06)", border: "1px solid rgba(248,113,113,0.2)", borderRadius: 8, padding: "14px 18px", fontSize: 13, color: "#f87171" }}>
            {error}
          </div>
        </div>
      )}

      {/* Results */}
      {results !== null && !loading && (
        <div style={{ maxWidth: 1100, margin: "0 auto", padding: "0 clamp(16px,4vw,32px)" }}>
          {/* Result header */}
          <div style={{ marginBottom: 24 }}>
            <h2 style={{
              fontFamily: "'Cormorant Garamond', Georgia, serif", fontStyle: "italic", fontWeight: 300,
              fontSize: "clamp(22px,3vw,32px)", color: S.text, marginBottom: 8,
            }}>
              {label || "Подобранные ароматы"}
            </h2>
            {(matchedNotes.length > 0 || matchedAccords.length > 0) && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: terms.length > 0 ? 8 : 0 }}>
                <span style={{ fontSize: 12, color: S.muted, marginRight: 4, alignSelf: "center" }}>Ноты:</span>
                {matchedNotes.map(n => (
                  <span key={n} style={{
                    fontSize: 11, padding: "3px 10px", borderRadius: 12,
                    background: "rgba(201,162,94,0.1)", border: "1px solid rgba(201,162,94,0.3)",
                    color: S.accent, letterSpacing: "0.04em",
                  }}>✦ {n}</span>
                ))}
                {matchedAccords.map(a => (
                  <span key={a} style={{
                    fontSize: 11, padding: "3px 10px", borderRadius: 12,
                    background: "rgba(139,92,246,0.08)", border: "1px solid rgba(139,92,246,0.25)",
                    color: "#a78bfa", letterSpacing: "0.04em",
                  }}>{a}</span>
                ))}
              </div>
            )}
            {terms.length > 0 && (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                <span style={{ fontSize: 12, color: S.muted, marginRight: 4 }}>По запросам:</span>
                {terms.map(t => (
                  <span key={t} style={{
                    fontSize: 11, padding: "3px 10px", borderRadius: 12,
                    background: "rgba(201,162,94,0.08)", border: "1px solid rgba(201,162,94,0.2)",
                    color: S.accent, letterSpacing: "0.04em",
                  }}>{t}</span>
                ))}
              </div>
            )}
          </div>

          {results.length === 0 ? (
            <div style={{ textAlign: "center", padding: "48px 24px", color: S.muted }}>
              <div style={{ fontSize: 40, marginBottom: 16 }}>🔍</div>
              <p style={{ fontSize: 15 }}>По вашему описанию ничего не нашлось.<br />Попробуйте другие слова.</p>
              <button onClick={() => { setResults(null); setQuery(""); textareaRef.current?.focus(); }}
                style={{ marginTop: 20, background: S.accent, color: "#09090b", border: "none", borderRadius: 8, padding: "10px 22px", cursor: "pointer", fontWeight: 600, fontSize: 13 }}>
                Попробовать снова
              </button>
            </div>
          ) : (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: 16 }}>
              {results.map(p => (
                <ProductCard key={p.id} product={p} onAdd={() => add(p, 1)} />
              ))}
            </div>
          )}

          {results.length > 0 && (
            <div style={{ marginTop: 32, textAlign: "center" }}>
              <Link to="/catalog" style={{
                display: "inline-flex", alignItems: "center", gap: 8,
                color: S.accent, textDecoration: "none", fontSize: 14, fontWeight: 500,
              }}>
                Смотреть весь каталог <ArrowRight size={16} />
              </Link>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ProductCard({ product: p, onAdd }: { product: Result; onAdd: () => void }) {
  const [added, setAdded] = useState(false);

  function handleAdd(e: React.MouseEvent) {
    e.preventDefault();
    onAdd();
    setAdded(true);
    setTimeout(() => setAdded(false), 1800);
  }

  return (
    <Link to={`/product/${encodeURIComponent(p.offerId)}`} style={{ textDecoration: "none" }}>
      <div className="product-card" style={{
        background: "rgba(17,17,19,0.9)",
        border: "1px solid rgba(255,255,255,0.07)",
        borderRadius: 12, overflow: "hidden",
        display: "flex", flexDirection: "column",
        transition: "border-color 0.3s, transform 0.3s",
        cursor: "pointer",
      }}
        onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(201,162,94,0.4)"; (e.currentTarget as HTMLElement).style.transform = "translateY(-4px)"; }}
        onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "rgba(255,255,255,0.07)"; (e.currentTarget as HTMLElement).style.transform = "translateY(0)"; }}
      >
        {/* Image */}
        <div style={{ aspectRatio: "1", background: "#161512", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }}>
          {p.images[0]
            ? <img src={p.images[0]} alt={p.name} style={{ width: "100%", height: "100%", objectFit: "contain" }} />
            : <span style={{ fontSize: 40, color: "rgba(242,237,230,0.1)" }}>{p.brand?.[0] ?? "?"}</span>
          }
        </div>

        {/* Info */}
        <div style={{ padding: "14px 16px", display: "flex", flexDirection: "column", gap: 6, flex: 1 }}>
          {p.brand && (
            <div style={{ fontSize: 10, fontWeight: 700, letterSpacing: "0.12em", color: "#c9a25e", textTransform: "uppercase" }}>{p.brand}</div>
          )}
          <div style={{ fontSize: 13, fontWeight: 600, color: "#f2ede6", lineHeight: 1.3 }}>{p.name}</div>
          {p.volume && <div style={{ fontSize: 11, color: "rgba(242,237,230,0.4)" }}>{p.volume}</div>}

          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: "auto", paddingTop: 10 }}>
            <span style={{ fontSize: 16, fontWeight: 700, color: "#f2ede6" }}>
              {p.priceRub.toLocaleString("ru-RU")} ₽
            </span>
            {p.inStock && (
              <button
                onClick={handleAdd}
                style={{
                  background: added ? "rgba(74,222,128,0.15)" : "rgba(201,162,94,0.12)",
                  border: `1px solid ${added ? "rgba(74,222,128,0.3)" : "rgba(201,162,94,0.25)"}`,
                  color: added ? "#4ade80" : "#c9a25e",
                  borderRadius: 8, padding: "7px 12px",
                  cursor: "pointer", fontSize: 12, fontWeight: 600,
                  display: "flex", alignItems: "center", gap: 5,
                  transition: "all 0.2s",
                }}
              >
                <ShoppingBag size={13} />
                {added ? "Добавлено" : "В корзину"}
              </button>
            )}
          </div>
        </div>
      </div>
    </Link>
  );
}
