"use client";
import { useState } from "react";
import Link from "next/link";
import { Send, Sparkles } from "lucide-react";

const BASE = process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru";

interface Suggestion {
  name: string;
  brand: string;
  reason: string;
  offerId?: string;
}

interface AiResult {
  text?: string;
  suggestions?: Suggestion[];
}

const PROMPTS = [
  "Хочу что-то свежее и лёгкое для лета",
  "Что-то тёплое и древесное на осень",
  "Ищу парфюм для офиса, ненавязчивый",
  "Хочу что-то статусное и дорогое на вечер",
];

const S = {
  border: "rgba(255,252,245,0.08)",
  accent: "#C9A96E",
  muted: "rgba(244,239,230,0.48)",
  text: "#F4EFE6",
  surface2: "#1D1C18",
};

export default function AiFinderClient() {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<AiResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function submit(q: string) {
    if (!q.trim()) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const res = await fetch(`${BASE}/api/shop/ai-finder`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: q }),
      });
      if (!res.ok) throw new Error(`Ошибка ${res.status}`);
      const data = await res.json();
      setResult(data);
    } catch {
      setError("Не удалось получить рекомендации. Попробуйте ещё раз.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      {/* Quick prompts */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 20 }}>
        {PROMPTS.map(p => (
          <button key={p} onClick={() => { setQuery(p); submit(p); }} style={{ padding: "8px 16px", border: `1px solid ${S.border}`, borderRadius: 20, fontSize: 12, color: S.muted, background: "none", cursor: "pointer", transition: "border-color 0.2s,color 0.2s" }}
            onMouseEnter={e => { const el = e.currentTarget; el.style.borderColor = "rgba(201,162,94,0.4)"; el.style.color = S.accent; }}
            onMouseLeave={e => { const el = e.currentTarget; el.style.borderColor = S.border; el.style.color = S.muted; }}>
            {p}
          </button>
        ))}
      </div>

      {/* Input */}
      <div style={{ display: "flex", gap: 8, marginBottom: 32 }}>
        <input value={query} onChange={e => setQuery(e.target.value)} onKeyDown={e => e.key === "Enter" && submit(query)} placeholder="Опишите, какой аромат вы ищете..." style={{ flex: 1, background: S.surface2, border: `1px solid rgba(255,255,255,0.12)`, borderRadius: 10, padding: "12px 16px", color: S.text, fontSize: 14, outline: "none" }} />
        <button onClick={() => submit(query)} disabled={loading || !query.trim()} style={{ width: 48, height: 48, background: query.trim() ? "#f2efe6" : "rgba(255,255,255,0.06)", border: "none", borderRadius: 10, cursor: query.trim() ? "pointer" : "not-allowed", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
          <Send size={16} style={{ color: query.trim() ? "#14120f" : "rgba(255,255,255,0.2)" }} />
        </button>
      </div>

      {/* Loading */}
      {loading && (
        <div style={{ textAlign: "center", padding: "48px 0", color: S.muted, fontSize: 14 }}>
          <Sparkles size={32} style={{ color: S.accent, display: "block", margin: "0 auto 16px" }} />
          Подбираем ароматы...
        </div>
      )}

      {/* Error */}
      {error && <div style={{ textAlign: "center", color: "#fca5a5", padding: "24px 0", fontSize: 14 }}>{error}</div>}

      {/* Result */}
      {result && (
        <div>
          {result.text && <p style={{ fontSize: 14, color: S.muted, lineHeight: 1.8, marginBottom: 28, whiteSpace: "pre-wrap" }}>{result.text}</p>}
          {result.suggestions && result.suggestions.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              {result.suggestions.map((s, i) => (
                <div key={i} style={{ padding: "20px 24px", border: `1px solid ${S.border}`, borderRadius: 14, background: "rgba(255,255,255,0.02)", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, flexWrap: "wrap" }}>
                  <div style={{ flex: 1 }}>
                    <div style={{ fontSize: 10, letterSpacing: "0.2em", color: S.accent, marginBottom: 4 }}>{s.brand}</div>
                    <div style={{ fontSize: 15, color: S.text, marginBottom: 8 }}>{s.name}</div>
                    <div style={{ fontSize: 13, color: S.muted, lineHeight: 1.6 }}>{s.reason}</div>
                  </div>
                  {s.offerId && (
                    <Link href={`/product/${encodeURIComponent(s.offerId)}`} style={{ padding: "8px 16px", border: `1px solid rgba(201,162,94,0.3)`, borderRadius: 8, fontSize: 11, letterSpacing: "0.1em", color: S.accent, textDecoration: "none", flexShrink: 0 }}>
                      Смотреть →
                    </Link>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
