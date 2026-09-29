"use client";
import { useState, useRef, useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Search, ShoppingBag, Check, ArrowRight, Sparkles, Plus, RotateCcw } from "lucide-react";
import { productImg } from "@/lib/img";
import { useCart } from "@/components/CartContext";
import { aiSearch, type AiSearchResult, type AiProduct } from "@/lib/client";
import { toProductSlug } from "@/lib/slug";
import { ymGoal } from "@/lib/metrika";

const EXAMPLES = [
  "Похожий на Baccarat Rouge 540, но дешевле",
  "Тёплый, как ваниль, но не приторный",
  "Свежий морской для офиса, мужской, до 10 000",
  "Подарок маме до 7000 ₽",
  "Без цветов: дерево и кожа, стойкий",
  "Лёгкий чистый, как свежее бельё",
  "Арабский уд на зимние вечера",
  "Пробники нишевой парфюмерии",
];

const CSS = `
.aif-box { background: var(--surface); border: 1.5px solid rgba(var(--ink-rgb),.1); border-radius: 26px; box-shadow: 0 18px 50px rgba(var(--ink-rgb),.08); overflow: hidden; transition: border-color .2s, box-shadow .2s; }
.aif-box:focus-within { border-color: var(--accent); box-shadow: 0 0 0 4px rgba(var(--accent-rgb),.12), 0 18px 50px rgba(var(--ink-rgb),.08); }
.aif-box textarea { width: 100%; border: 0; outline: 0; resize: none; background: transparent; padding: 20px 22px 6px; font: inherit; font-size: 16px; line-height: 1.55; color: var(--ink); box-sizing: border-box; }
.aif-bar { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 8px 12px 12px 22px; }
.aif-hint { font-size: 12px; color: var(--subtle); }
.aif-go { display: inline-flex; align-items: center; gap: 8px; border: 0; border-radius: 999px; padding: 12px 22px; font: inherit; font-weight: 700; font-size: 14px; background: var(--ink); color: var(--lime); cursor: pointer; transition: transform .15s, opacity .15s; }
.aif-go:disabled { opacity: .35; cursor: not-allowed; }
.aif-go:not(:disabled):hover { transform: translateY(-1px); }
.aif-chips { display: flex; flex-wrap: wrap; gap: 8px; }
.aif-chip { border: 1px solid rgba(var(--ink-rgb),.1); background: var(--surface); color: var(--ink); border-radius: 999px; padding: 8px 14px; font: inherit; font-size: 13px; cursor: pointer; transition: border-color .15s, background .15s; }
.aif-chip:hover { border-color: var(--ink); }
.aif-chip.add { display: inline-flex; align-items: center; gap: 6px; background: rgba(var(--accent-rgb),.06); border-color: rgba(var(--accent-rgb),.25); color: var(--accent2); font-weight: 600; }
.aif-tag { display: inline-flex; align-items: center; border-radius: 999px; padding: 5px 11px; font-size: 12.5px; font-weight: 600; background: rgba(var(--ink-rgb),.06); color: var(--ink); }
.aif-tag.ref { background: var(--ink); color: var(--lime); }
.aif-tag.note { background: rgba(var(--accent-rgb),.1); color: var(--accent2); }
.aif-tag.mood { background: #ffe8f0; color: #c2185b; }
.aif-tag.neg { background: rgba(var(--danger-rgb),.08); color: var(--danger); text-decoration: line-through; text-decoration-thickness: 1px; }
.aif-tag.price, .aif-tag.gender, .aif-tag.brand { background: #eef9c8; color: #3d4a00; }
.aif-head { display: flex; flex-direction: column; gap: 12px; margin: 34px 0 18px; }
.aif-title { font-family: var(--font-display); font-weight: 800; text-transform: uppercase; letter-spacing: -.02em; line-height: 1; font-size: clamp(24px, 4vw, 38px); margin: 0; }
.aif-ref { display: flex; align-items: center; gap: 14px; background: var(--surface); border-radius: 20px; padding: 12px 16px 12px 12px; text-decoration: none; color: var(--ink); border: 1px solid rgba(var(--ink-rgb),.08); }
.aif-ref img { width: 64px; height: 64px; object-fit: contain; border-radius: 12px; background: #fff; }
.aif-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }
@media (min-width: 640px) { .aif-grid { grid-template-columns: repeat(3, 1fr); gap: 16px; } }
.aif-card { position: relative; display: flex; flex-direction: column; background: var(--surface); border-radius: 22px; overflow: hidden; text-decoration: none; color: var(--ink); border: 1px solid rgba(var(--ink-rgb),.06); transition: transform .2s, box-shadow .2s; }
.aif-card:hover { transform: translateY(-3px); box-shadow: 0 14px 34px rgba(var(--ink-rgb),.1); }
.aif-img { aspect-ratio: 1; background: #fff; display: flex; align-items: center; justify-content: center; padding: 14px; }
.aif-img img { width: 100%; height: 100%; object-fit: contain; }
.aif-match { position: absolute; top: 10px; left: 10px; background: var(--ink); color: var(--lime); font-weight: 800; font-size: 12px; border-radius: 999px; padding: 4px 9px; }
.aif-body { padding: 12px 14px 14px; display: flex; flex-direction: column; gap: 6px; flex: 1; }
.aif-brand { font-size: 10.5px; font-weight: 800; letter-spacing: .1em; text-transform: uppercase; color: var(--accent); }
.aif-name { font-size: 13px; font-weight: 600; line-height: 1.3; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
.aif-why { font-size: 12px; line-height: 1.4; color: var(--muted); }
.aif-why b { color: var(--ink); font-weight: 600; }
.aif-foot { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-top: auto; padding-top: 8px; }
.aif-price { font-weight: 800; font-size: 16px; }
.aif-add { display: inline-flex; align-items: center; gap: 5px; border: 0; border-radius: 999px; padding: 8px 11px; font: inherit; font-size: 12px; font-weight: 700; cursor: pointer; background: rgba(var(--accent-rgb),.1); color: var(--accent2); }
.aif-add.ok { background: rgba(var(--success-rgb),.14); color: var(--success); }
.aif-skel { border-radius: 22px; background: linear-gradient(90deg, rgba(var(--ink-rgb),.05), rgba(var(--ink-rgb),.1), rgba(var(--ink-rgb),.05)); background-size: 200% 100%; animation: aifsk 1.2s linear infinite; aspect-ratio: .72; }
@keyframes aifsk { to { background-position: -200% 0; } }
.aif-note { font-size: 13px; color: var(--muted); background: #fff7d6; border-radius: 14px; padding: 10px 14px; }
@media (max-width: 559px) { .aif-hint { display: none; } .aif-body { padding: 10px 11px 12px; } .aif-why { font-size: 11.5px; } }
`;

function ResultCard({ p }: { p: AiProduct }) {
  const { add } = useCart();
  const [added, setAdded] = useState(false);
  const why = p._why || [];
  return (
    <Link href={`/product/${toProductSlug(p.name, p.offerId)}`} className="aif-card" onClick={() => ymGoal("ai_click", { offerId: p.offerId })}>
      {typeof p._match === "number" && <span className="aif-match">{p._match}%</span>}
      <div className="aif-img">
        {p.images?.[0]
          // eslint-disable-next-line @next/next/no-img-element
          ? <img src={productImg(p.images[0], 360)} alt={p.name} loading="lazy" />
          : <span style={{ fontSize: 40, color: "rgba(var(--ink-rgb),.1)" }}>{p.brand?.[0] ?? "?"}</span>}
      </div>
      <div className="aif-body">
        {p.brand && <div className="aif-brand">{p.brand}</div>}
        <div className="aif-name">{p.name}</div>
        {why[0] && <div className="aif-why">{/^как в /.test(why[0]) ? <><b>{why[0].split(":")[0]}:</b>{why[0].slice(why[0].indexOf(":") + 1)}</> : <><b>Совпадает:</b> {why[0]}</>}</div>}
        {why[1] && <div className="aif-why">{why[1]}</div>}
        {!why.length && p._notes?.length ? <div className="aif-why">Ноты: {p._notes.slice(0, 3).join(", ")}</div> : null}
        <div className="aif-foot">
          <span className="aif-price">{p.priceRub.toLocaleString("ru-RU")} ₽</span>
          <button type="button" className={`aif-add${added ? " ok" : ""}`} aria-label="В корзину"
            onClick={(e) => { e.preventDefault(); add(p as Parameters<typeof add>[0], 1); setAdded(true); setTimeout(() => setAdded(false), 1800); }}>
            {added ? <><Check size={13} />В корзине</> : <><ShoppingBag size={13} />В корзину</>}
          </button>
        </div>
      </div>
    </Link>
  );
}

export default function AiFinderClient() {
  const router = useRouter();
  const params = useSearchParams();
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<AiSearchResult | null>(null);
  const [error, setError] = useState("");
  const areaRef = useRef<HTMLTextAreaElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

  async function search(q = query, push = true) {
    const trimmed = q.trim();
    if (!trimmed || loading) return;
    setQuery(trimmed);
    setLoading(true); setError("");
    if (push) router.replace(`/find?q=${encodeURIComponent(trimmed)}`, { scroll: false });
    try {
      const d = await aiSearch(trimmed);
      setData(d);
      ymGoal("ai_search");
      requestAnimationFrame(() => resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Не получилось подобрать — попробуйте ещё раз");
    } finally {
      setLoading(false);
    }
  }

  // shared link /find?q=… opens with results
  useEffect(() => {
    const q = params.get("q");
    if (q && !data && !loading) { setQuery(q); void search(q, false); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const refine = (r: string) => search(`${query.replace(/[.\s]+$/, "")}, ${r}`);
  const reset = () => { setData(null); setQuery(""); router.replace("/find", { scroll: false }); areaRef.current?.focus(); };

  return (
    <div>
      <style>{CSS}</style>
      <form className="aif-box" onSubmit={(e) => { e.preventDefault(); void search(); }}>
        <textarea
          ref={areaRef} rows={3} value={query} maxLength={400}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); void search(); } }}
          placeholder="Опишите, какой аромат ищете: ноты, настроение, для кого, бюджет или «похожий на…»"
          aria-label="Опишите желаемый аромат"
        />
        <div className="aif-bar">
          <span className="aif-hint">Enter — найти · Shift+Enter — новая строка</span>
          <button type="submit" className="aif-go" disabled={loading || !query.trim()}>
            {loading ? <><Sparkles size={15} className="mv-spin" />Подбираю…</> : <><Search size={15} />Подобрать</>}
          </button>
        </div>
      </form>

      {!data && !loading && (
        <div style={{ marginTop: 18 }}>
          <div style={{ fontSize: 13, color: "var(--muted)", marginBottom: 10 }}>Например:</div>
          <div className="aif-chips">
            {EXAMPLES.map((ex) => <button key={ex} type="button" className="aif-chip" onClick={() => search(ex)}>{ex}</button>)}
          </div>
        </div>
      )}

      {error && <div className="aif-note" style={{ marginTop: 18, background: "rgba(var(--danger-rgb),.08)", color: "var(--danger)" }}>{error}</div>}

      <div ref={resultsRef} style={{ scrollMarginTop: 90 }}>
        {loading && (
          <div className="aif-grid" style={{ marginTop: 34 }}>{Array.from({ length: 6 }, (_, i) => <div key={i} className="aif-skel" />)}</div>
        )}

        {data && !loading && (
          <>
            <div className="aif-head">
              <h2 className="aif-title">{data.label || "Подборка ароматов"}</h2>
              {!!data.understood?.length && (
                <div className="aif-chips" aria-label="Как мы поняли запрос">
                  {data.understood.map((c, i) => <span key={i} className={`aif-tag ${c.kind}`}>{c.text}</span>)}
                </div>
              )}
              {data.reference && (
                <Link className="aif-ref" href={`/product/${toProductSlug(data.reference.name, data.reference.offerId)}`}>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  {data.reference.images?.[0] && <img src={productImg(data.reference.images[0], 160)} alt="" />}
                  <span style={{ flex: 1, minWidth: 0 }}>
                    <span style={{ display: "block", fontSize: 12, color: "var(--muted)" }}>Ищем похожие на</span>
                    <b style={{ fontSize: 14 }}>{data.reference.brand} {data.reference.short}</b>
                  </span>
                  <span style={{ fontWeight: 800, whiteSpace: "nowrap" }}>{data.reference.priceRub.toLocaleString("ru-RU")} ₽</span>
                </Link>
              )}
              {data.relaxed && <div className="aif-note">Точных совпадений не нашлось — показываем самые близкие варианты без части условий.</div>}
            </div>

            {data.products.length ? (
              <div className="aif-grid">{data.products.map((p) => <ResultCard key={p.offerId} p={p} />)}</div>
            ) : (
              <div className="aif-note">Ничего не нашлось. Попробуйте описать иначе: ноты («ваниль», «кедр»), настроение («свежий», «тёплый») или «похожий на …».</div>
            )}

            <div style={{ marginTop: 26 }}>
              {!!data.refinements?.length && (
                <>
                  <div style={{ fontSize: 13, color: "var(--muted)", marginBottom: 10 }}>Уточнить подборку:</div>
                  <div className="aif-chips">
                    {data.refinements.map((r) => <button key={r} type="button" className="aif-chip add" onClick={() => refine(r)}><Plus size={13} />{r}</button>)}
                    <button type="button" className="aif-chip" onClick={reset}><RotateCcw size={13} style={{ verticalAlign: -2, marginRight: 4 }} />Новый запрос</button>
                  </div>
                </>
              )}
              <div style={{ marginTop: 22, textAlign: "center" }}>
                <Link href="/catalog" style={{ display: "inline-flex", alignItems: "center", gap: 8, color: "var(--accent)", textDecoration: "none", fontSize: 14, fontWeight: 600 }}>
                  Весь каталог <ArrowRight size={16} />
                </Link>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
