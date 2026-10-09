"use client";
import { useEffect, useState } from "react";
import { Check, Copy, ExternalLink, Loader2, Search, X } from "lucide-react";
import type { ShopProduct } from "@/lib/types";
import { productImg } from "@/lib/img";
import { buyPath, CHANNEL_LABEL, type BuyChannel, type BuyCompare } from "@/lib/buy";

const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";
const SITE = "https://magicvibes.ru";
const UTM = "?utm_source=telegram&utm_medium=post";
const rub = (n: number) => `${Math.round(n).toLocaleString("ru-RU")} ₽`;

function CopyRow({ label, value }: { label: string; value: string }) {
  const [done, setDone] = useState(false);
  return (
    <div className="lk-row">
      <span className="lk-label">{label}</span>
      <div className="lk-value">
        <code>{value}</code>
        <button className="st-btn" onClick={async () => { await navigator.clipboard.writeText(value); setDone(true); setTimeout(() => setDone(false), 1500); }}>
          {done ? <Check size={15} /> : <Copy size={15} />} {done ? "Скопировано" : "Копировать"}
        </button>
        <a className="st-btn ghost" href={value} target="_blank" rel="noopener noreferrer" aria-label="Открыть"><ExternalLink size={15} /></a>
      </div>
    </div>
  );
}

export default function LinkStudio({ modeSwitch }: { modeSwitch: React.ReactNode }) {
  const [q, setQ] = useState("");
  const [results, setResults] = useState<ShopProduct[]>([]);
  const [searching, setSearching] = useState(false);
  const [picked, setPicked] = useState<ShopProduct | null>(null);
  const [compare, setCompare] = useState<BuyCompare | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const term = q.trim();
    if (term.length < 2) { setResults([]); return; }
    const id = setTimeout(async () => {
      setSearching(true);
      try {
        const r = await fetch(`${API}/catalog?pageSize=12&q=${encodeURIComponent(term)}`);
        setResults((await r.json()).products || []);
      } catch { setResults([]); }
      setSearching(false);
    }, 300);
    return () => clearTimeout(id);
  }, [q]);

  async function pick(p: ShopProduct) {
    setPicked(p); setCompare(null); setLoading(true);
    try {
      const r = await fetch(`${API}/compare/${encodeURIComponent(p.offerId)}`);
      if (r.ok) setCompare(await r.json());
    } catch { /* prices are optional here */ }
    setLoading(false);
  }

  const generalLink = `${SITE}/buy${UTM}`;
  const productLink = picked ? `${SITE}${buyPath(picked)}${UTM}` : "";

  const offers = compare ? (["site", "yandex", "ozon"] as BuyChannel[]).map((c) => ({ c, o: compare.offers[c] })) : [];
  const inStockPrices = offers.filter((x) => x.o?.inStock && x.o.price).map((x) => x.o!.price!);
  const min = inStockPrices.length ? Math.min(...inStockPrices) : null;

  return (
    <div className="st-root">
      <aside className="st-panel">
        <div className="st-panel-h"><b>Ссылки для Telegram</b><span>«Купить» · сравнение цен</span></div>
        {modeSwitch}

        <div className="st-group">
          <div className="st-label">Общая ссылка — сайт, Яндекс Маркет, Ozon</div>
          <CopyRow label="Страница «Где купить»" value={generalLink} />
        </div>

        <div className="st-group">
          <div className="st-label">Ссылка на товар — со сравнением цен</div>
          <div className="st-search">
            <Search size={16} />
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Бренд или название, напр. Kilian" />
            {searching ? <Loader2 size={16} className="mv-spin" /> : q && <button onClick={() => setQ("")} aria-label="Очистить"><X size={15} /></button>}
          </div>
          {results.length > 0 && (
            <div className="st-results">
              {results.map((p) => (
                <button key={p.id} onClick={() => pick(p)} className={picked?.id === p.id ? "on" : ""}>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img src={productImg(p.images?.[0], 160)} alt="" />
                  <span><b>{p.brand}</b>{p.name}</span>
                  <em>{rub(p.priceRub)}</em>
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="st-group">
          <div className="st-label">Как вставить в пост</div>
          <ol className="lk-help">
            <li>Скопируйте ссылку.</li>
            <li>В Telegram напишите слово, например <b>Купить</b> или <b>Где купить выгоднее</b>, и выделите его.</li>
            <li>Нажмите <kbd>Ctrl</kbd>+<kbd>K</kbd> (на телефоне: ⋮ → «Добавить ссылку») и вставьте.</li>
          </ol>
        </div>
      </aside>

      <main className="st-stage lk-stage">
        {!picked ? (
          <div className="lk-empty">
            <b>Найдите товар слева</b>
            <span>Здесь появится ссылка на страницу со сравнением цен на сайте, Яндекс Маркете и Ozon.</span>
          </div>
        ) : (
          <div className="lk-card">
            <div className="lk-product">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={productImg(picked.images?.[0], 240)} alt="" />
              <div><span>{picked.brand}</span><b>{picked.name}</b></div>
            </div>
            <CopyRow label="Ссылка на товар" value={productLink} />
            <div className="lk-prices">
              {loading && <div className="lk-muted"><Loader2 size={15} className="mv-spin" /> Загружаю цены…</div>}
              {!loading && !compare && <div className="lk-muted">Цены площадок не загрузились — ссылка всё равно работает.</div>}
              {offers.map(({ c, o }) => (
                <div key={c} className={`lk-price ${o?.inStock && o.price && inStockPrices.length > 1 && o.price === min ? "best" : ""}`}>
                  <span>{CHANNEL_LABEL[c]}</span>
                  <b>{!o ? "нет на площадке" : o.price ? rub(o.price) : "цена на площадке"}</b>
                  <em>{!o ? "→ ссылка на магазин" : !o.inStock ? "нет в наличии" : o.price && inStockPrices.length > 1 && o.price === min ? "Выгоднее" : "в наличии"}</em>
                </div>
              ))}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
