"use client";
// Video stories (1080×1920) for Instagram / Telegram / VK: 5 templates, generated royalty-free
// music with cuts on the beat, and a voice-over: the studio writes the narration text for
// Yandex SpeechKit Playground (with pauses sized to the scenes), you paste it there, download the
// MP3 and load it back here; the music ducks under the voice.
import { useEffect, useMemo, useRef, useState } from "react";
import { Check, Copy, Download, Loader2, Mic, Pause, Play, Search, Trash2, Upload, X, ArrowUp, ArrowDown } from "lucide-react";
import type { ShopProduct } from "@/lib/types";
import { productImg } from "@/lib/img";
import { W, H, PALETTES, TEMPLATES, buildTimeline, defaultLines, drawFrame, rub, type Assets, type Cfg, type Item, type TplId } from "./story/scenes";
import { MOODS, renderMusic } from "./story/music";

const API = (process.env.NEXT_PUBLIC_API_BASE ?? "https://davidsklad.ru") + "/api/shop";
const FPS = 30;

function itemFromProduct(p: ShopProduct): Item {
  const name = p.brand && p.name.toUpperCase().startsWith(p.brand.toUpperCase()) ? p.name.slice(p.brand.length).trim() : p.name;
  return { product: p, brand: p.brand || "", name, price: p.priceRub, old: p.oldPriceRub };
}
function pickMime() {
  const c = ["video/mp4;codecs=avc1.640028", "video/mp4;codecs=avc1", "video/mp4", "video/webm;codecs=vp9", "video/webm;codecs=vp8", "video/webm"];
  return c.find((m) => typeof MediaRecorder !== "undefined" && MediaRecorder.isTypeSupported(m)) || "";
}
const TPL_DEFAULTS: Record<TplId, Partial<Cfg>> = {
  top:       { title: "Хиты недели", script: "выбор покупателей", outro: "Заказывай на сайте" },
  spotlight: { title: "Аромат недели", script: "аромат недели", outro: "Успей заказать" },
  sale:      { title: "Распродажа", script: "только на сайте", outro: "Успей до конца акции" },
  grid:      { title: "Новинки", script: "только что на сайте", outro: "Смотри все новинки" },
  brand:     { title: "", script: "в Magic Vibes", outro: "Весь бренд на сайте" },
};

// Russian speech in SpeechKit ≈ 14 characters a second at speed 1.0
const speechSec = (s: string) => s.replace(/sil<\[\d+\]>|<\[\w+\]>/g, "").length / 14;

// voice loudness per 50 ms → where the music should duck
function voiceEnvelope(buf: AudioBuffer) {
  const step = Math.floor(buf.sampleRate * 0.05), d = buf.getChannelData(0), out: number[] = [];
  for (let i = 0; i < d.length; i += step) { let s = 0; for (let j = i; j < Math.min(d.length, i + step); j++) s += d[j] * d[j]; out.push(Math.sqrt(s / step)); }
  return out;
}

export default function StoryVideo({ modeSwitch }: { modeSwitch: React.ReactNode }) {
  const [cfg, setCfg] = useState<Cfg>({
    tpl: "top", title: "Хиты недели", script: "выбор покупателей", outro: "Заказывай на сайте", promo: "VIBES10", site: "magicvibes.ru",
    notes: "роза, личи, мускус", deadline: "до 5 октября", per: 2.8, palette: "brand", prices: true, items: [],
  });
  const [moodId, setMoodId] = useState("pop");
  const [sfx, setSfx] = useState(true);
  const [musicVol, setMusicVol] = useState(0.8);
  const [lines, setLines] = useState<string[] | null>(null);  // null = auto
  const [voice, setVoice] = useState<{ name: string; buf: AudioBuffer; env: number[] } | null>(null);
  const [voiceDelay, setVoiceDelay] = useState(0.2);
  const [voiceVol, setVoiceVol] = useState(1);
  const [q, setQ] = useState("");
  const [results, setResults] = useState<ShopProduct[]>([]);
  const [searching, setSearching] = useState(false);
  const [playing, setPlaying] = useState(false);
  const [time, setTime] = useState(0);
  const [rec, setRec] = useState<{ progress: number } | null>(null);
  const [out, setOut] = useState<{ url: string; ext: string; size: number } | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [musicBuf, setMusicBuf] = useState<AudioBuffer | null>(null);
  const [musicBusy, setMusicBusy] = useState(false);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const assets = useRef<Assets>({ imgs: [] });
  const [assetsVer, setAssetsVer] = useState(0);
  const fileRef = useRef<HTMLInputElement>(null);
  const voiceRef = useRef<HTMLInputElement>(null);

  const mood = MOODS.find((m) => m.id === moodId) || null;
  const tplDef = TEMPLATES.find((t) => t.id === cfg.tpl)!;
  // the list keeps every product; a template only shows as many as it takes (switching back and forth loses nothing)
  const view = useMemo(() => ({ ...cfg, items: cfg.items.slice(0, tplDef.max) }), [cfg, tplDef.max]);
  const tl = useMemo(() => buildTimeline(view, mood?.bpm ?? null), [view, mood]);
  const dur = tl.duration;
  const set = <K extends keyof Cfg>(k: K, v: Cfg[K]) => setCfg((c) => ({ ...c, [k]: v }));
  const autoLines = useMemo(() => defaultLines(view, tl.scenes), [view, tl.scenes]);
  const voLines = lines && lines.length === tl.scenes.length ? lines : autoLines;

  // SpeechKit text: each scene's line, then a pause so the next line starts on the next cut
  const speechkitText = useMemo(() => {
    let cursor = voiceDelay;
    const parts: string[] = [];
    tl.scenes.forEach((s, i) => {
      // the synthesizer reads the Latin brand as «мэджик вибес» — spell it the way we say it
      let line = (voLines[i] || "").trim().replace(/magic\s*vibes/gi, "Маджик Вайбс");
      if (line && !/[.!?…]$/.test(line)) line += ".";
      const gap = Math.round((s.start + 0.15 - cursor) * 1000);
      // always a breath between scenes; longer when the line ran short of its scene
      if (i > 0) { const ms = Math.min(Math.max(gap, 250), 10000); parts.push(`sil<[${ms}]>`); cursor += ms / 1000; }
      if (line) { parts.push(line); cursor += speechSec(line); }
    });
    return parts.join(" ");
  }, [tl.scenes, voLines, voiceDelay]);
  const overflow = tl.scenes.map((s, i) => speechSec(voLines[i] || "") > s.dur - 0.25);

  function pickTemplate(id: TplId) {
    setCfg((c) => ({ ...c, tpl: id, ...TPL_DEFAULTS[id], title: id === "brand" ? (c.items[0]?.brand || "") : TPL_DEFAULTS[id].title!,
    }));
    setLines(null);
  }

  // fonts before the first frame, or the canvas falls back to Arial
  useEffect(() => {
    Promise.all([document.fonts.load("800 100px Unbounded"), document.fonts.load("500 40px Onest"), document.fonts.load("600 40px Onest"),
      document.fonts.load("700 40px Onest"), document.fonts.load('400 80px "Marck Script"')]).then(() => setAssetsVer((v) => v + 1)).catch(() => {});
  }, []);

  // product photos through the same-origin /img proxy → the canvas stays exportable
  const imgKey = cfg.items.map((it) => it.upload || it.product?.images?.[0] || "").join("|");
  useEffect(() => {
    let alive = true;
    const srcs = cfg.items.map((it) => it.upload || productImg(it.product?.images?.[0], 900));
    Promise.all(srcs.map((src) => new Promise<HTMLImageElement | null>((res) => {
      if (!src) return res(null);
      const im = new Image(); im.decoding = "async"; im.onload = () => res(im); im.onerror = () => res(null); im.src = src;
    }))).then((imgs) => { if (alive) { assets.current.imgs = imgs; setAssetsVer((v) => v + 1); } });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [imgKey]);

  // music: re-render when the timeline or mood changes (debounced)
  const cutsKey = tl.scenes.map((s) => s.start.toFixed(3)).join(",");
  useEffect(() => {
    if (!mood) { setMusicBuf(null); return; }
    let alive = true;
    setMusicBusy(true);
    const id = setTimeout(() => {
      renderMusic({ mood, duration: dur, cuts: tl.scenes.map((s) => s.start), sfx })
        .then((b) => { if (alive) setMusicBuf(b); })
        .catch(() => { if (alive) setMusicBuf(null); })
        .finally(() => { if (alive) setMusicBusy(false); });
    }, 350);
    return () => { alive = false; clearTimeout(id); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [moodId, cutsKey, sfx, dur]);

  /* ───── audio graph (preview and recording use the same mix) ───── */
  const acRef = useRef<AudioContext | null>(null);
  const liveRef = useRef<{ stop: () => void } | null>(null);
  function startAudio(ac: AudioContext, dests: AudioNode[], offset: number, at: number) {
    const nodes: AudioScheduledSourceNode[] = [];
    const out = ac.createGain(); dests.forEach((d) => out.connect(d));
    if (musicBuf) {
      const src = ac.createBufferSource(); src.buffer = musicBuf;
      const g = ac.createGain(); g.gain.value = musicVol;
      // duck the music under the voice
      if (voice) {
        const env = voice.env, peak = Math.max(1e-4, ...env);
        for (let i = 0; i < env.length; i++) {
          const tt = voiceDelay + i * 0.05 - offset;
          if (tt < 0) continue;
          const talking = env[i] / peak > 0.08 || (env[i + 1] ?? 0) / peak > 0.08 || (env[i + 2] ?? 0) / peak > 0.08;
          g.gain.setTargetAtTime(talking ? musicVol * 0.28 : musicVol, at + tt, 0.08);
        }
      }
      src.connect(g).connect(out); src.start(at, Math.min(offset, musicBuf.duration)); nodes.push(src);
    }
    if (voice) {
      const src = ac.createBufferSource(); src.buffer = voice.buf;
      const g = ac.createGain(); g.gain.value = voiceVol;
      src.connect(g).connect(out);
      const rel = voiceDelay - offset;
      if (rel >= 0) src.start(at + rel); else if (-rel < voice.buf.duration) src.start(at, -rel);
      nodes.push(src);
    }
    return { stop: () => { nodes.forEach((n) => { try { n.stop(); } catch { /* not started */ } }); out.disconnect(); } };
  }
  const stopLive = () => { liveRef.current?.stop(); liveRef.current = null; };

  // preview: time runs on the audio clock so picture and sound stay together
  const timeRef = useRef(0);
  const clockRef = useRef<{ ac0: number; t0: number } | null>(null);
  useEffect(() => {
    if (rec) return;
    stopLive();
    if (playing) {
      const ac = acRef.current || (acRef.current = new AudioContext());
      ac.resume();
      const at = ac.currentTime + 0.05;
      liveRef.current = startAudio(ac, [ac.destination], timeRef.current, at);
      clockRef.current = { ac0: at, t0: timeRef.current };
    } else clockRef.current = null;
    return stopLive;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [playing, musicBuf, voice, voiceDelay, musicVol, voiceVol, rec]);

  useEffect(() => {
    const ctx = canvasRef.current?.getContext("2d");
    if (!ctx || rec) return;
    let raf = 0;
    const loop = () => {
      const ck = clockRef.current, ac = acRef.current;
      if (playing && ck && ac) {
        const t = ck.t0 + Math.max(0, ac.currentTime - ck.ac0);
        if (t >= dur) { timeRef.current = 0; setPlaying(false); } else timeRef.current = t;
        setTime(timeRef.current);
      }
      drawFrame(ctx, Math.min(timeRef.current, dur - 0.001), view, assets.current, tl.scenes);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [view, playing, dur, rec, assetsVer, tl.scenes]);

  // catalog search
  useEffect(() => {
    const term = q.trim();
    if (term.length < 2) { setResults([]); return; }
    const id = setTimeout(async () => {
      setSearching(true);
      try { const r = await fetch(`${API}/catalog?pageSize=12&inStock=true&q=${encodeURIComponent(term)}`); setResults((await r.json()).products || []); }
      catch { setResults([]); }
      setSearching(false);
    }, 300);
    return () => clearTimeout(id);
  }, [q]);

  async function loadTop() {
    setSearching(true);
    try {
      const r = await fetch(`${API}/popular?limit=8`);
      const items = ((await r.json()).products || []).slice(0, 8).map(itemFromProduct);
      setCfg((c) => ({ ...c, items, ...(c.tpl === "brand" && items[0] ? { title: items[0].brand } : {}) }));
    } catch { /* keep the list */ }
    setSearching(false);
  }
  const addItem = (it: Item) => setCfg((c) => ({ ...c, items: [...c.items, it].slice(-8) }));
  const move = (i: number, d: number) => setCfg((c) => { const a = [...c.items]; const j = i + d; if (j < 0 || j >= a.length) return c; [a[i], a[j]] = [a[j], a[i]]; return { ...c, items: a }; });
  const patchItem = (i: number, patch: Partial<Item>) => setCfg((c) => ({ ...c, items: c.items.map((x, j) => (j === i ? { ...x, ...patch } : x)) }));

  async function loadVoice(f?: File) {
    if (!f) return;
    try {
      const ac = acRef.current || (acRef.current = new AudioContext());
      const buf = await ac.decodeAudioData(await f.arrayBuffer());
      setVoice({ name: f.name, buf, env: voiceEnvelope(buf) });
      if (buf.duration + voiceDelay > dur + 0.5) setErr(`Озвучка длиннее ролика на ${(buf.duration + voiceDelay - dur).toFixed(1)} с — хвост обрежется. Увеличьте «секунд на товар» или сократите текст.`);
      else setErr(null);
    } catch { setErr("Не удалось прочитать аудио — нужен MP3, WAV или OGG"); }
  }

  async function record() {
    const canvas = canvasRef.current, ctx = canvas?.getContext("2d");
    if (!canvas || !ctx) return;
    const mime = pickMime();
    if (!mime) { setErr("Браузер не умеет записывать видео — откройте студию в Chrome или Edge"); return; }
    setErr(null); setOut(null); setPlaying(false); stopLive(); setRec({ progress: 0 });
    const ac = new AudioContext(); await ac.resume();
    const dest = ac.createMediaStreamDestination();
    const stream = canvas.captureStream(FPS);
    dest.stream.getAudioTracks().forEach((tr) => stream.addTrack(tr));
    const chunks: Blob[] = [];
    const mr = new MediaRecorder(stream, { mimeType: mime, videoBitsPerSecond: 14_000_000, audioBitsPerSecond: 192_000 });
    mr.ondataavailable = (e) => { if (e.data.size) chunks.push(e.data); };
    const done = new Promise<void>((res) => { mr.onstop = () => res(); });
    drawFrame(ctx, 0, view, assets.current, tl.scenes);
    mr.start(250);
    const at = ac.currentTime + 0.1;
    const live = startAudio(ac, [dest, ac.destination], 0, at);
    await new Promise<void>((res) => {
      const tick = () => {
        const t = ac.currentTime - at;
        drawFrame(ctx, clamp0(Math.min(t, dur - 0.001)), view, assets.current, tl.scenes);
        setRec({ progress: Math.min(1, Math.max(0, t) / dur) });
        if (t >= dur + 0.2) return res();
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    });
    mr.stop(); await done; live.stop(); ac.close();
    const blob = new Blob(chunks, { type: mime.split(";")[0] });
    setOut({ url: URL.createObjectURL(blob), ext: mime.startsWith("video/mp4") ? "mp4" : "webm", size: blob.size });
    setRec(null); timeRef.current = 0; setTime(0);
  }

  const secs = dur.toFixed(1);
  const needMore = cfg.items.length < tplDef.min;

  return (
    <div className="st-root">
      <aside className="st-panel">
        <div className="st-panel-h"><b>Студия</b><span>Видео-сторис · 1080×1920</span></div>
        {modeSwitch}

        <div className="st-group">
          <div className="st-label">Формат ролика</div>
          <div className="st-chips">
            {TEMPLATES.map((t) => <button key={t.id} className={cfg.tpl === t.id ? "on" : ""} onClick={() => pickTemplate(t.id)}>{t.label}</button>)}
          </div>
          <p className="sv-note">{tplDef.hint}. Товаров: {tplDef.min === tplDef.max ? tplDef.min : `${tplDef.min}–${tplDef.max}`}.</p>
        </div>

        <div className="st-group">
          <div className="st-label">Товары — в ролике {Math.min(cfg.items.length, tplDef.max)} из {cfg.items.length} (формат берёт до {tplDef.max})</div>
          <button className="st-btn ghost" onClick={loadTop} disabled={searching}>✦ Взять популярные с сайта</button>
          <div className="st-search">
            <Search size={16} />
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Найти товар: бренд или название" />
            {searching ? <Loader2 size={16} className="mv-spin" /> : q && <button onClick={() => setQ("")} aria-label="Очистить"><X size={15} /></button>}
          </div>
          {results.length > 0 && (
            <div className="st-results">
              {results.map((p) => (
                <button key={p.id} onClick={() => addItem(itemFromProduct(p))}>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img src={productImg(p.images?.[0], 160)} alt="" />
                  <span><b>{p.brand}</b>{p.name}</span>
                  <em>{rub(p.priceRub)}</em>
                </button>
              ))}
            </div>
          )}
          <div className="sv-items">
            {cfg.items.map((it, i) => (
              <div key={i} className="sv-item">
                <b>{i + 1}</b>
                <div>
                  <input value={it.brand || ""} onChange={(e) => patchItem(i, { brand: e.target.value })} placeholder="Бренд" />
                  <input value={it.name || ""} onChange={(e) => patchItem(i, { name: e.target.value })} placeholder="Название" />
                  <div className="sv-2">
                    <input type="number" value={it.price || ""} onChange={(e) => patchItem(i, { price: Number(e.target.value) || undefined })} placeholder="Цена, ₽" />
                    <input type="number" value={it.old || ""} onChange={(e) => patchItem(i, { old: Number(e.target.value) || undefined })} placeholder="Старая цена" />
                  </div>
                </div>
                <span>
                  <button onClick={() => move(i, -1)} aria-label="Выше"><ArrowUp size={14} /></button>
                  <button onClick={() => move(i, 1)} aria-label="Ниже"><ArrowDown size={14} /></button>
                  <button onClick={() => set("items", cfg.items.filter((_, j) => j !== i))} aria-label="Убрать"><Trash2 size={14} /></button>
                </span>
              </div>
            ))}
          </div>
          <button className="st-btn ghost" onClick={() => fileRef.current?.click()}><Upload size={15} /> Добавить своё фото</button>
          <input ref={fileRef} type="file" accept="image/*" hidden onChange={(e) => { const f = e.target.files?.[0]; if (f) addItem({ upload: URL.createObjectURL(f), brand: "Magic Vibes", name: "" }); e.target.value = ""; }} />
        </div>

        <div className="st-group">
          <div className="st-label">Тексты на экране</div>
          <label className="st-field"><span>{cfg.tpl === "brand" ? "Бренд" : "Заголовок"}</span><input value={cfg.title} onChange={(e) => set("title", e.target.value)} /></label>
          <label className="st-field"><span>Подпись (рукописная)</span><input value={cfg.script} onChange={(e) => set("script", e.target.value)} /></label>
          {cfg.tpl === "spotlight" && <label className="st-field"><span>Ноты через запятую</span><input value={cfg.notes} onChange={(e) => set("notes", e.target.value)} /></label>}
          {cfg.tpl === "sale" && <label className="st-field"><span>Срок акции</span><input value={cfg.deadline} onChange={(e) => set("deadline", e.target.value)} placeholder="до 5 октября" /></label>}
          <label className="st-field"><span>Финальная фраза</span><input value={cfg.outro} onChange={(e) => set("outro", e.target.value)} /></label>
          <label className="st-field"><span>Промокод (пусто — без него)</span><input value={cfg.promo} onChange={(e) => set("promo", e.target.value.toUpperCase())} /></label>
          <label className="st-field"><span>Сайт</span><input value={cfg.site} onChange={(e) => set("site", e.target.value)} /></label>
        </div>

        <div className="st-group">
          <div className="st-label">Музыка {musicBusy && <Loader2 size={12} className="mv-spin" />}</div>
          <div className="st-chips">
            <button className={!mood ? "on" : ""} onClick={() => setMoodId("none")}>Без музыки</button>
            {MOODS.map((m) => <button key={m.id} className={moodId === m.id ? "on" : ""} onClick={() => setMoodId(m.id)}>{m.label}</button>)}
          </div>
          <p className="sv-note">Музыка генерируется здесь же — своя, без авторских прав. Склейки сцен встают в такт.</p>
          {mood && <>
            <label className="sv-check"><input type="checkbox" checked={sfx} onChange={(e) => setSfx(e.target.checked)} /> Звуки переходов (свуш + удар на склейке)</label>
            <label className="st-field"><span>Громкость музыки</span><input type="range" min={0} max={1} step={0.05} value={musicVol} onChange={(e) => setMusicVol(Number(e.target.value))} /></label>
          </>}
          <label className="st-field"><span>Секунд на товар: {cfg.per.toFixed(1)}</span>
            <input type="range" min={1.8} max={6} step={0.1} value={cfg.per} onChange={(e) => set("per", Number(e.target.value))} /></label>
          <div className="st-chips">
            {Object.entries(PALETTES).map(([k, p]) => <button key={k} className={cfg.palette === k ? "on" : ""} onClick={() => set("palette", k)}>{p.label}</button>)}
          </div>
          <label className="sv-check"><input type="checkbox" checked={cfg.prices} onChange={(e) => set("prices", e.target.checked)} /> Показывать цены</label>
        </div>

        <div className="st-group">
          <div className="st-label"><Mic size={13} /> Голос диктора — Yandex SpeechKit</div>
          <div className="sv-lines">
            {tl.scenes.map((s, i) => (
              <label key={i} className={overflow[i] ? "long" : ""}>
                <span>{i + 1}. {s.kind.replace("spot-", "").replace("sale-", "")} · {s.dur.toFixed(1)} с{overflow[i] ? " · длинно" : ""}</span>
                <input value={voLines[i] || ""} onChange={(e) => { const next = [...voLines]; next[i] = e.target.value; setLines(next); }} />
              </label>
            ))}
          </div>
          {lines && <button className="st-btn ghost" onClick={() => setLines(null)}>Вернуть автотекст</button>}
          <div className="st-label" style={{ marginTop: 6 }}>Текст для SpeechKit Playground</div>
          <textarea className="st-caption" readOnly rows={6} value={speechkitText} />
          <button className="st-btn" onClick={async () => { await navigator.clipboard.writeText(speechkitText); setCopied(true); setTimeout(() => setCopied(false), 1500); }}>
            {copied ? <Check size={16} /> : <Copy size={16} />} {copied ? "Скопировано" : "Копировать текст"}
          </button>
          <p className="sv-note">
            1) Вставьте текст в Playground (Синтез речи), язык — русский, скорость 1.0, формат MP3. Подойдут голоса «Алёна», «Даша», «Марина»
            или «Василий» с амплуа «Продажи/Радостный». 2) Синтезируйте и скачайте MP3. 3) Загрузите его сюда — паузы <code>sil&lt;[мс]&gt;</code>
            уже выставлены так, чтобы каждая фраза звучала на своей сцене.
          </p>
          <button className="st-btn ghost" onClick={() => voiceRef.current?.click()}><Upload size={15} /> {voice ? `Озвучка: ${voice.name} · ${voice.buf.duration.toFixed(1)} с` : "Загрузить озвучку (MP3)"}</button>
          <input ref={voiceRef} type="file" accept="audio/*" hidden onChange={(e) => { loadVoice(e.target.files?.[0]); e.target.value = ""; }} />
          {voice && <>
            <label className="st-field"><span>Сдвиг голоса: {voiceDelay.toFixed(2)} с</span><input type="range" min={-1} max={2} step={0.05} value={voiceDelay} onChange={(e) => setVoiceDelay(Number(e.target.value))} /></label>
            <label className="st-field"><span>Громкость голоса</span><input type="range" min={0} max={1.5} step={0.05} value={voiceVol} onChange={(e) => setVoiceVol(Number(e.target.value))} /></label>
            <button className="st-btn ghost" onClick={() => setVoice(null)}><X size={15} /> Убрать озвучку</button>
          </>}
        </div>

        <div className="st-group">
          <div className="st-actions">
            <button className="st-btn" onClick={record} disabled={!!rec || needMore || (!!mood && !musicBuf)}>
              {rec ? <><Loader2 size={16} className="mv-spin" /> Запись {Math.round(rec.progress * 100)}%</> : <><Download size={16} /> Записать видео · {secs} с</>}
            </button>
          </div>
          {needMore && <p className="sv-note">Добавьте товары: нужно минимум {tplDef.min}.</p>}
          {rec && <p className="sv-note">Запись идёт в реальном времени со звуком — не сворачивайте вкладку.</p>}
          {err && <p className="sv-note" style={{ color: "#c62828" }}>{err}</p>}
          {out && (
            <a className="st-btn" href={out.url} download={`magicvibes-story-${cfg.tpl}-${Date.now()}.${out.ext}`}>
              <Download size={16} /> Скачать {out.ext.toUpperCase()} · {(out.size / 1024 / 1024).toFixed(1)} МБ
            </a>
          )}
          {out?.ext === "webm" && <p className="sv-note">Браузер записал WebM. Для Instagram/VK лучше MP4 — откройте студию в свежем Chrome или Edge.</p>}
        </div>
      </aside>

      <main className="st-stage">
        <div className="sv-player">
          <canvas ref={canvasRef} width={W} height={H} className="sv-canvas" />
          <div className="sv-controls">
            <button onClick={() => setPlaying((p) => !p)} disabled={!!rec} aria-label={playing ? "Пауза" : "Играть"}>{playing ? <Pause size={16} /> : <Play size={16} />}</button>
            <input type="range" min={0} max={dur} step={0.01} value={rec ? rec.progress * dur : time} disabled={!!rec}
              onChange={(e) => { timeRef.current = Number(e.target.value); setTime(timeRef.current); setPlaying(false); }} />
            <span>{(rec ? rec.progress * dur : time).toFixed(1)} / {secs} с</span>
          </div>
          <p className="sv-note">Со звуком: нажмите ▶ — музыка и озвучка играют синхронно с картинкой.</p>
        </div>
      </main>
    </div>
  );
}

const clamp0 = (x: number) => (x < 0 ? 0 : x);
