import { useQueryClient } from "@tanstack/react-query";
import { Download, ExternalLink, Loader2, RefreshCw, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { z } from "zod";
import { fetchJson, mutationBody } from "../api";
import { errorMessage } from "../lib/common";
import { toast } from "../lib/toast";

// Страница аромата, которую Фрагрантика не отдаёт серверу (Cloudflare), загружается из браузера двумя путями:
// закладкой «→ Склад» (открывает /app/fragrantica?receive=1) или расширением «→ Склад» по Ctrl+Shift+B
// (кладёт страницу в уже открытую вкладку склада сообщением ds-fragrantica-ext).

export const FRAGRANTICA_RECEIVE_PARAM = "receive";

type ImportResult = { id: number; name: string; brand: string; requeued?: number };

export function importFragranticaHtml(url: string, html: string) {
  return fetchJson<ImportResult>("/api/fragrantica/catalog/import-html", z.custom<ImportResult>(() => true), mutationBody({ url, html }));
}

export function importedMessage(res: ImportResult) {
  const requeued = Number(res.requeued) || 0;
  return `Загружено с Фрагрантики: ${res.brand} ${res.name}${requeued ? ` · пересобираем черновиков: ${requeued}` : ""}`;
}

function fragranticaBookmarklet(origin: string) {
  const target = JSON.stringify(`${origin}/app/fragrantica?${FRAGRANTICA_RECEIVE_PARAM}=1`);
  const from = JSON.stringify(origin);
  const code = String.raw`(async()=>{if(!/fragrantica\.[a-z.]+\/(perfume|parfum)\/.+-\d+\.html/i.test(location.href)){alert("Откройте страницу аромата на Фрагрантике и нажмите закладку ещё раз.");return}`
    + String.raw`var w=window.open(${target},"ds_fragrantica");var h="";`
    + String.raw`try{var r=await fetch(location.href,{credentials:"include"});h=r.ok?await r.text():""}catch(e){}`
    + String.raw`if(!h)h=document.documentElement.outerHTML;`
    + String.raw`h=h.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,"").replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi,"").replace(/<svg\b[^>]*>[\s\S]*?<\/svg>/gi,"<svg></svg>").replace(/<!--[\s\S]*?-->/g,"").replace(/\s{2,}/g," ");`
    + String.raw`var sent=false;addEventListener("message",function(e){if(e.origin===${from}&&e.data&&e.data.type==="ds-fragrantica-ready"&&!sent){sent=true;w.postMessage({type:"ds-fragrantica-page",url:location.href,html:h},${from})}})})()`;
  return `javascript:${encodeURIComponent(code)}`;
}

export function BookmarkletLink() {
  // React не ставит javascript:-ссылки через props, поэтому href выставляем сами.
  const href = useMemo(() => fragranticaBookmarklet(window.location.origin), []);
  return (
    <a className="fr-bookmarklet" ref={(el) => { el?.setAttribute("href", href); }} onClick={(e) => { e.preventDefault(); toast.info("Перетащите эту кнопку на панель закладок браузера"); }} title="Перетащите на панель закладок">
      → Склад
    </a>
  );
}

/** Черновик без данных аромата: открыть его страницу на Фрагрантике и отправить в склад одним нажатием. */
export function FragranticaQuickLoad({ url, name }: { url?: string; name?: string }) {
  if (!url) return null;
  return (
    <div className="fr-quickload">
      <a className="secondary-action compact" href={url} target="_blank" rel="noreferrer">
        <ExternalLink size={13} /> Открыть{name ? ` «${name}»` : ""} на Фрагрантике
      </a>
      <span className="fr-hint">
        Там нажмите <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>B</kbd> (расширение «→ Склад») или закладку <BookmarkletLink /> — страница загрузится, и черновики этого аромата соберутся заново сами.
      </span>
    </div>
  );
}

export type ExtRunState = { running: boolean; done: number; total: number; ok: number; failed: number; current?: string; message?: string; waitingCheck?: boolean };

/** The «→ Склад» extension (1.1+): is it installed, and its queue of perfume pages — start, stop, progress. */
export function useFragranticaExtRun() {
  const [installed, setInstalled] = useState(() => Boolean(document.documentElement.dataset.dsFragranticaExt));
  const [state, setState] = useState<ExtRunState | null>(null);
  useEffect(() => {
    // the bridge marks the page at document_start, or later when the extension is installed / updated into an open tab
    const check = () => setInstalled(Boolean(document.documentElement.dataset.dsFragranticaExt));
    const t = window.setInterval(check, 2000);
    const onMessage = (event: MessageEvent) => {
      if (event.source !== window || event.origin !== window.location.origin) return;
      if (event.data?.type === "ds-fragrantica-ext-hello") { check(); return; }
      if (event.data?.type !== "ds-fragrantica-ext-progress") return;
      const { type: _type, ...rest } = event.data as ExtRunState & { type: string };
      setState(rest);
    };
    window.addEventListener("message", onMessage);
    window.postMessage({ type: "ds-fragrantica-ext-status" }, window.location.origin);
    return () => { window.clearInterval(t); window.removeEventListener("message", onMessage); };
  }, []);
  const start = useCallback((items: Array<{ url: string; name: string }>) => {
    setState({ running: true, done: 0, total: items.length, ok: 0, failed: 0 });
    window.postMessage({ type: "ds-fragrantica-ext-run", items }, window.location.origin);
  }, []);
  const stop = useCallback(() => window.postMessage({ type: "ds-fragrantica-ext-stop" }, window.location.origin), []);
  return { installed, state, start, stop };
}

// the extension's folder zipped (tools/sklad-extension → frontend/public/sklad-extension.zip on each change)
const EXTENSION_ZIP = "/app-modern/sklad-extension.zip";

/** «Скачать расширение»: the zip and how to install it on any computer. */
export function ExtensionDownload({ installed }: { installed: boolean }) {
  return (
    <a
      className="secondary-action compact"
      href={EXTENSION_ZIP}
      download="sklad-extension.zip"
      title="Распакуйте архив в постоянную папку → chrome://extensions → «Режим разработчика» → «Загрузить распакованное» → папка sklad-extension"
      onClick={() => toast.info("Распакуйте архив в постоянную папку, откройте chrome://extensions, включите «Режим разработчика» и нажмите «Загрузить распакованное» → папка sklad-extension. Потом обновите эту вкладку (F5).")}
    >
      <Download size={13} /> {installed ? "Расширение «→ Склад»" : "Скачать расширение"}
    </a>
  );
}

/**
 * Perfumes without their Fragrantica page and the extension's queue for them: how many, progress, start / stop.
 * The extension takes up to 60 per run, the list's first ones (the caller orders them).
 */
export function ExtensionQueueStrip({ items, total, drafts }: { items: Array<{ url: string; name: string }>; total?: number; drafts?: number }) {
  const ext = useFragranticaExtRun();
  const running = Boolean(ext.state?.running);
  const count = total ?? items.length;
  if (!count && !ext.state) return null;
  return (
    <div className="fr-ext-run">
      <div className="fr-ext-run-text">
        {running ? (
          <b><Loader2 size={13} className="spin" /> Загружаем страницы с Фрагрантики: {ext.state!.done} из {ext.state!.total}{ext.state!.current ? ` · ${ext.state!.current}` : ""}</b>
        ) : ext.state && ext.state.total ? (
          <b>Загружено страниц: {ext.state.ok}{ext.state.failed ? `, не вышло: ${ext.state.failed}` : ""}. Черновики пересобираются сами.{count ? ` Осталось ароматов без страницы: ${count}.` : ""}</b>
        ) : (
          <b>Нет страницы Фрагрантики у {count} {count === 1 ? "аромата" : "ароматов"}{drafts ? ` (${drafts} карточек ждут)` : ""}</b>
        )}
        {ext.state?.message ? <span className={ext.state.waitingCheck ? "fr-warn" : "fr-hint"}>{ext.state.message}</span> : null}
        {!ext.installed ? (
          <span className="fr-hint">Расширение «→ Склад» не видно на этой вкладке: скачайте и поставьте его (chrome://extensions → «Загрузить распакованное») и обновите страницу (F5).</span>
        ) : !running ? (
          <span className="fr-hint">Расширение откроет их в фоновой вкладке по одному (до 60 за раз, сначала самые продаваемые), с паузами, и загрузит сюда. Не закрывайте эту вкладку.</span>
        ) : null}
      </div>
      {!ext.installed ? <ExtensionDownload installed={false} /> : null}
      {running ? (
        <button className="secondary-action compact" type="button" onClick={ext.stop}><X size={13} /> Остановить</button>
      ) : (
        <button className="secondary-action compact" type="button" disabled={!ext.installed || !items.length} onClick={() => ext.start(items.slice(0, 60))}>
          <RefreshCw size={13} /> Загрузить {Math.min(60, items.length) || ""} через расширение
        </button>
      )}
    </div>
  );
}

declare global {
  interface Window { __dsFragranticaReceiver?: boolean }
}

/** Приёмник расширения «→ Склад»: страница приходит в эту вкладку, ответ уходит обратно сообщением …-done. */
export function FragranticaExtReceiver() {
  const queryClient = useQueryClient();
  useEffect(() => {
    const onMessage = async (event: MessageEvent) => {
      if (event.source !== window || event.origin !== window.location.origin || event.data?.type !== "ds-fragrantica-ext") return;
      const nonce = String(event.data.nonce || "");
      const reply = (ok: boolean, text: string) => window.postMessage({ type: "ds-fragrantica-ext-done", nonce, ok, text }, window.location.origin);
      try {
        const res = await importFragranticaHtml(String(event.data.url || ""), String(event.data.html || ""));
        const text = importedMessage(res);
        // a page of the extension's queue: the conveyor shows the progress, no toast per page
        if (!event.data.quiet) toast.success(text);
        void queryClient.invalidateQueries({ queryKey: ["fragrantica"] });
        reply(true, text);
      } catch (error) {
        const text = errorMessage(error);
        if (!event.data.quiet) toast.error(text);
        reply(false, text);
      }
    };
    window.addEventListener("message", onMessage);
    window.__dsFragranticaReceiver = true;
    return () => { window.removeEventListener("message", onMessage); window.__dsFragranticaReceiver = false; };
  }, [queryClient]);
  return null;
}
