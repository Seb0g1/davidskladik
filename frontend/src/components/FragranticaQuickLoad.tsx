import { useQueryClient } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
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
    // the bridge script marks the page at document_start; checked once more in case it came late
    const t = window.setTimeout(() => setInstalled(Boolean(document.documentElement.dataset.dsFragranticaExt)), 1000);
    const onMessage = (event: MessageEvent) => {
      if (event.source !== window || event.origin !== window.location.origin || event.data?.type !== "ds-fragrantica-ext-progress") return;
      const { type: _type, ...rest } = event.data as ExtRunState & { type: string };
      setState(rest);
    };
    window.addEventListener("message", onMessage);
    window.postMessage({ type: "ds-fragrantica-ext-status" }, window.location.origin);
    return () => { window.clearTimeout(t); window.removeEventListener("message", onMessage); };
  }, []);
  const start = useCallback((items: Array<{ url: string; name: string }>) => {
    setState({ running: true, done: 0, total: items.length, ok: 0, failed: 0 });
    window.postMessage({ type: "ds-fragrantica-ext-run", items }, window.location.origin);
  }, []);
  const stop = useCallback(() => window.postMessage({ type: "ds-fragrantica-ext-stop" }, window.location.origin), []);
  return { installed, state, start, stop };
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
