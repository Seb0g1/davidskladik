import { useCallback, useEffect, useRef, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { ChevronLeft, ChevronRight, ExternalLink, Minus, Plus, RotateCcw, X } from "lucide-react";
import "./photo-lightbox.css";

// Просмотр фото поверх страницы (вместо новой вкладки): листание, увеличение колесом / щелчком / щипком,
// перетаскивание увеличенного фото. openPhotoLightbox(photos, index) можно звать откуда угодно — окно
// монтируется в свой корень в body, App его знать не должен.

let host: { root: Root; el: HTMLDivElement } | null = null;

export function openPhotoLightbox(photos: string[], index = 0, title = "") {
  const list = photos.filter(Boolean);
  if (!list.length) return;
  if (!host) {
    const el = document.createElement("div");
    document.body.appendChild(el);
    host = { root: createRoot(el), el };
  }
  const close = () => host?.root.render(null);
  host.root.render(<PhotoLightbox key={Date.now()} photos={list} start={Math.min(Math.max(0, index), list.length - 1)} title={title} onClose={close} />);
}

/** Thumbnail that opens the viewer on the whole set instead of a new tab. */
export function PhotoThumb({ photos, index, alt, className, title }: { photos: string[]; index: number; alt: string; className?: string; title?: string }) {
  return (
    <button type="button" className={`plb-thumb ${className || ""}`} title={title || "Открыть фото"} onClick={(e) => { e.preventDefault(); e.stopPropagation(); openPhotoLightbox(photos, index, title); }}>
      <img src={photos[index]} alt={alt} loading="lazy" />
    </button>
  );
}

const MIN = 1;
const MAX = 6;

function PhotoLightbox({ photos, start, title, onClose }: { photos: string[]; start: number; title: string; onClose: () => void }) {
  const [index, setIndex] = useState(start);
  const [view, setView] = useState({ scale: 1, x: 0, y: 0 });
  const [loaded, setLoaded] = useState(false);
  const stage = useRef<HTMLDivElement>(null);
  const pointers = useRef(new Map<number, { x: number; y: number }>());
  const drag = useRef<{ x: number; y: number; moved: number; pinch?: number; pinchScale?: number } | null>(null);

  const reset = useCallback(() => setView({ scale: 1, x: 0, y: 0 }), []);
  const go = useCallback((dir: number) => {
    setIndex((i) => (i + dir + photos.length) % photos.length);
    setView({ scale: 1, x: 0, y: 0 });
    setLoaded(false);
  }, [photos.length]);

  // zoom keeping the point under (px, py) — coordinates from the stage centre — in place
  const zoomAt = useCallback((next: number, px = 0, py = 0) => {
    setView((v) => {
      const scale = Math.min(MAX, Math.max(MIN, next));
      if (scale === 1) return { scale: 1, x: 0, y: 0 };
      const k = scale / v.scale;
      return { scale, x: px - (px - v.x) * k, y: py - (py - v.y) * k };
    });
  }, []);

  const fromCentre = (clientX: number, clientY: number) => {
    const r = stage.current?.getBoundingClientRect();
    return r ? { px: clientX - (r.left + r.width / 2), py: clientY - (r.top + r.height / 2) } : { px: 0, py: 0 };
  };

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      else if (e.key === "ArrowRight") go(1);
      else if (e.key === "ArrowLeft") go(-1);
      else if (e.key === "+" || e.key === "=") setView((v) => ({ ...v, scale: Math.min(MAX, v.scale * 1.4) }));
      else if (e.key === "-") zoomAt(view.scale / 1.4);
      else if (e.key === "0") reset();
    };
    window.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", onKey);
      document.body.style.overflow = prev;
    };
  }, [go, onClose, reset, zoomAt, view.scale]);

  // wheel zoom needs a non-passive listener to stop the page from scrolling
  useEffect(() => {
    const el = stage.current;
    if (!el) return undefined;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const { px, py } = fromCentre(e.clientX, e.clientY);
      setView((v) => {
        const scale = Math.min(MAX, Math.max(MIN, v.scale * (e.deltaY < 0 ? 1.18 : 1 / 1.18)));
        if (scale === 1) return { scale: 1, x: 0, y: 0 };
        const k = scale / v.scale;
        return { scale, x: px - (px - v.x) * k, y: py - (py - v.y) * k };
      });
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);

  const onPointerDown = (e: React.PointerEvent) => {
    (e.target as Element).setPointerCapture?.(e.pointerId);
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.current.size === 2) {
      const [a, b] = [...pointers.current.values()];
      drag.current = { x: 0, y: 0, moved: 99, pinch: Math.hypot(a.x - b.x, a.y - b.y), pinchScale: view.scale };
    } else {
      drag.current = { x: e.clientX, y: e.clientY, moved: 0 };
    }
  };
  const onPointerMove = (e: React.PointerEvent) => {
    if (!pointers.current.has(e.pointerId) || !drag.current) return;
    pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
    const d = drag.current;
    if (d.pinch && pointers.current.size === 2) {
      const [a, b] = [...pointers.current.values()];
      const { px, py } = fromCentre((a.x + b.x) / 2, (a.y + b.y) / 2);
      zoomAt((d.pinchScale || 1) * (Math.hypot(a.x - b.x, a.y - b.y) / d.pinch), px, py);
      return;
    }
    const dx = e.clientX - d.x;
    const dy = e.clientY - d.y;
    d.moved += Math.abs(dx) + Math.abs(dy);
    d.x = e.clientX;
    d.y = e.clientY;
    if (view.scale > 1) setView((v) => ({ ...v, x: v.x + dx, y: v.y + dy }));
  };
  const onPointerUp = (e: React.PointerEvent) => {
    const d = drag.current;
    pointers.current.delete(e.pointerId);
    if (pointers.current.size) return;
    drag.current = null;
    if (!d || d.moved > 6) return;
    // a click (not a drag): zoom in at that point, or back to fit
    if ((e.target as Element).closest(".plb-img")) {
      if (view.scale > 1) reset();
      else {
        const { px, py } = fromCentre(e.clientX, e.clientY);
        zoomAt(2.5, px, py);
      }
    } else if (view.scale === 1) {
      onClose();
    }
  };

  const url = photos[index];
  return (
    <div className="plb" role="dialog" aria-modal="true" aria-label={title || "Просмотр фото"}>
      <div className="plb-top">
        <span className="plb-count">{index + 1} / {photos.length}{title ? ` · ${title}` : ""}</span>
        <div className="plb-tools">
          <button type="button" onClick={() => zoomAt(view.scale / 1.4)} disabled={view.scale <= 1} aria-label="Уменьшить"><Minus size={17} /></button>
          <span className="plb-zoom">{Math.round(view.scale * 100)}%</span>
          <button type="button" onClick={() => zoomAt(view.scale * 1.4)} disabled={view.scale >= MAX} aria-label="Увеличить"><Plus size={17} /></button>
          <button type="button" onClick={reset} disabled={view.scale === 1} aria-label="По размеру окна"><RotateCcw size={16} /></button>
          <a href={url} target="_blank" rel="noreferrer" aria-label="Оригинал в новой вкладке" title="Оригинал в новой вкладке"><ExternalLink size={16} /></a>
          <button type="button" onClick={onClose} aria-label="Закрыть"><X size={19} /></button>
        </div>
      </div>

      <div
        ref={stage}
        className={`plb-stage${view.scale > 1 ? " is-zoomed" : ""}`}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
      >
        {!loaded ? <div className="plb-loading" /> : null}
        <img
          className="plb-img"
          src={url}
          alt={`Фото ${index + 1}`}
          draggable={false}
          onLoad={() => setLoaded(true)}
          style={{ transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`, opacity: loaded ? 1 : 0 }}
        />
        {photos.length > 1 ? (
          <>
            <button type="button" className="plb-nav is-prev" onPointerDown={(e) => e.stopPropagation()} onPointerUp={(e) => e.stopPropagation()} onClick={() => go(-1)} aria-label="Предыдущее"><ChevronLeft size={28} /></button>
            <button type="button" className="plb-nav is-next" onPointerDown={(e) => e.stopPropagation()} onPointerUp={(e) => e.stopPropagation()} onClick={() => go(1)} aria-label="Следующее"><ChevronRight size={28} /></button>
          </>
        ) : null}
      </div>

      {photos.length > 1 ? (
        <div className="plb-strip">
          {photos.map((p, i) => (
            <button key={`${p}-${i}`} type="button" className={i === index ? "is-on" : ""} onClick={() => { setIndex(i); reset(); setLoaded(false); }} aria-label={`Фото ${i + 1}`}>
              <img src={p} alt="" loading="lazy" />
            </button>
          ))}
        </div>
      ) : null}
      <p className="plb-hint">Колесо или щелчок — увеличить, перетаскивание — сдвинуть, ← → — листать, Esc — закрыть</p>
    </div>
  );
}
