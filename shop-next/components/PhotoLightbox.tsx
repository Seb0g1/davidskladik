"use client";
import { useCallback, useEffect, useRef, useState } from "react";
import { ChevronLeft, ChevronRight, X } from "lucide-react";
import { productImg } from "@/lib/img";

/**
 * Full-screen product photo viewer: swipe / arrows between photos,
 * tap or click to zoom 2.2x into that point (drag / move to pan), Esc to close.
 */
export default function PhotoLightbox({ images, start, alt, onClose }: { images: string[]; start: number; alt: string; onClose: () => void }) {
  const [i, setI] = useState(start);
  const [zoom, setZoom] = useState<{ x: number; y: number } | null>(null);
  const touch = useRef<{ x: number; y: number } | null>(null);
  const n = images.length;

  const go = useCallback((d: number) => { setZoom(null); setI((v) => (v + d + n) % n); }, [n]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
      else if (e.key === "ArrowRight") go(1);
      else if (e.key === "ArrowLeft") go(-1);
    };
    window.addEventListener("keydown", onKey);
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { window.removeEventListener("keydown", onKey); document.body.style.overflow = prev; };
  }, [go, onClose]);

  const point = (e: React.PointerEvent | React.MouseEvent) => {
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    return { x: ((e.clientX - r.left) / r.width) * 100, y: ((e.clientY - r.top) / r.height) * 100 };
  };

  return (
    <div className="mv-lb" role="dialog" aria-modal="true" aria-label={alt}>
      <button className="mv-lb-close" onClick={onClose} aria-label="Закрыть"><X size={22} /></button>
      <div
        className={`mv-lb-stage${zoom ? " is-zoomed" : ""}`}
        onClick={(e) => setZoom(zoom ? null : point(e))}
        onPointerMove={(e) => { if (zoom && e.pointerType === "mouse") setZoom(point(e)); }}
        onTouchStart={(e) => { touch.current = { x: e.touches[0].clientX, y: e.touches[0].clientY }; }}
        onTouchMove={(e) => {
          if (!zoom) return;
          const r = e.currentTarget.getBoundingClientRect();
          setZoom({ x: ((e.touches[0].clientX - r.left) / r.width) * 100, y: ((e.touches[0].clientY - r.top) / r.height) * 100 });
        }}
        onTouchEnd={(e) => {
          const t = touch.current; touch.current = null;
          if (!t || zoom || n < 2) return;
          const dx = e.changedTouches[0].clientX - t.x, dy = e.changedTouches[0].clientY - t.y;
          if (Math.abs(dx) > 50 && Math.abs(dx) > Math.abs(dy) * 1.5) { e.preventDefault(); go(dx < 0 ? 1 : -1); }
        }}
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          key={images[i]}
          src={productImg(images[i], 1200)}
          alt={`${alt} — фото ${i + 1}`}
          draggable={false}
          style={zoom ? { transform: "scale(2.2)", transformOrigin: `${zoom.x}% ${zoom.y}%` } : undefined}
        />
      </div>
      {n > 1 && (
        <>
          <button className="mv-lb-nav prev" onClick={() => go(-1)} aria-label="Предыдущее фото"><ChevronLeft size={26} /></button>
          <button className="mv-lb-nav next" onClick={() => go(1)} aria-label="Следующее фото"><ChevronRight size={26} /></button>
          <div className="mv-lb-dots">{images.map((_, k) => <span key={k} className={k === i ? "on" : ""} />)}</div>
        </>
      )}
      <div className="mv-lb-hint">{zoom ? "Нажмите, чтобы уменьшить" : "Нажмите на фото, чтобы приблизить"}</div>
    </div>
  );
}
