"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { Maximize2, Minimize2, Pause, Play, RotateCcw, Volume2, VolumeX } from "lucide-react";

// Own video player for Telegram news: nothing is downloaded until Play (videos are ~15 MB),
// a glass play button + duration badge like in Telegram, a brand-coloured scrubber that auto-hides.

const fmt = (s: number) => {
  if (!Number.isFinite(s) || s < 0) s = 0;
  const m = Math.floor(s / 60);
  return `${m}:${String(Math.floor(s % 60)).padStart(2, "0")}`;
};

type Props = {
  src: string;
  poster?: string | null;
  width?: number | null;
  height?: number | null;
  /** Album tile: square, smaller controls. */
  compact?: boolean;
  /** GIF-like clip: loops muted, no controls. */
  loopMuted?: boolean;
};

export default function NewsVideo({ src, poster, width, height, compact, loopMuted }: Props) {
  const wrapRef = useRef<HTMLDivElement>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const hideTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const [started, setStarted] = useState(false);
  const [playing, setPlaying] = useState(false);
  const [ended, setEnded] = useState(false);
  const [muted, setMuted] = useState(false);
  const [time, setTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [buffered, setBuffered] = useState(0);
  const [showUi, setShowUi] = useState(true);
  const [fullscreen, setFullscreen] = useState(false);
  const [waiting, setWaiting] = useState(false);

  const w = width || 16, h = height || 9;

  // Duration badge before the first play: read metadata only (a few KB).
  useEffect(() => {
    if (loopMuted) return;
    const probe = document.createElement("video");
    probe.preload = "metadata";
    probe.src = src;
    const onMeta = () => setDuration(probe.duration);
    probe.addEventListener("loadedmetadata", onMeta);
    return () => { probe.removeEventListener("loadedmetadata", onMeta); probe.removeAttribute("src"); probe.load(); };
  }, [src, loopMuted]);

  useEffect(() => {
    const onFs = () => setFullscreen(document.fullscreenElement === wrapRef.current);
    document.addEventListener("fullscreenchange", onFs);
    return () => document.removeEventListener("fullscreenchange", onFs);
  }, []);

  const poke = useCallback(() => {
    setShowUi(true);
    clearTimeout(hideTimer.current);
    hideTimer.current = setTimeout(() => {
      if (videoRef.current && !videoRef.current.paused) setShowUi(false);
    }, 2200);
  }, []);
  useEffect(() => () => clearTimeout(hideTimer.current), []);

  const toggle = () => {
    const v = videoRef.current;
    if (!v) return;
    if (!started) {
      v.src = src; // the element has no src until the first Play, so nothing is downloaded before that
      setStarted(true);
    }
    if (v.paused || v.ended) {
      if (v.ended) v.currentTime = 0;
      void v.play().catch(() => {});
    } else {
      v.pause();
    }
    poke();
  };

  const seek = (clientX: number, bar: HTMLElement) => {
    const v = videoRef.current;
    if (!v || !duration) return;
    const r = bar.getBoundingClientRect();
    v.currentTime = Math.min(1, Math.max(0, (clientX - r.left) / r.width)) * duration;
    setTime(v.currentTime);
  };

  const onBarDown = (e: React.PointerEvent<HTMLDivElement>) => {
    e.stopPropagation();
    const bar = e.currentTarget;
    bar.setPointerCapture(e.pointerId);
    seek(e.clientX, bar);
    const move = (ev: PointerEvent) => seek(ev.clientX, bar);
    const up = () => { bar.removeEventListener("pointermove", move); bar.removeEventListener("pointerup", up); };
    bar.addEventListener("pointermove", move);
    bar.addEventListener("pointerup", up);
    poke();
  };

  const toggleFullscreen = (e: React.MouseEvent) => {
    e.stopPropagation();
    const el = wrapRef.current, v = videoRef.current as (HTMLVideoElement & { webkitEnterFullscreen?: () => void }) | null;
    if (!el || !v) return;
    if (document.fullscreenElement) void document.exitFullscreen();
    else if (el.requestFullscreen) void el.requestFullscreen().catch(() => v.webkitEnterFullscreen?.());
    else v.webkitEnterFullscreen?.(); // iOS Safari
  };

  if (loopMuted) {
    return (
      <div style={{ position: "relative", aspectRatio: compact ? "1/1" : `${w}/${h}`, width: compact ? "100%" : `min(100%, calc(75vh * ${w} / ${h}))`, margin: "0 auto", background: "#000" }}>
        <video src={src} poster={poster ?? undefined} autoPlay loop muted playsInline style={{ position: "absolute", inset: 0, width: "100%", height: "100%", objectFit: "cover" }} />
      </div>
    );
  }

  const progress = duration ? (time / duration) * 100 : 0;
  const uiVisible = showUi || !playing;
  const btn: React.CSSProperties = {
    display: "grid", placeItems: "center", width: compact ? 30 : 36, height: compact ? 30 : 36,
    borderRadius: 999, border: 0, background: "transparent", color: "#fff", cursor: "pointer", padding: 0,
  };

  return (
    <div
      ref={wrapRef}
      className="nv-wrap"
      onMouseMove={started ? poke : undefined}
      onClick={toggle}
      style={{
        position: "relative", overflow: "hidden", background: "#0b0b0b", cursor: "pointer", userSelect: "none",
        aspectRatio: fullscreen ? undefined : compact ? "1/1" : `${w}/${h}`,
        width: fullscreen ? "100%" : compact ? "100%" : `min(100%, calc(75vh * ${w} / ${h}))`,
        height: fullscreen ? "100%" : undefined,
        margin: "0 auto",
        borderRadius: fullscreen || compact ? 0 : 14,
      }}
    >
      <video
        ref={videoRef}
        // src is assigned in toggle(): re-setting it from props on the next render would restart
        // loading and abort the play() that was just called
        poster={poster ?? undefined}
        playsInline
        muted={muted}
        preload="none"
        onPlay={() => { setPlaying(true); setEnded(false); poke(); }}
        onPause={() => { setPlaying(false); setShowUi(true); }}
        onEnded={() => { setPlaying(false); setEnded(true); setShowUi(true); }}
        onWaiting={() => setWaiting(true)}
        onPlaying={() => setWaiting(false)}
        onLoadedMetadata={(e) => setDuration(e.currentTarget.duration)}
        onTimeUpdate={(e) => setTime(e.currentTarget.currentTime)}
        onProgress={(e) => {
          const v = e.currentTarget;
          if (v.buffered.length && v.duration) setBuffered((v.buffered.end(v.buffered.length - 1) / v.duration) * 100);
        }}
        style={{ position: "absolute", inset: 0, width: "100%", height: "100%", objectFit: fullscreen ? "contain" : "cover", background: "#000" }}
      />

      {/* Poster state / paused: big glass button */}
      <div style={{
        position: "absolute", inset: 0, display: "grid", placeItems: "center", pointerEvents: "none",
        background: playing ? "transparent" : "radial-gradient(circle at center, rgba(0,0,0,0.18), rgba(0,0,0,0) 60%)",
        opacity: playing && !waiting ? 0 : 1, transition: "opacity .25s",
      }}>
        {waiting && playing ? (
          <span className="nv-spin" style={{ width: 46, height: 46, borderRadius: 999, border: "3px solid rgba(255,255,255,0.25)", borderTopColor: "var(--lime)" }} />
        ) : (
          <span className="nv-big" style={{
            width: compact ? 54 : 76, height: compact ? 54 : 76, borderRadius: 999, display: "grid", placeItems: "center",
            background: "rgba(10,10,10,0.42)", backdropFilter: "blur(14px) saturate(160%)", WebkitBackdropFilter: "blur(14px) saturate(160%)",
            border: "1px solid rgba(255,255,255,0.28)", boxShadow: "0 12px 40px -8px rgba(0,0,0,0.45)",
          }}>
            {ended
              ? <RotateCcw size={compact ? 22 : 30} color="#fff" />
              : <Play size={compact ? 22 : 32} color="#fff" fill="#fff" style={{ marginLeft: compact ? 3 : 5 }} />}
          </span>
        )}
      </div>

      {/* Duration badge before start, like Telegram */}
      {!started && duration > 0 && (
        <span style={{
          position: "absolute", top: 12, left: 12, padding: "4px 10px", borderRadius: 999, fontSize: 12, fontWeight: 600,
          color: "#fff", background: "rgba(0,0,0,0.45)", backdropFilter: "blur(8px)", WebkitBackdropFilter: "blur(8px)",
          fontVariantNumeric: "tabular-nums", letterSpacing: "0.02em",
        }}>{fmt(duration)}</span>
      )}

      {/* Control bar */}
      {started && (
        <div
          onClick={(e) => e.stopPropagation()}
          style={{
            position: "absolute", left: compact ? 6 : 12, right: compact ? 6 : 12, bottom: compact ? 6 : 12,
            padding: compact ? "6px 8px" : "8px 12px 10px", borderRadius: compact ? 12 : 16,
            background: "rgba(12,12,12,0.55)", backdropFilter: "blur(16px) saturate(160%)", WebkitBackdropFilter: "blur(16px) saturate(160%)",
            border: "1px solid rgba(255,255,255,0.12)", color: "#fff", cursor: "default",
            opacity: uiVisible ? 1 : 0, transform: uiVisible ? "none" : "translateY(8px)", transition: "opacity .25s, transform .25s",
            pointerEvents: uiVisible ? "auto" : "none",
          }}
        >
          <div
            className="nv-bar"
            onPointerDown={onBarDown}
            style={{ position: "relative", height: 16, display: "flex", alignItems: "center", cursor: "pointer", touchAction: "none" }}
          >
            <div style={{ position: "absolute", left: 0, right: 0, height: 4, borderRadius: 4, background: "rgba(255,255,255,0.2)", overflow: "hidden" }}>
              <div style={{ position: "absolute", inset: 0, width: `${buffered}%`, background: "rgba(255,255,255,0.28)" }} />
              <div style={{ position: "absolute", inset: 0, width: `${progress}%`, background: "var(--lime)" }} />
            </div>
            <span className="nv-thumb" style={{
              position: "absolute", left: `calc(${progress}% - 7px)`, width: 14, height: 14, borderRadius: 999,
              background: "var(--lime)", boxShadow: "0 0 0 4px rgba(217,248,74,0.25)",
            }} />
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 4, marginTop: 2 }}>
            <button type="button" aria-label={playing ? "Пауза" : "Смотреть"} onClick={toggle} style={btn}>
              {playing ? <Pause size={18} fill="#fff" /> : ended ? <RotateCcw size={18} /> : <Play size={18} fill="#fff" style={{ marginLeft: 2 }} />}
            </button>
            <span style={{ fontSize: 12, fontWeight: 600, fontVariantNumeric: "tabular-nums", opacity: 0.9, marginLeft: 2 }}>
              {fmt(time)} <span style={{ opacity: 0.55 }}>/ {fmt(duration)}</span>
            </span>
            <span style={{ flex: 1 }} />
            <button type="button" aria-label={muted ? "Включить звук" : "Выключить звук"} onClick={() => { setMuted((m) => !m); poke(); }} style={btn}>
              {muted ? <VolumeX size={18} /> : <Volume2 size={18} />}
            </button>
            <button type="button" aria-label="Во весь экран" onClick={toggleFullscreen} style={btn}>
              {fullscreen ? <Minimize2 size={17} /> : <Maximize2 size={17} />}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
