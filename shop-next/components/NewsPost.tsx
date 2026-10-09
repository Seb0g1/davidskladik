"use client";

import { Fragment, useEffect, useRef, useState, type ReactNode } from "react";
import NewsVideo from "./NewsVideo";
import type { TelegramNewsEmoji, TelegramNewsEntity, TelegramNewsMedia } from "@/lib/types";

// ─── Premium emoji ────────────────────────────────────────────────────────────
// Telegram premium emoji are Lottie animations (.tgs). The player and the JSON are loaded only once an
// emoji scrolls into view; until then the static thumbnail (or the plain emoji character) is shown.

type LottiePlayer = typeof import("lottie-web").default;
let lottiePromise: Promise<LottiePlayer> | null = null;
const loadLottie = () =>
  (lottiePromise ??= import("lottie-web/build/player/lottie_light").then((m) => (m.default ?? m) as unknown as LottiePlayer));

const animationCache = new Map<string, Promise<unknown>>();
function loadAnimation(url: string) {
  let p = animationCache.get(url);
  if (!p) {
    p = fetch(url).then((r) => {
      if (!r.ok) throw new Error(String(r.status));
      return r.json();
    });
    p.catch(() => animationCache.delete(url));
    animationCache.set(url, p);
  }
  return p;
}

const emojiBox: React.CSSProperties = {
  display: "inline-block", width: "1.3em", height: "1.3em", verticalAlign: "-0.28em", lineHeight: 1,
};

function PremiumEmoji({ emoji }: { emoji: TelegramNewsEmoji }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    if (emoji.kind !== "lottie") return;
    const el = ref.current;
    if (!el) return;
    let anim: { play(): void; pause(): void; destroy(): void; goToAndStop(v: number, f?: boolean): void } | null = null;
    let cancelled = false;
    const reduceMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;

    const start = async () => {
      try {
        const [lottie, data] = await Promise.all([loadLottie(), loadAnimation(emoji.url)]);
        if (cancelled || !el) return;
        anim = lottie.loadAnimation({
          container: el, renderer: "svg", loop: true, autoplay: !reduceMotion,
          animationData: structuredClone(data), // the player mutates its input
        });
        if (reduceMotion) anim.goToAndStop(0, true);
        setReady(true);
      } catch { /* keep the thumbnail */ }
    };

    const io = new IntersectionObserver(([entry]) => {
      if (!entry) return;
      if (entry.isIntersecting) {
        if (!anim) void start();
        else if (!reduceMotion) anim.play();
      } else {
        anim?.pause();
      }
    }, { rootMargin: "100px" });
    io.observe(el);
    return () => { cancelled = true; io.disconnect(); anim?.destroy(); };
  }, [emoji.kind, emoji.url]);

  if (emoji.kind === "image") {
    return <img src={emoji.url} alt={emoji.alt} style={emojiBox} loading="lazy" />;
  }
  if (emoji.kind === "video") {
    return <video src={emoji.url} poster={emoji.thumb ?? undefined} style={emojiBox} autoPlay loop muted playsInline aria-label={emoji.alt} />;
  }
  return (
    <span role="img" aria-label={emoji.alt} style={{ ...emojiBox, position: "relative" }}>
      <span ref={ref} style={{ position: "absolute", inset: 0 }} />
      {!ready && (emoji.thumb
        ? <img src={emoji.thumb} alt="" style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }} />
        : <span style={{ fontSize: "0.95em" }}>{emoji.alt}</span>)}
    </span>
  );
}

// ─── Text with premium emoji, links and formatting ────────────────────────────

type Format = NonNullable<TelegramNewsEntity["t"]>;
type Piece = { text: string; emojiId?: string; url?: string; quote?: number; formats: Format[] };

const covers = (e: TelegramNewsEntity, s: number) => e.offset <= s && s < e.offset + e.length;

/** Splits the post text at entity boundaries; hashtags are dropped like before. */
function toPieces(text: string, entities: TelegramNewsEntity[], maxChars?: number): Piece[] {
  const cuts = new Set([0, text.length]);
  for (const e of entities) { cuts.add(e.offset); cuts.add(Math.min(text.length, e.offset + e.length)); }
  const bounds = [...cuts].sort((a, b) => a - b);
  const pieces: Piece[] = [];
  let used = 0;
  for (let i = 0; i < bounds.length - 1 && (maxChars == null || used < maxChars); i++) {
    const s = bounds[i], end = bounds[i + 1];
    const emoji = entities.find((e) => e.id && covers(e, s));
    if (emoji && emoji.offset !== s) continue; // rest of an emoji already rendered
    const link = entities.find((e) => e.url && covers(e, s));
    const quote = entities.find((e) => e.t === "quote" && covers(e, s));
    const formats = entities.filter((e) => e.t && e.t !== "quote" && covers(e, s)).map((e) => e.t as Format);
    const base = { url: link?.url, quote: quote?.offset, formats };
    if (emoji) {
      pieces.push({ ...base, text: text.slice(emoji.offset, emoji.offset + emoji.length), emojiId: emoji.id });
      used += 1;
      continue;
    }
    let chunk = text.slice(s, end);
    if (!link) chunk = chunk.replace(/#[^\s#]+/g, "");
    if (maxChars != null && used + chunk.length > maxChars) chunk = chunk.slice(0, maxChars - used).trimEnd() + "…";
    used += chunk.length;
    pieces.push({ ...base, text: chunk });
  }
  // A quote is a block element already — the line breaks right around it would add empty lines.
  for (let i = 0; i < pieces.length; i++) {
    const p = pieces[i], prev = pieces[i - 1], next = pieces[i + 1];
    if (p.quote != null || p.emojiId) continue;
    let t = p.text;
    if (prev && prev.quote != null) t = t.replace(/^\n/, "");
    if (next && next.quote != null) t = t.replace(/\n$/, "");
    if (t !== p.text) pieces[i] = { ...p, text: t };
  }
  // Trim like the old `.replace(/#\S+/g, "").trim()`.
  const blank = (p?: Piece) => p && !p.emojiId && !p.text.trim();
  while (blank(pieces[0])) pieces.shift();
  while (blank(pieces[pieces.length - 1])) pieces.pop();
  if (pieces[0] && !pieces[0].emojiId) pieces[0] = { ...pieces[0], text: pieces[0].text.trimStart() };
  const last = pieces.length - 1;
  if (pieces[last] && !pieces[last].emojiId) pieces[last] = { ...pieces[last], text: pieces[last].text.trimEnd() };
  return pieces;
}

function Spoiler({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  return (
    <span
      onClick={(e) => { if (!open) { e.preventDefault(); setOpen(true); } }}
      style={open ? undefined : { filter: "blur(5px)", background: "rgba(var(--ink-rgb),0.12)", borderRadius: 4, cursor: "pointer", userSelect: "none" }}
    >{children}</span>
  );
}

function applyFormats(node: ReactNode, formats: Format[]): ReactNode {
  return formats.reduce<ReactNode>((acc, f) => {
    switch (f) {
      case "b": return <strong style={{ fontWeight: 700 }}>{acc}</strong>;
      case "i": return <em>{acc}</em>;
      case "u": return <u>{acc}</u>;
      case "s": return <s>{acc}</s>;
      case "code": return <code style={{ fontFamily: "ui-monospace, monospace", fontSize: "0.92em", background: "rgba(var(--ink-rgb),0.06)", borderRadius: 4, padding: "0 4px" }}>{acc}</code>;
      case "spoiler": return <Spoiler>{acc}</Spoiler>;
      default: return acc;
    }
  }, node);
}

const linkStyle: React.CSSProperties = { color: "var(--accent)", textDecoration: "none", fontWeight: 500 };
const quoteStyle: React.CSSProperties = {
  display: "block", margin: "4px 0", padding: "4px 12px", borderLeft: "3px solid var(--accent)",
  background: "rgba(var(--accent-rgb),0.06)", borderRadius: "0 8px 8px 0",
};

export function NewsText({ text, entities, emoji, maxChars, plainLinks, style }: {
  text: string;
  entities?: TelegramNewsEntity[];
  emoji: Record<string, TelegramNewsEmoji>;
  maxChars?: number;
  /** Inside a card that is itself a link — nested <a> is invalid HTML. */
  plainLinks?: boolean;
  style?: React.CSSProperties;
}) {
  const valid = (entities ?? []).filter((e) => e.url || e.t || (e.id && emoji[e.id]));
  const pieces = toPieces(text || "", valid, maxChars);

  const renderPiece = (p: Piece, k: number) => (
    <Fragment key={k}>{applyFormats(p.emojiId ? <PremiumEmoji emoji={emoji[p.emojiId]} /> : p.text, p.formats)}</Fragment>
  );
  // Adjacent pieces of the same link are merged into one <a>, of the same quote into one block.
  const renderLinks = (list: Piece[]) => {
    const out: ReactNode[] = [];
    for (let i = 0; i < list.length;) {
      const url = list[i].url;
      const group: Piece[] = [];
      while (i < list.length && list[i].url === url) group.push(list[i++]);
      const inner = group.map(renderPiece);
      out.push(url && !plainLinks
        ? <a key={i} href={url} target="_blank" rel="noopener noreferrer" className="news-link" style={linkStyle}>{inner}</a>
        : <Fragment key={i}>{inner}</Fragment>);
    }
    return out;
  };
  const out: ReactNode[] = [];
  for (let i = 0; i < pieces.length;) {
    const quote = pieces[i].quote;
    const group: Piece[] = [];
    while (i < pieces.length && pieces[i].quote === quote) group.push(pieces[i++]);
    out.push(quote != null
      ? <span key={i} style={quoteStyle}>{renderLinks(group)}</span>
      : <Fragment key={i}>{renderLinks(group)}</Fragment>);
  }
  return (
    <p style={style}>
      {out}
    </p>
  );
}

// ─── Photos / videos ──────────────────────────────────────────────────────────
// Like in Telegram: a single photo/video keeps its own proportions (not cropped); a tall one is limited
// to the screen height and centred. Albums are a tight grid of square tiles.

function MediaItem({ m, single }: { m: TelegramNewsMedia; single: boolean }) {
  const w = m.width || 16, h = m.height || 9;
  const box: React.CSSProperties = single
    ? { display: "block", margin: "0 auto", aspectRatio: `${w}/${h}`, width: `min(100%, calc(75vh * ${w} / ${h}))`, height: "auto", objectFit: "cover", background: "#000" }
    : { display: "block", aspectRatio: "1/1", width: "100%", height: "100%", objectFit: "cover", background: "#000" };
  if (m.type === "video" || m.type === "animation") {
    return <NewsVideo src={m.url} poster={m.poster} width={m.width} height={m.height} compact={!single} loopMuted={m.type === "animation"} />;
  }
  return <img src={m.url} alt="" loading="lazy" style={{ ...box, background: "transparent" }} />;
}

export function NewsMedia({ media }: { media: TelegramNewsMedia[] }) {
  if (!media.length) return null;
  if (media.length === 1) {
    const video = media[0].type !== "photo";
    return (
      <div style={{ width: "100%", background: video ? "linear-gradient(180deg, rgba(var(--ink-rgb),0.035), rgba(var(--ink-rgb),0.07))" : "rgba(var(--ink-rgb),0.04)", padding: video ? "clamp(12px, 3vw, 24px)" : 0 }}>
        <MediaItem m={media[0]} single />
      </div>
    );
  }
  return (
    <div style={{ display: "grid", gridTemplateColumns: media.length === 2 || media.length === 4 ? "1fr 1fr" : "repeat(3, 1fr)", gap: 2 }}>
      {media.map((m, i) => <div key={i} style={{ minWidth: 0 }}><MediaItem m={m} single={false} /></div>)}
    </div>
  );
}
